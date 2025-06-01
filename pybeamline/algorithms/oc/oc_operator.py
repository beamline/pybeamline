from typing import Dict, Optional, Protocol, Callable, Any, Union, Set
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable, merge, empty, just
from reactivex.abc import DisposableBase
from reactivex.subject import Subject

from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import object_relations_miner_lossy_counting
from pybeamline.algorithms.oc.object_lossy_counting_operator import object_lossy_counting_operator, Command
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting

class StreamMiner(Protocol):
    """
    Protocol representing a callable that consumes a stream of BOEvents and emits HeuristicsNet models.
    """
    def __call__(self, stream: Observable[BOEvent]) -> Observable[Any]:
        ...


def oc_operator(
    control_flow: Optional[Dict[str, Callable[[], StreamMiner]]] = None,
    object_max_approx_error: float = 0.0001,
    relation_model_update_frequency: int = 30,
    relation_max_approx_error: float = 0.01
) -> Callable[[Observable[BOEvent]], Observable[dict]]:
    """
        Factory function for creating an object-centric process mining operator.
        Validates control flow dictionary if supplied and instantiates the OCOperator.
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    return OCOperator(
        control_flow=control_flow or {},
        object_max_approx_error=object_max_approx_error,
        relation_max_approx_error=relation_max_approx_error
    ).operator


class OCOperator:
    """
    Object-Centric Operator for reactive stream processing of BOEvents.
    Manages per-object-type miner streams by the use of object-lossy-counting on dynamically or statically chosen object types.
    """
    def __init__(self, control_flow: Optional[Dict[str, Callable[[], StreamMiner]]], object_max_approx_error: float = 0.0001,relation_model_update_frequency: int = 30, relation_max_approx_error: float = 0.01):
        self.__relation_model_update_frequency = relation_model_update_frequency
        self.__relation_max_approx_error = relation_max_approx_error
        self.__object_max_approx_error = object_max_approx_error
        self.__control_flow = control_flow
        self.__dynamic_mode = not bool(control_flow)

        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__output_subject: Subject = Subject()

        for obj_type, miner in control_flow.items():
            self._register_stream(obj_type, miner())
        self._register_aer_stream()

    def _register_aer_stream(self):
        subject = Subject[BOEvent]()
        self.__miner_subjects["AERStream"] = subject
        miner_op = object_relations_miner_lossy_counting(model_update_frequency=self.__relation_model_update_frequency,
                                                        control_flow=self.__control_flow,
                                                        max_approx_error=self.__relation_max_approx_error)
        aer_stream = subject.pipe(
            miner_op,
            ops.map(lambda model: {
                "type": "aer_diagram",
                "model": model
            }),
        )
        aer_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[AER-STREAM] error:", e),
            on_completed=lambda: None
        )


    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        """
        Register a new miner stream for the given object type.
        """
        subject = Subject[BOEvent]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(20)

        dfg_stream = subject.pipe(
            miner_op,
            ops.map(lambda model: {
                "type": "model",
                "object_type": obj_type,
                "model": model
            }),
        )

        dfg_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None
        )

    def _route_to_miner(self, event: BOEvent):
        """
        Flatten the incoming BOEvent and route it to its corresponding miner subject.
        If dynamic mode is enabled, miners are created on-the-fly if not present.
        """
        self.__miner_subjects["AERStream"].on_next(event)
        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]

            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                if obj_type in self.__control_flow:
                    # Reregistration of selected miner in control flow
                    self._register_stream(obj_type, miner=self.__control_flow[obj_type]())
                else:
                    # Dynamically create a new miner subject
                    self._register_stream(obj_type)

            if obj_type not in self.__miner_subjects:
                continue
            self.__miner_subjects[obj_type].on_next(flat_event)

    def _handle_deregistration_event(self, event: dict):
        """
        Handle deregistration events and clean up associated streams.
        """
        if isinstance(event, dict) and event.get("command") == Command.DEREGISTER:
            obj_type = event.get("object_type")
            subject = self.__miner_subjects[obj_type]
            self.__miner_subjects.pop(obj_type, None)


    def _build_operator_pipeline(self, stream: Observable[BOEvent]) -> Observable[dict]:
        """
        Main reactive pipeline: routes events, applies lossy counting, and merges miner outputs.
        """
        def process(event: Union[BOEvent, dict]) -> Observable[dict]:
            if isinstance(event, BOEvent):
                self._route_to_miner(event)
                return empty()
            else:
                self._handle_deregistration_event(event)
                return just(event)

        return stream.pipe(
            object_lossy_counting_operator(self.__object_max_approx_error, self.__control_flow),
            ops.flat_map(process),
            ops.merge(self.__output_subject),
        )

    def get_mode(self) -> bool:
        """
        Returns True if the operator is in dynamic mode (no control flow), False otherwise.
        """
        return self.__dynamic_mode

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[dict]]:
        return self._build_operator_pipeline
