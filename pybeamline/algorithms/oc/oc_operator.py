from typing import Dict, Optional, Protocol, Callable, Any, Union, List
from reactivex import operators as ops, Observable
from reactivex.subject import Subject
from pybeamline.algorithms.discovery.activity_object_relation_miner_lossy_counting import \
    activity_object_relations_miner_lossy_counting
from pybeamline.algorithms.oc.strategies.base import EmissionStrategy, \
    RelativeFrequencyBasedStrategy, LossyCountingStrategy
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting

class StreamMiner(Protocol):
    """
    Protocol representing a callable that consumes a stream of BOEvents and emits process models.
    """
    def __call__(self, stream: Observable[BOEvent]) -> Observable[Any]:
        ... # pragma: no cover

def oc_operator(
    strategy_handler: Optional[EmissionStrategy] = None,
    control_flow: Optional[Dict[str, Callable[[], StreamMiner]]] = None,
    aer_model_update_frequency: int = 30,
    aer_model_max_approx_error: float = 0.01,
    default_miner: Optional[Callable[[], StreamMiner]] = None,
) -> Callable[[Observable[BOEvent]], Observable[dict]]:
    """
    Factory function to create a configured OCOperator.

    :param strategy_handler:
        Optional object-type concept drift strategy handler,
        Determines when object types should be considered active or inactive.
    :param control_flow:
        Optional dictionary mapping object types (str) to miner factory callables (i.e., `Callable[[], StreamMiner]`).
        If omitted, the operator dynamically registers miners for object types on-the-fly.
    :param aer_model_update_frequency:
        Number of events between emissions of updated Activity-Entity Relationship (AER) diagrams.
    :param aer_model_max_approx_error:
        Maximum approximation error used for lossy counting in the AER miner.
        Smaller values reduce approximation error but increase memory usage.
    :param default_miner:
        Optional default miner to use for dynamic mode when no control flow is provided.
        All object types will use the miner specifications provided in the `default_miner` callable.
    :return:
        A callable that takes an `Observable[BOEvent]` as input and returns an `Observable[dict]` containing
        emitted models, AER diagrams, and control commands such as active/inactive.
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    if strategy_handler is None:
        strategy_handler = RelativeFrequencyBasedStrategy(0.05)

    return OCOperator(
        strategy_handler=strategy_handler,
        control_flow=control_flow or {},
        aer_model_update_frequency=aer_model_update_frequency,
        aer_model_max_approx_error=aer_model_max_approx_error,
        default_miner=default_miner
    ).operator


class OCOperator:
    """
    Object-Centric Reactive Operator for managing obj-type stream miners for processing of BOEvents.
    """
    def __init__(self, control_flow: Optional[Dict[str, Callable[[], StreamMiner]]] = None,
                 strategy_handler: EmissionStrategy = None ,
                 aer_model_update_frequency: int = 30,
                 aer_model_max_approx_error: float = 0.01,
                 default_miner: Optional[Callable[[], StreamMiner]] = None):
        self.__strategy_handler = strategy_handler or RelativeFrequencyBasedStrategy()
        self.__control_flow = control_flow
        self.__dynamic_mode = not bool(control_flow)
        self.__default_miner = default_miner or (lambda: heuristics_miner_lossy_counting(20))
        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__output_subject: Subject = Subject()

        for obj_type, miner in control_flow.items():
            self._register_stream(obj_type, miner())
        self._register_aer_stream(aer_model_update_frequency, aer_model_max_approx_error)

    def _register_aer_stream(self, model_update_frequency, max_approx_error: float):
        """
        Register a stream for Activity-Entity Relationship (AER) diagrams using lossy counting.
        """
        subject = Subject[BOEvent]()
        self.__miner_subjects["AERStream"] = subject
        miner_op = activity_object_relations_miner_lossy_counting(
            model_update_frequency=model_update_frequency,
            control_flow=self.__control_flow,
            max_approx_error=max_approx_error
        )
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
        Register a new miner subject for the given object type.
        If a miner is provided, it will be used; otherwise, a default miner is created.
        """
        subject = Subject[BOEvent]()
        self.__miner_subjects[obj_type] = subject
        miner_op = (miner or self.__default_miner)()

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
        for flat_event in self._flatten(event):
            obj_type = flat_event.get_omap_types()[0]

            if obj_type not in self.__miner_subjects and self.__dynamic_mode:
                # Dynamically create a new miner subject
                self._register_stream(obj_type)
            if obj_type not in self.__miner_subjects:
                continue

            self.__miner_subjects[obj_type].on_next(flat_event)

    def _flatten(self, event: BOEvent) -> List[BOEvent]:
        flattened_events = []
        for obj_type, obj_ids in event.omap.items():
            for obj_id in obj_ids:
                new_omap = {obj_type: {obj_id}}  # Single-entry omap
                flattened_events.append(
                    BOEvent(
                        event_id=event.event_id,
                        activity_name=event.activity_name,
                        timestamp=event.timestamp,
                        omap=new_omap,
                        vmap=event.vmap
                    )
                )
        return flattened_events

    def get_mode(self) -> bool:
        """
        Returns True if the operator is in dynamic mode (no control flow), False otherwise.
        """
        return self.__dynamic_mode

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[dict]]:
        def pipeline(stream: Observable[BOEvent]) -> Observable[dict]:
            return stream.pipe(
                ops.do_action(self._route_to_miner),
                ops.filter(lambda e: not isinstance(e, BOEvent)),
                ops.merge(self.__output_subject),
                ops.flat_map(self.__strategy_handler.evaluate),
            )
        return pipeline
