from typing import Dict, Optional, Protocol, Callable, Any
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable
from reactivex.subject import Subject
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import \
    object_relations_miner_lossy_counting


class StreamMiner(Protocol):
    def __call__(self, stream: Observable[BOEvent]) -> Observable[HeuristicsNet]:
        ...  # pragma: no cover

def oc_operator(control_flow: Optional[Dict[str, StreamMiner]] = None, track_relations: bool = False) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    return OCOperator(control_flow=control_flow or {}, track_relations=track_relations).op()

class OCOperator:
    def __init__(self, control_flow: Dict[str, StreamMiner], track_relations: bool = False):
        self.__control_flow: Dict[str, StreamMiner] = control_flow
        self.__dynamic_mode: bool = not bool(control_flow)
        self.__track_relations: bool = track_relations
        self.__subjects: Dict[str, Subject[BOEvent]] = {}
        self.__output_subject: Subject = Subject()

        if self.__track_relations:
            self.__relational_subject = Subject()
            self.__relation_operator = object_relations_miner_lossy_counting()

            self.__relational_subject.pipe(
                self.__relation_operator,
                ops.map(lambda rel_model: {
                    "relations": rel_model
                })
            ).subscribe(
                self.__output_subject,
                on_error=lambda e: print(f"Error in relation miner: {e}")
            )

        if not self.__dynamic_mode:
            for obj_type, miner in self.__control_flow.items():
                self._register_stream(obj_type, miner)

    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        subject: Subject[BOEvent] = Subject()
        miner_op: StreamMiner = miner or heuristics_miner_lossy_counting(50)
        self.__subjects[obj_type] = subject

        stream_op = self._relation_stream(obj_type, miner_op) if self.__track_relations else self._basic_stream(obj_type, miner_op)

        subject.pipe(
            stream_op,
        ).subscribe(
            self.__output_subject,
            on_error=lambda e: print(f"Error in miner for '{obj_type}': {e}")
        )
    def _route_to_miner(self, event: BOEvent):
        for obj_type in event.get_omap_types():
            if obj_type not in self.__subjects:
                if self.__dynamic_mode:
                    self._register_stream(obj_type)
                else:
                    continue
            self.__subjects[obj_type].on_next(event)

    def op(self) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
        def operator(stream: Observable[BOEvent]) -> Observable[Dict[str, Any]]:
            return self._miner_stream(stream)
        return operator

    def _miner_stream(self, stream: Observable[BOEvent]) -> Observable[Dict[str, Any]]:
        routing = stream.pipe(
            ops.do_action(lambda e: self.__relational_subject.on_next(e) if self.__track_relations else None),
            ops.flat_map(lambda e: e.flatten()),
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        )
        return routing.pipe(ops.merge(self.__output_subject))

    def _basic_stream(self, obj_type: str, miner: StreamMiner) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
        return lambda src: src.pipe(
            miner,
            ops.filter(lambda model: bool(getattr(model, "dfg", {}))),
            ops.map(lambda model: {"object_type": obj_type, "model": model})
        )

    def _relation_stream(self, obj_type: str, miner: StreamMiner) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
        return lambda src: src.pipe(
            miner,
            ops.filter(lambda model: bool(getattr(model, "dfg", {}))),
            ops.map(lambda model: {
                "object_type": obj_type,
                "model": model,
            })
        )

    def get_mode(self) -> bool:
        return self.__dynamic_mode

    def get_control_flow(self) -> Dict[str, StreamMiner]:
        return self.__control_flow
