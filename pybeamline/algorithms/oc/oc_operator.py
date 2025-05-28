import math
from typing import Dict, Optional, Protocol, Callable, Any, Union, Set
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable, merge, empty, just
from reactivex.abc import DisposableBase
from reactivex.subject import Subject

from pybeamline.algorithms.oc.object_lossy_counting_operator import object_lossy_counting_operator
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting

# Protocol defining how a miner should behave: transforms BOEvent stream → messages (dicts)
class StreamMiner(Protocol):
    def __call__(self, stream: Observable[BOEvent]) -> Observable[HeuristicsNet]:
        ...


def oc_operator(
    control_flow: Optional[Dict[str, StreamMiner]] = None,
    object_max_approx_error: float = 0.0001
) -> Callable[[Observable[BOEvent]], Observable[dict]]:
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be callables, got {type(miner).__name__} for '{obj_type}'")

    return OCOperator(
        control_flow=control_flow or {},
        object_max_approx_error=object_max_approx_error
    ).operator


class OCOperator:
    def __init__(self, control_flow: Dict[str, StreamMiner], object_max_approx_error: float = 0.0001):
        self.__object_max_approx_error = object_max_approx_error
        self.__control_flow = control_flow
        self.__dynamic_mode = not bool(control_flow)

        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__subscriptions: Dict[str, DisposableBase] = {}
        self.__output_subject: Subject = Subject()
        self.__alive_streams: Set[str] = set()

    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        subject = Subject[Union[BOEvent, dict]]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(20)
        self.__alive_streams.add(obj_type)

        dfg_stream = subject.pipe(
            miner_op,
            ops.map(lambda model: {
                "type": "model",
                "object_type": obj_type,
                "model": model
            }),
        )

        disp = dfg_stream.subscribe(
            on_next=lambda msg: self.__output_subject.on_next(msg),
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None
        )
        self.__subscriptions[obj_type] = disp

    def _route_to_miner(self, event: BOEvent):
        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]

            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                self._register_stream(obj_type, self.__control_flow.get(obj_type))

            self.__miner_subjects[obj_type].on_next(flat_event)

    def _check_cleanup(self, event: Union[BOEvent, dict]):
        if isinstance(event, dict) and event.get("command") == "deregister":
            obj_type = event.get("object_type")
            if obj_type in self.__alive_streams:
                print(f"[CLEANUP] Sending terminate signal for {obj_type}")
                subject = self.__miner_subjects[obj_type]
                self.__miner_subjects.pop(obj_type, None)
                self.__subscriptions.pop(obj_type, None)
                self.__alive_streams.pop(obj_type)

    def _miner_stream(self, stream: Observable[BOEvent]) -> Observable[dict]:
        def process(event: Union[BOEvent, dict]) -> Observable[dict]:
            if isinstance(event, BOEvent):
                self._route_to_miner(event)
                return empty()
            else:
                self._check_cleanup(event)
                return just(event)

        return stream.pipe(
            object_lossy_counting_operator(self.__object_max_approx_error, self.__control_flow),
            ops.flat_map(process),
            ops.merge(self.__output_subject),
        )

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[dict]]:
        return self._miner_stream
