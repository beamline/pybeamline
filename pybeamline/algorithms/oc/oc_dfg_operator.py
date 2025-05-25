import math
from typing import Dict, Optional, Protocol, Callable, Any, Union
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable
from reactivex.abc import DisposableBase
from reactivex.subject import Subject
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting

# Protocol defining how a miner should behave: transforms BOEvent stream → messages (dicts)
class StreamMiner(Protocol):
    def __call__(self, stream: Observable[Union[BOEvent, dict]]) -> Observable[dict]:
        ...

def oc_dfg_operator(
    control_flow: Optional[Dict[str, StreamMiner]] = None,
    object_max_approx_error: float = 0.0001
) -> Callable[[Observable[BOEvent]], Observable[dict]]:
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be callables, got {type(miner).__name__} for '{obj_type}'")

    return OCDFGOperator(
        control_flow=control_flow or {},
        object_max_approx_error=object_max_approx_error
    ).operator


class OCDFGOperator:
    def __init__(self, control_flow: Dict[str, StreamMiner], object_max_approx_error: float = 0.0001):
        self.__bucket_width = int(math.ceil(1 / object_max_approx_error))
        self.__observed_events = 0
        self.__control_flow = control_flow
        self.__dynamic_mode = not bool(control_flow)

        self.__miner_subjects: Dict[str, Subject[Union[BOEvent, dict]]] = {}
        self.__subscriptions: Dict[str, DisposableBase] = {}
        self.__obj_tracking: Dict[str, tuple[int, int]] = {}
        self.__output_subject: Subject = Subject()

        self.__alive_streams: Dict[str, bool] = {}

    def _current_bucket(self) -> int:
        return int(math.ceil(self.__observed_events / self.__bucket_width))

    def _check_cleanup(self, obj_type: str, current_bucket: int) -> bool:
        freq, last_bucket = self.__obj_tracking.get(obj_type, (0, current_bucket))
        return freq + last_bucket <= current_bucket

    def _handle_output_message(self, msg: dict):
        if msg.get("type") == "deregister":
            obj_type = msg.get("object_type")
            print(f"[FINAL DEREGISTER] {obj_type}")
            self.__alive_streams[obj_type] = False
            self.__miner_subjects.pop(obj_type, None)
            self.__subscriptions.pop(obj_type, None)
        self.__output_subject.on_next(msg)

    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        subject = Subject[Union[BOEvent, dict]]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(20)
        self.__alive_streams[obj_type] = True

        dfg_stream = subject.pipe(
            miner_op
        )

        disp = dfg_stream.subscribe(
            on_next=self._handle_output_message,
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None
        )
        self.__subscriptions[obj_type] = disp

    def _route_to_miner(self, event: BOEvent):
        self.__observed_events += 1
        current_bucket = self._current_bucket()

        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]
            freq, _ = self.__obj_tracking.get(obj_type, (0, current_bucket))
            self.__obj_tracking[obj_type] = (freq + 1, current_bucket)

            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                self._register_stream(obj_type, self.__control_flow.get(obj_type))

            self.__miner_subjects[obj_type].on_next(flat_event)

        if self.__observed_events % self.__bucket_width == 0:
            self._cleanup(current_bucket)

    def _cleanup(self, current_bucket: int):
        for obj_type in list(self.__miner_subjects.keys()):
            if self._check_cleanup(obj_type, current_bucket):
                if self.__alive_streams.get(obj_type, True):
                    print(f"[CLEANUP] Sending terminate signal for {obj_type}")
                    subject = self.__miner_subjects[obj_type]
                    self.__alive_streams[obj_type] = False
                    subject.on_next({"signal": "terminate",
                                     "object_type": obj_type,})

    def _miner_stream(self, stream: Observable[BOEvent]) -> Observable[dict]:
        routing = stream.pipe(
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        )
        return routing.pipe(ops.merge(self.__output_subject))

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[dict]]:
        return self._miner_stream
