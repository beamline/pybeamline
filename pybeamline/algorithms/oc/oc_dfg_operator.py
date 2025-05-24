import math
from typing import Dict, Optional, Protocol, Callable, Any

from httpcore import stream
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable, from_iterable
from reactivex.disposable import Disposable
from reactivex.scheduler import ImmediateScheduler
from reactivex.subject import Subject
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import \
    object_relations_miner_lossy_counting

# Protocol defining how a miner should behave: transforms BOEvent stream → HeuristicsNet stream
class StreamMiner(Protocol):
    def __call__(self, stream: Observable[BOEvent]) -> Observable[HeuristicsNet]:
        ...  # pragma: no cover

# Factory function that return stream operator
def oc_dfg_operator(control_flow: Optional[Dict[str, StreamMiner]] = None, object_max_approx_error: float = 0.0001) -> Callable[[Observable[BOEvent]], Observable[Dict[str, HeuristicsNet]]]:
    """
    Creates an object-centric operator for processing streams of BOEvents.
    Reactive operator that routes incoming events to the appropriate miner
    based on the object type. If a miner is not registered for an object type,
    it will be auto-registered.

    :param object_max_approx_error:
    :param track_relations: (bool): Whether to track object relations withing activities.
    :param control_flow: (Optional[Dict[str, Callable]]):
    A mapping from object type names to their mining operators.
    If None, dynamic discovery will be enabled and default miners used.

    :return:
    Callable: A streaming operator that takes a stream of events and outputs discovered object specific DFG updates or object relations.
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    # Returns a callable operator for an Observable[BOEvent]
    return OCDFGOperator(control_flow=control_flow or {}, object_max_approx_error=object_max_approx_error).op()

class OCDFGOperator:
    def __init__(self, control_flow: Dict[str, StreamMiner], object_max_approx_error: float = 0.0001):
        self.__bucket_width = int(math.ceil(1 / object_max_approx_error))
        self.__observed_events = 1
        self.__control_flow: Dict[str, StreamMiner] = control_flow
        self.__dynamic_mode: bool = not bool(control_flow)
        self.__miner_subjects: Dict[str, Subject[BOEvent]] = {}
        self.__output_subject: Subject = Subject()
        self.__obj_tracking = {} # obj_type → (frequency, last_bucket)
        self.__subscriptions: Dict[str, Disposable] = {}

        # Pre-register static streams if not in dynamic mode
        if not self.__dynamic_mode:
            for obj_type, miner in self.__control_flow.items():
                self._register_stream(obj_type, miner)


    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None):
        subject = Subject[BOEvent]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(20)
        stream_op = self._basic_stream(obj_type, miner_op)

        disp = subject.pipe(
            stream_op
        ).subscribe(
            self.__output_subject.on_next,
            lambda e: print("err", e),
            lambda: None)

        self.__subscriptions[obj_type] = disp

    def _route_to_miner(self, event: BOEvent):
        # flatten the event to get the object type
        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width))

        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]

            freq, _ = self.__obj_tracking.get(obj_type, (0, current_bucket))
            self.__obj_tracking[obj_type] = (freq + 1, current_bucket)

            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                self._register_stream(obj_type, self.__control_flow.get(obj_type))

            self.__miner_subjects[obj_type].on_next(flat_event)

        self.__observed_events += 1
        current_bucket = math.ceil(self.__observed_events / self.__bucket_width)
        self._object_cleanup(current_bucket)




    def op(self) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
        # Returns the actual operator function that transforms a BOEvent stream
        def operator(stream: Observable[BOEvent]) -> Observable[Dict[str, Any]]:
            return self._miner_stream(stream)
        return operator

    def _miner_stream(self, stream: Observable[BOEvent]) -> Observable[Dict[str, Any]]:
        routing = stream.pipe(
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        )
        return routing.pipe(ops.merge(self.__output_subject))

    def _object_cleanup(self, current_bucket: int):
        stale = [
            t for t, (freq, bucket) in self.__obj_tracking.items()
            if freq + bucket <= current_bucket
        ]
        #print(f"Current Bucket: {current_bucket}")
        #print(f"Object Tracking: {self.__obj_tracking}")
        for t in stale:
            print(f"\n[CLEANUP @ bucket {current_bucket}] Removing object type: {t}")
            self._deregister_stream(t)

    def _deregister_stream(self, obj_type: str):
        # 1) complete the subject so it flushes all pending emissions
        self.__miner_subjects.pop(obj_type).on_completed()

        # 2) drop it from your dict so no new events get routed
        self.__subscriptions.pop(obj_type, None)
        self.__obj_tracking.pop(obj_type, None)


    # Basic mining pipeline per object type
    def _basic_stream(self, obj_type: str, miner: StreamMiner) -> Callable:
        return lambda src: src.pipe(
            miner,
            ops.filter(lambda model: bool(getattr(model, "dfg", {}))),
            ops.map(lambda model: {"object_type": obj_type, "model": model})
        )