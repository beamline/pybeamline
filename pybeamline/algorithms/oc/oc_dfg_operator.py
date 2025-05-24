import math
from typing import Dict, Optional, Protocol, Callable, Any
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable
from reactivex.abc import DisposableBase
from reactivex.subject import Subject
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.boevent import BOEvent


# Protocol defining how a miner should behave: transforms BOEvent stream → HeuristicsNet stream
class StreamMiner(Protocol):
    """
    A StreamMiner consumes a stream of BOEvent and emits HeuristicsNet models.
    """
    def __call__(self, stream: Observable[BOEvent]) -> Observable[HeuristicsNet]:
        ...  # pragma: no cover

def oc_dfg_operator(
        control_flow: Optional[Dict[str, StreamMiner]] = None,
        object_max_approx_error: float = 0.0001
    ) -> Callable[[Observable[BOEvent]], Observable[Dict[str, HeuristicsNet]]]:
    """
    Factory: returns a Rx operator that builds per-object-type DFGs (HeuristicsNet)
    and merges them into an object-centric DFG stream.

    :param control_flow:
       Mapping from object_type to custom StreamMiner. If None, any new type is
       discovered dynamically using the default heuristics_miner_lossy_counting.
    :param object_max_approx_error:
        Approximation error threshold for lossy counting of low-activity object-type streams.
    :return:
        A function that can be applied via `.pipe(...)` on an Observable[BOEvent].
    """
    if control_flow is not None and not isinstance(control_flow, dict):
        raise TypeError("control_flow must be a dict mapping object types to StreamMiner callables.")
    for obj_type, miner in (control_flow or {}).items():
        if not callable(miner):
            raise ValueError(f"control_flow values must be StreamMiner callables, got {type(miner).__name__} for '{obj_type}'")

    # Returns a callable operator for an Observable[BOEvent]
    operator = OCDFGOperator(
        control_flow=control_flow or {},
        object_max_approx_error=object_max_approx_error).operator
    return operator

class OCDFGOperator:
    """
    Internal class that manages per-object-type Subjects and subscriptions,
    applying a StreamMiner to each object stream, and cleaning up low-activity object-type
    streams  using a lossy counting heuristic.
    """
    def __init__(self, control_flow: Dict[str, StreamMiner], object_max_approx_error: float = 0.0001):
        # Number of event per bucket = 1/max_approx_error
        self.__bucket_width = int(math.ceil(1 / object_max_approx_error))
        self.__observed_events = 0
        # Static or dynamic control flow
        self.__control_flow: Dict[str, StreamMiner] = control_flow
        self.__dynamic_mode: bool = not bool(control_flow)
        # Subject for each object type miner
        self.__miner_subjects: Dict[str, Subject[BOEvent]] = {}
        # Track seen freq and last bucket for each object type
        self.__obj_tracking = {} # obj_type → (frequency, last_bucket)
        # Subscriptions to each miner subject
        self.__subscriptions: Dict[str, DisposableBase] = {}
        # Central output subject for all model updates
        self.__output_subject: Subject = Subject()

        # Pre-register static streams if not in dynamic mode
        if not self.__dynamic_mode:
            for obj_type, miner in self.__control_flow.items():
                self._register_stream(obj_type, miner)


    def _register_stream(self, obj_type: str, miner: Optional[StreamMiner] = None) -> None:
        """
        Create a Subject for obj_type, apply Stream miner, and subscribe into the shared output.
        If miner is None, use the default heuristics_miner_lossy_counting with a bucket width of 50.
        """
        subject = Subject[BOEvent]()
        self.__miner_subjects[obj_type] = subject
        miner_op = miner or heuristics_miner_lossy_counting(50)

        # Build per-type model stream
        dfg_stream = subject.pipe(
            miner_op,
            ops.filter(lambda m: bool(getattr(m, "dfg", {}))),
            ops.map(lambda m: {"object_type": obj_type, "dfg": m}),
        )

        # When the subscription is deregistered (cleanup), emit a registration notice
        teardown = dfg_stream.pipe(
            ops.finally_action(
                lambda:self.__output_subject.on_next({"deregister": obj_type}))
        )

        # Subscribe side-channel into the main output
        disp = teardown.subscribe(
            on_next=self.__output_subject.on_next,
            on_error=lambda e: print(f"[{obj_type}] error:", e),
            on_completed=lambda: None # keep the outer stream alive
        )
        self.__subscriptions[obj_type] = disp

    def _route_to_miner(self, event: BOEvent):
        """
        Called for each BOEvent: flatten, update counters, register new streams (if self.__dynamic_mode),
        and push events into the correct Object Type Subject Streams.
        """
        self.__observed_events += 1
        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width))

        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0] # Only one type per event when flattened
            # Update lossy counter: (count, last_bucket)
            freq, _ = self.__obj_tracking.get(obj_type, (0, current_bucket))
            self.__obj_tracking[obj_type] = (freq + 1, current_bucket)

            # If the obj type is not registered, and we are in dynamic mode or it is in control flow,
            if obj_type not in self.__miner_subjects and (self.__dynamic_mode or obj_type in self.__control_flow):
                self._register_stream(obj_type, self.__control_flow.get(obj_type))

            # Push the flat event into corresponding obj miner subject
            self.__miner_subjects[obj_type].on_next(flat_event)

        # Cleanup low-activity object streams at bucket boundary
        if self.__observed_events % self.__bucket_width == 0:
            self._cleanup(current_bucket)

    def _miner_stream(self, stream: Observable[BOEvent]) -> Observable[Dict[str, Any]]:
        """
        Main operator: route BOEvents into sub-streams, swallow them, then merge
        all side-channel outputs (models and deregistration notices).
        """
        routing = stream.pipe(
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        )
        return routing.pipe(ops.merge(self.__output_subject))

    def _cleanup(self, current_bucket: int):
        """
        Tear down any object streams whose (count + last_bucket) <= current_bucket.
        """
        stale = [
            obj for obj, (freq, bucket) in self.__obj_tracking.items()
            if freq + bucket <= current_bucket
        ]
        for obj in stale:
            # Complete and remove
            self.__miner_subjects.pop(obj).on_completed()
            self.__subscriptions.pop(obj)  # Let it finish silently
            self.__obj_tracking.pop(obj, None)
            # Deregistration notice is emitted via finally_action

    @property
    def operator(self) -> Callable[[Observable[BOEvent]], Observable[Dict[str, Any]]]:
        """
        Expose the operator function that can be used in Rx pipelines.
        """
        return self._miner_stream
