import copy
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting import heuristics_miner_lossy_counting
from pybeamline.boevent import BOEvent
from reactivex import operators as ops
from reactivex.subject import Subject
from typing import Callable, Dict
from pybeamline.utils.object_relation_tracker import ObjectRelationTracker


def oc_operator(control_flow: Dict[str, Callable] = None, uml_version: bool = False) -> Callable:
    """
    Creates an object-centric operator for processing streams of BOEvents.
    Reactive operator that routes incoming events to the appropriate miner
    based on the object type. If a miner is not registered for an object type,
    it will be auto-registered.

    :param uml_version:
    :param control_flow: (Optional[Dict[str, Callable]]):
    A mapping from object type names to their mining operators.
    If None, dynamic discovery will be enabled and default miners used.

    :return:
    Callable: A streaming operator that takes a stream of events and outputs discovered object specific DFG updates.
    """
    # Validate control_flow
    if control_flow is not None and not isinstance(control_flow, dict):
        raise ValueError("control_flow must be a dictionary mapping object types to miner functions.")
    for key, value in (control_flow or {}).items():
        if not isinstance(value, Callable):
            raise ValueError(
                f"control_flow values must be callables (stream operators), got {type(value).__name__} for object type '{key}'")

    return OCOperator(control_flow=control_flow, uml_version=uml_version).op()


DEFAULT = object()


class OCOperator:
    def __init__(self, control_flow: Dict[str, Callable] = None, uml_version: bool = False):
        self.control_flow = {} if control_flow is None else control_flow
        self.dynamic_mode = (control_flow is None)
        self.output_subject = Subject()
        self.subjects = {}
        self.uml_version = uml_version
        self.relation_tracker = ObjectRelationTracker() if uml_version else None

        # If static config was supplied
        if not self.dynamic_mode:
            for obj_type, miner in self.control_flow.items():
                self._register_stream(obj_type, miner)

    def _register_stream(self, obj_type, miner_func=None):
        """Set up subject and output stream for a new object type."""
        if obj_type in self.subjects:
            return

        print(f"[OCOperator] Registering stream for object type: {obj_type}")
        subject = Subject()

        # Use provided miner_func or default to a new lossy counting instance
        miner = miner_func or heuristics_miner_lossy_counting(50)

        self.subjects[obj_type] = subject
        if self.uml_version:
            subject.pipe(self._uml_stream(obj_type, miner)).subscribe(self.output_subject)
        else:
            subject.pipe(self._basic_stream(obj_type, miner)).subscribe(self.output_subject)

    def _route_to_miner(self, flat_event: BOEvent):
        object_type = flat_event.get_omap_types()[0]  # Assuming single object type per event
        if object_type not in self.subjects:
            if self.dynamic_mode:
                self._register_stream(object_type)  # Auto-register
            else:
                return  # Ignore unknown types in static mode

        self.subjects[object_type].on_next(flat_event)

    def _update_relation_tracker(self, event: BOEvent):
        if self.uml_version:
            self.relation_tracker.ingest_event(event)

    def op(self) -> Callable:
        """
        Determines which pipeline to use based on the uml_version flag.
        """
        def _route_and_process(event_stream):
            if self.uml_version:
                return self._pipeline_with_uml(event_stream)
            else:
                return self._pipeline_basic(event_stream)
        return _route_and_process

    def _basic_stream(self, obj_type: str, miner: Callable):
        """
        Defines the basic stream without relation tracker.
        """
        return lambda stream: stream.pipe(
            miner,
            ops.map(lambda model, t=obj_type:
                    {"object_type": t, "model": model})
        )

    def _uml_stream(self, obj_type: str, miner: Callable):
        """
        Defines the stream with UML tracking (relation tracker).
        """
        return lambda stream: stream.pipe(
            miner,
            ops.map(lambda model, t=obj_type:
                    {"object_type": t,
                     "model": model,
                     "relation": copy.deepcopy(self.relation_tracker)})
        )

    def _pipeline_with_uml(self, event_stream):
        """
        Event stream pipeline with UML tracking.
        """
        return event_stream.pipe(
            ops.do_action(lambda event: self._update_relation_tracker(event)),
            ops.flat_map(lambda event: event.flatten()),
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        ).pipe(
            ops.merge(self.output_subject)
        )

    def _pipeline_basic(self, event_stream):
        """
        Event stream pipeline without UML tracking.
        """
        return event_stream.pipe(
            ops.flat_map(lambda event: event.flatten()),
            ops.do_action(self._route_to_miner),
            ops.ignore_elements()
        ).pipe(
            ops.merge(self.output_subject)
        )
