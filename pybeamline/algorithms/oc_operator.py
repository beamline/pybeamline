from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import oc_heuristics_miner_lossy_counting
from pybeamline.boevent import BOEvent
from reactivex import operators as ops
from reactivex.subject import Subject
from typing import Callable, Dict

DEFAULT = object()

class OCOperator:
    def __init__(self, control_flow: Dict[str, Callable] = DEFAULT):
        self.control_flow = {} if control_flow is DEFAULT else control_flow
        self.dynamic_mode = (control_flow is DEFAULT)
        self.output_subject = Subject()
        self.subjects = {}

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
        miner = miner_func or oc_heuristics_miner_lossy_counting(model_update_frequency=50)

        self.subjects[obj_type] = subject
        subject.pipe(
            miner,
            ops.map(lambda model, t=obj_type: {"object_type": t, "model": model})
        ).subscribe(self.output_subject)

    def _route_to_miner(self, flat_event: BOEvent):
        object_type = flat_event.ocel_omap[0]["ocel:type"]

        if object_type not in self.subjects:
            if self.dynamic_mode:
                self._register_stream(object_type)  # Auto-register
            else:
                return  # Ignore unknown types in static mode

        self.subjects[object_type].on_next(flat_event)
        #print("[OCOperator] Routed event to miner:" + str(flat_event) + "miner: " + str(object_type))

    def op(self) -> Callable:
        def _route_and_process(event_stream):
            return event_stream.pipe(
                ops.flat_map(lambda event: event.flatten()),
                ops.do_action(self._route_to_miner),
                ops.ignore_elements(),
            ).pipe(
                ops.merge(self.output_subject)
            )

        return _route_and_process
