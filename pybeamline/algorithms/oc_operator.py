# Object-Centric Operator
# Controls the flow of the object-centric process mining algorithms
from typing import Callable, Dict
from reactivex import operators as ops, Subject

from pybeamline.boevent import BOEvent


class OCOperator:
    def __init__(self, control_flow: Dict[str, Callable]):
        """
        control_flow: Dict mapping object types (e.g., "Order") to mining operators
               like heuristics_miner_lossy_counting(...)
        """
        self.control_flow = control_flow
        #self.miners = {obj_type: subj.pipe(miner) for obj_type, miner in control_flow.items()}
        self.subjects = {obj_type: Subject() for obj_type in control_flow}

        # Each miner gets its subject's stream piped through its operator
        self.outputs = [
            self.subjects[obj_type].pipe(
                ops.do_action(lambda x: print(f"Miner {obj_type} input: {x}")),
                miner,
                ops.map(lambda model: {"object_type": obj_type, "model": model}), # Maps the output to include the object type
                ops.do_action(lambda x: print(f"Miner {obj_type} output: {x}")),
                )
            for obj_type, miner in control_flow.items()
        ]

    def op(self) -> Callable:
        """
        Apply the operator to the event.
        """
        def _route_and_process(event_stream):
            return event_stream.pipe(
                ops.flat_map(lambda event: event.flatten()),
                ops.do_action(self._route_to_miner),
                ops.ignore_elements(),  # Ignores events with type not in control flow
                ops.merge(*self.outputs)
            )

        return _route_and_process

    def _route_to_miner(self, flat_event: BOEvent):
        # Determine object type of the flattened event
        object_type = flat_event.ocel_omap[0]["ocel:type"]
        if object_type in self.subjects:
            print(f"[ROUTER] Event routed to: {object_type}")
            self.subjects[object_type].on_next(flat_event)


