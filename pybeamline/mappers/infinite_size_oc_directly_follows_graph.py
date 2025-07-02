# Class that will be handling the infinite size oc directly follows graph
from reactivex import operators as ops
import ast
from datetime import datetime
from pybeamline.abstractevent import AbstractEvent
from pybeamline.boevent import BOEvent
from pybeamline.models.dfg.ocdfg_manager import OCDFGManager


def infinite_size_oc_directly_follows_graph():
    manager = OCDFGManager()

    def operator(source):
        return source.pipe(
            ops.map(lambda event: manager.update(event))
        )

    return operator


def create_boevent_from_string(event: str):
    """
       Convert a string in the format:
       "(e0, Register Customer, 2025-04-08 14:50:12.533836, [{'ocel:oid': 'c1_0', 'ocel:type': 'Customer'}])"
       into a BOEvent object.
    """
    # Remove outer parentheses and split
    inner_str = event.strip()[1:-1]
    parts = inner_str.split(", ", 3)

    if len(parts) != 4:
        raise ValueError("Invalid event format")

    event_id = parts[0]
    activity = parts[1]
    timestamp = datetime.fromisoformat(parts[2])
    object_refs = ast.literal_eval(parts[3])  # Safely parse the list of dicts

    return BOEvent(
        event_id=event_id,
        activity_name=activity,
        timestamp=timestamp,
        object_refs=object_refs
    )