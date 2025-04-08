from typing import Dict, Set, Optional

from reactivex.operators import window_with_count
from reactivex import operators as ops
from pm4py import view_ocpn, view_ocdfg, OCEL
from pm4py.read import read_ocel2
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
import networkx as nx
import matplotlib.pyplot as plt

from pybeamline.boevent import BOEvent
from pybeamline.mappers.sliding_window_to_ocel import sliding_window_to_ocel
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.dict_to_ocel import dict_test_ocel_log

test_events_phaseflow = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i1"]}},
    {"activity": "Pack Item", "objects": {"Item": ["i1"], "Order": ["o1"]}},
    {"activity": "Ship Item", "objects": {"Item": ["i1"], "Shipment": ["s1"]}},
    {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
    {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
]

test_events_phaseflow_ends_early = [
    {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
    {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
]

combined_log = dict_test_ocel_source([(test_events_phaseflow, 1)], shuffle=True)

test_log = dict_test_ocel_log([(test_events_phaseflow, 10)], shuffle=False)




phases = {
    "Customer and Order Initialization": {
        "activities": {"Register Customer Order", "Create Order"},
        "object_types": {"Customer", "Order"},
    },
    "Item Processing Coordination": {
        "activities": {"Add Item", "Reserve Item"},
        "object_types": {"Order", "Item"},
    },
    "Fulfillment and Shipping": {
        "activities": {"Pack Item", "Ship Item"},
        "object_types": {"Item", "Order", "Shipment"},
    },
    "Post-Delivery and Financial": {
        "activities": {"Send Invoice", "Receive Review"},
        "object_types": {"Order", "Invoice", "Customer"},
    }
}

# Token buckets for each phase (we store active object IDs per phase)
phases_token = {phase: set() for phase in phases}

def get_exit_activities(ocdfg, phase_activitites):
    """
    Detect exit activities within a phase based on the structural edges in the OC-DFG.
    An activity is considered an exit if it has outgoing edges to activities outside the phase.
    """
    # Flatten edges from all object types
    event_couples = ocdfg["edges"]["event_couples"]
    flat_edges = set()
    for obj_type, edges in event_couples.items():
        flat_edges.update(edges)
    print("flat_edges", flat_edges)

    for src, tgt in flat_edges:
        continue

def get_terminal_activities(ocdfg, phase_activities):
    """
    Detect terminal activities within a phase based on the structural edges in the OC-DFG.
    An activity is considered terminal if it has no outgoing edge to another activity in the same phase.
    """
    event_couples = ocdfg["edges"]["event_couples"]
    terminals = set()
    #print("phase", phase_activities)
    #print(event_couples)


    exit_activities = get_exit_activities(ocdfg, phase_activities)
    # Flatten edges from all object types

    flat_edges = set()
    for obj_type, edges in event_couples.items():
        flat_edges.update(edges)
    #print("Flat Edges:", flat_edges)
    for act in phase_activities:
        leads_to_other_in_phase = any(
            src == act and tgt not in phase_activities for (src, tgt) in flat_edges
        )
        if not leads_to_other_in_phase:
            #print("activity:", act)
            terminals.add(act)

    print(f"Terminal activities for phase '{phase_activities}': {terminals}")
    return terminals

# Assuming you've already discovered your ocdfg
#ocel = read_ocel2("tests/ocel.jsonocel")
ocdfg = ocdfg_discovery.apply(test_log)
print(ocdfg["edges"]["event_couples"].keys())

print("👀 OC-DFG edges preview:", list(ocdfg["edges"])[:5])
# Show ocdfg
#view_ocdfg(ocdfg)

# Cache terminal activities for each phase
phase_terminals = {
    phase: get_terminal_activities(ocdfg, details["activities"])
    for phase, details in phases.items()
}

def match_event_to_phase(event: BOEvent, phase_definitions: dict) -> Optional[str]:
    activity = event.get_event_name()
    involved_types = {ref["ocel:type"] for ref in event.get_object_refs()}

    for phase_name, details in phase_definitions.items():
        if activity in details["activities"]:
            if involved_types & details["object_types"]:  # Any shared object type
                return phase_name
    return None


def process_event(event: BOEvent):
    activity = event.get_event_name()
    involved_objects = {f"{ref['ocel:type']}:{ref['ocel:oid']}" for ref in event.get_object_refs()}

    for phase_name, details in phases.items():
        if activity in details["activities"]:
            # Add relevant objects to token bucket
            for obj in event.get_object_refs():
                if obj["ocel:type"] in details["object_types"]:
                    phases_token[phase_name].add(f"{obj['ocel:type']}:{obj['ocel:oid']}")

            # If this activity is a terminal for the phase, we remove the tokens
            if activity in phase_terminals[phase_name]:
                for obj in event.get_object_refs():
                    if obj["ocel:type"] in details["object_types"]:
                        phases_token[phase_name].discard(f"{obj['ocel:type']}:{obj['ocel:oid']}")

    # Print the current active tokens
    print(f"\n🔄 Event: {activity}")
    for phase, tokens in phases_token.items():
        print(f"📊 Phase '{phase}' → Active Objects: {len(tokens)}")



combined_log.pipe(
    ops.do_action(process_event)
).subscribe()






