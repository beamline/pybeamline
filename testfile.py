from reactivex.operators import window_with_count
from reactivex import operators as ops
from pm4py import view_ocpn, view_ocdfg
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
import networkx as nx
import matplotlib.pyplot as plt

from pybeamline.mappers.sliding_window_to_ocel import sliding_window_to_ocel
from pybeamline.sources.string_ocel_test_source import dict_test_ocel_source

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

log_without_drift = dict_test_ocel_source([(test_events_phaseflow,500),(test_events_phaseflow_ends_early,50)], shuffle=True)

def create_activity_object_map(log):
    activity_object_map = {}
    for event in log:
        activity = event["activity"]
        objects = event["objects"]
        if activity not in activity_object_map:
            activity_object_map[activity] = set()
        for obj_type, obj_ids in objects.items():
            for obj_id in obj_ids:
                activity_object_map[activity].add(f"{obj_type}:{obj_id}")
    return {k: list(v) for k, v in activity_object_map.items()}

print("Creating activity-object map...")
activity_object_map = create_activity_object_map(log_without_drift)


def plot_activity_object_bipartite(activity_object_map):
    B = nx.Graph()
    for activity, objects in activity_object_map.items():
        B.add_node(activity, bipartite='activity')
        for obj in objects:
            B.add_node(obj, bipartite='object')
            B.add_edge(activity, obj)

    pos = nx.spring_layout(B, seed=42)
    colors = ['skyblue' if B.nodes[n].get('bipartite') == 'activity' else 'lightgreen' for n in B.nodes]

    plt.figure(figsize=(12, 6))
    nx.draw(B, pos, with_labels=True, node_color=colors, node_size=1500, font_size=10)
    plt.title("Activity-Object Type Bipartite Graph")
    plt.axis('off')
    plt.show()

# Example usage
activity_object_map = {
    "Register Customer": ["Customer"],
    "Create Order": ["Order", "Customer"],
    "Add Item": ["Item", "Order"],
    "Reserve Item": ["Item", "Order"],
    "Pack Item": ["Item", "Order"],
    "Ship Item": ["Item", "Shipment"],
    "Send Invoice": ["Invoice", "Order", "Customer"],
    "Receive Review": ["Customer", "Order"],
    "Cancel Order": ["Order", "Customer"],
}

plot_activity_object_bipartite(activity_object_map)




log_without_drift.pipe(
    window_with_count(4500),
    sliding_window_to_ocel(),
    ops.map(lambda ocel: ocdfg_discovery.apply(ocel))
).subscribe(lambda x: view_ocdfg(x))






