from typing import Dict, Set, Optional

from reactivex.operators import window_with_count
from reactivex import operators as ops
from pm4py import view_ocpn, view_ocdfg, OCEL
from pm4py.read import read_ocel2
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms import bipartite
from pybeamline.boevent import BOEvent
from pybeamline.mappers.sliding_window_to_ocel import sliding_window_to_ocel
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source

def extract_activity_object_bipartite_graph(ocel: OCEL) -> nx.Graph:
    """
    Constructs a bipartite graph where one set of nodes are activities
    and the other set are object types, based on the OCEL log.
    """
    activity_object_map: Dict[str, Set[str]] = {}

    for _, event in ocel.get_extended_table().iterrows():
        activity = event["ocel:activity"]
        object_refs = [
            {"ocel:oid": oid, "ocel:type": col.split(":")[-1]}
            for col in event.keys()
            if col.startswith("ocel:type:")
            for oid in (event[col] if isinstance(event[col], list) else [])
        ]
        related_objects = object_refs
        if activity not in activity_object_map:
            activity_object_map[activity] = set()
        for obj in related_objects:
            activity_object_map[activity].add(obj["ocel:type"])

    # Create the bipartite graph
    B = nx.Graph()
    for activity, obj_types in activity_object_map.items():
        B.add_node(activity, bipartite='activity')
        for obj_type in obj_types:
            B.add_node(obj_type, bipartite='object')
            B.add_edge(activity, obj_type)
    return B

def plot_bipartite_graph(B: nx.Graph):
    pos = nx.spring_layout(B, seed=42)
    colors = ['skyblue' if B.nodes[n].get('bipartite') == 'activity' else 'lightgreen' for n in B.nodes]

    plt.figure(figsize=(12, 6))
    nx.draw(B, pos, with_labels=True, node_color=colors, node_size=1500, font_size=10)
    plt.title("Activity-Object Type Bipartite Graph")
    plt.axis('off')
    plt.show()

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

def plot_bipartite_graph_readable(B: nx.Graph):


    activities = [n for n, d in B.nodes(data=True) if d.get('bipartite') == 'activity']
    object_types = [n for n in B if n not in activities]

    # Assign positions in two rows
    pos = {}
    pos.update((node, (i, 1)) for i, node in enumerate(activities))        # Top row
    pos.update((node, (i, 0)) for i, node in enumerate(object_types))       # Bottom row

    plt.figure(figsize=(15, 6))
    nx.draw_networkx_nodes(B, pos, nodelist=activities, node_color='skyblue', node_size=1500, edgecolors='black')
    nx.draw_networkx_nodes(B, pos, nodelist=object_types, node_color='lightgreen', node_size=1500, edgecolors='black')
    nx.draw_networkx_edges(B, pos)
    nx.draw_networkx_labels(B, pos, font_size=10)

    plt.title("Activity-Object Type Bipartite Graph", fontsize=14)
    plt.axis('off')
    plt.tight_layout()
    plt.show()

print("Creating activity-object map...")

ocel = read_ocel2("tests/ocel.jsonocel")
B = extract_activity_object_bipartite_graph(ocel)
plot_bipartite_graph_readable(B)
