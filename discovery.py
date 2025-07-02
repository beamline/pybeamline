from typing import Tuple, List, Any, Dict
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pm4py.read import read_ocel2_json
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from reactivex import operators as ops
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


# PM4Py
def read_ocel2_log(filename: str):
    return read_ocel2_json(filename)

oc_event_log = read_ocel2_log("tests/logistics.jsonocel")
assert oc_event_log is not None

# Do OCDFG discovery
ocdfg = ocdfg_discovery.apply(oc_event_log)

def conform_ocdfg(ocdfg_pm4py) -> set[Tuple[str, str, str]]:
    """
    Convert PM4Py OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type in ocdfg_pm4py["edges"]["event_couples"].keys():
        for src, tgt in ocdfg_pm4py["edges"]["event_couples"][obj_type].keys():
            result.add((src, obj_type, tgt))
    return result
# Convert the PM4Py OCDFG to a set of edges
ocdfg_edges_pm4py = conform_ocdfg(ocdfg)

def egdes_pr_object_type(ocdfg: set[Tuple[str,str,str]]) -> Dict[str, set[Tuple[str, str, str]]]:
    """
    Convert OCDFG to a dictionary of edges per object type.
    """
    result = {}
    for src, obj_type, tgt in ocdfg:
        if obj_type not in result:
            result[obj_type] = set()
        result[obj_type].add((src,obj_type, tgt))
    return result

# Convert the PM4Py OCDFG edges to a dictionary of edges per object type
ocdfg_edges_per_object_type = egdes_pr_object_type(ocdfg_edges_pm4py)


# Started the streaming process
# Log
log = ocel_log_source_from_file("tests/logistics.jsonocel")

# Set based
def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = model.intersection(ref_model)
    #if intersection is not None:
    #    print(f"The missing edges in the model: {ref_model - intersection}")
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

emitted_ocdfgs = []
def append_ocdfg(output: dict):
    emitted_ocdfgs.append(output["ocdfg"])


log.pipe(
    oc_operator(object_emit_threshold=0.002),
    #ops.do_action(print),
    oc_merge_operator(),
).subscribe(append_ocdfg)


# Conform the emitted OCDFGs to the set notation of edges
def conform_emit_ocdfg(ocdfg: OCDFG) -> set[tuple[str, str, str]]:
    """
    Convert OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type, transitions in ocdfg.edges.items():
        for (src, tgt), freq in transitions.items():
            result.add((src, obj_type, tgt))
    return result

emitted_ocdfgs_edge_set = [ conform_emit_ocdfg(ocdfg) for ocdfg in emitted_ocdfgs]

obj_type_jacc_similarities = {}

for snapshot in emitted_ocdfgs_edge_set:
    snapshot_per_obj = egdes_pr_object_type(snapshot)

    for obj_type, ref_edges in ocdfg_edges_per_object_type.items():
        # Initialize list if first time
        if obj_type not in obj_type_jacc_similarities:
            obj_type_jacc_similarities[obj_type] = []

        model_edges = snapshot_per_obj.get(obj_type, set())
        similarity = jaccard_similarity(model_edges, ref_edges)
        obj_type_jacc_similarities[obj_type].append(similarity)

records = []

for obj_type, similarities in obj_type_jacc_similarities.items():
    for i, sim in enumerate(similarities[:200], start=1):
        records.append({
            "Snapshot": i,
            "Object Type": obj_type,
            "Jaccard Similarity": sim
        })

df_long = pd.DataFrame(records)

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_long, x="Snapshot", y="Jaccard Similarity", hue="Object Type", marker="o")
plt.title("Jaccard Similarity per Object Type Over OCDFG Snapshots")
plt.xlabel("Snapshot Index")
plt.ylabel("Jaccard Similarity")
plt.grid(True)
plt.tight_layout()
plt.show()

"""
jacc_similarities = []
for ocdfg_edge_set in emitted_ocdfgs_edge_set:
    similarity = jaccard_similarity(ocdfg_edge_set, ocdfg_edges_pm4py)
    jacc_similarities.append(similarity)


# Create a DataFrame for Seaborn
df = pd.DataFrame({
    "Snapshot": range(1, len(jacc_similarities) + 1),
    "Jaccard Similarity": jacc_similarities
})

# Plot
plt.figure(figsize=(10, 4))
sns.lineplot(data=df, x="Snapshot", y="Jaccard Similarity", marker="o")
plt.title("Jaccard Similarity per Emitted OCDFG")
plt.xlabel("OCDFG Snapshot Index")
plt.ylabel("Jaccard Similarity")
plt.grid(True)
plt.tight_layout()
plt.show()

"""


