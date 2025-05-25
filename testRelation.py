from typing import Tuple, List, Any

from pm4py import view_ocdfg

from pybeamline.objects.ocdfg import OCDFG
from pybeamline.algorithms.oc.oc_dfg_merge_operator import oc_dfg_merge_operator
from pybeamline.algorithms.oc.oc_dfg_operator import oc_dfg_operator
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pm4py.read import read_ocel2_json
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from reactivex import operators as ops

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

# Started the streaming process
# Log
log = ocel_log_source_from_file("tests/logistics.jsonocel")

# Set based
def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = model.intersection(ref_model)
    if intersection is not None:
        print(f"The missing edges in the model: {ref_model - intersection}")
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

emitted_ocdfgs = []
def append_ocdfg(ocdfg: OCDFG):
    global emitted_ocdfgs
    emitted_ocdfgs.append(ocdfg)


log.pipe(
    oc_dfg_operator(),
    oc_dfg_merge_operator()
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

jacc_similarities = []
for ocdfg_edge_set in emitted_ocdfgs_edge_set:
    similarity = jaccard_similarity(ocdfg_edge_set, ocdfg_edges_pm4py)
    jacc_similarities.append(similarity)

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

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

