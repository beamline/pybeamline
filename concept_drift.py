from numpy.matlib import empty

from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator, MiningStrategy
from pybeamline.algorithms.oc.strategies.base import LossyCountingStrategy
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pybeamline.utils.visualizer import Visualizer

booking_flow = [
    {"activity": "Register Guest", "objects": {"Guest": ["g1"]}},
    {"activity": "Create Booking", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
    {"activity": "Reserve Room", "objects": {"Room": ["r1"], "Booking": ["b1"]}},
    {"activity": "Check In", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
    {"activity": "Check Out", "objects": {"Guest": ["g1"], "Booking": ["b1"]}}
]

test_customers = [
    {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
    {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
]

source = dict_test_ocel_source([(booking_flow, 5), (test_customers, 20)], shuffle=False)

booking_flow_set = {
    ("Register Guest", "Guest", "Create Booking"),
    ("Create Booking", "Guest", "Check In"),
    ("Create Booking", "Booking", "Reserve Room"),
    ("Reserve Room", "Booking", "Check In"),
    ("Check In", "Guest", "Check Out"),
    ("Check In", "Booking", "Check Out")
}

test_customers_set = {
    ("Register Customer", "Customer", "Create Order"),
    ("Create Order", "Customer", "Cancel Order"),
    ("Create Order", "Order", "Add Item"),
    ("Add Item", "Order", "Cancel Order"),
    ("Add Item", "Item", "Reserve Item"),
}

control_flow = {
    "Guest": heuristics_miner_lossy_counting(5),
    "Booking": heuristics_miner_lossy_counting(5),
    "Room": heuristics_miner_lossy_counting(5),
    "Customer": heuristics_miner_lossy_counting(5),
    "Order": heuristics_miner_lossy_counting(5),
    "Item": heuristics_miner_lossy_counting(5),
}


# Set based
def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = model.intersection(ref_model)
    if intersection is not None and not bool(intersection):
        print(f"The missing edges in the model: {ref_model - intersection}")
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

emitted_ocdfgs = []
def append_ocdfg(ocdfg):
    emitted_ocdfgs.append(ocdfg["ocdfg"])
from reactivex import operators as ops
strategy = LossyCountingStrategy(max_approx_error=0.15)
source.pipe(
    oc_operator(strategy_handler=strategy),
    oc_merge_operator(),
    ops.do_action(print),
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

visualizer = Visualizer()

visualizer.save(emitted_ocdfgs[-1])

booking_similarities = []
customer_similarities = []

for ocdfg_edge_set in emitted_ocdfgs_edge_set:
    booking_sim = jaccard_similarity(ocdfg_edge_set, booking_flow_set)
    customer_sim = jaccard_similarity(ocdfg_edge_set, test_customers_set)
    booking_similarities.append(booking_sim)
    customer_similarities.append(customer_sim)


import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Build long-format dataframe
df = pd.DataFrame({
    "Snapshot": range(1, len(emitted_ocdfgs_edge_set) + 1),
    "Booking Flow": booking_similarities,
    "Customer Flow": customer_similarities
})

df_melted = df.melt(id_vars="Snapshot",
                    value_vars=["Booking Flow", "Customer Flow"],
                    var_name="Reference",
                    value_name="Jaccard Similarity")




# Plot
plt.figure(figsize=(10, 4))
sns.lineplot(data=df_melted, x="Snapshot", y="Jaccard Similarity", hue="Reference", marker="o")
plt.title("Jaccard Similarity of Emitted OCDFGs vs. Reference Flows")
plt.xlabel("OCDFG Snapshot Index")
plt.ylabel("Jaccard Similarity")
plt.grid(True)
plt.tight_layout()
plt.show()