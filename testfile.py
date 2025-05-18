from threading import Event
from typing import List, Dict, Any

from IPython.core.display_functions import display
from graphviz import render

from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.ocdfg_merge_operator import ocdfg_merge_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pybeamline.utils.visualizer import Visualizer

test_events_phaseflow = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1", "i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i1", "i2"]}},
    {"activity": "Pack Item", "objects": {"Item": ["i1","i2"], "Order": ["o1"]}},
    {"activity": "Ship Item", "objects": {"Item": ["i1","i2"], "Shipment": ["s1"]}},
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

#combined_log = dict_test_ocel_source([(test_events_phaseflow_ends_early,200),(test_events_phaseflow, 50)], shuffle=False)
combined_log = ocel_log_source_from_file('tests/logistics.jsonocel')

#dict_test_ocel_source([(test_events_phaseflow_ends_early,25),(test_events_phaseflow, 2500)], shuffle=False)


control_flow = {
    "Order": heuristics_miner_lossy_counting(model_update_frequency=20, max_approx_error=0.1),
    "Item": heuristics_miner_lossy_counting(model_update_frequency=20),
    "Customer": heuristics_miner_lossy_counting(model_update_frequency=20, max_approx_error=0.1),
    "Shipment": heuristics_miner_lossy_counting(model_update_frequency=20),
    "Invoice": heuristics_miner_lossy_counting(model_update_frequency=20),
}


visualizer = Visualizer()

def save_snapshots(ocdfg, relation_tracker=None):
    """
    Save DFM and UML snapshots using the visualizers.
    """
    visualizer.save(ocdfg, relation_tracker)

from reactivex import operators as ops
# pipe the combined log to the OCOperator op
emitted_relations = []
emitted_ocdfgs = []
def get_relations(x):
    global emitted_relations
    global emitted_ocdfgs
    if x.get("relations") is not None:
        return emitted_relations.append(x["relations"])
    if x.get("ocdfg") is not None:
        return emitted_ocdfgs.append(x["ocdfg"])


combined_log.pipe(
    oc_operator(track_relations=True),
    ocdfg_merge_operator(),
).subscribe(on_next=lambda x: get_relations(x))




print(f"Length of emitted: {len(emitted_relations)}")
for i, m in enumerate(emitted_relations):
    if i% 200 == 0:
        visualizer.save_relation(m)

visualizer.generate_relation_gif()

for i, m in enumerate(emitted_ocdfgs):
    if i% 200 == 0:
        visualizer.save(m)

visualizer.generate_ocdfg_gif()

#for i, m in enumerate(emitted):
#    print(m["relation"])


#visualizer.generate_side_by_side_gif()


