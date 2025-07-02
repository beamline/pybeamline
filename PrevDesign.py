from graphviz import render
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.utils.visualizer import Visualizer

test_events_phaseflow_ends_early = [
    {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i3"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i2", "i3"]}},
    {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
    {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
]

combined_log = dict_test_ocel_source([(test_events_phaseflow_ends_early,50)], shuffle=False)

emitted = []
combined_log.pipe(
    oc_operator(frequency_threshold=0.002),
    oc_merge_operator(),
    #ops.do_action(print),
).subscribe(lambda e: emitted.append(e["ocdfg"]))#lambda x: append_emitted(x))

for i, ocdfg in enumerate(emitted):
    print(ocdfg)

last = emitted[-1]

visualizer = Visualizer()

visualizer.save(last)