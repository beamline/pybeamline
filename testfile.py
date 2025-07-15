from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.strategies.base import LossyCountingStrategy, SlidingWindowStrategy, \
    RelativeFrequencyBasedStrategy
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.ocel2_log_source_from_file import ocel2_log_source_from_file
from pybeamline.utils.visualizer import Visualizer

test_trace_1 = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
    {"activity": "Link Order To Shipment", "objects":  {"Order": ["o1"], "Shipment": ["s1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1", "i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i1", "i2"]}},
    {"activity": "Pack Item", "objects": {"Item": ["i1","i2"], "Order": ["o1"]}},
    {"activity": "Ship Item", "objects": {"Item": ["i1","i2"], "Shipment": ["s1"]}},
    {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
    {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
]

test_trace_2 = [
        {"activity": "Create Order", "objects": {"Receipt": ["r1"]}},
        {"activity": "Fast Track", "objects": {"Receipt": ["r1"]}},
        {"activity": "Add Item", "objects": {"Recept": ["r1"], "Item": ["i3", "i4"]}},
        {"activity": "Pack Item", "objects": {"Item": ["i3"], "Receipt": ["r1"]}},
        {"activity": "Pack Item", "objects": {"Item": ["i4"], "Receipt": ["r1"]}},
        {"activity": "Ship Item", "objects": {"Item": ["i3","i4"], "Shipment": ["s2"]}},
        {"activity": "Send Invoice", "objects": {"Receipt": ["r1"], "Invoice": ["inv2"]}},
        {"activity": "Receive Review", "objects": {"Receipt": ["r1"]}},
    ]



#combined_log = dict_test_ocel_source([(test_trace_1,25),(test_trace_2,25)], shuffle=False)
combined_log = ocel2_log_source_from_file('tests/logistics.jsonocel')

#dict_test_ocel_source([(test_events_phaseflow_ends_early,25),(test_events_phaseflow, 2500)], shuffle=False)


control_flow = {
    "Order": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
    "Item": lambda : heuristics_miner_lossy_counting(model_update_frequency=5),
    #"Customer": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
    #"Shipment": lambda : heuristics_miner_lossy_counting(model_update_frequency=1),
    #"Invoice": lambda :heuristics_miner_lossy_counting(model_update_frequency=1),
}

visualizer = Visualizer()

from reactivex import operators as ops
# pipe the combined log to the OCOperator op
emitted_models = []
def append_emitted(m):
    """
    Callback to append emitted OCDFGs to the list.
    """
    emitted_models.append(m)


strategy = RelativeFrequencyBasedStrategy(frequency_threshold=0.002)
combined_log.pipe(
    oc_operator(inclusion_strategy=strategy),
    #ops.do_action(print),
    oc_merge_operator(),
    #ops.do_action(print),
).subscribe(lambda x: append_emitted(x))


for m in emitted_models:
    print(m)

print(f"Length of emitted: {len(emitted_models)}")
# Assert aer_diagram is in the emitted models

for i, m in enumerate(emitted_models):
    aer_diagram = m.get("aer")
    if aer_diagram is not None:
        visualizer.save_aer_diagram(aer_diagram)
    else:
        print(f"Model {i+1} does not contain an AER diagram.")

visualizer.generate_aer_gif()


for i, m in enumerate(emitted_models):
    ocdfg = m.get("ocdfg")
    if ocdfg is not None:
        visualizer.save_ocdfg(ocdfg)
    else:
        print(f"Model {i+1} does not contain an OCDFG.")

visualizer.generate_ocdfg_gif(out_file="ocdfg_evolution.gif", duration=500)

#for i, m in enumerate(emitted_models):
#    if i%1== 0:
#        visualizer.save(m["ocdfg"])

#

#for i, m in enumerate(emitted):
#    print(m["relation"])


#visualizer.generate_side_by_side_gif()


