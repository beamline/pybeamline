from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.strategies.base import LossyCountingStrategy
from pybeamline.objects.ocdfg import OCDFG
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

test_only_order = [
    {"activity": "Register Customer", "objects": {"Obj1": ["obj1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}}]


test = [
    {"activity": "Register Guest", "objects": {"Guest": ["g1"]}},
    {"activity": "Create Booking", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
    {"activity": "Reserve Room", "objects": {"Booking": ["b1"]}},
    {"activity": "Check In", "objects": {"Guest": ["g1"], "Booking": ["b1"]}},
    {"activity": "Check Out", "objects": {"Guest": ["g1"], "Booking": ["b1"]}}
]

test_receptonist= [
    {"activity": "Greet Guest", "objects": {"Guest": ["g2"]}, "Receptionist": ["r1"]},
    {"activity": "No Booking", "objects": {"Guest": ["g2"], "Receptionist": ["r1"]}},
    {"activity": "Make Bed", "objects": {"Room": ["r2"], "Receptionist": ["h1"]}},
]

test_events_phaseflow_ends_early = [
    {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i2"]}},
    {"activity": "Cancel Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}}
]

combined_log = dict_test_ocel_source([(test_events_phaseflow_ends_early,10), (test_events_phaseflow, 50)], shuffle=False)
#combined_log = ocel_log_source_from_file('tests/ocel2-p2p.json')

#dict_test_ocel_source([(test_events_phaseflow_ends_early,25),(test_events_phaseflow, 2500)], shuffle=False)


control_flow = {
    "Order": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
    "Item": lambda : heuristics_miner_lossy_counting(model_update_frequency=5),
    #"Customer": lambda : heuristics_miner_lossy_counting(model_update_frequency=10, max_approx_error=0.1),
    #"Shipment": lambda : heuristics_miner_lossy_counting(model_update_frequency=1),
    #"Invoice": lambda :heuristics_miner_lossy_counting(model_update_frequency=1),
}

visualizer = Visualizer()

def save_snapshots(ocdfg, relation_tracker=None):
    """
    Save DFM and UML snapshots using the visualizers.
    """
    visualizer.save(ocdfg, relation_tracker)

from reactivex import operators as ops
# pipe the combined log to the OCOperator op
emitted_models = []
def append_emitted(m):
    """
    Callback to append emitted OCDFGs to the list.
    """
    emitted_models.append(m)

def topology_heuristics(ocdfg_old: OCDFG, ocdfg_new: OCDFG) -> bool:
    """
    Heuristic to determine if the topology of the new OCDFG is significantly different from the old one.
    This can be used to decide whether to update the heuristic net or not.
    """
    # Example heuristic: if the number of nodes changes significantly, return True
    def significant_edge_change(ocdfg_old: OCDFG, ocdfg_new: OCDFG) -> bool:
        """
        Check if the number of edges has changed significantly.
        """
        return abs(len(ocdfg_new.edges) - len(ocdfg_old.edges)) > 0


    return abs(len(ocdfg_new.object_types) - len(ocdfg_old.object_types)) > 0 or \
            abs(len(ocdfg_new.activities) - len(ocdfg_old.activities)) > 0 or \
            significant_edge_change(ocdfg_old, ocdfg_new)


strategy = LossyCountingStrategy(max_approx_error=0.02)
combined_log.pipe(
    oc_operator(strategy_handler=strategy),
    #ops.do_action(print),
    oc_merge_operator(),
    #ops.do_action(print),
).subscribe(lambda x: append_emitted(x))




print(f"Length of emitted: {len(emitted_models)}")
# Assert aer_diagram is in the emitted models
""""
for i, m in enumerate(emitted_models):
    aer_diagram = m.get("aer_diagram")
    if aer_diagram is not None:
        visualizer.save_aer_diagram(aer_diagram)
    else:
        print(f"Model {i+1} does not contain an AER diagram.")

visualizer.generate_relation_gif()
"""

for i, m in enumerate(emitted_models):
    if i%1== 0:
        visualizer.save(m["ocdfg"])

visualizer.generate_ocdfg_gif(out_file="ocdfg_evolution.gif", duration=1000)

#for i, m in enumerate(emitted):
#    print(m["relation"])


#visualizer.generate_side_by_side_gif()


