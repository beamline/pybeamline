from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import oc_heuristics_miner_lossy_counting
from pybeamline.algorithms.oc_operator import OCOperator, oc_operator
from pybeamline.algorithms.ocdfg_merge_operator import ocdfg_merge_operator
from pybeamline.objects.ocdfgvisualizer import OCDFGVisualizer
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.ocel_json_log_source import ocel_json_log_source_from_file
from reactivex import operators as ops

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

#combined_log = dict_test_ocel_source([(test_events_phaseflow, 25)], shuffle=False)
combined_log = ocel_json_log_source_from_file('tests/logistics.jsonocel')

#dict_test_ocel_source([(test_events_phaseflow_ends_early,25),(test_events_phaseflow, 2500)], shuffle=False)


control_flow = {
    "Order": oc_heuristics_miner_lossy_counting(model_update_frequency=2, max_approx_error=0.1),
    "Item": oc_heuristics_miner_lossy_counting(model_update_frequency=2),
    "Customer": oc_heuristics_miner_lossy_counting(model_update_frequency=2, max_approx_error=0.1),
    "Shipment": oc_heuristics_miner_lossy_counting(model_update_frequency=1),
    "Invoice": oc_heuristics_miner_lossy_counting(model_update_frequency=1),
}


oc_visualizer = OCDFGVisualizer()

# pipe the combined log to the OCOperator op
combined_log.pipe(
    ops.take(50),
    ops.do_action(lambda x: print(f"Event: {x}")),

    #oc_operator(control_flow),
    #ocdfg_merge_operator()
).subscribe()#lambda x: oc_visualizer.render(x))


