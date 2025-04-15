from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import oc_heuristics_miner_lossy_counting
from pybeamline.algorithms.oc_operator import OCOperator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.dict_to_ocel import dict_test_ocel_log

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

combined_log = dict_test_ocel_source([(test_events_phaseflow, 10)], shuffle=False)

test_log = dict_test_ocel_log([(test_events_phaseflow, 10)], shuffle=False)


control_flow = {
    "Order": oc_heuristics_miner_lossy_counting(model_update_frequency=4),
    #"Item": oc_heuristics_miner_lossy_counting(model_update_frequency=1),
}

oc_operator = OCOperator(control_flow)

# pipe the combined log to the OCOperator op
combined_log.pipe(
    oc_operator.op()
).subscribe(lambda x: print(f"Received: {x}"))






