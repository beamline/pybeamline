from pybeamline.boevent import BOEvent
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pybeamline.utils.object_relation_tracker import ObjectRelationTracker
from reactivex import operators as ops

test_events_one_obj = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Leave Customer", "objects": {"Customer": ["c1"]}},
]

test_events_two_obj = [
    {"activity": "Register Customer", "objects": {"Customer": ["c2"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c2"], "Order": ["o2"]}},
]

#combined_log = dict_test_ocel_source([(test_events_two_obj,50),(test_events_one_obj, 200)], shuffle=False)
combined_log = ocel_log_source_from_file("tests/socel2_hinge.xml")

objT = ObjectRelationTracker()


def ingest(event: BOEvent):
    objT.ingest_event(event)

combined_log.pipe(
    ops.do_action(lambda event: ingest(event)),
).subscribe()

for node in objT.nodes.values():
    if node.name == "SteelCoil":
        print(f"Node: {node.name}")
        print(f"  Frequency: {node.frequency}")
        print(f"  Activities: {node.activities}")
        print(f"  Attributes: {node.attributes}")
        #print(f"  Relations: {node.relations}")
        print(f"  Buckets: {node.bucket}, Delta: {node.delta}")
