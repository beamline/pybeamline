import random
import uuid
from itertools import chain

from pm4py import OCEL, ocel_sort_by_additional_column, discover_ocdfg, read_ocel2, read_ocel2_json, \
    discover_oc_petri_net, view_ocpn
from reactivex.operators import window_with_count, flat_map, to_list
from reactivex import operators as ops, concat
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pm4py.objects.ocel.obj import OCEL
from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery
from river import drift

from pybeamline.mappers import sliding_window_to_log
from pybeamline.mappers.sliding_window_to_ocel import log_to_ocel_operator

from pybeamline.sources.string_ocel_test_source import dict_test_ocel_source

test_events_1 = [
    {"activity": "Create Order", "objects": {"Order": ["o1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
    {"activity": "Approve Order", "objects": {"Order": ["o1"]}},
    {"activity": "Pack Items", "objects": {"Order": ["o1"], "Item": ["i1"], "Package": ["p1"]}},
    {"activity": "Ship Order", "objects": {"Order": ["o1"], "Shipment": ["s1"]}},
    {"activity": "Invoice Order", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
]

test_events_2 = [
    {"activity": "Create Order", "objects": {"Order": ["o2"]}},
    {"activity": "Add Digital Product", "objects": {"Order": ["o2"], "DigitalItem": ["di1"]}},
    {"activity": "Approve Order", "objects": {"Order": ["o2"]}},
    {"activity": "Activate License", "objects": {"Order": ["o2"], "DigitalItem": ["di1"], "License": ["lic1"]}},
    {"activity": "Send Download Link", "objects": {"Order": ["o2"], "DigitalItem": ["di1"]}},
    {"activity": "Invoice Order", "objects": {"Order": ["o2"], "Invoice": ["inv2"]}},
]

combined_log = dict_test_ocel_source([(test_events_1, 100), (test_events_2, 500), (test_events_1,100)], shuffle=True)


pattern = ("Approve Order", "Pack Items")

def object_centric_pattern_detector(activity_pair):
    def _op(obs):
        return obs.pipe(
            ops.do_action(lambda x: print(f"🔬 Inside detector: {x.get_event_name()}")),
            ops.buffer_with_count(2, 1),
            ops.filter(lambda pair: (
                pair[0].get_event_name() == activity_pair[0] and
                pair[1].get_event_name() == activity_pair[1] and
                any(obj in pair[0].get_object_ids() for obj in pair[1].get_object_ids())
            )),
            ops.do_action(lambda pair: print(f"🎯 Matched pair: {pair[0].get_event_name()} → {pair[1].get_event_name()}")),
            ops.count()
        )
    return _op


from river import drift

drift_detector = drift.ADWIN()
drift_points = []

def check_for_drift():
    index = 0
    def _process(x):
        nonlocal index
        drift_detector.update(x)
        index += 1
        if drift_detector.drift_detected:
            drift_points.append(index)
            print(f"🚨 Drift detected at window index {index} (pattern count: {x})")
    return ops.do_action(_process)

import reactivex
from reactivex import operators as ops

combined_log.pipe(
    ops.buffer_with_count(4),
    ops.flat_map(lambda window: reactivex.from_iterable(window).pipe(
        object_centric_pattern_detector(pattern)
    )),
    ops.do_action(lambda x: print(f"📊 Pattern count in window: {x}")),
    check_for_drift()
).subscribe(lambda x: print(f"✅ Final output: {x}"))

#combined_log.pipe(
#    ops.do_action(lambda x: print(f"🔄 Passing through: {x.get_event_name()}")),
#    ops.buffer_with_count(4),
#    ops.do_action(lambda x: print(f"🧩 Buffered window: {[e.get_event_name() for e in x]}")),
#    ops.flat_map(lambda window: reactivex.from_iterable(window).pipe(
#        object_centric_pattern_detector(pattern)
#    )),
#    ops.do_action(lambda x: print(f"📊 Pattern count in window: {x}")),
#    check_for_drift()
#).subscribe(lambda x: print(f"✅ Final output: {x}"))

#combined_log.pipe(
#    ops.do_action(lambda e: print(f"🔄 Event flowing in: {e.get_event_name()}")),
#    ops.buffer_with_count(4),
#    ops.flat_map(lambda window: reactivex.from_iterable(window).pipe(
#        ops.do_action(lambda e: print(f"🔍 Event: {e.get_event_name()}, Objects: {e.get_object_ids()}")),
#        object_centric_pattern_detector(pattern)
#    )),
#    check_for_drift()
#).subscribe(lambda count: print(f"✅ Pattern count: {count}"))
