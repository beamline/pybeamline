from datetime import datetime
import random
from typing import List, Tuple
from pm4py.objects.ocel.obj import OCEL
import pandas as pd

from pybeamline.mappers.sliding_window_to_ocel import extract_relations_table, extract_object_table


def generate_shuffled_traces(flows: List[Tuple[List[dict], int]], shuffle: bool = True) -> List[dict]:
    all_traces = []
    trace_id = 0

    for flow_template, repetitions in flows:
        for _ in range(repetitions):
            trace = []
            suffix = f"{trace_id}"
            for event in flow_template:
                updated_event = {
                    "activity": event["activity"],
                    "objects": {
                        obj_type: [f"{oid}_{suffix}" for oid in obj_ids]
                        for obj_type, obj_ids in event["objects"].items()
                    }
                }
                trace.append(updated_event)
            all_traces.append(trace)
            trace_id += 1

    if shuffle:
        random.shuffle(all_traces)

    return [event for trace in all_traces for event in trace]


def dict_test_ocel_log(flows: List[Tuple[List[dict], int]], shuffle: bool = False) -> OCEL:
    all_events = generate_shuffled_traces(flows, shuffle=shuffle)

    # Construct OCEL events and objects
    event_data = []
    object_data = []
    seen_objects = set()

    for idx, event in enumerate(all_events):
        event_id = f"e{idx}"
        timestamp = datetime.now()

        event_data.append({
            "ocel:eid": event_id,
            "ocel:activity": event["activity"],
            "ocel:timestamp": timestamp,
            "ocel:omap": [
                {"ocel:oid": oid, "ocel:type": obj_type}
                for obj_type, ids in event["objects"].items()
                for oid in ids
            ]
        })

        for obj_type, ids in event["objects"].items():
            for oid in ids:
                if (oid, obj_type) not in seen_objects:
                    object_data.append({
                        "ocel:oid": oid,
                        "ocel:type": obj_type
                    })
                    seen_objects.add((oid, obj_type))

    events_df = pd.DataFrame(event_data)
    objects_df = extract_object_table(events_df)
    relations_df = extract_relations_table(events_df)

    return OCEL(events=events_df, objects=objects_df, relations=relations_df)
