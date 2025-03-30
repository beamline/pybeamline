import uuid
from pm4py import OCEL, ocel_sort_by_additional_column, discover_ocdfg, read_ocel2, read_ocel2_json, \
    discover_oc_petri_net, view_ocpn
from reactivex.operators import window_with_count, flat_map, to_list
from reactivex import operators as ops
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pm4py.objects.ocel.obj import OCEL
from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery
from pybeamline.mappers import sliding_window_to_log

from pybeamline.sources.ocel_json_log_source import ocel_json_log_source_from_file
import pandas as pd
from pandas import DataFrame

def extract_object_table(events_df: DataFrame) -> pd.DataFrame:
    object_list = []
    for omap in events_df["ocel:omap"]:
        for obj in omap:
            object_list.append({
                "ocel:oid": obj["ocel:oid"],
                "ocel:type": obj["ocel:type"]
            })
    return pd.DataFrame(object_list).drop_duplicates()

def discover_ocpn_per_window():
    return ops.map(lambda ocel: ocpn_discovery.apply(ocel))

def discover_ocdfg_per_window():
    return ops.map(lambda df: ocdfg_discovery.apply(OCEL(events=df)))

def extract_relations_table(events_df: DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in events_df.iterrows():
        for obj in row["ocel:omap"]:
            rows.append({
                "ocel:eid": row["ocel:eid"],
                "ocel:activity": row["ocel:activity"],
                "ocel:timestamp": row["ocel:timestamp"],
                "ocel:oid": obj["ocel:oid"],
                "ocel:type": obj["ocel:type"]
            })
    return pd.DataFrame(rows)

def events_df_to_minimal_ocel(events_df: DataFrame) -> OCEL:
    if "ocel:eid" not in events_df.columns:
        events_df["ocel:eid"] = [str(uuid.uuid4()) for _ in range(len(events_df))]
    # Extract the object table from the events DataFrame
    objects_df = extract_object_table(events_df)
    # Create the relations DataFrame
    relations_df = extract_relations_table(events_df)

    res = OCEL(events=events_df, objects=objects_df, relations=relations_df)
    return res


log_source = ocel_json_log_source_from_file('tests/ocel.jsonocel')

log_source.pipe(
    window_with_count(1000),
    sliding_window_to_log(),
    ops.map(events_df_to_minimal_ocel),
    discover_ocpn_per_window()
).subscribe(lambda x: view_ocpn(x))
