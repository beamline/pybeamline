import uuid

import pandas as pd
from pandas import DataFrame
from pm4py import OCEL
from reactivex import operators as ops
from pandas import DataFrame
from pm4py import OCEL

def log_to_ocel_operator():
    def convert(events_df: DataFrame) -> OCEL:
        objects_df = extract_object_table(events_df)
        relations_df = extract_relations_table(events_df)
        return OCEL(events=events_df, objects=objects_df, relations=relations_df)
    return ops.map(convert)

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

def extract_object_table(events_df: DataFrame) -> pd.DataFrame:
    object_list = []
    for omap in events_df["ocel:omap"]:
        for obj in omap:
            object_list.append({
                "ocel:oid": obj["ocel:oid"],
                "ocel:type": obj["ocel:type"]
            })
    return pd.DataFrame(object_list).drop_duplicates()