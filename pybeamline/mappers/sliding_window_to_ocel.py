import uuid
import pandas as pd
from pybeamline.mappers import sliding_window_to_log
from reactivex import operators as ops, just, empty
from typing import Callable
from reactivex import Observable
from pandas import DataFrame
from pm4py import OCEL
from reactivex import operators as ops

from pybeamline.mappers.sliding_window_to_log import list_to_log


def sliding_window_to_ocel() -> Callable[[Observable[Observable]], Observable[OCEL]]:
    def convert(events_df: DataFrame) -> OCEL:
        objects_df = extract_object_table(events_df)
        relations_df = extract_relations_table(events_df)
        return OCEL(events=events_df, objects=objects_df, relations=relations_df)

    return ops.flat_map(
        lambda obs: obs.pipe(
            ops.to_iterable(),
            ops.flat_map(lambda events: just(list_to_log(events)) if events else empty()),
            ops.map(convert)
        )
    )
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