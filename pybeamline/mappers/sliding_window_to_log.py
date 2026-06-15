from typing import List, Optional
from typing_extensions import override
from pybeamline.abstractevent import AbstractEvent
from pandas import DataFrame
from pybeamline.stream.base_map import BaseMap


def sliding_window_to_log() -> BaseMap[List[AbstractEvent], DataFrame]:
    return SlidingWindowToLog()


class SlidingWindowToLog(BaseMap[List[AbstractEvent], DataFrame]):

    @override
    def transform(self, value: List[AbstractEvent]) -> Optional[List[DataFrame]]:
        converted_log = self.list_to_log(value)
        return [converted_log] if converted_log is not None else None

    @staticmethod
    def list_to_log(events: List[AbstractEvent]) -> DataFrame | None:
        df = DataFrame([e.to_dict() for e in events])
        if not {"event_attributes", "concept:name", "time:timestamp"}.issubset(df.columns):
            return None

        df = df.rename(columns={"concept:name": "case:concept:name"})
        df["concept:name"] = df["event_attributes"].str["concept:name"]
        return df