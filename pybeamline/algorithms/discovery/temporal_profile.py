from typing import List, Union, Optional

from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery
from pm4py.objects.log.obj import EventLog
import pandas as pd

from pybeamline.bevent import BEvent, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY, DEFAULT_TRACEID_KEY
from pm4py.util.typing import TemporalProfile
from pybeamline.stream.base_map import BaseMap

def temporal_profile_discovery_mapper() -> BaseMap[List[BEvent], TemporalProfile]:
    return TemporalProfileDiscoveryMapper()

class TemporalProfileDiscoveryMapper(BaseMap[List[BEvent], TemporalProfile]):

    def __init__(self):
        self.events = []

    def transform(self, value: List[BEvent]) -> Optional[List[TemporalProfile]]:
        self.events.append(value)
        tp_discovery = TemporalProfileDiscovery(self.events)
        return [tp_discovery.apply()]


class TemporalProfileDiscovery:

    def __init__(self, ls: List[BEvent]):
        self.events = ls

    @staticmethod
    def _convert_b_event_to_dataframe(b_events: List[BEvent]) -> Union[EventLog, pd.DataFrame]:
        if not b_events:
            return pd.DataFrame(columns=[
                DEFAULT_NAME_KEY,
                DEFAULT_TIMESTAMP_KEY,
                f"case:{DEFAULT_TRACEID_KEY}"
            ])

        rows = []
        for e in b_events:
            row = {
                DEFAULT_NAME_KEY: e.get_event_name(),
                DEFAULT_TIMESTAMP_KEY: e.get_event_time(),
                f"case:{DEFAULT_TRACEID_KEY}": e.get_trace_name(),
            }

            protected_event_keys = {DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY}
            for k, v in e.event_attributes.items():
                if k not in protected_event_keys:
                    row[k] = v

            for k, v in e.trace_attributes.items():
                trace_key = f"case:{k}"
                if trace_key not in row:
                    row[trace_key] = v

            rows.append(row)

        df = pd.DataFrame(rows)

        if DEFAULT_TIMESTAMP_KEY in df.columns:
            df[DEFAULT_TIMESTAMP_KEY] = pd.to_datetime(df[DEFAULT_TIMESTAMP_KEY], utc=True)
            df = df.sort_values(by=[DEFAULT_TIMESTAMP_KEY]).reset_index(drop=True)

        return df


    def apply(self) -> TemporalProfile:
        raw_log = self._convert_b_event_to_dataframe(self.events)
        return temporal_profile_discovery.apply(raw_log)
