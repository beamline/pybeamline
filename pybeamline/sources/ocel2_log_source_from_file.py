import math
from typing import Dict, Any
from pm4py import OCEL, ocel_sort_by_additional_column, read_ocel2
from pybeamline.boevent import BOEvent
from pybeamline.stream.base_source import BaseSource
from pybeamline.stream.stream import Stream


def ocel2_log_source_from_file(log_path: str, ) -> Stream[BOEvent]:
    """
    Loads an OCEL 2.0 log from a file path and returns it as an Observable of BOEvent objects.
    :param log_path: str
    :return: Observable[BOEvent]
    """
    return Stream.source(Ocel2LogSource(read_ocel2(log_path)))

class Ocel2LogSource(BaseSource[BOEvent]):

    def __init__(self, log: OCEL):
        self.log = log
        """
        Converts an OCEL object into an Observable stream of BOEvent objects,
        ordered by timestamp if available.
        """
        if self.log.event_timestamp is not None:
            self.log = ocel_sort_by_additional_column(log, "ocel:timestamp")

    def execute(self):
        for _, event in self.log.get_extended_table().iterrows():

            # Build the omap using columns that start with "ocel:type:" and are not NaN
            omap = {
                col.split("ocel:type:")[1]: event[col]
                for col in event.keys()
                if col.startswith("ocel:type:") and event[col] is not None
                and not (isinstance(event[col], float) and math.isnan(event[col]))
            }

            vmap: Dict[str,Any] = {
                col: event[col]
                for col in event.keys()
                if col not in {"ocel:eid", "ocel:activity", "ocel:timestamp"} and not col.startswith("ocel:type:")
                and event[col] is not None
                and not (isinstance(event[col], float) and math.isnan(event[col]))
            }

            bo_event = BOEvent(
                event_id=event["ocel:eid"],
                activity_name=event["ocel:activity"],
                timestamp=event["ocel:timestamp"],
                omap=omap,
                vmap=vmap
            )
            self.produce(bo_event)
        self.completed()