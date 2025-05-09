import math
from typing import Optional, Dict, Set, Any
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable
from pm4py import OCEL, ocel_sort_by_additional_column, read_ocel2
from pybeamline.boevent import BOEvent

def ocel_log_source_from_file(log_path: str) -> Observable[BOEvent]:
    """
    Loads an OCEL 2.0 JSON log from a file path and returns it as an Observable of BOEvent objects.
    :param log_path: str
    :return: Observable[BOEvent]
    """
    return ocel_log_source(read_ocel2(log_path))


def ocel_log_source(
    log: OCEL,
    scheduler: Optional[abc.SchedulerBase] = None
) -> Observable[BOEvent]:
    """
    Converts an OCEL object into an Observable stream of BOEvent objects,
    ordered by timestamp if available.
    """
    if log.event_timestamp is not None:
        log = ocel_sort_by_additional_column(log, "ocel:timestamp")

    def subscribe(
        observer: abc.ObserverBase[BOEvent],
        scheduler_: Optional[abc.SchedulerBase] = None
    ) -> abc.DisposableBase:

        for _, event in log.get_extended_table().iterrows():

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
            observer.on_next(bo_event)

        observer.on_completed()
        return CompositeDisposable()

    return Observable(subscribe)
