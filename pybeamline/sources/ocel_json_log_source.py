from typing import Optional
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable
from pm4py import read_ocel2_json, OCEL, ocel_sort_by_additional_column, convert_to_dataframe
from pybeamline.boevent import BOEvent

def ocel_json_log_source_from_file(log_path: str) -> Observable[BOEvent]:
    """
    Loads an OCEL 2.0 JSON log from a file path and returns it as an Observable of BOEvent objects.
    """
    return ocel_json_log_source(read_ocel2_json(log_path))


def ocel_json_log_source(
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
            object_refs = [
                {"ocel:oid": oid, "ocel:type": col.split(":")[-1]}
                for col in event.keys()
                if col.startswith("ocel:type:")
                for oid in (event[col] if isinstance(event[col], list) else [])
            ]

            bo_event = BOEvent(
                event_id=event["ocel:eid"],
                activity_name=event["ocel:activity"],
                timestamp=event["ocel:timestamp"],
                object_refs=object_refs
            )
            observer.on_next(bo_event)

        observer.on_completed()
        return CompositeDisposable()

    return Observable(subscribe)
