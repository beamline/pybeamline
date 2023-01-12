from pybeamline.bevent import BEvent
from typing import Optional, Union
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable
from pm4py.objects.log.obj import EventLog
from pm4py.util import xes_constants as xes_util
from pm4py import read_xes, convert_to_dataframe
import pandas as pd


def xes_log_source_from_file(log: str) -> Observable[BEvent]:
    return xes_log_source(read_xes(log))


def xes_log_source(
        log: Union[EventLog, pd.DataFrame], scheduler: Optional[abc.SchedulerBase] = None
) -> Observable[BEvent]:
    if type(log) is not pd.DataFrame:
        log = convert_to_dataframe(log)
    if xes_util.DEFAULT_TIMESTAMP_KEY in log.columns:
        log = log.sort_values(by=[xes_util.DEFAULT_TIMESTAMP_KEY])

    def subscribe(
            observer: abc.ObserverBase[BEvent], scheduler_: Optional[abc.SchedulerBase] = None
    ) -> abc.DisposableBase:

        for index, event in log.iterrows():
            e = BEvent(
                event[xes_util.DEFAULT_NAME_KEY],
                event["case:" + xes_util.DEFAULT_TRACEID_KEY],
                "log-file",
                event[xes_util.DEFAULT_TIMESTAMP_KEY])
            for col in log.columns:
                if col not in [xes_util.DEFAULT_NAME_KEY, "case:" + xes_util.DEFAULT_NAME_KEY, xes_util.DEFAULT_TIMESTAMP_KEY]:
                    if event[col] == event[col]: # verify for nan
                        if col.startswith("case:"):
                            e.trace_attributes[col[5:]] = event[col]
                        else:
                            e.event_attributes[col] = event[col]
            observer.on_next(e)
        observer.on_completed()
        return CompositeDisposable()

    return Observable(subscribe)
