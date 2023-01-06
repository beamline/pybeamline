from pybeamline.bevent import BEvent
from typing import Optional, Union
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable
from pm4py.objects.log.obj import EventLog
import pandas as pd
from pm4py.util import xes_constants as xes_util
import pm4py


def xes_log_source(log: Union[EventLog, pd.DataFrame], scheduler: Optional[abc.SchedulerBase] = None) -> Observable[BEvent]:

    if type(log) is pd.DataFrame:
        log = pm4py.convert_to_dataframe(log)
    log = log.sort_values(by=[xes_util.DEFAULT_TIMESTAMP_KEY])

    def subscribe(observer: abc.ObserverBase[BEvent], scheduler_: Optional[abc.SchedulerBase] = None) -> abc.DisposableBase:

        for index, event in log.iterrows():
            e = BEvent(
                event[xes_util.DEFAULT_NAME_KEY],
                event["case:" + xes_util.DEFAULT_TRACEID_KEY],
                "log-file",
                event[xes_util.DEFAULT_TIMESTAMP_KEY])
            observer.on_next(e)
        observer.on_completed()
        return CompositeDisposable()
    return Observable(subscribe)
