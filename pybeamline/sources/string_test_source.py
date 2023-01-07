from pybeamline.bevent import BEvent
from typing import Iterable, Optional
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable


def string_test_source(iterable: Iterable[str], scheduler: Optional[abc.SchedulerBase] = None) -> Observable[BEvent]:
    def subscribe(observer: abc.ObserverBase[BEvent], scheduler_: Optional[abc.SchedulerBase] = None) -> abc.DisposableBase:
        trace_id = 1
        for trace in iterable:
            for event in trace:
                observer.on_next(
                    BEvent(event, "case_" + str(trace_id), "Process"))
            trace_id += 1
        observer.on_completed()
        return CompositeDisposable()
    return Observable(subscribe)
