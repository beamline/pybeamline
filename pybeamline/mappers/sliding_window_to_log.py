from typing import List, Callable
from reactivex import Observable, empty, just
from reactivex import operators as ops
from pybeamline.bevent import BEvent
from pandas import DataFrame


def list_to_log(events: List[BEvent]) -> DataFrame:
    list_of_events = []
    for e in events:
        data_attributes = e.event_attributes
        data_attributes.update({"case:" + n: e.trace_attributes[n] for n in e.trace_attributes})
        data_attributes.update({"process:" + n: e.process_attributes[n] for n in e.process_attributes})
        list_of_events.append(data_attributes)
    log = DataFrame(list_of_events)
    return log


def sliding_window_to_log() -> Callable[[Observable[Observable[BEvent]]], Observable[DataFrame]]:
    def o2l(obs: Observable[BEvent]) -> Observable[DataFrame]:
        return obs.pipe(
            ops.to_iterable(),
            ops.flat_map(lambda x: empty() if not x else just(list_to_log(x)))
        )

    return ops.flat_map(o2l)
