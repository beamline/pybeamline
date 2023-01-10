from typing import List, Callable
from reactivex import Observable
from reactivex import operators as ops
from pybeamline.bevent import BEvent
from pandas import DataFrame


def list_to_log(events: List[BEvent]) -> DataFrame:
    list_of_events = []
    for e in events:
        data_attributes = e.eventAttributes
        data_attributes.update({"case:" + n: e.traceAttributes[n] for n in e.traceAttributes})
        data_attributes.update({"process:" + n: e.processAttributes[n] for n in e.processAttributes})
        list_of_events.append(data_attributes)
    log = DataFrame(list_of_events)
    return log


def sliding_window_to_log() -> Callable[[Observable[Observable[BEvent]]], Observable[DataFrame]]:
    def o2l(obs: Observable[BEvent]) -> Observable[DataFrame]:
        return obs.pipe(
            ops.to_iterable(),
            ops.map(lambda x: list_to_log(x)))

    return ops.flat_map(o2l)
