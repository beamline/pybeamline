from typing import List, Callable
from reactivex import Observable, empty, just
from reactivex import operators as ops

from pybeamline.abstractevent import AbstractEvent
from pybeamline.bevent import BEvent
from pandas import DataFrame

def list_to_log(events: List[AbstractEvent]) -> DataFrame:
    return DataFrame([e.to_dict() for e in events])


def sliding_window_to_log() -> Callable[[Observable[Observable[AbstractEvent]]], Observable[DataFrame]]:
    def o2l(obs: Observable[AbstractEvent]) -> Observable[DataFrame]:
        return obs.pipe(
            ops.to_iterable(),  # Converts Observable[AbstractEvent] → Iterable[AbstractEvent]
            ops.flat_map(lambda x: empty() if not x else just(list_to_log(x)))
        )

    return ops.flat_map(o2l)