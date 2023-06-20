from reactivex import just, empty
from reactivex import operators as ops
from reactivex import Observable
from pybeamline.bevent import BEvent
from typing import Callable, Tuple


def infinite_size_directly_follows_mapper() -> Callable[[Observable[BEvent]], Observable[Tuple[str, str]]]:
    map_cases = {}

    def extractor(event: BEvent) -> Observable[Tuple[str, str]]:
        to_ret = None
        if event.get_trace_name() in map_cases.keys():
            to_ret = (map_cases[event.get_trace_name()], event.get_event_name())
        map_cases[event.get_trace_name()] = event.get_event_name()
        if to_ret is None:
            return empty()
        else:
            return just(to_ret)
    return ops.flat_map(extractor)
