from reactivex import just, empty
from reactivex import operators as ops
from reactivex import Observable
from pybeamline.bevent import BEvent
from typing import Callable


def infinite_size_directly_follows_mapper() -> Callable[[Observable[BEvent]], Observable[tuple[str, str]]]:
    map = {}

    def extractor(event: BEvent) -> Observable[tuple[str, str]]:
        toRet = None
        if event.getTraceName() in map.keys():
            toRet = (map[event.getTraceName()], event.getEventName())
        map[event.getTraceName()] = event.getEventName()
        if toRet == None:
            return empty()
        else:
            return just(toRet)
    return ops.flat_map(extractor)
