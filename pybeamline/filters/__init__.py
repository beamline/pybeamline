from typing import Callable, Iterable
from pybeamline.bevent import BEvent
from reactivex import Observable
from reactivex import operators as ops


def retains_activity_filter(activityNames: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.getEventName() in activityNames)


def excludes_activity_filter(activityNames: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.getEventName() not in activityNames)
