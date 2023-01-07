from typing import Callable, Iterable
from pybeamline.bevent import BEvent
from reactivex import Observable
from reactivex import operators as ops


def retains_activity_filter(activityNames: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.getEventName() in activityNames)


def excludes_activity_filter(activityNames: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.getEventName() not in activityNames)


def retains_on_event_attribute_equal_filter(attributeName: str, attributeValues: Iterable) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: attributeName in x.eventAttributes.keys() and x.eventAttributes[attributeName] in attributeValues)


def excludes_on_event_attribute_equal_filter(attributeName: str, attributeValues: Iterable) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: attributeName not in x.eventAttributes.keys() or x.eventAttributes[attributeName] not in attributeValues)


def retains_on_trace_attribute_equal_filter(attributeName: str, attributeValues: Iterable) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: attributeName in x.traceAttributes.keys() and x.traceAttributes[attributeName] in attributeValues)


def excludes_on_trace_attribute_equal_filter(attributeName: str, attributeValues: Iterable) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: attributeName not in x.traceAttributes.keys() or x.traceAttributes[attributeName] not in attributeValues)
