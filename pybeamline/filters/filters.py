from typing import Callable, Iterable
from pybeamline.bevent import BEvent
from reactivex import Observable
from reactivex import operators as ops


def retains_activity_filter(activity_names: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.get_event_name() in activity_names)


def excludes_activity_filter(activity_names: Iterable[str]) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(lambda x: x.get_event_name() not in activity_names)


def retains_on_event_attribute_equal_filter(
        attribute_name: str, attribute_values: Iterable
) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(
        lambda x: attribute_name in x.event_attributes.keys()
                  and x.event_attributes[attribute_name] in attribute_values)


def excludes_on_event_attribute_equal_filter(
        attribute_name: str, attribute_values: Iterable
) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(
        lambda x: attribute_name not in x.event_attributes.keys()
                  or x.event_attributes[attribute_name] not in attribute_values)


def retains_on_trace_attribute_equal_filter(
        attribute_name: str, attribute_values: Iterable
) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(
        lambda x: attribute_name in x.trace_attributes.keys()
                  and x.trace_attributes[attribute_name] in attribute_values
    )


def excludes_on_trace_attribute_equal_filter(
        attribute_name: str, attribute_values: Iterable
) -> Callable[[Observable[BEvent]], Observable[BEvent]]:
    return ops.filter(
        lambda x: attribute_name not in x.trace_attributes.keys()
                  or x.trace_attributes[attribute_name] not in attribute_values
    )
