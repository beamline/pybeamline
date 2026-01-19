from abc import abstractmethod, ABC
from typing import Iterable
from typing_extensions import override
from pybeamline.bevent import BEvent
from pybeamline.stream.base_filter import BaseFilter


class AbstractEventFilter(BaseFilter[BEvent], ABC):

    def __init__(self, attribute_name: str, attribute_values: Iterable):
        self.attribute_name = attribute_name
        self.attribute_values = attribute_values

    @abstractmethod
    def condition(self, value: BEvent) -> bool:
        pass


class RetainsActivityFilter(BaseFilter[BEvent]):

    def __init__(self, activity_names: Iterable[str]):
        self.activity_names = activity_names

    @override
    def condition(self, value: BEvent) -> bool:
        return value.get_event_name() in self.activity_names


class ExcludesActivityFilter(BaseFilter[BEvent]):

    def __init__(self, activity_names: Iterable[str]):
        self.activity_names = activity_names

    @override
    def condition(self, value: BEvent) -> bool:
        return value.get_event_name() not in self.activity_names


class RetainsOnEventAttributeEqualFilter(AbstractEventFilter):

    @override
    def condition(self, value: BEvent) -> bool:
        return (self.attribute_name in value.event_attributes.keys()
                and value.event_attributes[self.attribute_name] in self.attribute_values)


class ExcludesOnEventAttributeEqualFilter(AbstractEventFilter):

    @override
    def condition(self, value: BEvent) -> bool:
        return (self.attribute_name not in value.event_attributes.keys()
                or value.event_attributes[self.attribute_name] not in self.attribute_values)


class RetainsOnTraceAttributeEqualFilter(AbstractEventFilter):

    @override
    def condition(self, value: BEvent) -> bool:
        return (self.attribute_name in value.trace_attributes.keys()
                and value.trace_attributes[self.attribute_name] in self.attribute_values)


class ExcludesOnTraceAttributeEqualFilter(AbstractEventFilter):

    @override
    def condition(self, value: BEvent) -> bool:
        return (self.attribute_name not in value.trace_attributes.keys()
                or value.trace_attributes[self.attribute_name] not in self.attribute_values)


def retains_on_trace_attribute_equal_filter(attribute_name: str, attribute_values: Iterable) -> BaseFilter[BEvent]:
    return RetainsOnTraceAttributeEqualFilter(attribute_name, attribute_values)

def excludes_on_trace_attribute_equal_filter(attribute_name: str, attribute_values: Iterable) -> BaseFilter[BEvent]:
    return ExcludesOnTraceAttributeEqualFilter(attribute_name, attribute_values)

def excludes_on_event_attribute_equal_filter(attribute_name: str, attribute_values: Iterable) ->BaseFilter[BEvent]:
    return ExcludesOnEventAttributeEqualFilter(attribute_name, attribute_values)

def retains_on_event_attribute_equal_filter(attribute_name: str, attribute_values: Iterable) -> BaseFilter[BEvent]:
    return RetainsOnEventAttributeEqualFilter(attribute_name, attribute_values)

def excludes_activity_filter(activity_names: Iterable[str]) -> BaseFilter[BEvent]:
    return ExcludesActivityFilter(activity_names)

def retains_activity_filter(activity_names: Iterable[str]) -> BaseFilter[BEvent]:
    return RetainsActivityFilter(activity_names)
