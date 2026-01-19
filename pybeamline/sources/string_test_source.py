from pybeamline.bevent import BEvent
from typing import Iterable

from pybeamline.stream.base_source import BaseSource
from pybeamline.stream.stream import Stream

def string_test_source(iterable: Iterable[str]) -> Stream[BEvent]:
    return Stream.source(StringTestSource(iterable))


class StringTestSource(BaseSource[BEvent]):

    def __init__(self, iterable: Iterable[str]):
        self._iterable = iterable

    def execute(self) -> None:
        trace_id = 1
        for trace in self._iterable:
            for event in trace:
                self.produce(BEvent(event, "case_" + str(trace_id), "Process"))
            trace_id += 1
        self.completed()