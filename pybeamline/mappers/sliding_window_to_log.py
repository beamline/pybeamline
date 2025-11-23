from typing import List, Optional
from typing_extensions import override
from pybeamline.abstractevent import AbstractEvent
from pandas import DataFrame
from pybeamline.stream.base_map import BaseMap



def sliding_window_to_log() -> BaseMap[List[AbstractEvent], DataFrame]:
    return SlidingWindowToLog()


class SlidingWindowToLog(BaseMap[List[AbstractEvent], DataFrame]):

    @override
    def transform(self, value: List[AbstractEvent]) -> Optional[List[DataFrame]]:
        return [self._list_to_log(value)]

    def _list_to_log(self, events: List[AbstractEvent]) -> DataFrame:
        return DataFrame([e.to_dict() for e in events])