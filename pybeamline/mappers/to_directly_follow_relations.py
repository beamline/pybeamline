from typing import List, Optional, Tuple

from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap


def to_directly_follow_relations():
    return ToDirectlyFollowRelations()


class ToDirectlyFollowRelations(BaseMap[BEvent, Tuple[str, str]]):

    def __init__(self):
        self.map_cases = dict()

    def transform(self, value: BEvent) -> Optional[List[Tuple[str, str]]]:
        ret = None
        if value.get_trace_name() in self.map_cases.keys():
            ret = (self.map_cases[value.get_trace_name()], value.get_event_name())
        self.map_cases[value.get_trace_name()] = value.get_event_name()
        return [ret] if ret is not None else None
