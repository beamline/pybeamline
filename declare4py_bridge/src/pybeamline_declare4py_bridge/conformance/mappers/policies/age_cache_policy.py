from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.base_cache_policy import BaseCachePolicy
from pybeamline.bevent import BEvent

class AgeCachePolicy(BaseCachePolicy):

    # max_age is in milliseconds
    def __init__(self, max_age : int):
        self.max_age = max_age

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        if len(data) > 0:
            current = data[0].get_event_time()
            return [e for e in data if (current - e.get_event_time()).total_seconds() * 1000 < self.max_age]
