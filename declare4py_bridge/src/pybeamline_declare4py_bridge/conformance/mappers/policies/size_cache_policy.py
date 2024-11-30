from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.base_cache_policy import BaseCachePolicy
from pybeamline.bevent import BEvent


class SizeCachePolicy(BaseCachePolicy):

    def __init__(self, size: int):
        self.size = size

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        return data[-self.size:]