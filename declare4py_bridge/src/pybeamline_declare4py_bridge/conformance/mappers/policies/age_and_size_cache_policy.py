from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.age_cache_policy import \
    AgeCachePolicy
from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.base_cache_policy import BaseCachePolicy
from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.size_cache_policy import \
    SizeCachePolicy
from pybeamline.bevent import BEvent


class AgeAndSizePolicy(BaseCachePolicy):

    # max_age is in milliseconds
    def __init__(self, max_age: int, max_size: int):
        self.age_policy = AgeCachePolicy(max_age)
        self.size_policy = SizeCachePolicy(max_size)

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        return self.size_policy.apply(self.age_policy.apply(data))