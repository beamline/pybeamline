from abc import ABC, abstractmethod

from pybeamline.bevent import BEvent


class BaseCachePolicy(ABC):

    @abstractmethod
    def apply(self, data: list[BEvent]) -> list[BEvent]:
        pass