from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from typing_extensions import final

T = TypeVar("T")

class BaseSource(ABC, Generic[T]):

    @abstractmethod
    def execute(self):
        pass

    def close(self):
        pass

    @final
    def produce(self, item: T): pass

    @final
    def error(self, e: Exception): pass

    @final
    def completed(self): pass


