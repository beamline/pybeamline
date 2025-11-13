from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from typing_extensions import final

T = TypeVar("T")

class BaseSource(ABC, Generic[T]):

    @abstractmethod
    def read(self):
        pass

    def close(self):

        pass

    @final
    def on_next(self, item: T): pass

    @final
    def on_error(self, e: Exception): pass

    @final
    def on_completed(self): pass


