from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional

T = TypeVar("T")

class BaseSink(ABC, Generic[T]):

    @abstractmethod
    def write(self, item: T) -> None:
        pass

    def close(self) -> None:
        return None