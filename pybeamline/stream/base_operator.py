from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar('T')
K = TypeVar('K')

class BaseOperator(Generic[T, K], ABC):

    @abstractmethod
    def apply(self, value: T) -> K:
        pass

    def __call__(self, vale: T) -> K:
        return self.apply(vale)
