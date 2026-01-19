from abc import abstractmethod
from typing import TypeVar, Generic

from typing_extensions import override

from pybeamline.stream.base_operator import BaseOperator
from pybeamline.stream.stream import Stream

T = TypeVar('T')

class BaseFilter(BaseOperator[Stream[T], Stream[T]], Generic[T]):

    @abstractmethod
    def condition(self, value: T) -> bool:
        pass

    @override
    def apply(self, stream: Stream[T]) -> Stream[T]:
        return stream.filter(self.condition)
