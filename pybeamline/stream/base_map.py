from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional
from typing_extensions import final

from reactivex import create

from pybeamline.stream.base_operator import BaseOperator
from pybeamline.stream.stream import Stream

T = TypeVar('T')
R = TypeVar('R')


class BaseMap(Generic[T, R], BaseOperator[Stream[T], Stream[R]], ABC):

    @abstractmethod
    def transform(self, value: T) -> Optional[List[R]]:
        pass

    @final
    def apply(self, stream: Stream[T]) -> Stream[R]:
        def on_subscribe(observer, scheduler):
            def on_next(item):
                results = self.transform(item)
                if results is not None:
                    for r in results:
                        observer.on_next(r)
            def on_error(e):
                observer.on_error(e)
            def on_completed():
                observer.on_completed()
            stream.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed, blocking=False)
        return Stream(create(on_subscribe))
