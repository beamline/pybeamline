import threading

from reactivex import operators as ops, Observable, from_iterable, create, concat, empty, from_, merge
from typing import Callable, Any, List, Optional, Generic, TypeVar, Iterable

from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable

from pybeamline.stream.base_operator import BaseOperator
from pybeamline.stream.base_sink import BaseSink
from pybeamline.stream.base_source import BaseSource
from pybeamline.stream.connectable import Connectable

T = TypeVar('T')
R = TypeVar('R')


class Stream(Generic[T]):

    def __init__(self, observable: Observable[T], value_type: type = None):
        self._observable: Observable[T] = observable
        self._value_type = value_type

    @staticmethod
    def of(*args: T) -> 'Stream[T]':
        return Stream(from_iterable(args), value_type=type(args[0]) if args else None)

    @staticmethod
    def from_iterable(iterable: Iterable[T]) -> 'Stream[T]':
        return Stream(from_(iterable))

    @staticmethod
    def empty():
        return Stream(empty())

    @staticmethod
    def source(base_source: BaseSource[T]) -> 'Stream[T]':

        def on_subscribe(observer, _):

            completed_event = threading.Event()

            def _on_completed():
                completed_event.set()
                observer.on_completed()

            base_source.produce = lambda item: observer.on_next(item)
            base_source.completed = _on_completed
            base_source.error = lambda e: observer.on_error(e)

            def _execute():
                try:
                    base_source.execute()
                finally:
                    if not completed_event.is_set():
                        observer.on_completed()

            thread = threading.Thread(target=_execute, daemon=True)
            thread.start()

            def dispose():
                if hasattr(base_source, "close"):
                    try:
                        base_source.close()
                    except Exception("Closing failed"):
                        pass

            return Disposable(dispose)

        return Stream(create(on_subscribe))


    def sink(self, base_sink: BaseSink[T], blocking: bool = True) -> DisposableBase:
        completed_event = threading.Event()

        def on_next(item):
            base_sink.consume(item)

        def on_completed():
            base_sink.close()
            completed_event.set()

        def on_error(e: Exception):
            try:
                base_sink.close()
            finally:
                completed_event.set()
            raise e

        subscription = self._observable.subscribe(
            on_next=on_next,
            on_error=on_error,
            on_completed=on_completed
        )

        if blocking:
            completed_event.wait()

        return subscription

    def map(self, func: Callable[[T], R]) -> 'Stream[R]':
        return Stream(self._observable.pipe(ops.map(func)))

    def filter(self, func: Callable[[T], bool]) -> 'Stream[T]':
        return Stream(self._observable.pipe(ops.filter(func)))

    def flat_map(self, func: Callable[[T], Observable[R]]) -> 'Stream[R]':
        return Stream(self._observable.pipe(ops.flat_map(func)))

    def all_match(self, predicate: Callable[[T], bool]) -> bool:
        return self._observable.pipe(ops.all(predicate)).run()

    def find_first(self) -> Optional[T]:
        return self._observable.pipe(
            ops.first_or_default(default_value=None)
        ).run()

    def find_any(self) -> Optional[T]:
        return self.find_first()

    def to_list(self) -> List[T]:
        result: List[T] = []
        self._observable.subscribe(result.append)
        return result

    def subscribe(
            self,
            base_sink: BaseSink[T] = None,
            blocking: bool = True,
            on_next: Optional[Callable[[T], None]] = None,
            on_error: Optional[Callable[[Exception], None]] = None,
            on_completed: Optional[Callable[[], None]] = None,
            scheduler=None) -> DisposableBase:

        # If a base_sink is provided, use the existing sink logic
        if base_sink is not None:
            return self.sink(base_sink, blocking)

        # Otherwise, handle the functional subscription
        completed_event = threading.Event()

        def _on_completed():
            if on_completed:
                on_completed()
            completed_event.set()

        def _on_error(e):
            if on_error:
                on_error(e)
            completed_event.set()

        subscription = self._subscribe(
            on_next=on_next,
            on_error=_on_error,
            on_completed=_on_completed,
            scheduler=scheduler
        )

        if blocking:
            completed_event.wait()

        return subscription

    def _subscribe(
            self,
            on_next: Optional[Callable[[T], None]] = None,
            on_error: Optional[Callable[[Exception], None]] = None,
            on_completed: Optional[Callable[[], None]] = None,
            scheduler=None
    ) -> DisposableBase:
        # Note: Added return to ensure the subscription object is passed back
        return self._observable.subscribe(
            on_next=on_next,
            on_error=on_error,
            on_completed=on_completed,
            scheduler=scheduler
        )

    def to_observable(self) -> Observable[T]:
        return self._observable

    def pipe(self, *operators: BaseOperator['Stream[Any]', 'Stream[Any]']) -> 'Stream[Any]':
        stream: Stream[Any] = self
        for op in operators:
            stream = op(stream)
        return stream

    def merge(self, *others: 'Stream[T]') -> 'Stream[T]':
        observables = [self._observable] + [o._observable for o in others]
        concatenated = merge(*observables)
        return Stream(concatenated, value_type=self._value_type)

    def concat(self, *others: 'Stream[T]') -> 'Stream[T]':
        observables = [self._observable] + [o._observable for o in others]
        concatenated = concat(*observables)
        return Stream(concatenated, value_type=self._value_type)

    def share(self) -> 'Stream[T]':
        return Stream(self._observable.pipe(ops.share()), value_type=self._value_type)

    def publish(self) -> tuple['Stream[T]', Connectable]:
        connectable = self._observable.pipe(ops.publish())
        return Stream(connectable, value_type=self._value_type), Connectable(connectable)