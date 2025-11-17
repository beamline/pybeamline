import time
import threading
import unittest
from typing import Optional, Callable, Any, List

from pybeamline.stream.base_filter import BaseFilter
from pybeamline.stream.base_map import BaseMap
from pybeamline.stream.base_sink import BaseSink
from pybeamline.stream.base_source import BaseSource, T
from pybeamline.stream.stream import Stream

class TestStream(unittest.TestCase):

    def setUp(self):
        pass

    def test_empty(self):
        empty = Stream.empty().to_list()
        self.assertEqual(empty, [])

    def test_empty_of(self):
        empty_list = Stream.of().to_list()
        self.assertEqual(empty_list, [])

    def test_non_empty_of(self):
        non_empty_list = Stream.of(1,2,3).to_list()
        self.assertEqual(non_empty_list, [1, 2, 3])

    def test_empty_from_iterable(self):
        empty_iterable= Stream.from_iterable([]).to_list()
        self.assertEqual(empty_iterable, [])

    def test_non_empty_from_iterable(self):
        non_empty_iterable = Stream.from_iterable([0, 1, 2]).to_list()
        self.assertEqual(non_empty_iterable, [0, 1, 2])

    def test_simple_source(self):

        class SimpleTestSource(BaseSource[int]):

            def __init__(self, items: list[Any]) -> None:
                self._items = items

            def execute(self) -> None:
                for item in self._items:
                    self.produce(item)

            def close(self) -> None:
                print('Closing Simple Source')

        stream = Stream.source(SimpleTestSource([1, 2, 3]))
        self.assertEqual(stream.to_list(), [1, 2, 3])


    def test_async_source_to_list(self):

        class AsserSink(BaseSink[int]):

            def __init__(self, expected_items: List[Any], asser_equals: Callable):
                super().__init__()
                self.expected_items = expected_items
                self.assert_equals = asser_equals
                self.counter = 0

            def consume(self, item: T) -> None:
                self.assert_equals(self.expected_items[self.counter], item)
                self.counter += 1

        class AsyncSource(BaseSource[int]):

            def __init__(self, items, delay=0.01):
                self.items = items
                self.delay = delay

            def execute(self) -> None:
                for item in self.items:
                    time.sleep(self.delay)
                    self.produce(item)
                self.completed()

            def close(self):
                print('Closing Async Source')

        async_source = AsyncSource([1, 2, 3, 4, 5], delay=0.1)
        Stream.source(async_source).sink(AsserSink([1, 2, 3, 4, 5], self.assertEqual))

    def test_source_calls_on_completed_explicit(self):

        class ExplicitCompleteSource(BaseSource[int]):
            def __init__(self, items: list[int]):
                self._items = items

            def execute(self) -> None:
                for item in self._items:
                    self.produce(item)
                self.completed()

        collector = []

        class CollectorSink(BaseSink[int]):

            def consume(self, item: int) -> None:
                collector.append(item)

            def close(self) -> None:
                return None

        src = ExplicitCompleteSource([10, 20, 30])
        Stream.source(src).sink(CollectorSink())
        self.assertEqual(collector, [10, 20, 30])

    def test_sink_non_blocking(self):

        class AsyncSource(BaseSource[int]):

            def __init__(self, items, delay=0.01):
                self.items = items
                self.delay = delay

            def execute(self) -> None:
                for item in self.items:
                    time.sleep(self.delay)
                    self.produce(item)
                self.completed()

            def close(self):
                print('Closing Async Source')

        async_source = AsyncSource([1, 2, 3, 4, 5], delay=0.02)

        class CollectorSink(BaseSink[int]):
            def __init__(self):
                self.items: list[int] = []
                self.closed = threading.Event()

            def consume(self, item: int) -> None:
                self.items.append(item)

            def close(self) -> None:
                self.closed.set()

        sink = CollectorSink()
        subscription = Stream.source(async_source).sink(sink, blocking=False)
        self.assertIsNotNone(subscription)
        sink.closed.wait(timeout=1.0)
        self.assertEqual(sink.items, [1, 2, 3, 4, 5])

    def test_single_filter_test(self):

        class EvenFilter(BaseFilter[int]):
            def condition(self, value: int) -> bool:
                return value % 2 == 0

        lst = Stream.of(1,2,3,4,5,6).pipe(EvenFilter()).to_list()
        self.assertEqual(lst, [2,4,6])

    def test_filter_pipe(self):

        class EvenFilter(BaseFilter[int]):
            def condition(self, value: int) -> bool:
                return value % 2 == 0

        class StrictFilter(BaseFilter[int]):

            def __init__(self, items):
                self.items = items

            def condition(self, value: int) -> bool:
                return value in self.items

        st = Stream.of(1, 2, 3, 4, 5, 6).pipe(EvenFilter(), StrictFilter([1,2,6])).to_list()
        self.assertEqual(st, [2, 6])

    def test_one_to_one_map_test(self):

        class ToStringMap(BaseMap[int, str]):
            def transform(self, value: int) -> Optional[List[str]]:
                return [str(value)]

        lst = Stream.of(1,2,3).pipe(ToStringMap()).to_list()
        self.assertEqual(lst, ['1', '2', '3'])

    def test_one_to_many_map(self):

        class DuplicateMap(BaseMap[int, int]):
            def transform(self, value: int) -> Optional[List[int]]:
                return [value, value]

        lst = Stream.of(1, 2, 3).pipe(DuplicateMap()).to_list()
        self.assertEqual(lst, [1,1,2,2,3,3])

    def test_many_to_one_map(self):

        class SumMap(BaseMap[int, int]):

            def __init__(self, win_size: int):
                self.window_size = win_size
                self.window = []

            def transform(self, value: int) -> Optional[List[int]]:
                self.window.append(value)
                self.window = self.window[-self.window_size:]
                if len(self.window) == self.window_size:
                    return [sum(self.window)]
                return None

        lst = Stream.of(1,2,3,4,5).pipe(SumMap(3)).to_list()
        self.assertEqual(lst, [6, 9, 12])

    def test_many_to_many_map(self):

        class PowerMap(BaseMap[int, List[int]]):

            def __init__(self, win_size: int):
                self.window_size = win_size
                self.window: List[int] = []

            def transform(self, value: int) -> Optional[List[List[int]]]:
                self.window.append(value)
                self.window = self.window[-self.window_size:]
                if len(self.window) == self.window_size:
                    results: List[List[int]] = []
                    n = len(self.window)
                    for mask in range(1, 1 << n):
                        subset: List[int] = []
                        for i in range(n):
                            if mask & (1 << i):
                                subset.append(self.window[i])
                        results.append(subset)
                    return results
                return None

        lst = Stream.of(1, 2, 3).pipe(PowerMap(2)).to_list()
        self.assertEqual(lst, [[1], [2], [1, 2], [2], [3], [2, 3]])

    def test_operation_chain(self):

        class SlidingWindowMap(BaseMap[int, List[int]]):

            def __init__(self, win_size: int):
                self.window_size = win_size
                self.window: List[int] = []

            def transform(self, value: int) -> Optional[List[List[int]]]:
                self.window.append(value)
                self.window = self.window[-self.window_size:]
                if len(self.window) == self.window_size:
                    return [self.window.copy()]
                return None

        class SumMap(BaseMap[List[int], int]):

            def transform(self, value: List[int]) -> Optional[List[int]]:
                return [sum(value)]

        class AssertMap(BaseMap[Any, Any]):

            def __init__(self, expected_items: List[Any], asser_equals: Callable):
                super().__init__()
                self.expected_items = expected_items
                self.assert_equals = asser_equals
                self.counter = 0

            def transform(self, value: Any) -> Optional[List[Any]]:
                self.assert_equals(self.expected_items[self.counter], value)
                self.counter += 1
                return [value]

        class EvenFilter(BaseFilter[int]):

            def condition(self, value: T) -> bool:
                return value % 2 == 0

        lst = Stream.of(1, 2, 3, 4, 5).pipe(
            SlidingWindowMap(3),
            AssertMap([[1,2,3], [2, 3, 4], [3, 4, 5]], self.assertEqual),
            SumMap(),
            AssertMap([6, 9, 12], self.assertEqual),
            EvenFilter()
        ).to_list()

        self.assertEqual(lst, [6, 12])

    def test_concat(self):

        stream1 = Stream.of(1,2,3)
        stream2 = Stream.of(4,5, 6)

        lst = stream1.concat(stream2).to_list()
        self.assertEqual(lst, [1, 2, 3, 4, 5, 6])

    def test_merge(self):

        class AsyncSource(BaseSource[int]):

            def __init__(self, items: list[int], delay=0.01):
                self.items = items
                self.delay = delay

            def execute(self) -> None:
                for item in self.items:
                    time.sleep(self.delay)
                    self.produce(item)
                self.completed()

            def close(self):
                print('Closing Async Source')


        class CollectorSink(BaseSink[int]):
            def __init__(self):
                self.items: list[int] = []

            def consume(self, item: int) -> None:
                self.items.append(item)

            def close(self) -> None:
                print('Closing Collector Sink')

        sink = CollectorSink()
        stream1 = Stream.source(AsyncSource([1, 1, 1, 1]))
        stream2 = Stream.source(AsyncSource([2, 2, 2, 2]))
        stream1.merge(stream2).sink(sink)
        self.assertTrue(sink.items == [1, 2, 1, 2, 1, 2, 1, 2] or sink.items == [2, 1, 2, 1, 2, 1, 2, 1])






