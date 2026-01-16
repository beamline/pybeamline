from pybeamline.stream.base_map import BaseMap

from typing import Any, List, Optional, override


def sliding_window(window_size: int, skip: Optional[int] = None) -> BaseMap[Any, List[Any]]:
    return SlidingWindow(window_size, skip)

def tumbling_window(window_size: int) -> BaseMap[Any, List[Any]]:
    return SlidingWindow(window_size, window_size)


class SlidingWindow(BaseMap[Any, List[Any]]):
    def __init__(self, window_size: int, skip: Optional[int] = None) -> None:
        self.window_size = window_size
        self.skip = skip if skip is not None else 1
        self.data: List[Any] = []
        self.counter = 0

    @override
    def transform(self, value: Any) -> Optional[List[List[Any]]]:
        self.counter += 1
        results = []

        if self.counter <= self.window_size:
            self.data.append(value)

        if len(self.data) == self.window_size:
            results.append(list(self.data))
            self.data = self.data[self.skip:] if self.skip < self.window_size else []

        if self.counter == self.skip:
            self.counter = 0

        return results.copy() if results else None


from typing import Any, List, Optional, override





