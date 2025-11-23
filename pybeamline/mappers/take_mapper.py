from typing import Any, Optional, List

from typing_extensions import override

from pybeamline.stream.base_map import BaseMap

def take(maxx: int) -> BaseMap[Any, Any]:
    return TakeMapper(maxx)

class TakeMapper(BaseMap[Any, Any]):

    def __init__(self, maxx: int) -> None:
        self.max = maxx
        self.count = 0

    @override
    def transform(self, value: Any) -> Optional[List[Any]]:
        if self.count < self.max:
            self.count += 1
            return [value]
        return None