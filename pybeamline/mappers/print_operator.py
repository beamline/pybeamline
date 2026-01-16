from typing import Any, Optional, List, Callable
from typing_extensions import override

from pybeamline.stream.base_map import BaseMap

def print_operator(format_string: str = None) -> BaseMap[Any, Any]:
    return PrintOperator(format_string=format_string)


class PrintOperator(BaseMap[Any, Any]):

    def __init__(self, format_string: str = None):
        self.format = format_string


    @override
    def transform(self, value: Any) -> Optional[List[Any]]:
        if self.format is not None:
            print(self.format.format(value))
        else:
            print(value)
        return [value]