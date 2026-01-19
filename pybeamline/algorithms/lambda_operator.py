from typing import Callable, Any, Optional, List

from typing_extensions import override

from pybeamline.stream.base_map import BaseMap


def lambda_operator(func: Callable[[Any], Any]) -> BaseMap[Any, Any]:
    return LambdaOperator(func)


class LambdaOperator(BaseMap[Any, Any]):

    def __init__(self, _lambda: Callable[[Any], Any]):
        self._lambda = _lambda

    @override
    def transform(self, value: Any) -> Optional[List[Any]]:
        res = self._lambda(value)
        return [res] if res is not None else None

