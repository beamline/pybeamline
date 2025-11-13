from typing import Callable, Any

from typing_extensions import override

from pybeamline.stream.base_operator import BaseOperator, T, K
from pybeamline.stream.stream import Stream


class RxOperator(BaseOperator):

    def __init__(self, opt: Callable[[Any], Any]):
        self.opt = opt

    @override
    def apply(self, stream: T) -> K:
        return Stream(stream.to_observable().pipe(self.opt))