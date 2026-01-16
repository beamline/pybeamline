from typing import Callable, Any

from typing_extensions import override

from pybeamline.stream.base_operator import BaseOperator, T, K
from pybeamline.stream.stream import Stream
from pybeamline.stream.connectable import Connectable
from reactivex import Observable, operators as ops, from_


class RxOperator(BaseOperator):

    def __init__(self, opt: Callable[[Any], Any]):
        self.opt = opt

    @override
    def apply(self, stream: T) -> K:
        def is_observable(obj):
            return isinstance(obj, Observable)

        return Stream(stream.to_observable().pipe(
            self.opt,
            ops.flat_map(lambda x: x.pipe(ops.to_list()) if is_observable(x) else from_([x]))
        ))