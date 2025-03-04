from reactivex import operators as ops
from typing import Callable

from reactivex import Observable


def lambda_operator(func) -> Callable[[Observable], Observable]:
	return lambda stream: stream.pipe(
		ops.map(func),
		ops.filter(lambda x: x is not None)
	)
