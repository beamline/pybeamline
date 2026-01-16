from typing import TypeVar, Callable, Any
from typing_extensions import override

from pybeamline.stream.base_sink import BaseSink


def print_sink(format_method: Callable = None) -> BaseSink[Any]:
	return PrintSink(format_method=format_method)


class PrintSink(BaseSink[Any]):

	def __init__(self, format_method: Callable = None):
		self.format_method = format_method

	@override
	def consume(self, item: Any) -> None:
		if self.format_method is not None:
			print(self.format_method(item))
		else:
			print(str(item))
