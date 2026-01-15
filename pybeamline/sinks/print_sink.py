from typing import TypeVar
from pybeamline.stream.base_sink import BaseSink

T = TypeVar("T")

class print_sink(BaseSink[T]):
	def consume(self, item: T) -> None:
		print(str(item))

	def close(self) -> None:
		pass