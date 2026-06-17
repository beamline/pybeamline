import time
from typing import Any, Optional, List
from typing_extensions import override
from time import sleep

from pybeamline.stream.base_map import BaseMap

def sleep_operator(secs: int) -> BaseMap[Any, Any]:
    return SleepOperator(secs=secs)


class SleepOperator(BaseMap[Any, Any]):

    def __init__(self, secs: int = 1):
        self.secs = secs


    @override
    def transform(self, value: Any) -> Optional[List[Any]]:
        time.sleep(self.secs)
        return [value]