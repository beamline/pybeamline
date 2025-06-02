import enum
import math
from typing import Callable, Union, Dict, List, Tuple, Optional, Set
from reactivex import Observable, from_iterable
from reactivex import operators as ops
from pybeamline.boevent import BOEvent

class Command(enum.Enum):
    DEREGISTER = "deregister"
    REGISTER = "register"


def object_lossy_counting_operator(object_max_approx_error: float = 0.0001, control_flow_object_types: Optional[Dict] = None, alive_streams: Optional[Set[str]] = None) -> Callable[[Observable[BOEvent]], Observable[Union[BOEvent, dict]]]:
    lossy_counter = ObjectLossyCountingOperator(object_max_approx_error, control_flow_object_types=control_flow_object_types, alive_streams=alive_streams)

    def _operator(source: Observable[BOEvent]) -> Observable[Union[BOEvent, dict]]:
        def process_event(event: BOEvent) -> List[Union[BOEvent, dict]]:
            output: List[Union[BOEvent, dict]] = [event]  # always pass through
            deregistered = lossy_counter.ingest_event(event)
            for obj_type in deregistered:
                output.append({
                     "type": "command",
                     "command": Command.DEREGISTER,
                     "object_type": obj_type
                })
            return output

        return source.pipe(
            ops.flat_map(lambda event: from_iterable(process_event(event))),
        )

    return _operator


class ObjectLossyCountingOperator:
    def __init__(self, object_max_approx_error: float = 0.0001, control_flow_object_types: Optional[Dict] = None, alive_streams: Optional[Set[str]] = None):
        self.__control_flow_object_types = control_flow_object_types
        self.__alive_streams = alive_streams
        self.__bucket_width = int(math.ceil(1 / object_max_approx_error))
        self.__observed_events = 0
        self.__obj_tracking: Dict[str, Tuple[int, int]] = {}  # {obj_type: (freq, last_bucket)}

    def ingest_event(self, event: BOEvent) -> List[str]:

        current_bucket = int(math.ceil(self.__observed_events / self.__bucket_width))

        to_deregister: List[str] = []
        for flat_event in event.flatten():
            obj_type = flat_event.get_omap_types()[0]

            if bool(self.__control_flow_object_types.keys()):
                if obj_type not in self.__control_flow_object_types:
                    continue


            freq, last_bucket = self.__obj_tracking.get(obj_type, (0, current_bucket))
            self.__obj_tracking[obj_type] = (freq + 1, current_bucket)

        if self.__observed_events % self.__bucket_width == 0:
            for obj_type, (freq, last_bucket) in list(self.__obj_tracking.items()):
                if freq + last_bucket <= current_bucket:
                    to_deregister.append(obj_type)
                    del self.__obj_tracking[obj_type]

        self.__observed_events += 1

        return to_deregister