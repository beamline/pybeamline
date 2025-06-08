import enum
import math
from asyncio import Protocol
from collections import deque
from typing import Any, Dict, Set, Tuple
from reactivex import operators as ops
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import Observable, from_iterable, just
from pybeamline.boevent import BOEvent
from pybeamline.utils.commands import Command, create_command


class EmissionStrategy(Protocol):
    def evaluate(self, model_event: dict) -> Observable[dict]:
        ... # pragma: no cover

class RelativeFrequencyBasedStrategy(EmissionStrategy):
    """
    Relative Frequency-Based Emission Strategy.
    - Registers object types if their relative frequency exceeds a threshold.
    - Deregisters object types if their relative frequency falls below the threshold.
    Utilising global frequency counts to determine when to register or deregister object types.
    """
    def __init__(self, frequency_threshold: float = 0.05):
        self.__threshold = int(math.ceil(1 / frequency_threshold))
        self.__emitted_models: Dict[str, int] = {}
        self.__observed_emitted_models = 0
        self.__registered: Set[str] = set()

    def evaluate(self, model_event: dict) -> Observable[dict]:
        if model_event.get("type") != "model":
            return just(model_event)

        obj_type = model_event["object_type"]
        self.__emitted_models[obj_type] = self.__emitted_models.get(obj_type, 0) + 1
        self.__observed_emitted_models += 1

        count = self.__emitted_models[obj_type]
        total = self.__observed_emitted_models

        commands = []
        if self.__threshold * count >= total and obj_type not in self.__registered:
            self.__registered.add(obj_type)
            commands.append(create_command(Command.REGISTER, obj_type))

        for ot in list(self.__registered):
            if self.__threshold * self.__emitted_models.get(ot, 0) < total:
                self.__registered.remove(ot)
                commands.append(create_command(Command.DEREGISTER, ot))

        commands.append(model_event)
        return from_iterable(commands)

class LossyCountingStrategy(EmissionStrategy):
    """
    Lossy Counting Emission Strategy.
    Tracks the frequency of emitted object types using the Lossy Counting algorithm,
    which approximates frequency counts within a user-defined error bound.

    - Registers an object type when it is first observed.
    - Deregisters an object type if its estimated frequency becomes negligible.

    The strategy uses fixed-size buckets, and objects are periodically pruned
    if their estimated total count (frequency + error) falls below the current bucket index.

    :param max_approx_error: The maximum allowed error (ε) in frequency approximation.
                             Determines the bucket width as ceil(1 / ε).
    """
    def __init__(self, max_approx_error: float):
        self.__max_approx_error = max_approx_error
        self.__bucket_width = int(math.ceil(1 / self.__max_approx_error))
        self.__bucket = 1
        self.__count: Dict[str, Tuple[int, int]] = {}  # (freq, delta)
        self.__registered: Set[str] = set()
        self.__observed_emitted_models = 0

    def evaluate(self, model_event: dict) -> Observable[dict]:
        if model_event.get("type") != "model":
            return just(model_event)

        self.__observed_emitted_models += 1
        obj_type = model_event["object_type"]
        is_new = obj_type not in self.__count

        # Update or initialize frequency
        if is_new:
            self.__count[obj_type] = (1, self.__bucket - 1)
        else:
            freq, delta = self.__count[obj_type]
            self.__count[obj_type] = (freq + 1, delta)

        commands = []
        if is_new and obj_type not in self.__registered:
            self.__registered.add(obj_type)
            commands.append(create_command(Command.REGISTER, obj_type))

        if self.__observed_emitted_models % self.__bucket_width == 0:
            self.__bucket += 1
            for ot in list(self.__count):
                freq, delta = self.__count[ot]
                if freq + delta <= self.__bucket:
                    del self.__count[ot]
                    if ot in self.__registered:
                        self.__registered.remove(ot)
                        commands.append(create_command(Command.DEREGISTER, ot))

        commands.append(model_event)
        return from_iterable(commands)

class SlidingWindowActivityStrategy(EmissionStrategy):
    """
    Sliding Window Activity-Based Emission Strategy.
    - Registers object types if they emit within the current window.
    - Deregisters object types if they have not emitted within the last `window_size` events.
    This is inspired by the sliding window model in stream mining.
    """
    def __init__(self, window_size: int = 30):
        self.window_size = window_size
        self.event_index = 0
        self.last_seen: Dict[str, int] = {}
        self.active_object_types: Set[str] = set()

    def evaluate(self, model_event: dict) -> Observable[dict]:
        if model_event.get("type") != "model":
            return just(model_event)

        self.event_index += 1
        current_index = self.event_index
        obj_type = model_event["object_type"]

        commands = []

        # Update last seen
        self.last_seen[obj_type] = current_index

        # Register if newly active
        if obj_type not in self.active_object_types:
            self.active_object_types.add(obj_type)
            commands.append(create_command(Command.REGISTER, obj_type))

        # Check for expired (inactive) object types
        for ot in list(self.active_object_types):
            if (current_index - self.last_seen.get(ot, 0)) >= self.window_size:
                self.active_object_types.remove(ot)
                commands.append(create_command(Command.DEREGISTER, ot))

        commands.append(model_event)
        return from_iterable(commands)
