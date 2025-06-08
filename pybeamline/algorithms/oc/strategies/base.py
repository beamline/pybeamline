import enum
import math
from asyncio import Protocol
from typing import Any, Dict, Set, Tuple

from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import Observable, from_iterable, just
from pybeamline.boevent import BOEvent
from pybeamline.utils.commands import Command, create_command


class EmissionStrategy(Protocol):
    def evaluate(self, model_event: dict) -> Observable[dict]:
        ... # pragma: no cover

class RelativeFrequencyBasedStrategy(EmissionStrategy):
    def __init__(self, frequency_threshold: float = 0.05):
        self._threshold = int(math.ceil(1 / frequency_threshold))
        self._emitted_models: Dict[str, int] = {}
        self._observed_emitted_models = 0
        self._registered: Set[str] = set()

    def evaluate(self, model_event: dict) -> Observable[dict]:
        if model_event.get("type") != "model":
            return just(model_event)

        obj_type = model_event["object_type"]
        self._emitted_models[obj_type] = self._emitted_models.get(obj_type, 0) + 1
        self._observed_emitted_models += 1

        count = self._emitted_models[obj_type]
        total = self._observed_emitted_models

        commands = []
        if self._threshold * count >= total and obj_type not in self._registered:
            self._registered.add(obj_type)
            commands.append(create_command(Command.REGISTER, obj_type))

        for ot in list(self._registered):
            if self._threshold * self._emitted_models.get(ot, 0) < total:
                self._registered.remove(ot)
                commands.append(create_command(Command.DEREGISTER, ot))

        commands.append(model_event)
        return from_iterable(commands)

class LossyCountingStrategy(EmissionStrategy):
    def __init__(self, max_approx_error: float):
        self._max_approx_error = max_approx_error
        self._bucket_width = int(math.ceil(1 / self._max_approx_error))
        self._bucket = 1
        self._count: Dict[str, Tuple[int, int]] = {}  # (freq, delta)
        self._registered: Set[str] = set()
        self._observed_emitted_models = 0

    def evaluate(self, model_event: dict) -> Observable[dict]:
        if model_event.get("type") != "model":
            return just(model_event)

        self._observed_emitted_models += 1
        obj_type = model_event["object_type"]
        is_new = obj_type not in self._count

        # Update or initialize frequency
        if is_new:
            self._count[obj_type] = (1, self._bucket - 1)
        else:
            freq, delta = self._count[obj_type]
            self._count[obj_type] = (freq + 1, delta)

        commands = []
        if is_new and obj_type not in self._registered:
            self._registered.add(obj_type)
            commands.append(create_command(Command.REGISTER, obj_type))

        if self._observed_emitted_models % self._bucket_width == 0:
            self._bucket += 1
            for ot in list(self._count):
                freq, delta = self._count[ot]
                if freq + delta <= self._bucket:
                    del self._count[ot]
                    if ot in self._registered:
                        self._registered.remove(ot)
                        commands.append(create_command(Command.DEREGISTER, ot))

        commands.append(model_event)
        return from_iterable(commands)
