import math
from typing import Dict, Set, Tuple, Protocol

from pybeamline.stream.stream import Stream
from pybeamline.utils.commands import Command, create_command

class InclusionStrategy(Protocol):
    def evaluate(self, model_event: dict) -> Stream[dict]:
        ... # pragma: no cover

class RelativeFrequencyBasedStrategy(InclusionStrategy):
    """
    Relative Frequency-Based Inclusion Strategy.
    - Emits ACTIVE when an object type is over the relative frequency threshold.
    - Emits INACTIVE when it falls below the threshold.

    :param frequency_threshold: Relative frequency threshold for object types to be considered active.
    Utilising global frequency counts to determine when to register or deregister object types.
    """
    def __init__(self, frequency_threshold: float = 0.05):
        self.__threshold = frequency_threshold
        self.__D_F: Dict[str, int] = {}
        self.__N = 0
        self.__D_A: Set[str] = set() # Set of active object types

    def evaluate(self, model_event: dict) -> Stream[dict]:
        if model_event.get("type") != "dfg":
            return Stream.of(model_event)

        obj_type = model_event["object_type"]
        self.__D_F[obj_type] = self.__D_F.get(obj_type, 0) + 1
        self.__N += 1

        count = self.__D_F[obj_type]
        total = self.__N

        commands = []
        # Floating point division to avoid early activation and late de-activation
        if (count / total) >= self.__threshold and obj_type not in self.__D_A:
            self.__D_A.add(obj_type)
            commands.append(create_command(Command.ACTIVE, obj_type))

        for ot in list(self.__D_A):
            if (self.__D_F.get(ot, 0) / total) < self.__threshold:
                self.__D_A.remove(ot)
                commands.append(create_command(Command.INACTIVE, ot))

        commands.append(model_event)
        return Stream.from_iterable(commands)


class LossyCountingStrategy(InclusionStrategy):
    """
    Lossy Counting Inclusion Strategy.
    Uses the Lossy Counting algorithm to approximate frequency counts of emitted object types.

    - Emits ACTIVE when an object type is first seen and added to D_C.
    - Emits INACTIVE when it is pruned during cleanup.

    :param max_approx_error: Maximum allowed approximation error (ε), determines bucket width.
    """
    def __init__(self, max_approx_error: float):
        self.__max_approx_error = max_approx_error
        self.__bucket_width = int(math.ceil(1 / self.__max_approx_error))
        self.__observed_emitted_models = 1
        self.__D_C: Dict[str, Tuple[int, int]] = {}  # {object_type: (frequency, delta)}

    def evaluate(self, model_event: dict) -> Stream[dict]:
        if model_event.get("type") != "dfg":
            return Stream.of(model_event)

        b_curr = int(math.ceil(self.__observed_emitted_models / self.__bucket_width))

        obj_type = model_event["object_type"]
        commands = []

        if obj_type in self.__D_C:
            freq, delta = self.__D_C[obj_type]
            self.__D_C[obj_type] = (freq + 1, delta)
        else:
            self.__D_C[obj_type] = (1, b_curr - 1)
            commands.append(create_command(Command.ACTIVE, obj_type))

        if self.__observed_emitted_models % self.__bucket_width == 0:
            to_remove = []
            for ot, (freq, delta) in self.__D_C.items():
                if freq + delta <= b_curr:
                    to_remove.append(ot)

            for ot in to_remove:
                del self.__D_C[ot]
                commands.append(create_command(Command.INACTIVE, ot))

        commands.append(model_event)
        self.__observed_emitted_models += 1
        return Stream.from_iterable(commands)

class SlidingWindowStrategy(InclusionStrategy):
    """
    Sliding Window Inclusion Strategy.
    - Emits ACTIVE when an object type is observed entering the window.
    - Emits INACTIVE when the object type is no longer present in the window after it slides.

    The strategy dynamically updates which object types are considered active by checking
    their presence in the current window. This supports responsiveness to recent trends
    and enables concept drift detection over time.
    :param window_size: The window sized considered in the sliding window.
    """
    def __init__(self, window_size: int = 30):
        self.window_size = window_size
        self.observed_events = 0
        self.D_W: Dict[str, int] = {}  # Last seen index for each object type

    def evaluate(self, model_event: dict):
        if model_event.get("type") != "dfg":
            return Stream.of(model_event)

        self.observed_events += 1
        obj_type = model_event["object_type"]
        commands = []

        # Update last seen
        if obj_type in self.D_W:
            self.D_W[obj_type] = self.observed_events
        else:
            self.D_W[obj_type] = self.observed_events
            commands.append(create_command(Command.ACTIVE, obj_type))


        to_remove = []
        # Prune inactive object types
        for obj_type, last_seen in self.D_W.items():
            if self.observed_events - last_seen >= self.window_size:
                commands.append(create_command(Command.INACTIVE, obj_type))
                to_remove.append(obj_type)

        for obj_type in to_remove:
            del self.D_W[obj_type]

        commands.append(model_event)
        return Stream.from_iterable(commands)
