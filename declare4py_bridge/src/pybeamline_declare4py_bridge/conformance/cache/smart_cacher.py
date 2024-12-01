from pybeamline.bevent import BEvent
from reactivex import Observable
from reactivex import operators as ops
from typing import List, Callable
from abc import ABC, abstractmethod

class BaseCachePolicy(ABC):

    @abstractmethod
    def apply(self, data: list[BEvent]) -> list[BEvent]:
        pass

class AgeAndSizePolicy(BaseCachePolicy):

    # max_age is in milliseconds
    def __init__(self, max_age: int, max_size: int):
        self.age_policy = AgeCachePolicy(max_age)
        self.size_policy = SizeCachePolicy(max_size)

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        return self.size_policy.apply(self.age_policy.apply(data))
    
class AgeCachePolicy(BaseCachePolicy):

    # max_age is in milliseconds
    def __init__(self, max_age : int):
        self.max_age = max_age

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        if len(data) > 0:
            current = data[0].get_event_time()
            return [e for e in data if (current - e.get_event_time()).total_seconds() * 1000 < self.max_age]

class SizeCachePolicy(BaseCachePolicy):

    def __init__(self, size: int):
        self.size = size

    def apply(self, data: list[BEvent]) -> list[BEvent]:
        return data[-self.size:]

class Smart_Cacher:

    def __init__(self, policy: BaseCachePolicy) -> None:
        self.cache: dict[str, list[BEvent]] = {}
        self.policy = policy

    def __call__(self, event_stream: Observable[BEvent]):
        return event_stream.pipe(
            self.__add_event_to_cache()
        )
    
    def __add_event(self, event: BEvent):
        event_trace = event.get_trace_name()
        # Check if trace already exists
        if event_trace not in self.cache:
            self.cache[event_trace] = []
        # Add event to list
        self.cache[event_trace].append(event)
        self.cache[event_trace] = self.policy.apply(self.cache[event_trace])
        return self.cache[event_trace]


    def __add_event_to_cache(self) -> Callable[[Observable[BEvent]], Observable[List[BEvent]]]:
        def __add_to_cache(obs: Observable[BEvent]) -> Observable[List[BEvent]]:
            return obs.pipe(
                ops.map(lambda event: self.__add_event(event))
            )
        return __add_to_cache
        
    def print_longest_list_key(self):
        if not self.cache:
            print("Cache is empty.")
            return

        longest_key = max(self.cache, key=lambda k: len(self.cache[k]))

        print(f"Key with the longest list: {longest_key}")
        print(f"Length of the list: {len(self.cache[longest_key])}")

    



if __name__ == "__main__":
    import os
    from pybeamline.sources import log_source
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)


    log_path = os.path.join("data/extension_log/extension-log-4.xes")
    event_stream = log_source(log_path)

    smart_cache = Smart_Cacher(SizeCachePolicy(3))

    smart_cache.print_longest_list_key()

    event_stream.pipe(
        smart_cache
    ).subscribe(lambda x: print(x) if len(x) > 0 else None)

    # for event in events:
    #     smart_cache.add_event_handler()

    #smart_cache.print_longest_list_key()