from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.age_cache_policy import \
    AgeCachePolicy
from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.size_cache_policy import \
    SizeCachePolicy
from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.base_cache_policy import BaseCachePolicy
from pybeamline.bevent import BEvent
from reactivex import Observable
from typing import List, Callable

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
    from reactivex import operators as ops
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