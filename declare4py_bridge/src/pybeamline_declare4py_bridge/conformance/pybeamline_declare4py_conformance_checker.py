from declare4py_bridge.src.pybeamline_declare4py_bridge.conformance.mappers.policies.size_cache_policy import \
    SizeCachePolicy
from pybeamline.bevent import BEvent
from Declare4Py.ProcessModels.DeclareModel import DeclareModel
from Declare4Py.ProcessMiningTasks.ConformanceChecking.MPDeclareAnalyzer import MPDeclareAnalyzer
from Declare4Py.ProcessMiningTasks.ConformanceChecking.MPDeclareResultsBrowser import MPDeclareResultsBrowser
from Declare4Py.D4PyEventLog import D4PyEventLog
from reactivex import Observable
from reactivex import operators as ops
from reactivex import Observable, empty, just
from typing import List, Callable
from pandas import DataFrame
import pandas
from pybeamline.mappers import sliding_window_to_log
from pybeamline.mappers.sliding_window_to_log import list_to_log
import pm4py
from mappers.smart_cacher import Smart_Cacher

class Pybeamline_Bridge_Conformance_Checker():
    def __init__(self, 
                 model: DeclareModel,
                 event_stream: Observable[BEvent],
                 timestamp_key: str = "time:timestamp",
                 activity_key:str = 'concept:name'):
        self.model = model
        self.event_stream = event_stream
        self.smart_cacher = Smart_Cacher(SizeCachePolicy(10))
        self.timestamp_key = timestamp_key
        self.activity_key = activity_key

    @staticmethod
    def __observable_list_to_log() -> Callable[[Observable[List[BEvent]]], Observable[DataFrame]]:
        def o2l(obs: Observable[List[BEvent]]) -> Observable[DataFrame]:
            return obs.pipe(
                ops.map(lambda events: list_to_log(events))  # Directly convert each list of BEvent to a DataFrame
            )

        return o2l

    
    @staticmethod
    def __smart_cache() -> Callable[[Observable[list[BEvent]]], Observable[list[BEvent]]]:
        def cache(obs: Observable[list[BEvent]]) -> Observable[list[BEvent]]:
            return obs.pipe(
                ops.buffer_with_count(10)
            )
        return cache

    
    def __check_conformance_of_list(self, events:list[BEvent]):
        if len(events) > 0:
            event_log = pm4py.convert_to_event_log(events)
            event_log._properties['pm4py:param:timestamp_key'] = self.timestamp_key
            event_log._properties['pm4py:param:activity_key'] = self.activity_key

            # TODO: How do we want to handle logs? Put entire Observable into one log object, or split further?
            declare_log:D4PyEventLog = D4PyEventLog(log=event_log)
            # Conformance Check on Eventlog
            # TODO: What is consider_vacuity?
            conformance_result = MPDeclareAnalyzer(
                log=declare_log, 
                declare_model=self.model,
                consider_vacuity=False
                ).run()
            
            # return results
            return conformance_result.get_metric(metric='state')
        else:
            return None
    
    def run_trace_conformance(self) -> Observable[pandas.DataFrame]:

        def conformance_check(events: list[BEvent]):
            return self.__check_conformance_of_list(events)
        
        # Generate Declare4py EventLog
        return self.event_stream.pipe(
            self.smart_cacher,
            ops.filter(lambda events: len(events) > 0),
            # For the conformance check 
            self.__observable_list_to_log(),
            ops.map(lambda events: conformance_check(events))
        )


if __name__ == "__main__":
    import os
    from Declare4Py.ProcessMiningTasks.Discovery.DeclareMiner import DeclareMiner
    from pybeamline.sources import log_source
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)


    log_path = os.path.join("data/extension_log/extension-log-4.xes")
    event_log = D4PyEventLog(case_name="case:concept:name")
    event_log.parse_xes_log(log_path)
    extension_log_model = DeclareMiner(log=event_log, consider_vacuity=False, min_support=0.2, itemsets_support=0.9, max_declare_cardinality=3).run()

    log_path = os.path.join("data/extension_log/extension-log-noisy-4.xes")
    event_stream = log_source(log_path)

    conformance_checker = Pybeamline_Bridge_Conformance_Checker(
        model=extension_log_model, event_stream=event_stream
    )

    observe_conformance = conformance_checker.run_trace_conformance()
    observe_conformance.subscribe(
        lambda df: print(df) if df is not None else None,  # Print the resulting DataFrame
        lambda e: print(f"Error: {e}"),  # Handle any errors
        lambda: print("Conformance check completed!")  # Completion message
    )
