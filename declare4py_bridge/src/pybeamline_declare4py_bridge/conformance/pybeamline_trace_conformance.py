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
import pm4py

def get_trace_conformance(
        model:DeclareModel, 
        event_stream: Observable[BEvent], 
        timestamp_key:str = 'time:timestamp', 
        activity_key:str = 'concept:name'
        ) -> Observable[pandas.DataFrame]:
    
    def conformance_check(events: list[BEvent]):
        return check_conformance_of_list(model, events, timestamp_key, activity_key)
    
    # Generate Declare4py EventLog
    return event_stream.pipe(
        smart_cache(),
        observable_list_to_log(),
        ops.map(conformance_check)
    )

def check_conformance_of_list(
        model:DeclareModel,
        events:list[BEvent], 
        timestamp_key:str = 'time:timestamp', 
        activity_key:str = 'concept:name'
        ):
    event_log = pm4py.convert_to_event_log(events)
    event_log._properties['pm4py:param:timestamp_key'] = timestamp_key
    event_log._properties['pm4py:param:activity_key'] = activity_key

    # TODO: How do we want to handle logs? Put entire Observable into one log object, or split further?
    declare_log:D4PyEventLog = D4PyEventLog(log=event_log) 
    # Conformance Check on Eventlog
    # TODO: What is consider_vacuity?
    conformance_result = MPDeclareAnalyzer(
        log=declare_log, 
        declare_model=model,
        consider_vacuity=False
        ).run()
    
    # return results
    return conformance_result.get_metric(metric='state')

def observable_list_to_log() -> Callable[[Observable[List[BEvent]]], Observable[DataFrame]]:
    def o2l(obs: Observable[List[BEvent]]) -> Observable[DataFrame]:
        return obs.pipe(
            ops.map(lambda events: list_to_log(events))  # Directly convert each list of BEvent to a DataFrame
        )

    return o2l

# Shamelessly stolen from pybeamline (since it is not available from the import)
def list_to_log(events: List[BEvent]) -> DataFrame:
    list_of_events = []
    for e in events:
        data_attributes = e.event_attributes
        data_attributes.update({"case:" + n: e.trace_attributes[n] for n in e.trace_attributes})
        data_attributes.update({"process:" + n: e.process_attributes[n] for n in e.process_attributes})
        list_of_events.append(data_attributes)
    log = DataFrame(list_of_events)
    return log




# TODO: not so smart jet
def smart_cache() -> Observable[list[BEvent]]:
    return lambda event_stream: event_stream.pipe(
        ops.buffer_with_count(10)
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

    observe_conformance = get_trace_conformance(model=extension_log_model, event_stream=event_stream)
    observe_conformance.pipe(
        ops.take(1)
    ).subscribe(
        lambda df: print(df),  # Print the resulting DataFrame
        lambda e: print(f"Error: {e}"),  # Handle any errors
        lambda: print("Conformance check completed!")  # Completion message
    )
