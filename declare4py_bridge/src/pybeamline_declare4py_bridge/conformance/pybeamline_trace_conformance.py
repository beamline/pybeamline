from pybeamline.bevent import BEvent
from Declare4Py.ProcessModels.DeclareModel import DeclareModel
from Declare4Py.ProcessMiningTasks.ConformanceChecking.MPDeclareAnalyzer import MPDeclareAnalyzer
from Declare4Py.ProcessMiningTasks.ConformanceChecking.MPDeclareResultsBrowser import MPDeclareResultsBrowser
from Declare4Py.D4PyEventLog import D4PyEventLog
from reactivex import Observable
import pm4py

def get_trace_conformance(
        model:DeclareModel, 
        event_stream: Observable[BEvent], 
        timestamp_key:str = 'time:timestamp', 
        activity_key:str = 'concept:name'):
    # Generate Declare4py EventLog
    logs = []
    # TODO: Change function, so I dont have to pipe the input first (How do deal with observables)
    event_stream.pipe(
        
        # lambda x: print(x)
    ).subscribe(
        lambda log: logs.append(pm4py.convert_to_event_log(log)) 
    )

    for l in logs:
        l._properties['pm4py:param:timestamp_key'] = timestamp_key
        l._properties['pm4py:param:activity_key'] = activity_key

    # TODO: How do we want to handle logs? Put entire Observable into one log object, or split further?
    declare_log:D4PyEventLog = D4PyEventLog(log=logs[0]) 
    # Conformance Check on Eventlog
    # TODO: What is consider_vacuity?
    conformance_result = MPDeclareAnalyzer(
        log=declare_log, 
        declare_model=model,
        consider_vacuity=False
        ).run()
    
    # return results
    return conformance_result.get_metric(metric='state')


def handle_event(event:BEvent) -> Observable[list[BEvent]]:
    pass