from pm4py.objects.log.obj import Event, Trace, EventLog
from typing import List, Callable
from reactivex import Observable
from reactivex import operators as ops
from pybeamline.bevent import BEvent


def list_to_log(events: List[BEvent]) -> EventLog:
    log = {}
    for e in events:
        caseid = e.getTraceName()
        if not caseid in log.keys():
            log[caseid] = []
        log[caseid].append(e)
    newLog = EventLog()
    for caseid in log:
        trace = Trace(
            attributes={"case:concept:name": log[caseid][0].getTraceName()})
        for event in log[caseid]:
            e = Event()
            e["concept:name"] = event.getEventName()
            e["time:timestamp"] = event.getEventTime()
            trace.append(e)
        newLog.append(trace)
    return newLog


def sliding_window_to_log() -> Callable[[Observable[Observable[BEvent]]], Observable[EventLog]]:
    def o2l(obs: Observable[BEvent]) -> Observable[EventLog]:
        return obs.pipe(
            ops.to_iterable(),
            ops.map(lambda x: list_to_log(x)))

    return ops.flat_map(o2l)
