import random
from datetime import datetime
from typing import List, Tuple, Optional, Dict
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable
from pybeamline.boevent import BOEvent


def generate_shuffled_traces(flows: List[Tuple[List[dict], int]], shuffle: bool = True) -> List[dict]:
    all_traces = []
    trace_id = 0

    for flow_template, repetitions in flows:
        for _ in range(repetitions):
            trace = []
            suffix = f"{trace_id}"
            for event in flow_template:
                updated_event = {
                    "activity": event["activity"],
                    "objects": {
                        obj_type: [f"{oid}_{suffix}" for oid in obj_ids]
                        for obj_type, obj_ids in event["objects"].items()
                    }
                }
                trace.append(updated_event)
            all_traces.append(trace)
            trace_id += 1

    if shuffle:
        random.shuffle(all_traces)

    return [event for trace in all_traces for event in trace]


def dict_test_ocel_source(flows: List[Tuple[List[dict], int]], shuffle: bool = False, scheduler: Optional[abc.SchedulerBase] = None) -> Observable[BOEvent]:
    """
    :param flows: A list of tuples, where each tuple is of the form (flow_template, repetitions).
                  - flow_template is a list of event dictionaries with keys "activity" and "objects".
                  - repetitions is the number of traces to generate from that template.
                  Example:
                  [
                      (
                          [
                              {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
                              {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}}
                          ],
                          20
                      )
                  ]
                  This means: generate 20 traces with those two events.
    :param shuffle: Whether to shuffle the events from different traces in the final output.
    :param scheduler: (Optional) A ReactiveX scheduler to control event emission timing.
    :return: An Observable stream of BOEvent objects, one per event.
    """
    def subscribe(observer: abc.ObserverBase[BOEvent], scheduler_: Optional[abc.SchedulerBase] = None) -> abc.DisposableBase:
        all_events = generate_shuffled_traces(flows, shuffle=shuffle)
        for idx, event in enumerate(all_events):

            omap: Dict[str,set[str]] = {
                obj_type: set(ids)
                for obj_type, ids in event["objects"].items()
            }

            bo_event = BOEvent(
                event_id=f"e{idx}",
                activity_name=event["activity"],
                timestamp=datetime.now(),
                omap=omap,
                vmap=None,
            )
            observer.on_next(bo_event)

        observer.on_completed()
        return CompositeDisposable()

    return Observable(subscribe)


