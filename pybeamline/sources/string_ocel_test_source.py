import random
from datetime import datetime
from pybeamline.boevent import BOEvent
from reactivex import Observable
from reactivex.disposable import CompositeDisposable


def dict_test_ocel_source(flows_with_counts, shuffle=False) -> Observable[BOEvent]:
    """
    Accepts a list of (event_list, count) tuples.
    Expands and emits BOEvent instances.
    """
    all_events = []
    event_counter = 0

    for event_flow, repetitions in flows_with_counts:
        for _ in range(repetitions):
            for event in event_flow:
                object_refs = [
                    {"ocel:oid": oid, "ocel:type": obj_type}
                    for obj_type, ids in event["objects"].items()
                    for oid in ids
                ]
                bo_event = BOEvent(
                    event_id=f"e{event_counter}",
                    activity_name=event["activity"],
                    timestamp=datetime.now(),
                    object_refs=object_refs
                )
                all_events.append(bo_event)
                event_counter += 1

    if shuffle:
        random.shuffle(all_events)

    def subscribe(observer, scheduler=None):
        for e in all_events:
            observer.on_next(e)
        observer.on_completed()
        return CompositeDisposable()

    return Observable(subscribe)