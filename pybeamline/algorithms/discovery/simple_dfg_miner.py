from reactivex import operators as ops
from reactivex import Observable
from pybeamline.bevent import BEvent
from typing import Callable


def simple_dfg_miner(
        model_update_frequency=10,
        min_relative_frequency=0.75) -> Callable[[Observable[BEvent]], Observable]:

    latest_event = dict()  # latest event for each case
    complete_dfg = dict()  # dfg: tuple -> frequency
    observed_events = 0

    def miner(event: BEvent) -> Observable:
        nonlocal observed_events
        nonlocal latest_event
        nonlocal complete_dfg

        activity_name = event.get_event_name()
        case_id = event.get_trace_name()

        if case_id in latest_event:
            relation = (latest_event[case_id], activity_name)
            if relation in complete_dfg:
                complete_dfg[relation] += 1
            else:
                complete_dfg[relation] = 1
        latest_event[case_id] = activity_name

        observed_events += 1
        if observed_events % model_update_frequency == 0:
            max_frequency = max(complete_dfg.values())
            R = {k: v/max_frequency for k, v in complete_dfg.items() if v/max_frequency > min_relative_frequency}
            return R
        else:
            return None

    return lambda stream: stream.pipe(
        ops.map(miner),
        ops.filter(lambda x: x is not None)
    )
