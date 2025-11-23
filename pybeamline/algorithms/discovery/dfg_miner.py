from pybeamline.bevent import BEvent
from typing import Optional, List, Tuple, Dict

from pybeamline.stream.base_map import BaseMap


def simple_dfg_miner(model_update_frequency=10,min_relative_frequency=0.75) -> BaseMap[BEvent, Tuple[int, Dict]]:
    return SimpleDfgMiner(model_update_frequency=model_update_frequency, min_relative_frequency=min_relative_frequency)


class SimpleDfgMiner(BaseMap[BEvent, Tuple[int, Dict]]):

    def __init__(self,  model_update_frequency=10, min_relative_frequency=0.75):
        self.model_update_frequency = max(model_update_frequency, 2)
        self.min_relative_frequency = min_relative_frequency
        self.latest_event = dict()  # latest event for each case
        self.complete_dfg = dict()  # dfg: tuple -> frequency
        self.observed_events = 0

    def transform(self, event: BEvent) -> Optional[List[Tuple[int, Dict]]]:
        activity_name = event.get_event_name()
        case_id = event.get_trace_name()

        if case_id in self.latest_event:
            relation = (self.latest_event[case_id], activity_name)
            if relation in self.complete_dfg:
                self.complete_dfg[relation] += 1
            else:
                self.complete_dfg[relation] = 1
        self.latest_event[case_id] = activity_name

        self.observed_events += 1
        if self.observed_events % self.model_update_frequency == 0 and len(self.complete_dfg) > 0:
            max_frequency = max(self.complete_dfg.values())
            if max_frequency > 0:
                m = {k: v / max_frequency for k, v in self.complete_dfg.items() if
                     v / max_frequency > self.min_relative_frequency}
                return [(self.observed_events, m)]
        return None



