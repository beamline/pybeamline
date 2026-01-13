from typing import Optional, List, Any, Dict

from pm4py.streaming.algo.conformance.temporal import algorithm as temporal_conformance_checker
from pm4py.streaming.algo.conformance.temporal.variants.classic import TemporalProfileStreamingConformance
from pm4py.util.typing import TemporalProfile

from pybeamline.bevent import (
	BEvent,
	DEFAULT_NAME_KEY,
	DEFAULT_TIMESTAMP_KEY,
	DEFAULT_TRACEID_KEY,
)
from pybeamline.stream.base_map import BaseMap


def temporal_profile_conformance(temporal_profile: TemporalProfile, parameters: Optional[Dict] = None) -> "TemporalProfileConformanceMapper":
	if parameters is not None:
		return TemporalProfileConformanceMapper(temporal_profile, parameters)
	return TemporalProfileConformanceMapper(temporal_profile)


class TemporalProfileConformanceMapper(BaseMap[BEvent, Any]):

	def __init__(self, temporal_profile: TemporalProfile, parameters: Optional[Dict] = None):
		print("params",parameters)
		if parameters is not None:
			self._streaming: TemporalProfileStreamingConformance  = temporal_conformance_checker.apply(temporal_profile, parameters=parameters)
		else:
			self._streaming: TemporalProfileStreamingConformance = temporal_conformance_checker.apply(temporal_profile)
		self._processed_events: int = 0

	def transform(self, value: BEvent) -> Optional[List[Any]]:
		ev = self._b_event_to_pm4py_event(value)
		self._streaming.receive(ev)
		self._processed_events += 1
		res = self._streaming.get()
		print('res:', res)
		return [res] if res is not None else None

	def get_processed_event_num(self) -> int:
		return self._processed_events

	@staticmethod
	def _b_event_to_pm4py_event(e: BEvent) -> dict:
		ev = {
			DEFAULT_NAME_KEY: e.get_event_name(),
			DEFAULT_TIMESTAMP_KEY: e.get_event_time(),
			f"case:{DEFAULT_TRACEID_KEY}": e.get_trace_name(),
		}
		start_keys = {"start_timestamp", "start:timestamp", "time:start"}
		for sk in start_keys:
			if sk in e.event_attributes:
				ev["start_timestamp"] = e.event_attributes[sk]
				break

		protected_keys = {DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY, "start_timestamp"}
		for k, v in e.event_attributes.items():
			if k not in protected_keys and k not in start_keys:
				ev[k] = v

		return ev


