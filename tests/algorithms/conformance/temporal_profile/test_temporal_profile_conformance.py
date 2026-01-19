import unittest
from datetime import datetime, timedelta

import pandas as pd

from pybeamline.bevent import BEvent
from pybeamline.algorithms.discovery.temporal_profile import TemporalProfileDiscovery
from pybeamline.algorithms.conformance.temporal_profile.temporal_profile_conformance import temporal_profile_conformance, TemporalProfileConformanceMapper
from pybeamline.stream.stream import Stream

class TestTemporalProfileConformance(unittest.TestCase):

    def setUp(self):

        base_time = datetime(2025, 1, 1, 9, 0, 0)
        self.events = [
            BEvent("A", "c1", event_time=base_time),
            BEvent("B", "c1", event_time=base_time + timedelta(seconds=5)),
            BEvent("C", "c1", event_time=base_time + timedelta(seconds=15)),
            BEvent("A", "c2", event_time=base_time + timedelta(minutes=1)),
            BEvent("B", "c2", event_time=base_time + timedelta(minutes=1, seconds=5)),
            BEvent("C", "c2", event_time=base_time + timedelta(minutes=1, seconds=15)),
        ]
        self.profile = TemporalProfileDiscovery(self.events).apply()

    def test_mapper_emits_diagnostics_results(self):
        mapper = temporal_profile_conformance(self.profile)
        out = Stream.from_iterable(self.events).pipe(mapper).to_list()
        self.assertGreater(len(out), 0)
        for result in out:
            self.assertIsNotNone(result)

        self.assertEqual(mapper.get_processed_event_num(), len(self.events))

    def test_direct_conversion_helper_single_event(self):
        mapper = TemporalProfileConformanceMapper(self.profile)
        ev = mapper._b_event_to_pm4py_event(self.events[0])
        self.assertIn("concept:name", ev)
        self.assertIn("time:timestamp", ev)
        self.assertIn("case:concept:name", ev)

    def test_direct_conversion_helper_multiple_events_monotonic(self):
        mapper = TemporalProfileConformanceMapper(self.profile)
        converted = [mapper._b_event_to_pm4py_event(e) for e in self.events]
        df = pd.DataFrame(converted)
        self.assertTrue(df["time:timestamp"].is_monotonic_increasing)

    def test_conversion_handles_start_timestamp(self):
        # Add a start timestamp attribute in different possible keys
        e = BEvent("X", "cx", event_time=self.events[0].get_event_time() + timedelta(seconds=30))
        e.event_attributes["start_timestamp"] = self.events[0].get_event_time()
        mapper = TemporalProfileConformanceMapper(self.profile)
        ev = mapper._b_event_to_pm4py_event(e)
        self.assertIn("start_timestamp", ev)


if __name__ == "__main__":
    unittest.main()
