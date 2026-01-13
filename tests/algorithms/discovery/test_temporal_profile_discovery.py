import unittest
from datetime import datetime, timedelta

from pybeamline.bevent import BEvent, DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY, DEFAULT_TRACEID_KEY
from pybeamline.algorithms.discovery.temporal_profile import (
    TemporalProfileDiscovery,
    TemporalProfileDiscoveryMapper,
)


class TestTemporalProfileDiscovery(unittest.TestCase):
    def test_convert_b_event_to_dataframe_mapping_and_sort(self):
        t0 = datetime(2024, 1, 1, 12, 0, 0)
        e1 = BEvent("A", "case1", event_time=t0 + timedelta(seconds=10))
        e2 = BEvent("B", "case1", event_time=t0)

        e1.event_attributes["resource"] = "r1"
        e1.trace_attributes["Customer"] = "c1"

        df = TemporalProfileDiscovery([e1, e2])._convert_b_event_to_dataframe([e1, e2])

        self.assertIn(DEFAULT_NAME_KEY, df.columns)
        self.assertIn(DEFAULT_TIMESTAMP_KEY, df.columns)
        self.assertIn(f"case:{DEFAULT_TRACEID_KEY}", df.columns)

        self.assertIn("resource", df.columns)
        self.assertIn("case:Customer", df.columns)

        row0 = df.iloc[0]
        row1 = df.iloc[1]
        self.assertLess(row0[DEFAULT_TIMESTAMP_KEY], row1[DEFAULT_TIMESTAMP_KEY])
        self.assertEqual(row0[f"case:{DEFAULT_TRACEID_KEY}"], "case1")
        self.assertEqual(row1[f"case:{DEFAULT_TRACEID_KEY}"], "case1")
        self.assertEqual({row0[DEFAULT_NAME_KEY], row1[DEFAULT_NAME_KEY]}, {"A", "B"})

    def test_mapper_returns_temporal_profile_list(self):
        base = datetime(2024, 1, 1, 9, 0, 0)
        events = [
            BEvent("A", "c1", event_time=base),
            BEvent("B", "c1", event_time=base + timedelta(seconds=5)),
            BEvent("C", "c1", event_time=base + timedelta(seconds=10)),
        ]

        mapper = TemporalProfileDiscoveryMapper()
        out = mapper.transform(events)

        self.assertIsInstance(out, list)
        self.assertEqual(len(out), 1)
        self.assertIsNotNone(out[0])


if __name__ == "__main__":
    unittest.main()
