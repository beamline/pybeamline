from datetime import datetime
from unittest import TestCase
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import OCHeuristicsMinerLossyCounting


class TestOCHeuristicsMinerLossyCounting(TestCase):
    def setUp(self):
        # Reusable event sequence for linear and self-loop test
        self.linear_trace = [
            BOEvent("e1", "A", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e3", "A", datetime(2025, 1, 3), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e4", "B", datetime(2025, 1, 4), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e5", "C", datetime(2025, 1, 5), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
        ]

    def test_self_loop_and_sequencing(self):
        miner = OCHeuristicsMinerLossyCounting(max_approx_error=0.1)
        for e in self.linear_trace:
            miner.ingest_event(e)

        dfg = miner.get_model().dfg

        self.assertIn(("A", "A"), dfg)
        self.assertIn(("A", "B"), dfg)
        self.assertIn(("B", "C"), dfg)

    def test_and_split_discovery(self):
        miner = OCHeuristicsMinerLossyCounting(
            max_approx_error=0.1,
            and_threshold=0.0,           # allow weak AND
            dependency_threshold=0.0     # no filtering
        )

        and_events = [
            BOEvent("e1", "X", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e3", "B", datetime(2025, 1, 3), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e4", "X", datetime(2025, 1, 4), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e5", "B", datetime(2025, 1, 5), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e6", "A", datetime(2025, 1, 6), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
        ]

        for e in and_events:
            miner.ingest_event(e)

        dfg = miner.get_model().dfg
        self.assertIn(("X", "A"), dfg)
        self.assertIn(("X", "B"), dfg)

    def test_xor_split_discovery(self):
        miner = OCHeuristicsMinerLossyCounting(
            max_approx_error=0.1,
            and_threshold=1.0,           # prohibit AND
            dependency_threshold=0.0
        )

        xor_events = [
            BOEvent("e1", "X", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e3", "X", datetime(2025, 1, 3), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e4", "B", datetime(2025, 1, 4), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
        ]

        for e in xor_events:
            miner.ingest_event(e)

        dfg = miner.get_model().dfg
        self.assertIn(("X", "A"), dfg)
        self.assertIn(("X", "B"), dfg)
