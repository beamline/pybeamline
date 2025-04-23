from datetime import datetime
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.oc_heuristics_miner_lossy_counting import OCHeuristicsMinerLossyCounting
from unittest import TestCase

class TestOCHeuristicsMinerLossyCounting(TestCase):
    def setUp(self):
        self.miner = OCHeuristicsMinerLossyCounting(max_approx_error=0.1)

    def test_selfLoop_and_sequencing(self):
        events = [
            BOEvent("e1", "A", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e3", "A", datetime(2025, 1, 3), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e4", "B", datetime(2025, 1, 4), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e5", "C", datetime(2025, 1, 5), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
        ]
        for e in events:
            self.miner.ingest_event(e)

        model = self.miner.get_model()
        edges = model.dfg
        self.assertEqual(("A","A") in edges, True)
        self.assertEqual(("A","B") in edges, True)
        self.assertEqual(("B","C") in edges, True)

    def test_and_split_discovery(self):
        miner = OCHeuristicsMinerLossyCounting(
            max_approx_error=0.1,
            and_threshold=0.0,  # ensure we allow weak ANDs
            dependency_threshold=0.0
        )

        # These traces alternate successors after X, should imply X → A AND B
        events = [
            BOEvent("e1", "X", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e3", "B", datetime(2025, 1, 3), [{"ocel:oid": "o1", "ocel:type": "Order"}]),

            BOEvent("e4", "X", datetime(2025, 1, 4), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e5", "B", datetime(2025, 1, 5), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e6", "A", datetime(2025, 1, 6), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
        ]

        for e in events:
            miner.ingest_event(e)
        model = miner.get_model()
        dfg = model.dfg
        # Check DFG structure
        self.assertEqual(("X","A") in dfg and ("X","B") in dfg, True)

    def test_xor_split_discovery(self):
        miner = OCHeuristicsMinerLossyCounting(
            max_approx_error=0.1,
            and_threshold=1.0,  # very high, so AND will not be inferred
            dependency_threshold=0.0
        )

        events = [
            BOEvent("e1", "X", datetime(2025, 1, 1), [{"ocel:oid": "o1", "ocel:type": "Order"}]),
            BOEvent("e2", "A", datetime(2025, 1, 2), [{"ocel:oid": "o1", "ocel:type": "Order"}]),

            BOEvent("e3", "X", datetime(2025, 1, 3), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
            BOEvent("e4", "B", datetime(2025, 1, 4), [{"ocel:oid": "o2", "ocel:type": "Order"}]),
        ]

        for e in events:
            miner.ingest_event(e)

        model = miner.get_model()
        dfg = model.dfg
        self.assertEqual(("X", "A") in dfg and ("X","B") in dfg, True)

