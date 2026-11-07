import unittest

from pybeamline.algorithms.discovery.dcr_miner import DCRMiner, dcr_miner
from pybeamline.bevent import BEvent
from pybeamline.models.dcr_graph import DCRGraph
from pybeamline.stream.stream import Stream


class TestDCRMiner(unittest.TestCase):

    @staticmethod
    def event(activity, case_id):
        return BEvent(activity, case_id)

    def test_emits_dcr_graph_at_configured_event_frequency(self):
        miner = dcr_miner(
            model_update_frequency=3,
            max_traces=5,
            max_events_per_trace=5,
        )
        events = [
            self.event("A", "case-1"),
            self.event("B", "case-1"),
            self.event("A", "case-2"),
            self.event("B", "case-2"),
        ]

        models = Stream.from_iterable(events).pipe(miner).to_list()

        self.assertEqual(len(models), 1)
        self.assertIsInstance(models[0], DCRGraph)
        self.assertEqual(models[0].activities, frozenset({"A", "B"}))
        self.assertEqual(models[0].marking.executed, frozenset())
        self.assertEqual(models[0].marking.pending, frozenset())
        self.assertEqual(models[0].marking.included, frozenset({"A", "B"}))

    def test_discovers_condition_response_and_include_exclude_relations(self):
        miner = dcr_miner(
            model_update_frequency=4,
            max_traces=4,
            max_events_per_trace=4,
        )
        events = [
            self.event("A", "case-1"), self.event("B", "case-1"),
            self.event("A", "case-2"), self.event("B", "case-2"),
        ]

        model = Stream.from_iterable(events).pipe(miner).to_list()[0]

        self.assertIn(("A", "B"), model.conditions)
        self.assertIn(("A", "B"), model.responses)
        self.assertIn(("A", "B"), model.includes)
        self.assertIn(("A", "A"), model.excludes)
        self.assertIn(("B", "B"), model.excludes)
        self.assertIn(("B", "A"), model.excludes)

    def test_pattern_selection_limits_returned_relations(self):
        miner = dcr_miner(
            model_update_frequency=2,
            dcr_patterns=("Response",),
        )
        events = [self.event("A", "case"), self.event("B", "case")]

        model = Stream.from_iterable(events).pipe(miner).to_list()[0]

        self.assertIn(("A", "B"), model.responses)
        self.assertEqual(model.conditions, frozenset())
        self.assertEqual(model.includes, frozenset())
        self.assertEqual(model.excludes, frozenset())

    def test_relation_threshold_filters_low_frequency_dfg_edges(self):
        miner = dcr_miner(
            model_update_frequency=2,
            relations_threshold=1,
        )
        events = [self.event("A", "case"), self.event("B", "case")]

        model = Stream.from_iterable(events).pipe(miner).to_list()[0]

        self.assertNotIn(("A", "B"), model.conditions)
        self.assertNotIn(("A", "B"), model.responses)

    def test_sliding_event_window_recomputes_dfg_statistics(self):
        miner = DCRMiner(
            model_update_frequency=100,
            max_traces=2,
            max_events_per_trace=2,
        )
        events = [
            self.event("A", "case-1"),
            self.event("B", "case-1"),
            self.event("A", "case-2"),
            self.event("C", "case-2"),
            self.event("D", "case-1"),
        ]
        for event in events:
            miner.transform(event)

        self.assertEqual(
            miner.windowed_traces,
            {"case-1": ("B", "D"), "case-2": ("A", "C")},
        )
        dfg = miner.extended_dfg
        self.assertEqual(dfg.relations, {("B", "D"): 1, ("A", "C"): 1})
        self.assertEqual(dfg.activities["A"].occurrences, 1)
        self.assertEqual(dfg.activities["A"].trace_appearances, 1)
        self.assertEqual(dfg.activities["A"].average_index, 0)
        self.assertEqual(dfg.activities["C"].average_index, 1)

    def test_lru_trace_eviction_removes_evicted_activities_and_relations(self):
        miner = DCRMiner(
            model_update_frequency=100,
            max_traces=2,
            max_events_per_trace=3,
        )
        events = [
            self.event("A", "case-1"),
            self.event("B", "case-2"),
            self.event("C", "case-1"),  # Refresh case-1; case-2 is least recent.
            self.event("D", "case-3"),
        ]
        for event in events:
            miner.transform(event)

        self.assertEqual(
            miner.windowed_traces,
            {"case-1": ("A", "C"), "case-3": ("D",)},
        )
        self.assertNotIn("B", miner.extended_dfg.activities)
        self.assertEqual(miner.extended_dfg.relations, {("A", "C"): 1})

    def test_reentered_case_after_eviction_does_not_create_cross_trace_edge(self):
        miner = DCRMiner(
            model_update_frequency=100,
            max_traces=1,
            max_events_per_trace=3,
        )
        for event in (
                self.event("A", "case-1"),
                self.event("B", "case-2"),
                self.event("C", "case-1"),
        ):
            miner.transform(event)

        self.assertEqual(miner.windowed_traces, {"case-1": ("C",)})
        self.assertEqual(miner.extended_dfg.relations, {})

    def test_transitive_reduction_removes_redundant_response_edge(self):
        miner = dcr_miner(
            model_update_frequency=5,
            max_traces=5,
            max_events_per_trace=5,
            dcr_patterns=("Response",),
            transitive_reduction=("Response",),
        )
        events = [
            self.event("A", "case-1"),
            self.event("B", "case-1"),
            self.event("C", "case-1"),
            self.event("A", "case-2"),
            self.event("C", "case-2"),
        ]

        model = Stream.from_iterable(events).pipe(miner).to_list()[0]

        self.assertIn(("A", "B"), model.responses)
        self.assertIn(("B", "C"), model.responses)
        self.assertNotIn(("A", "C"), model.responses)

    def test_invalid_parameters_and_event_types_are_rejected(self):
        for kwargs in (
                {"model_update_frequency": 0},
                {"max_traces": 0},
                {"max_events_per_trace": 0},
                {"relations_threshold": -1},
                {"dcr_patterns": ("NotAPattern",)},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    DCRMiner(**kwargs)

        miner = dcr_miner()
        with self.assertRaises(TypeError):
            miner.transform("not an event")


if __name__ == "__main__":
    unittest.main()
