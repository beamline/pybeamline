import unittest

from pm4py.objects.bpmn.obj import BPMN

from pybeamline.algorithms.discovery import (
    split_miner_lossy_counting,
    split_miner_lossy_counting_budget,
    split_miner_sliding_window,
)
from pybeamline.bevent import BEvent
from pybeamline.stream.stream import Stream


class TestSplitMiner(unittest.TestCase):

    @staticmethod
    def event(activity, case_id):
        return BEvent(activity, case_id)

    @staticmethod
    def modes(model_update_frequency=8, **parameters):
        return (
            split_miner_sliding_window(
                model_update_frequency=model_update_frequency,
                max_capacity=100,
                **parameters,
            ),
            split_miner_lossy_counting(
                model_update_frequency=model_update_frequency,
                max_approx_error=0.01,
                **parameters,
            ),
            split_miner_lossy_counting_budget(
                model_update_frequency=model_update_frequency,
                cases_budget=20,
                relations_budget=20,
                **parameters,
            ),
        )

    def test_all_modes_emit_pm4py_bpmn_on_refresh(self):
        events = [
            self.event("A", "case-1"), self.event("B", "case-1"),
            self.event("A", "case-2"), self.event("B", "case-2"),
        ]

        for miner in self.modes(model_update_frequency=4):
            with self.subTest(miner=type(miner).__name__):
                models = Stream.from_iterable(events).pipe(miner).to_list()
                self.assertEqual(len(models), 1)
                self.assertIsInstance(models[0], BPMN)

    def test_all_modes_emit_one_model_per_refresh_period(self):
        events = [self.event("A", "case") for _ in range(4)]

        for miner in self.modes(model_update_frequency=2):
            with self.subTest(miner=type(miner).__name__):
                models = Stream.from_iterable(events).pipe(miner).to_list()
                self.assertEqual(len(models), 2)
                self.assertTrue(all(isinstance(model, BPMN) for model in models))

    def test_bpmn_contains_boundary_events_and_observed_tasks(self):
        events = [
            self.event("A", "case-1"), self.event("B", "case-1"),
            self.event("A", "case-2"), self.event("B", "case-2"),
        ]
        model = Stream.from_iterable(events).pipe(
            split_miner_sliding_window(
                model_update_frequency=4,
                frequency_percentile=0,
            )
        ).to_list()[0]

        nodes = model.get_nodes()
        self.assertEqual(
            {node.get_name() for node in nodes if isinstance(node, BPMN.Task)},
            {"A", "B"},
        )
        self.assertEqual(sum(isinstance(node, BPMN.StartEvent) for node in nodes), 1)
        self.assertEqual(sum(isinstance(node, BPMN.EndEvent) for node in nodes), 1)
        self.assertTrue(model.get_flows())

    def test_parallel_behavior_produces_parallel_gateway(self):
        events = [
            self.event(activity, case_id)
            for case_id, trace in (("case-1", "ABCD"), ("case-2", "ACBD"))
            for activity in trace
        ]
        for miner in self.modes(
                model_update_frequency=8,
                concurrency_threshold=0.5,
                frequency_percentile=0,
        ):
            with self.subTest(miner=type(miner).__name__):
                model = Stream.from_iterable(events).pipe(miner).to_list()[0]
                parallel_gateways = [
                    node for node in model.get_nodes()
                    if isinstance(node, BPMN.ParallelGateway)
                ]
                self.assertGreaterEqual(len(parallel_gateways), 2)

                flows = model.get_flows()
                self.assertTrue(any(
                    len([flow for flow in flows if flow.get_target() == gateway]) > 1
                    for gateway in parallel_gateways
                ), "concurrent branches should reconverge through a parallel join")

    def test_choice_behavior_produces_exclusive_gateway(self):
        events = [
            self.event(activity, case_id)
            for case_id, trace in (("case-1", "ABD"), ("case-2", "ACD"))
            for activity in trace
        ]
        for miner in self.modes(
                model_update_frequency=6,
                frequency_percentile=0,
        ):
            with self.subTest(miner=type(miner).__name__):
                model = Stream.from_iterable(events).pipe(miner).to_list()[0]
                exclusive_gateways = [
                    node for node in model.get_nodes()
                    if isinstance(node, BPMN.ExclusiveGateway)
                ]
                self.assertGreaterEqual(len(exclusive_gateways), 2)

    def test_sliding_window_discards_oldest_events(self):
        miner = split_miner_sliding_window(
            model_update_frequency=100,
            max_capacity=2,
        )
        for event in (
                self.event("A", "case"),
                self.event("B", "case"),
                self.event("C", "case"),
        ):
            miner.transform(event)

        self.assertEqual(miner.retained_event_count, 2)
        self.assertEqual(
            [event.get_event_name() for event in miner.event_window],
            ["B", "C"],
        )

    def test_lossy_counting_cleans_expired_cases_at_bucket_boundaries(self):
        miner = split_miner_lossy_counting(
            model_update_frequency=100,
            max_approx_error=0.5,
        )
        miner.transform(self.event("A", "old-case"))
        miner.transform(self.event("X", "new-case"))

        self.assertEqual(miner.tracked_case_count, 1)

    def test_lossy_counting_budget_respects_case_and_relation_budgets(self):
        miner = split_miner_lossy_counting_budget(
            model_update_frequency=100,
            cases_budget=1,
            relations_budget=1,
        )
        for event in (
                self.event("A", "case-1"),
                self.event("B", "case-1"),
                self.event("X", "case-2"),
                self.event("Y", "case-2"),
                self.event("Z", "case-2"),
        ):
            miner.transform(event)

        self.assertLessEqual(miner.tracked_case_count, 1)
        self.assertLessEqual(miner.tracked_relation_count, 1)

    def test_invalid_parameters_are_rejected(self):
        invalid_factories = (
            (split_miner_sliding_window, {"model_update_frequency": 0}),
            (split_miner_sliding_window, {"max_capacity": 0}),
            (split_miner_lossy_counting, {"max_approx_error": 0}),
            (split_miner_lossy_counting, {"max_approx_error": 1.1}),
            (split_miner_lossy_counting_budget, {"cases_budget": 0}),
            (split_miner_lossy_counting_budget, {"relations_budget": 0}),
            (split_miner_sliding_window, {"concurrency_threshold": -0.1}),
            (split_miner_lossy_counting, {"frequency_percentile": 1.1}),
        )
        for factory, parameters in invalid_factories:
            with self.subTest(factory=factory.__name__, parameters=parameters):
                with self.assertRaises(ValueError):
                    factory(**parameters)

        miner = split_miner_sliding_window()
        with self.assertRaises(TypeError):
            miner.transform("not a BEvent")


if __name__ == "__main__":
    unittest.main()
