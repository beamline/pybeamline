import unittest
from datetime import datetime, timedelta
from pybeamline.bevent import BEvent
from pybeamline.boevent import BOEvent
from pybeamline.algorithms.discovery.heuristics_miner_lossy_counting_budget import (
    HeuristicsMinerLossyCountingBudget,
    heuristics_miner_lossy_counting_budget
)
from reactivex import from_iterable
from reactivex.operators import to_list
from pybeamline.sources import log_source


class TestHeuristicsMinerLossyCountingBudget(unittest.TestCase):

    def setUp(self):
        self.hm = HeuristicsMinerLossyCountingBudget(budget=10)

    def test_event_count(self):
        count_before = self.hm.observed_events()
        e1 = BEvent("A", "case1", datetime.now())
        self.hm.ingest_event(e1)
        self.assertEqual(self.hm.observed_events(), count_before + 1)

    def test_flatmap_operator_yields_model(self):
        base_time = datetime(2024, 1, 1)
        boevents = [
            BOEvent("e1", "A", {"Customer": {"c1"}}, base_time),
            BOEvent("e2", "B", {"Customer": {"c1"}}, base_time + timedelta(seconds=10)),
            BOEvent("e3", "C", {"Customer": {"c1"}}, base_time + timedelta(seconds=20)),
            BOEvent("e4", "D", {"Customer": {"c1"}}, base_time + timedelta(seconds=30)),
            BOEvent("e5", "E", {"Customer": {"c1"}}, base_time + timedelta(seconds=40)),
            BOEvent("e6", "F", {"Customer": {"c1"}}, base_time + timedelta(seconds=50)),
            BOEvent("e7", "G", {"Customer": {"c1"}}, base_time + timedelta(seconds=60)),
            BOEvent("e8", "H", {"Customer": {"c1"}}, base_time + timedelta(seconds=70)),
            BOEvent("e9", "I", {"Customer": {"c1"}}, base_time + timedelta(seconds=80)),
            BOEvent("e10", "J", {"Customer": {"c1"}}, base_time + timedelta(seconds=90)),
        ]

        miner = heuristics_miner_lossy_counting_budget(model_update_frequency=10)
        result_stream = from_iterable(boevents).pipe(miner, to_list())
        models = result_stream.run()

        self.assertGreaterEqual(len(models), 1)
        for model in models:
            self.assertIsInstance(model, type(models[0]))  # HeuristicsNet

    def test_invalid_event_type_raises(self):
        miner = heuristics_miner_lossy_counting_budget()
        with self.assertRaises(TypeError):
            list(from_iterable(["not-an-event"]).pipe(miner, to_list()).run())

    def test_unflattened_bo_event_raises(self):
        unflat_event = BOEvent("e1", "A", {"Customer": {"c1", "c2"}}, datetime.now())
        miner = heuristics_miner_lossy_counting_budget()
        with self.assertRaises(ValueError):
            list(from_iterable([unflat_event]).pipe(miner, to_list()).run())

    def test_lossy_counting_budget_cleaning(self):
        emitted = []
        log_source(["ADCB", "ABCD", "ABCD", "ABCD", "ABCD", "ABCD"]).pipe(
            heuristics_miner_lossy_counting_budget(budget=10, model_update_frequency=6)
        ).subscribe(lambda e: emitted.append(e))

        self.assertGreaterEqual(len(emitted), 1)
        first_model = emitted[0]
        final_model = emitted[-1]

        self.assertIn(('A', 'D'), first_model.dfg)
        self.assertNotIn(('A', 'D'), final_model.dfg, msg="Expected edge A → D to be pruned due to budget constraints")
        self.assertIn(('A', 'B'), final_model.dfg, msg="Expected edge A → B to remain due to repeated support")

