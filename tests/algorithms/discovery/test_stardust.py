import unittest

from pybeamline.algorithms.discovery import (
    StardustConfig,
    StardustMapper,
    frequency_based_sample,
    stardust_mapper,
)
from pybeamline.bevent import BEvent
from pybeamline.stream.stream import Stream


class TestStardust(unittest.TestCase):

    @staticmethod
    def event(activity, case_id):
        return BEvent(activity, case_id)

    def test_interleaved_cases_complete_as_distinct_variants_and_prune_prefixes(self):
        calls = []
        mapper = stardust_mapper(
            initialization_traces=2,
            sampling_rate=1,
            forgetting_size=5,
            max_consecutive_repetitions=None,
            discovery_algorithm=lambda sample: calls.append(dict(sample)) or "model",
        )

        mapper.transform(self.event("A", "case-1"))
        mapper.transform(self.event("A", "case-2"))
        mapper.transform(self.event("B", "case-1"))
        mapper.transform(self.event("C", "case-2"))
        self.assertIsNone(mapper.transform(self.event("?", "case-1")))

        self.assertEqual(mapper.completed_traces, 1)
        self.assertEqual(mapper.running_case_count, 1)
        self.assertEqual(mapper.active_variants, {("A", "B"): 1})
        self.assertEqual(mapper.prefix_node_count, 2)

        update = mapper.transform(self.event("?", "case-2"))[0]
        self.assertTrue(update.initial_model)
        self.assertFalse(update.drift_detected)
        self.assertEqual(update.sampled_variants, {("A", "B"): 1, ("A", "C"): 1})
        self.assertEqual(mapper.prefix_node_count, 0)
        self.assertEqual(calls, [{("A", "B"): 1, ("A", "C"): 1}])

    def test_frequency_sample_covers_requested_fraction_with_frequencies(self):
        sample = frequency_based_sample(
            {("A",): 7, ("B",): 6, ("C",): 3},
            sampling_rate=0.8,
        )
        self.assertEqual(sample, {("A",): 7, ("B",): 6})

    def test_model_is_retrained_only_when_sampled_behavior_changes(self):
        calls = []
        mapper = stardust_mapper(
            initialization_traces=2,
            sampling_rate=0.8,
            forgetting_size=10,
            discovery_algorithm=lambda sample: calls.append(dict(sample)) or len(calls),
        )

        mapper.transform(self.event("A", "a-1"))
        initial = mapper.complete_case("a-1")
        self.assertIsNone(initial)

        mapper.transform(self.event("A", "a-2"))
        initial = mapper.complete_case("a-2")
        self.assertTrue(initial.initial_model)
        self.assertFalse(initial.drift_detected)

        mapper.transform(self.event("B", "b-1"))
        drift = mapper.complete_case("b-1")
        self.assertTrue(drift.drift_detected)
        self.assertEqual(set(drift.sampled_variants), {("A",), ("B",)})

        mapper.transform(self.event("A", "a-3"))
        unchanged = mapper.complete_case("a-3")
        self.assertIsNone(unchanged)
        self.assertEqual(len(calls), 2)

    def test_inactive_variants_are_forgotten_after_forgetting_size(self):
        mapper = stardust_mapper(
            initialization_traces=1,
            sampling_rate=1,
            forgetting_size=2,
            discovery_algorithm=lambda sample: dict(sample),
        )

        mapper.transform(self.event("A", "a"))
        mapper.complete_case("a")
        mapper.transform(self.event("B", "b"))
        mapper.complete_case("b")
        mapper.transform(self.event("C", "c"))
        mapper.complete_case("c")

        self.assertNotIn(("A",), mapper.active_variants)
        self.assertEqual(mapper.active_variants, {("B",): 1, ("C",): 1})

    def test_self_loop_compression_is_configurable(self):
        compressed = StardustMapper(
            config=StardustConfig(initialization_traces=1),
            discovery_algorithm=lambda sample: dict(sample),
        )
        preserved = stardust_mapper(
            initialization_traces=1,
            max_consecutive_repetitions=None,
            discovery_algorithm=lambda sample: dict(sample),
        )

        for mapper in (compressed, preserved):
            for _ in range(4):
                mapper.transform(self.event("A", "case"))
            mapper.complete_case("case")

        self.assertEqual(compressed.active_variants, {("A", "A"): 1})
        self.assertEqual(preserved.active_variants, {("A", "A", "A", "A"): 1})

    def test_stream_emits_initial_model_and_drift_updates(self):
        mapper = stardust_mapper(
            initialization_traces=1,
            sampling_rate=1,
            forgetting_size=10,
            discovery_algorithm=lambda sample: tuple(sample),
        )
        events = [
            self.event("A", "case-1"), self.event("?", "case-1"),
            self.event("B", "case-2"), self.event("?", "case-2"),
        ]

        updates = Stream.from_iterable(events).pipe(mapper).to_list()

        self.assertEqual(len(updates), 2)
        self.assertTrue(updates[0].initial_model)
        self.assertTrue(updates[1].drift_detected)
        self.assertEqual(updates[1].completed_traces, 2)

    def test_explicit_completion_supports_sources_without_a_sentinel_event(self):
        mapper = stardust_mapper(
            initialization_traces=1,
            discovery_algorithm=lambda sample: "model",
        )
        mapper.transform(self.event("A", "case"))

        update = mapper.complete_case("case")

        self.assertTrue(update.initial_model)
        self.assertEqual(update.sampled_variants, {("A",): 1})

    def test_conformance_is_measured_before_updating_the_model(self):
        models = []
        evaluations = []
        mapper = stardust_mapper(
            initialization_traces=1,
            sampling_rate=1,
            discovery_algorithm=lambda sample: models.append(dict(sample)) or len(models),
            conformance_evaluator=lambda model, trace: evaluations.append((model, trace)) or 0.75,
        )

        mapper.transform(self.event("A", "initial"))
        initial = mapper.complete_case("initial")
        mapper.transform(self.event("A", "stable"))
        stable = mapper.complete_case("stable")

        self.assertTrue(initial.model_updated)
        self.assertFalse(stable.model_updated)
        self.assertEqual(stable.model, 1)
        self.assertEqual(stable.conformance, 0.75)
        self.assertEqual(evaluations, [(1, ("A",))])

        mapper.transform(self.event("B", "drift"))
        drift = mapper.complete_case("drift")
        self.assertEqual(drift.conformance, 0.75)
        self.assertTrue(drift.drift_detected)
        self.assertEqual(evaluations[-1], (1, ("B",)))
        self.assertEqual(mapper.current_model, 2)

    def test_invalid_hyperparameters_are_rejected(self):
        for kwargs in (
            {"initialization_traces": 0},
            {"initialization_traces": 1.5},
            {"sampling_rate": 0},
            {"sampling_rate": 1.1},
            {"forgetting_size": 0},
            {"forgetting_size": 2.5},
            {"max_consecutive_repetitions": 0},
            {"max_consecutive_repetitions": 1.5},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    StardustConfig(**kwargs)

    def test_frequency_sample_rejects_nonpositive_frequencies(self):
        with self.assertRaises(ValueError):
            frequency_based_sample({("A",): 0}, sampling_rate=0.8)

    def test_cannot_complete_a_case_that_has_not_started(self):
        mapper = stardust_mapper(discovery_algorithm=lambda sample: dict(sample))
        with self.assertRaisesRegex(KeyError, "no events were recorded"):
            mapper.complete_case("missing")


if __name__ == "__main__":
    unittest.main()
