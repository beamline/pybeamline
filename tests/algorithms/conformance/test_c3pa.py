import unittest

from pm4py.objects.petri_net.obj import Marking, PetriNet
from pm4py.objects.petri_net.utils import petri_utils

from pybeamline.algorithms.conformance import C3PAResult, c3pa_conformance
from pybeamline.bevent import BEvent
from pybeamline.stream.stream import Stream


def build_sequence_net():
    net = PetriNet("sequence")
    start = PetriNet.Place("start")
    middle = PetriNet.Place("middle")
    end = PetriNet.Place("end")
    net.places.update((start, middle, end))

    activity_a = PetriNet.Transition("t_a", "A")
    activity_b = PetriNet.Transition("t_b", "B")
    net.transitions.update((activity_a, activity_b))
    petri_utils.add_arc_from_to(start, activity_a, net)
    petri_utils.add_arc_from_to(activity_a, middle, net)
    petri_utils.add_arc_from_to(middle, activity_b, net)
    petri_utils.add_arc_from_to(activity_b, end, net)

    initial_marking = Marking({start: 1})
    final_marking = Marking({end: 1})
    return net, initial_marking, final_marking


def make_event(activity, case_id="case"):
    return BEvent(activity, case_id)


class TestC3PA(unittest.TestCase):

    def make_mapper(self, proxy_traces, **kwargs):
        net, initial_marking, final_marking = build_sequence_net()
        return c3pa_conformance(
            net,
            initial_marking,
            final_marking,
            proxy_traces=proxy_traces,
            **kwargs,
        )

    def run_events(self, mapper, *activities, case_id="case"):
        events = [make_event(activity, case_id) for activity in activities]
        return Stream.from_iterable(events).pipe(mapper).to_list()

    def test_stream_emits_a_pm4py_independent_c3pa_result_for_each_event(self):
        mapper = self.make_mapper([["A", "B"]])

        updates = self.run_events(mapper, "A", "B")

        self.assertEqual(len(updates), 2)
        self.assertTrue(all("case" in update for update in updates))
        result = updates[-1]["case"]
        self.assertIsInstance(result, C3PAResult)
        self.assertEqual(result.alignment, (("A", "A"), ("B", "B")))
        self.assertEqual(result.cost, 0)
        self.assertEqual(result.completeness, 0)
        self.assertEqual(result.confidence, 0)

    def test_unexpected_activity_is_reported_as_a_log_move(self):
        mapper = self.make_mapper([["A", "B"]])

        result = self.run_events(mapper, "A", "X")[-1]["case"]

        self.assertEqual(result.cost, 1)
        self.assertIn(("X", None), result.alignment)

    def test_warm_start_tracks_unobserved_model_prefix_as_completeness(self):
        mapper = self.make_mapper([["A", "B", "C"]])

        result = self.run_events(mapper, "B")[-1]["case"]

        self.assertEqual(result.alignment, (("B", "B"),))
        self.assertEqual(result.cost, 0)
        self.assertEqual(result.completeness, 1)

    def test_model_move_can_precede_a_synchronous_move(self):
        mapper = self.make_mapper([["A", "B", "C"]])

        result = self.run_events(mapper, "A", "C")[-1]["case"]

        self.assertEqual(result.alignment,
                         (("A", "A"), (None, "B"), ("C", "C")))
        self.assertEqual(result.cost, 1)

    def test_warm_start_can_be_disabled(self):
        mapper = self.make_mapper([["A", "B"]], warm_start=False)

        result = self.run_events(mapper, "B")[-1]["case"]

        self.assertEqual(result.completeness, 0)
        self.assertEqual(result.cost, 1)

    def test_state_decay_releases_stale_model_positions(self):
        mapper = self.make_mapper([["A", "B"]], state_decay_time=1)

        result = self.run_events(mapper, "A", "X", "B")[-1]["case"]

        self.assertEqual(result.alignment, (("B", "B"),))
        self.assertEqual(result.completeness, 1)

    def test_confidence_is_average_remaining_distance_to_trie_leaves(self):
        mapper = self.make_mapper([["A", "B"], ["A", "B", "C"]])

        result = self.run_events(mapper, "A")[-1]["case"]

        self.assertEqual(result.confidence, 1.5)

    def test_case_capacity_evicts_the_least_recent_case(self):
        mapper = self.make_mapper([["A", "B"]], max_cases=1)
        events = [make_event("A", "case-1"), make_event("A", "case-2")]

        updates = Stream.from_iterable(events).pipe(mapper).to_list()

        self.assertEqual(set(updates[-1]), {"case-2"})
        self.assertEqual(mapper.active_case_count, 1)

    def test_proxy_traces_can_be_supplied_without_a_petri_net(self):
        mapper = c3pa_conformance(proxy_traces=[["A", "B"]])

        result = self.run_events(mapper, "A", "B")[-1]["case"]

        self.assertEqual(result.cost, 0)
        self.assertEqual(result.alignment, (("A", "A"), ("B", "B")))

    def test_state_budget_bounds_the_number_of_alignment_candidates(self):
        mapper = self.make_mapper(
            [["A", "B", "C"], ["A", "D", "C"]],
            max_states_per_case=1,
        )

        self.run_events(mapper, "A", "C")

        self.assertLessEqual(mapper.tracked_state_count, 1)

    def test_separate_cases_keep_separate_prefix_alignments(self):
        mapper = self.make_mapper([["A", "B"]])
        events = [
            make_event("A", "case-1"), make_event("A", "case-2"),
            make_event("B", "case-1"),
        ]

        updates = Stream.from_iterable(events).pipe(mapper).to_list()

        self.assertEqual(updates[-1]["case-1"].alignment,
                         (("A", "A"), ("B", "B")))
        self.assertEqual(updates[-1]["case-2"].alignment, (("A", "A"),))

    def test_petri_net_proxy_traces_are_sampled_with_pm4py_semantics(self):
        net, initial_marking, final_marking = build_sequence_net()
        mapper = c3pa_conformance(
            net,
            initial_marking,
            final_marking,
            num_model_traces=3,
            max_model_steps=2,
            random_seed=0,
        )

        result = self.run_events(mapper, "A", "B")[-1]["case"]

        self.assertEqual(result.alignment, (("A", "A"), ("B", "B")))
        self.assertEqual(result.cost, 0)
        self.assertEqual(mapper.proxy_trace_count, 3)

    def test_invalid_parameters_and_events_are_rejected(self):
        net, initial_marking, final_marking = build_sequence_net()
        for parameters in (
                {"max_cases": 0},
                {"max_states_per_case": 0},
                {"state_decay_time": 0},
                {"max_model_steps": 0},
                {"num_model_traces": 0},
        ):
            with self.subTest(parameters=parameters):
                with self.assertRaises(ValueError):
                    c3pa_conformance(net, initial_marking, final_marking, **parameters)

        with self.assertRaises(ValueError):
            self.make_mapper([])
        with self.assertRaises(TypeError):
            self.make_mapper([["A"]]).transform("not a BEvent")


if __name__ == "__main__":
    unittest.main()
