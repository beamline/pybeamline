"""
Tests for the incremental prefix-alignment conformance checker,
based on the examples from:

  van Zelst, S.J., Bolt, A.,"; Aalst, W.M.P. van der (2019).
  "Online conformance checking: relating event streams to process
  models using prefix-alignments."
  Int J Data Sci Anal, 8, 269–284.
  https://doi.org/10.1007/s41060-017-0078-6

The reference Petri net N1 (Fig. 2 of the paper) is used throughout.
"""

import unittest
from datetime import datetime, timedelta

from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.utils import petri_utils

from pybeamline.bevent import BEvent
from pybeamline.algorithms.conformance.prefix_alignments.prefix_alignments_conformance import PrefixAlignmentsConformanceMapper

from pybeamline.sources import string_test_source


# ---------------------------------------------------------------------------
#  Helper: build the N1 Petri net from Fig. 2 of the paper
# ---------------------------------------------------------------------------

def build_n1():
    net = PetriNet("N1")

    # Places
    pi = PetriNet.Place("pi")
    p1 = PetriNet.Place("p1")
    p2 = PetriNet.Place("p2")
    p3 = PetriNet.Place("p3")
    p4 = PetriNet.Place("p4")
    p5 = PetriNet.Place("p5")
    po = PetriNet.Place("po")
    for p in [pi, p1, p2, p3, p4, p5, po]:
        net.places.add(p)

    # Transitions
    t1 = PetriNet.Transition("t1", "a")
    t2 = PetriNet.Transition("t2", "b")
    t3 = PetriNet.Transition("t3", "c")
    t4 = PetriNet.Transition("t4", "d")
    t5 = PetriNet.Transition("t5", "d")
    t6 = PetriNet.Transition("t6", None)    # invisible (τ)
    t7 = PetriNet.Transition("t7", "e")
    t8 = PetriNet.Transition("t8", "f")
    for t in [t1, t2, t3, t4, t5, t6, t7, t8]:
        net.transitions.add(t)

    # Arcs  (place -> transition and transition -> place)
    # t1: pi -> t1 -> p1, p2
    petri_utils.add_arc_from_to(pi, t1, net)
    petri_utils.add_arc_from_to(t1, p1, net)
    petri_utils.add_arc_from_to(t1, p2, net)

    # t2: p1 -> t2 -> p3
    petri_utils.add_arc_from_to(p1, t2, net)
    petri_utils.add_arc_from_to(t2, p3, net)

    # t3: p2 -> t3 -> p4
    petri_utils.add_arc_from_to(p2, t3, net)
    petri_utils.add_arc_from_to(t3, p4, net)

    # t4: p1, p4 -> t4 -> p5
    petri_utils.add_arc_from_to(p1, t4, net)
    petri_utils.add_arc_from_to(p4, t4, net)
    petri_utils.add_arc_from_to(t4, p5, net)

    # t5: p3, p4 -> t5 -> p5
    petri_utils.add_arc_from_to(p3, t5, net)
    petri_utils.add_arc_from_to(p4, t5, net)
    petri_utils.add_arc_from_to(t5, p5, net)

    # t6 (τ): p5 -> t6 -> p1, p2
    petri_utils.add_arc_from_to(p5, t6, net)
    petri_utils.add_arc_from_to(t6, p1, net)
    petri_utils.add_arc_from_to(t6, p2, net)

    # t7: p5 -> t7 -> po
    petri_utils.add_arc_from_to(p5, t7, net)
    petri_utils.add_arc_from_to(t7, po, net)

    # t8: p5 -> t8 -> po
    petri_utils.add_arc_from_to(p5, t8, net)
    petri_utils.add_arc_from_to(t8, po, net)

    # Markings
    im = Marking()
    im[pi] = 1
    fm = Marking()
    fm[po] = 1

    return net, im, fm


# ---------------------------------------------------------------------------
#  Convenience helpers used across tests
# ---------------------------------------------------------------------------

def _make_event(activity: str, case: str, offset_seconds: int = 0) -> BEvent:
    """Create a BEvent with a deterministic timestamp."""
    return BEvent(activity, case, event_time=datetime(2025, 1, 1) + timedelta(seconds=offset_seconds))


def _label(step):
    """Return the label string for the model side of an alignment step."""
    t = step.alignment_step[1]
    return t.label if t is not None else None


def _log(step):
    """Return the log-side (activity name) of an alignment step."""
    return step.alignment_step[0]


def _alignment_labels(case_state):
    """Return the alignment as a list of (log_label, model_label|None) pairs."""
    return [(_log(s), _label(s)) for s in case_state.alignment]


# ===========================================================================
#  Test cases derived from the paper
# ===========================================================================

class TestPrefixAlignmentsPaperExamples(unittest.TestCase):

    def setUp(self):
        self.net, self.im, self.fm = build_n1()

    # -----------------------------------------------------------------------
    #  1. Perfect trace  ⟨a, b, c, d, e⟩
    #     Expected: all synchronous moves, cost = 0
    # -----------------------------------------------------------------------
    def test_perfect_trace_abcde(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)

        string_test_source(["abcde"]).pipe(mapper).subscribe()

        result = mapper.get_trace_conformance("case_1")
        labels = _alignment_labels(result)

        # Every move is synchronous  (log == model label)
        for log_lbl, model_lbl in labels:
            self.assertIsNotNone(model_lbl, f"expected sync move, got activity move for '{log_lbl}'")
            self.assertEqual(log_lbl, model_lbl)

        self.assertEqual(result.get_last().cost, 0.0)


    # -----------------------------------------------------------------------
    #  2. Trace with unknown activities  ⟨x, a, d, e, z⟩
    #     x and z are NOT labels in N1 → activity moves (cost 1 each)
    # -----------------------------------------------------------------------
    def test_trace_with_unknown_activities(self):
        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)

        string_test_source(["xadez"]).pipe(checker).subscribe()

        result = checker.get_trace_conformance("case_1")

        # Cost must be 3 (x and z are each 1 as activity moves +1 for log move between a and d)
        self.assertEqual(result.get_last().cost, 3.0)

    # -----------------------------------------------------------------------
    #  3. Prefix-alignment for incomplete trace ⟨a, c, d⟩
    #     Expected: ⟨(a,t1),(c,t3),(d,t4)⟩  cost = 0
    #     t4 is the "decide" transition that skips examination (p1,p4 → p5)
    # -----------------------------------------------------------------------
    def test_prefix_alignment_acd(self):
        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        string_test_source(["acd"]).pipe(checker).subscribe()
        result = checker.get_trace_conformance("case_1")
        labels = _alignment_labels(result)

        # All synchronous
        for log_lbl, model_lbl in labels:
            self.assertIsNotNone(model_lbl)

        self.assertEqual(result.get_last().cost, 0.0)


    # -----------------------------------------------------------------------
    #  4. Incremental: first event for a case → from Mi
    #     If a sync transition is enabled in Mi, cost = 0
    # -----------------------------------------------------------------------
    def test_first_event_sync(self):

        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)

        string_test_source(["a"]).pipe(checker).subscribe()
        result = checker.get_trace_conformance("case_1")
        self.assertEqual(len(result.alignment), 1)
        self.assertEqual(_log(result.alignment[0]), "a")
        self.assertIsNotNone(result.alignment[0].alignment_step[1])
        self.assertEqual(result.get_last().cost, 0.0)

    # -----------------------------------------------------------------------
    #  5. First event is an unknown activity -> activity move
    # -----------------------------------------------------------------------
    def test_first_event_unknown(self):
        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        string_test_source(["z"]).pipe(checker).subscribe()
        result = checker.get_trace_conformance("case_1")
        self.assertEqual(len(result.alignment), 1)
        self.assertEqual(_log(result.alignment[0]), "z")
        self.assertIsNone(result.alignment[0].alignment_step[1])
        self.assertEqual(result.get_last().cost, 1.0)

    # -----------------------------------------------------------------------
    #  6. Multiple cases interleaved on the stream
    #     Each case's prefix-alignment is maintained independently.
    # -----------------------------------------------------------------------
    def test_interleaved_cases(self):

        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)

        string_test_source(["abce", 'acd']).pipe(checker).subscribe()

        # case_A: ⟨a,b,c⟩ all sync, cost 0
        res_a = checker.get_trace_conformance("case_1")
        self.assertEqual(res_a.get_last().cost, 1.0)

        # case_B: ⟨a,c,d⟩ all sync, cost 0  (same as Fig. 6 γ1)
        res_b = checker.get_trace_conformance("case_2")
        self.assertEqual(res_b.get_last().cost, 0.0)

    # -----------------------------------------------------------------------
    #  7. Revert with k=2
    #     Existing alignment for ⟨a,b,x,c,d⟩
    #     Then receive 'b'.  With k=2 we revert last 2 moves and recompute.
    # -----------------------------------------------------------------------
    def test_revert_k2_fig7(self):
        checker = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm, revert_num=2)
        string_test_source(["abxcdbe"]).pipe(checker).subscribe()

        result = checker.get_trace_conformance("case_1")

        self.assertEqual(result.get_last().cost, 2.0)

    # -----------------------------------------------------------------------
    #  8. Revert with k=∞inf (revert_num=None -> full re-alignment)
    # -----------------------------------------------------------------------
    def test_revert_full_realignment(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)  # revert_num=None

        string_test_source(["abxcdb"]).pipe(mapper).subscribe()

        result = mapper.get_trace_conformance("case_1")
        log_activities = [_log(s) for s in result.alignment if _log(s) is not None]
        self.assertEqual(log_activities, ["a", "b", "x", "c", "d", "b"])
        self.assertEqual(result.get_last().cost, 2.0)

    # -----------------------------------------------------------------------
    #  9. Synchronous move updates marking correctly
    # -----------------------------------------------------------------------
    def test_marking_after_sync(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        mapper.transform(_make_event("a", "test", 0))

        result = mapper.get_trace_conformance("test")
        marking = result.get_last().marking

        place_names = {p.name for p in marking}
        self.assertEqual(place_names, {"p1", "p2"})

    # -----------------------------------------------------------------------
    #  10. Activity move does NOT advance the marking
    #      Unknown activity 'x' -> marking stays the same
    #      Paper: activity move appends (a, ≫) with no model progress
    # -----------------------------------------------------------------------
    def test_activity_move_preserves_marking(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        mapper.transform(_make_event("a", "test", 0))
        marking_after_a = dict(mapper.get_trace_conformance("test").get_last().marking)

        mapper.transform(_make_event("x", "test", 1))
        marking_after_x = dict(mapper.get_trace_conformance("test").get_last().marking)

        self.assertEqual(marking_after_a, marking_after_x)

    # -----------------------------------------------------------------------
    #  11. Cost monotonicity — prefix-alignment cost never decreases when
    #       only appending activity moves  (Proposition 1)
    # -----------------------------------------------------------------------
    def test_cost_monotonicity(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        costs = []

        string_test_source(["axbyc"]).pipe(mapper).subscribe(on_next=lambda x: costs.append(x["case_1"].get_last().cost))

        for i in range(1, len(costs)):
            self.assertGreaterEqual(costs[i], costs[i - 1],f"cost decreased at step {i}: {costs}")

    # -----------------------------------------------------------------------
    #  12. Full trace through the loop  ⟨a, c, d, b, c, d, e⟩
    #      The redo-loop fires t6(τ) to go from p5 back to p1,p2
    # -----------------------------------------------------------------------
    def test_trace_with_loop(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        string_test_source(["acdbcde"]).pipe(mapper).subscribe()

        result = mapper.get_trace_conformance("case_1")

        self.assertEqual(result.get_last().cost, 0.0)

    # -----------------------------------------------------------------------
    #  13. Silent (τ) transitions cost 0
    # -----------------------------------------------------------------------
    def test_silent_transition_cost_zero(self):
        mapper = PrefixAlignmentsConformanceMapper(self.net, self.im, self.fm)
        # ⟨a, c, d, e⟩ is a conforming trace via t1, t3, t4, t7 — cost 0
        string_test_source(["acdc"]).pipe(mapper).subscribe()
        result = mapper.get_trace_conformance("case_1")
        self.assertEqual(result.get_last().cost,  0.0)


if __name__ == "__main__":
    unittest.main()
