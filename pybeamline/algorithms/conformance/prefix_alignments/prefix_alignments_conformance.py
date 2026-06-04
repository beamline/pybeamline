from typing import Dict, List, Optional, Any
from pm4py.objects.log.obj import Trace, Event
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.petri_net.semantics import enabled_transitions, execute
from pm4py.algo.conformance.alignments.petri_net.variants import state_equation_a_star
from pm4py.algo.conformance.alignments.petri_net.variants.state_equation_a_star import Parameters as AlignParameters
from pm4py.objects.petri_net.utils.align_utils import STD_MODEL_LOG_MOVE_COST, STD_SYNC_COST
from pybeamline.algorithms.conformance.prefix_alignments.case_state import CaseState, AlignmentState
from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap
import copy

"""
Incremental prefix-alignment conformance checker.

Based on:
  van Zelst, S.J., Bolt, A., & van der Aalst, W.M.P. (2019).
  "Online conformance checking: relating event streams to process
  models using prefix-alignments."
  Int J Data Sci Anal, 8, 269–284.
  https://doi.org/10.1007/s41060-017-0078-6
"""

class PrefixAlignmentsConformanceMapper(BaseMap[BEvent, Dict]):

    def __init__(self,
                 reference_model: PetriNet,
                 init_marking: Marking,
                 final_marking: Marking,
                 revert_num: int = None,
                 log_cost_fun: Dict = None,
                 model_cost_fun: Dict = None):
        self.prefixAlignmentConformanceChecker = PrefixAlignmentConformanceChecker(
            reference_model,
            init_marking,
            final_marking,
            revert_num = revert_num,
            log_cost_fun = log_cost_fun,
            model_cost_fun = model_cost_fun
        )

    def transform(self, value: BEvent) -> Optional[List[Dict]]:
        self.prefixAlignmentConformanceChecker.handle_event(value)
        return [self.prefixAlignmentConformanceChecker.get_conformance_dict()]

    def get_trace_conformance(self, case_id: str) -> Optional[CaseState]:
        return self.prefixAlignmentConformanceChecker.get_trace_conformance(case_id)

    def get_fitness(self, case_id: str) -> Optional[float]:
        return self.prefixAlignmentConformanceChecker.get_fitness(case_id)

    def get_conformance_dict(self) -> Dict[str, CaseState]:
        return self.prefixAlignmentConformanceChecker.get_conformance_dict()


class PrefixAlignmentConformanceChecker:

    def __init__(self,
                 reference_model: PetriNet,
                 initial_marking: Marking,
                 final_marking: Marking,
                 revert_num: int = None,
                 log_cost_fun: Dict = None,
                 model_cost_fun: Dict = None):
        self.reference_model: PetriNet = reference_model
        self.initial_marking: Marking = initial_marking
        self.final_marking: Marking = final_marking
        self.case_administration : Dict[str, CaseState] = {}
        self.valid_transition_labels = [t.label for t in self.reference_model.transitions]
        self.revert_num = revert_num
        self.log_cost_fun = log_cost_fun
        self.model_cost_fun = model_cost_fun


    def handle_event(self, event: BEvent):

        if event.get_trace_name() not in self.case_administration:
            self.case_administration[event.get_trace_name()] = CaseState()

        case = self.case_administration[event.get_trace_name()]
        activity_name = event.get_event_name()

        last_alignment_step = self.init_last_alignment_step(case)

        if not self.is_valid_transition(activity_name):
            next_alignment_step = self.init_next_alignment_step(case, activity_name)
            next_alignment_step.alignment_step = (activity_name, None)
            next_alignment_step.cost += self.calculate_cost(activity_name, None, self.log_cost_fun, self.model_cost_fun)
            case.alignment.append(next_alignment_step)
        else:
            _enabled_transitions = enabled_transitions(self.reference_model, last_alignment_step.marking)
            sync_trans = next((t for t in _enabled_transitions if t.label == activity_name), None)
            if sync_trans:
                next_alignment_step = self.init_next_alignment_step(case, activity_name)
                next_alignment_step.alignment_step = (activity_name, sync_trans)
                new_marking = execute(sync_trans, self.reference_model, last_alignment_step.marking)
                next_alignment_step.marking = new_marking
                case.alignment.append(next_alignment_step)
            else:
                head_alignment = case.revert(self.revert_num)
                trace = self.activity_sequence_to_trace(head_alignment)
                trace.append(Event({"concept:name": activity_name}))
                prev_alignment = self.init_last_alignment_step(case)
                optimal_alignment = state_equation_a_star.apply(
                    trace,
                    self.reference_model,
                    prev_alignment.marking,
                    self.final_marking,
                    parameters={
                        AlignParameters.PARAM_MODEL_COST_FUNCTION: self.a_star_model_cost(),
                        AlignParameters.PARAM_SYNC_COST_FUNCTION: self.a_star_sync_cost(),
                        AlignParameters.PARAM_TRACE_COST_FUNCTION: [STD_MODEL_LOG_MOVE_COST + 1] * len(trace),
                    }
                )
                converted_head = self.pm4py_alignment_to_case_state(
                    optimal_alignment,
                    prev_alignment.marking,
                    prev_alignment.cost,
                )
                case.join(converted_head)

    def get_trace_conformance(self, case_id: str) -> Optional[CaseState]:
        if case_id not in self.case_administration:
            return None
        return copy.deepcopy(self.case_administration[case_id])


    def get_fitness(self, case_id: str) -> Optional[float]:
        case = self.case_administration.get(case_id)
        if case is None or case.get_last() is None:
            return None
        actual_cost = case.get_last().cost
        n_log = sum(1 for s in case.alignment if s.alignment_step[0] is not None)
        if n_log == 0:
            return 1.0

        return 1.0 - actual_cost / n_log

    def a_star_model_cost(self):
        return {
            t: (0 if t.label is None else STD_MODEL_LOG_MOVE_COST)
            for t in self.reference_model.transitions
        }

    def a_star_sync_cost(self):
        return {
            t: STD_SYNC_COST
            for t in self.reference_model.transitions
            if t.label is not None
        }


    def get_conformance_dict(self) -> Dict[str, CaseState]:
        return copy.deepcopy(self.case_administration)


    def is_valid_transition(self, activity_name: str) -> bool:
        return activity_name in self.valid_transition_labels

    def init_next_alignment_step(self, case: CaseState, activity_name: str) -> AlignmentState:
        next_alignment_step = AlignmentState()
        last_step = case.get_last()
        if last_step is not None:
            next_alignment_step = copy.deepcopy(last_step)
        else:
            next_alignment_step.marking = Marking(self.initial_marking)
        next_alignment_step.alignment_step = (activity_name, None)
        return next_alignment_step

    def init_last_alignment_step(self, case: CaseState) -> AlignmentState:
        last_step = case.get_last()
        if last_step is not None:
            return last_step
        last_alignment_step = AlignmentState()
        last_alignment_step.marking = Marking(self.initial_marking)
        return last_alignment_step


    def pm4py_alignment_to_case_state(self, alignment: Dict[str, Any], marking: Marking, cost: float) -> CaseState:
        converted_alignments: List[AlignmentState] = []
        if "alignment" in alignment.keys():
            for i, pair in enumerate(alignment["alignment"]):
                alignment_state = AlignmentState()
                current_marking = converted_alignments[i-1].marking if i > 0 else marking
                alignment_state.marking = current_marking
                alignment_state.cost = converted_alignments[i-1].cost if i > 0 else cost
                log_move = pair[0] if pair[0] != ">>" else None
                if pair[1] == ">>":
                    model_move = None
                else:
                    model_move = self.get_transition_by_label(pair[1], current_marking)
                alignment_state.alignment_step = (log_move, model_move)
                alignment_state.cost += self.calculate_cost(log_move, model_move, self.log_cost_fun, self.model_cost_fun)
                if model_move is not None:
                    new_marking = execute(model_move, self.reference_model, alignment_state.marking)
                    alignment_state.marking = new_marking
                converted_alignments.append(alignment_state)
        while converted_alignments and converted_alignments[-1].alignment_step[0] is None:
            converted_alignments.pop()
        return CaseState(converted_alignments)

    def get_transition_by_label(self, label, marking: Marking = None) -> Optional[PetriNet.Transition]:
        candidates = [t for t in self.reference_model.transitions if t.label == label]
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        if marking is not None:
            enabled = enabled_transitions(self.reference_model, marking)
            for t in candidates:
                if t in enabled:
                    return t
        return candidates[0]

    @staticmethod
    def calculate_cost(log_move: Optional[str], model_move: Optional[PetriNet.Transition], log_cost: Dict = None, model_cost: Dict = None) -> float:
        cost = 0.0
        if log_move is not None and model_move is not None:
            return cost
        if log_move is not None:
            if log_cost is not None and log_move in log_cost:
                cost += log_cost[log_move]
            else:
                cost += 1.0
        if model_move is not None:
            if model_move.label is None:
                return cost
            if model_cost is not None and model_move in model_cost:
                cost += model_cost[model_move]
            else:
                cost += 1.0
        return cost


    @staticmethod
    def activity_sequence_to_trace(case_state: CaseState) -> Trace:
        trace = Trace()
        for element in case_state.alignment:
            activity_name = element.alignment_step[0]
            if activity_name is not None:
                trace.append(Event({"concept:name": activity_name}))
        return trace





