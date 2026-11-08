"""C-3PA streaming conformance checking over a finite proxy-log trie.

The reference Petri net is sampled with PM4Py semantics to create the finite
proxy log used by C-3PA. Callers may provide proxy traces directly when they
already have a representative model language sample.
"""

from collections import OrderedDict, deque
from dataclasses import dataclass, field
import random
from typing import Dict, Hashable, Iterable, List, Mapping, Optional, Sequence, Tuple

from pm4py.objects.petri_net.obj import Marking, PetriNet
from pm4py.objects.petri_net.semantics import enabled_transitions, execute

from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap


_Move = Tuple[Optional[str], Optional[str]]


@dataclass(frozen=True)
class C3PAResult:
    """Best current C-3PA prefix alignment and its three measures.

    ``None`` on either side of an alignment pair denotes a skip (``>>``).
    ``cost`` counts log and model moves. ``completeness`` estimates the
    behavior that may have occurred before monitoring began. ``confidence``
    is the average shortest remaining distance to a trie leaf from the chosen
    state.
    """

    alignment: Tuple[_Move, ...]
    cost: int
    completeness: int
    confidence: float

    @property
    def guiding_cost(self) -> int:
        """Return the cost used to compare states (conformance + completeness)."""
        return self.cost + self.completeness


@dataclass(eq=False)
class _TrieNode:
    identifier: int
    activity: Optional[str]
    depth: int
    parent: Optional["_TrieNode"] = None
    children: Dict[str, "_TrieNode"] = field(default_factory=dict)
    is_leaf: bool = False
    min_remaining: int = 0
    confidence: float = 0.0


class _Trie:
    def __init__(self, traces: Iterable[Sequence[str]]) -> None:
        self.root = _TrieNode(identifier=0, activity=None, depth=0)
        self.nodes: List[_TrieNode] = [self.root]
        self.nodes_by_activity: Dict[str, List[_TrieNode]] = {}
        self.trace_count = 0

        for trace in traces:
            self.add_trace(trace)
        self._annotate(self.root)
        for nodes in self.nodes_by_activity.values():
            nodes.sort(key=lambda node: (node.depth, node.identifier))

    def add_trace(self, trace: Sequence[str]) -> None:
        node = self.root
        self.trace_count += 1
        for activity in trace:
            child = node.children.get(activity)
            if child is None:
                child = _TrieNode(
                    identifier=len(self.nodes),
                    activity=activity,
                    depth=node.depth + 1,
                    parent=node,
                )
                node.children[activity] = child
                self.nodes.append(child)
                self.nodes_by_activity.setdefault(activity, []).append(child)
            node = child
        node.is_leaf = True

    def _annotate(self, node: _TrieNode) -> Tuple[int, int, int]:
        """Return descendant leaf count, distance sum, and minimum distance."""
        leaf_count = 1 if node.is_leaf else 0
        distance_sum = 0
        minimum = 0 if node.is_leaf else None
        for child in node.children.values():
            child_count, child_sum, child_minimum = self._annotate(child)
            leaf_count += child_count
            distance_sum += child_sum + child_count
            distance = child_minimum + 1
            minimum = distance if minimum is None else min(minimum, distance)

        node.min_remaining = 0 if minimum is None else minimum
        node.confidence = distance_sum / leaf_count if leaf_count else 0.0
        return leaf_count, distance_sum, node.min_remaining


@dataclass(frozen=True)
class _State:
    node: _TrieNode
    alignment: Tuple[_Move, ...]
    cost: int
    completeness: int
    last_progress: int

    @property
    def guiding_cost(self) -> int:
        return self.cost + self.completeness


class C3PAMapper(BaseMap[BEvent, Dict[Hashable, C3PAResult]]):
    """Stream mapper implementing C-3PA's trie-based prefix alignments.

    If ``proxy_traces`` is omitted, complete model traces are sampled from the
    supplied PM4Py Petri net. The mapper emits a snapshot mapping active case
    identifiers to their latest ``C3PAResult`` after each input event.
    """

    def __init__(
            self,
            reference_model: Optional[PetriNet] = None,
            initial_marking: Optional[Marking] = None,
            final_marking: Optional[Marking] = None,
            proxy_traces: Optional[Iterable[Iterable[str]]] = None,
            num_model_traces: int = 2000,
            max_model_steps: int = 100,
            random_seed: int = 0,
            warm_start: bool = True,
            state_decay_time: Optional[int] = 100,
            max_states_per_case: int = 1000,
            max_cases: int = 10000,
    ) -> None:
        self.num_model_traces = _positive_integer("num_model_traces", num_model_traces)
        self.max_model_steps = _positive_integer("max_model_steps", max_model_steps)
        self.max_states_per_case = _positive_integer(
            "max_states_per_case", max_states_per_case
        )
        self.max_cases = _positive_integer("max_cases", max_cases)
        if state_decay_time is not None:
            state_decay_time = _positive_integer("state_decay_time", state_decay_time)
        self.state_decay_time = state_decay_time
        if not isinstance(random_seed, int) or isinstance(random_seed, bool):
            raise ValueError("random_seed must be an integer")
        if not isinstance(warm_start, bool):
            raise ValueError("warm_start must be a boolean")
        self.warm_start = warm_start

        if proxy_traces is None:
            if reference_model is None or initial_marking is None or final_marking is None:
                raise ValueError(
                    "provide proxy_traces or a Petri net with initial and final markings"
                )
            _validate_petri_net(reference_model, initial_marking, final_marking)
            traces = _sample_model_traces(
                reference_model,
                initial_marking,
                final_marking,
                self.num_model_traces,
                self.max_model_steps,
                random_seed,
            )
        else:
            if reference_model is not None:
                if not isinstance(reference_model, PetriNet):
                    raise TypeError("reference_model must be a PM4Py PetriNet")
                if (initial_marking is None) != (final_marking is None):
                    raise ValueError(
                        "initial_marking and final_marking must be provided together"
                    )
                if initial_marking is not None:
                    _validate_petri_net(reference_model, initial_marking, final_marking)
            elif initial_marking is not None or final_marking is not None:
                raise ValueError("markings require a reference_model")
            traces = _normalize_proxy_traces(proxy_traces)
            if not traces:
                raise ValueError("proxy_traces must contain at least one trace")

        self._trie = _Trie(traces)
        self.proxy_trace_count = self._trie.trace_count
        self._event_index = 0
        self._case_states: "OrderedDict[Hashable, List[_State]]" = OrderedDict()

    @property
    def active_case_count(self) -> int:
        return len(self._case_states)

    @property
    def tracked_state_count(self) -> int:
        return sum(len(states) for states in self._case_states.values())

    def transform(self, event: BEvent) -> Optional[List[Dict[Hashable, C3PAResult]]]:
        case_id, activity = _read_event(event)
        self._event_index += 1
        existing = self._case_states.get(case_id)

        if existing is None:
            if len(self._case_states) >= self.max_cases:
                self._case_states.popitem(last=False)
            states = [_State(self._trie.root, (), 0, 0, self._event_index)]
        else:
            states = [state for state in existing if self._is_fresh(state)]
            if not states:
                states = [_State(self._trie.root, (), 0, 0, self._event_index)]

        advanced = self._advance(states, activity, self._event_index)
        self._case_states[case_id] = self._limit_states(advanced)
        self._case_states.move_to_end(case_id)
        return [self.get_conformance_dict()]

    def get_trace_conformance(self, case_id: Hashable) -> Optional[C3PAResult]:
        states = self._case_states.get(case_id)
        return self._result(min(states, key=_state_sort_key)) if states else None

    def get_conformance_dict(self) -> Dict[Hashable, C3PAResult]:
        return {
            case_id: self._result(min(states, key=_state_sort_key))
            for case_id, states in self._case_states.items()
            if states
        }

    def _is_fresh(self, state: _State) -> bool:
        return (
            self.state_decay_time is None
            or self._event_index - state.last_progress <= self.state_decay_time
        )

    def _advance(
            self,
            states: List[_State],
            activity: str,
            event_index: int,
    ) -> List[_State]:
        synchronous: List[_State] = []
        for state in states:
            child = state.node.children.get(activity)
            if child is not None:
                synchronous.append(_State(
                    child,
                    state.alignment + ((activity, activity),),
                    state.cost,
                    state.completeness,
                    event_index,
                ))
        if synchronous:
            return self._limit_states(synchronous)

        cost_limit = min(state.guiding_cost for state in states) + 1
        candidates: List[_State] = []
        for state in states:
            # A log move consumes this event while leaving the model position
            # unchanged. The state progress time is therefore not reset.
            candidates.append(_State(
                state.node,
                state.alignment + ((activity, None),),
                state.cost + 1,
                state.completeness,
                state.last_progress,
            ))

            remaining_model_budget = cost_limit - state.guiding_cost
            if remaining_model_budget > 0:
                candidates.extend(self._model_sync_moves(
                    state, activity, event_index, remaining_model_budget
                ))

            if self.warm_start:
                for target in self._trie.nodes_by_activity.get(activity, ()):
                    # The target event itself is observed; only the path
                    # preceding it contributes to missing-prefix completeness.
                    completeness = max(state.completeness, target.depth - 1)
                    candidate = _State(
                        target,
                        state.alignment + ((activity, activity),),
                        state.cost,
                        completeness,
                        event_index,
                    )
                    if candidate.guiding_cost <= cost_limit:
                        candidates.append(candidate)

        within_limit = [
            state for state in candidates
            if state.guiding_cost <= cost_limit
        ]
        return self._limit_states(within_limit or candidates)

    def _model_sync_moves(
            self,
            state: _State,
            activity: str,
            event_index: int,
            max_model_moves: int,
    ) -> List[_State]:
        result = []
        queue = deque([(state.node, ())])
        while queue:
            node, model_path = queue.popleft()
            synchronous_child = node.children.get(activity)
            if synchronous_child is not None and model_path:
                model_moves = tuple((None, label) for label in model_path)
                result.append(_State(
                    synchronous_child,
                    state.alignment + model_moves + ((activity, activity),),
                    state.cost + len(model_path),
                    state.completeness,
                    event_index,
                ))
            if len(model_path) >= max_model_moves:
                continue
            for child in node.children.values():
                queue.append((child, model_path + (child.activity,)))
        return result

    def _limit_states(self, states: Iterable[_State]) -> List[_State]:
        unique: Dict[Tuple[object, ...], _State] = {}
        for state in states:
            key = (
                state.node.identifier,
                state.cost,
                state.completeness,
                state.alignment,
            )
            previous = unique.get(key)
            if previous is None or state.last_progress > previous.last_progress:
                unique[key] = state
        ordered = sorted(unique.values(), key=_state_sort_key)
        return ordered[:self.max_states_per_case]

    @staticmethod
    def _result(state: _State) -> C3PAResult:
        return C3PAResult(
            alignment=state.alignment,
            cost=state.cost,
            completeness=state.completeness,
            confidence=state.node.confidence,
        )


def c3pa_conformance(
        reference_model: Optional[PetriNet] = None,
        initial_marking: Optional[Marking] = None,
        final_marking: Optional[Marking] = None,
        proxy_traces: Optional[Iterable[Iterable[str]]] = None,
        num_model_traces: int = 2000,
        max_model_steps: int = 100,
        random_seed: int = 0,
        warm_start: bool = True,
        state_decay_time: Optional[int] = 100,
        max_states_per_case: int = 1000,
        max_cases: int = 10000,
) -> C3PAMapper:
    """Create a C-3PA conformance operator from a PM4Py net or proxy traces.

    ``proxy_traces`` is an iterable of activity sequences. If omitted, the
    supplied PM4Py net and markings are used to sample complete traces.
    """
    return C3PAMapper(
        reference_model=reference_model,
        initial_marking=initial_marking,
        final_marking=final_marking,
        proxy_traces=proxy_traces,
        num_model_traces=num_model_traces,
        max_model_steps=max_model_steps,
        random_seed=random_seed,
        warm_start=warm_start,
        state_decay_time=state_decay_time,
        max_states_per_case=max_states_per_case,
        max_cases=max_cases,
    )


def _normalize_proxy_traces(
        proxy_traces: Iterable[Iterable[str]],
) -> List[Tuple[str, ...]]:
    traces: List[Tuple[str, ...]] = []
    try:
        for trace in proxy_traces:
            activities = []
            for activity in trace:
                if isinstance(activity, Mapping):
                    activity = activity.get("concept:name")
                if not isinstance(activity, str):
                    raise TypeError("proxy trace activities must be strings")
                activities.append(activity)
            traces.append(tuple(activities))
    except TypeError as error:
        if "proxy trace activities" in str(error):
            raise
        raise TypeError("proxy_traces must be an iterable of activity sequences") from error
    return traces


def _sample_model_traces(
        net: PetriNet,
        initial_marking: Marking,
        final_marking: Marking,
        trace_count: int,
        max_steps: int,
        random_seed: int,
) -> List[Tuple[str, ...]]:
    rng = random.Random(random_seed)
    traces: List[Tuple[str, ...]] = []
    attempts = max(trace_count * 20, trace_count)
    for _ in range(attempts):
        if len(traces) >= trace_count:
            break
        marking = Marking(initial_marking)
        activities: List[str] = []
        completed = marking == final_marking
        for _step in range(max_steps):
            if completed:
                break
            enabled = sorted(
                enabled_transitions(net, marking),
                key=lambda transition: (
                    str(transition.name),
                    "" if transition.label is None else str(transition.label),
                ),
            )
            if not enabled:
                break
            transition = rng.choice(enabled)
            marking = execute(transition, net, marking)
            if transition.label is not None:
                activities.append(str(transition.label))
            completed = marking == final_marking
        if completed:
            traces.append(tuple(activities))
    if not traces:
        raise ValueError(
            "could not sample a complete proxy trace; increase max_model_steps "
            "or provide proxy_traces explicitly"
        )
    return traces


def _validate_petri_net(
        net: PetriNet,
        initial_marking: Marking,
        final_marking: Marking,
) -> None:
    if not isinstance(net, PetriNet):
        raise TypeError("reference_model must be a PM4Py PetriNet")
    if not isinstance(initial_marking, Marking):
        raise TypeError("initial_marking must be a PM4Py Marking")
    if not isinstance(final_marking, Marking):
        raise TypeError("final_marking must be a PM4Py Marking")
    if any(place not in net.places for place in initial_marking):
        raise ValueError("initial_marking contains places outside reference_model")
    if any(place not in net.places for place in final_marking):
        raise ValueError("final_marking contains places outside reference_model")


def _read_event(event: BEvent) -> Tuple[Hashable, str]:
    if not isinstance(event, BEvent):
        raise TypeError("C-3PA accepts BEvent values")
    case_id = event.get_trace_name()
    activity = event.get_event_name()
    try:
        hash(case_id)
    except TypeError as error:
        raise TypeError("BEvent trace names must be hashable") from error
    if not isinstance(activity, str):
        raise TypeError("BEvent activity names must be strings")
    return case_id, activity


def _positive_integer(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("{} must be a positive integer".format(name))
    return value


def _alignment_sort_key(alignment: Tuple[_Move, ...]) -> Tuple[Tuple[int, str, int, str], ...]:
    return tuple(
        (
            1 if log_activity is None else 0,
            "" if log_activity is None else log_activity,
            1 if model_activity is None else 0,
            "" if model_activity is None else model_activity,
        )
        for log_activity, model_activity in alignment
    )


def _state_sort_key(state: _State) -> Tuple[object, ...]:
    return (
        state.guiding_cost,
        state.cost,
        state.completeness,
        -state.last_progress,
        state.node.identifier,
        _alignment_sort_key(state.alignment),
    )
