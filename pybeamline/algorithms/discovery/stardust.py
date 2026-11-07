"""STARDUST discovery for streams of events with explicit case completion.

STARDUST keeps a prefix tree for running cases and a frequency table for
recently active trace variants. A process discovery function is run once the
initialization window is full, then again only when frequency-based sampling
changes the sampled behavior.

Algorithm described by Pasquadibisceglie et al., "STARDUST: A Novel Process
Mining Approach to Discover Evolving Models From Trace Streams," IEEE
Transactions on Services Computing, 16(4), 2023,
https://doi.org/10.1109/TSC.2022.3215502.
"""

from dataclasses import dataclass, field
import math
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap


TraceVariant = Tuple[str, ...]
DiscoveryAlgorithm = Callable[[Mapping[TraceVariant, int]], Any]
ConformanceEvaluator = Callable[[Any, TraceVariant], Any]


@dataclass(frozen=True)
class StardustConfig:
    """Configurable STARDUST parameters.

    Args:
        initialization_traces: Completed cases to collect before the first
            model is discovered (``h`` in the paper).
        sampling_rate: Fraction of active trace occurrences to cover with the
            most frequent variants (``m`` in the paper, between 0 and 1).
        forgetting_size: Number of completed cases after which an inactive
            variant is removed (``w`` in the paper).
        completion_activity: Activity label that marks a completed case.
            STARDUST uses ``?`` for this marker by default.
        max_consecutive_repetitions: Cap on a run of identical activities.
            The reference implementation caps repeated self-loops at two;
            set to ``None`` to preserve every repetition.
    """

    initialization_traces: int = 100
    sampling_rate: float = 0.8
    forgetting_size: int = 100
    completion_activity: str = "?"
    max_consecutive_repetitions: Optional[int] = 2

    def __post_init__(self) -> None:
        if (not isinstance(self.initialization_traces, int)
                or isinstance(self.initialization_traces, bool)
                or self.initialization_traces < 1):
            raise ValueError("initialization_traces must be at least 1")
        if (isinstance(self.sampling_rate, bool)
                or not isinstance(self.sampling_rate, (int, float))
                or not math.isfinite(self.sampling_rate)
                or not 0 < self.sampling_rate <= 1):
            raise ValueError("sampling_rate must be greater than 0 and at most 1")
        if (not isinstance(self.forgetting_size, int)
                or isinstance(self.forgetting_size, bool)
                or self.forgetting_size < 1):
            raise ValueError("forgetting_size must be at least 1")
        if self.completion_activity is None:
            raise ValueError("completion_activity cannot be None")
        if (self.max_consecutive_repetitions is not None
                and (not isinstance(self.max_consecutive_repetitions, int)
                     or isinstance(self.max_consecutive_repetitions, bool)
                     or self.max_consecutive_repetitions < 1)):
            raise ValueError("max_consecutive_repetitions must be at least 1 or None")


@dataclass(frozen=True)
class StardustUpdate:
    """A model update or prequential observation for a completed case."""

    completed_traces: int
    case_id: Any
    initial_model: bool
    drift_detected: bool
    model_updated: bool
    active_variants: Mapping[TraceVariant, int]
    sampled_variants: Mapping[TraceVariant, int]
    model: Any
    conformance: Any = None


@dataclass
class _VariantStats:
    frequency: int
    last_seen: int


@dataclass
class _PrefixNode:
    activity: Optional[str]
    parent: Optional["_PrefixNode"]
    children: Dict[str, "_PrefixNode"] = field(default_factory=dict)
    active_cases: int = 0


def frequency_based_sample(
        variants: Mapping[TraceVariant, int], sampling_rate: float
) -> Dict[TraceVariant, int]:
    """Return the smallest most-frequent prefix covering ``sampling_rate``.

    Ties retain the input mapping's iteration order, making samples stable
    between runs. The returned mapping preserves variant frequencies, as the
    process discovery algorithm consumes a multiset rather than a set.
    """
    if (isinstance(sampling_rate, bool)
            or not isinstance(sampling_rate, (int, float))
            or not math.isfinite(sampling_rate)
            or not 0 < sampling_rate <= 1):
        raise ValueError("sampling_rate must be greater than 0 and at most 1")
    if not variants:
        return {}
    if any(not isinstance(frequency, int) or isinstance(frequency, bool) or frequency < 1
           for frequency in variants.values()):
        raise ValueError("variant frequencies must be positive integers")

    total = sum(variants.values())
    target = sampling_rate * total
    selected: Dict[TraceVariant, int] = {}
    covered = 0
    ranked = sorted(variants.items(), key=lambda item: -item[1])
    for variant, frequency in ranked:
        selected[variant] = frequency
        covered += frequency
        if covered >= target:
            break
    return selected


def _discover_inductive_petri_net(
        variants: Mapping[TraceVariant, int]
) -> Any:
    """Discover a Petri net with PM4Py's Inductive Miner (the default backend)."""
    from pm4py import discover_petri_net_inductive
    from pm4py.objects.log.obj import Event, EventLog, Trace

    log = EventLog()
    for variant, frequency in variants.items():
        for _ in range(frequency):
            trace = Trace()
            for activity in variant:
                trace.append(Event({"concept:name": activity}))
            log.append(trace)
    return discover_petri_net_inductive(log)


class StardustMapper(BaseMap[BEvent, StardustUpdate]):
    """Incrementally discover process models when sampled behavior changes.

    Input events are ``BEvent`` objects. A case is completed when an event
    with the configured completion activity is received. The callback receives
    a mapping from activity tuples to frequencies and should return a process
    model. If no callback is supplied, PM4Py's Inductive Miner is used. An
    optional conformance callback is evaluated on each completed trace after
    initialization and before that trace can trigger a model update.
    """

    def __init__(
            self,
            config: Optional[StardustConfig] = None,
            discovery_algorithm: Optional[DiscoveryAlgorithm] = None,
            conformance_evaluator: Optional[ConformanceEvaluator] = None,
    ) -> None:
        self.config = config or StardustConfig()
        self.discovery_algorithm = (
            _discover_inductive_petri_net
            if discovery_algorithm is None else discovery_algorithm
        )
        self.conformance_evaluator = conformance_evaluator

        self._root = _PrefixNode(activity=None, parent=None)
        self._running_cases: Dict[Any, _PrefixNode] = {}
        self._variants: Dict[TraceVariant, _VariantStats] = {}
        self._completed_traces = 0
        self._previous_sampled_behavior: Optional[frozenset] = None
        self._current_model: Any = None

    @property
    def completed_traces(self) -> int:
        return self._completed_traces

    @property
    def running_case_count(self) -> int:
        return len(self._running_cases)

    @property
    def current_model(self) -> Any:
        return self._current_model

    @property
    def active_variants(self) -> Dict[TraceVariant, int]:
        """A copy of the active trace-variant frequency table."""
        return {variant: stats.frequency for variant, stats in self._variants.items()}

    @property
    def prefix_node_count(self) -> int:
        """Number of currently retained prefix nodes, excluding the root."""
        count = 0
        pending = list(self._root.children.values())
        while pending:
            node = pending.pop()
            count += 1
            pending.extend(node.children.values())
        return count

    def transform(self, event: BEvent) -> Optional[List[StardustUpdate]]:
        if not isinstance(event, BEvent):
            raise TypeError(f"StardustMapper accepts BEvent values, got {type(event)}")

        case_id = event.get_trace_name()
        activity = event.get_event_name()
        if activity == self.config.completion_activity:
            update = self.complete_case(case_id)
            return [update] if update is not None else None

        self._record_activity(case_id, activity)
        return None

    def complete_case(self, case_id: Any) -> Optional[StardustUpdate]:
        """Complete a case explicitly, for sources without a sentinel event."""
        node = self._running_cases.pop(case_id, None)
        if node is None:
            raise KeyError(f"Cannot complete case {case_id!r}: no events were recorded")
        trace = self._trace_for(node)
        self._release_prefix(node)

        conformance = None
        if self._previous_sampled_behavior is not None and self.conformance_evaluator is not None:
            conformance = self.conformance_evaluator(self._current_model, trace)

        self._completed_traces += 1
        stats = self._variants.get(trace)
        if stats is None:
            self._variants[trace] = _VariantStats(frequency=1, last_seen=self._completed_traces)
        else:
            stats.frequency += 1
            stats.last_seen = self._completed_traces

        self._forget_inactive_variants()
        return self._update_model_if_needed(case_id, conformance)

    def _record_activity(self, case_id: Any, activity: str) -> None:
        parent = self._running_cases.get(case_id, self._root)
        if self._is_repetition_capped(parent, activity):
            return

        child = parent.children.get(activity)
        if child is None:
            child = _PrefixNode(activity=activity, parent=parent)
            parent.children[activity] = child
        child.active_cases += 1
        self._running_cases[case_id] = child

    def _is_repetition_capped(self, node: _PrefixNode, activity: str) -> bool:
        cap = self.config.max_consecutive_repetitions
        if cap is None or node is self._root or node.activity != activity:
            return False

        run_length = 0
        current: Optional[_PrefixNode] = node
        while current is not None and current is not self._root and current.activity == activity:
            run_length += 1
            current = current.parent
        return run_length >= cap

    @staticmethod
    def _trace_for(node: _PrefixNode) -> TraceVariant:
        activities = []
        while node is not None and node.parent is not None:
            activities.append(node.activity)
            node = node.parent
        activities.reverse()
        return tuple(activities)

    def _release_prefix(self, node: _PrefixNode) -> None:
        while node is not None and node.parent is not None:
            parent = node.parent
            node.active_cases -= 1
            if node.active_cases == 0 and not node.children:
                parent.children.pop(node.activity, None)
            node = parent

    def _forget_inactive_variants(self) -> None:
        cutoff = self._completed_traces - self.config.forgetting_size
        expired = [
            variant for variant, stats in self._variants.items()
            if stats.last_seen <= cutoff
        ]
        for variant in expired:
            del self._variants[variant]

    def _update_model_if_needed(
            self, case_id: Any, conformance: Any = None
    ) -> Optional[StardustUpdate]:
        initial_model = self._previous_sampled_behavior is None
        if initial_model and self._completed_traces < self.config.initialization_traces:
            return None

        active = self.active_variants
        sampled = frequency_based_sample(active, self.config.sampling_rate)
        sampled_behavior = frozenset(sampled)
        drift_detected = (
            not initial_model and sampled_behavior != self._previous_sampled_behavior
        )
        if not initial_model and not drift_detected:
            if self.conformance_evaluator is None:
                return None
            return StardustUpdate(
                completed_traces=self._completed_traces,
                case_id=case_id,
                initial_model=False,
                drift_detected=False,
                model_updated=False,
                active_variants=active,
                sampled_variants=dict(sampled),
                model=self._current_model,
                conformance=conformance,
            )

        model = self.discovery_algorithm(dict(sampled))
        self._current_model = model
        self._previous_sampled_behavior = sampled_behavior
        return StardustUpdate(
            completed_traces=self._completed_traces,
            case_id=case_id,
            initial_model=initial_model,
            drift_detected=drift_detected,
            model_updated=True,
            active_variants=active,
            sampled_variants=dict(sampled),
            model=model,
            conformance=conformance,
        )


def stardust_mapper(
        initialization_traces: int = 100,
        sampling_rate: float = 0.8,
        forgetting_size: int = 100,
        completion_activity: str = "?",
        max_consecutive_repetitions: Optional[int] = 2,
        discovery_algorithm: Optional[DiscoveryAlgorithm] = None,
        conformance_evaluator: Optional[ConformanceEvaluator] = None,
) -> StardustMapper:
    """Create a STARDUST ``BaseMap`` for a pybeamline stream.

    All STARDUST hyperparameters are exposed here. Pipe the returned mapper
    after a source, for example ``source.pipe(stardust_mapper(...))``.
    """
    config = StardustConfig(
        initialization_traces=initialization_traces,
        sampling_rate=sampling_rate,
        forgetting_size=forgetting_size,
        completion_activity=completion_activity,
        max_consecutive_repetitions=max_consecutive_repetitions,
    )
    return StardustMapper(
        config=config,
        discovery_algorithm=discovery_algorithm,
        conformance_evaluator=conformance_evaluator,
    )
