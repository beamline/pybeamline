"""Streaming discovery of Dynamic Condition Response graphs.

The miner maintains a bounded window of partial traces, derives an extended
directly-follows graph, then periodically mines Condition, Response, Include,
and Exclude relations from that summary.

Method described by Burattin, López, and Starklit, "Uncovering Change: A
Streaming Approach for Declarative Processes," SA4PM 2022.
"""

from collections import Counter, OrderedDict, defaultdict
from dataclasses import dataclass
from typing import Dict, FrozenSet, Iterable, List, Mapping, Optional, Set, Tuple

from pybeamline.bevent import BEvent
from pybeamline.models.dcr_graph import DCRGraph, DCRRelation
from pybeamline.stream.base_map import BaseMap


@dataclass(frozen=True)
class ActivityStatistics:
    """Extended-DFG statistics for one activity in the active trace window."""

    occurrences: int
    average_index: float
    trace_appearances: int
    average_first_occurrence: float


@dataclass(frozen=True)
class ExtendedDFG:
    """The finite directly-follows summary used for DCR relation discovery."""

    activities: Mapping[str, ActivityStatistics]
    relations: Mapping[DCRRelation, int]


_PATTERN_NAMES = {
    "condition": "Condition",
    "response": "Response",
    "sequence": "Sequence",
    "exclude": "Exclude",
    "include": "Include",
}
_OUTPUT_RELATION_NAMES = {
    "Condition": "conditions",
    "Response": "responses",
    "Include": "includes",
    "Exclude": "excludes",
}
_REDUCIBLE_RELATIONS = {
    "Condition": "conditions",
    "Response": "responses",
    "Include": "includes",
    "Exclude": "excludes",
}
_REDUCIBLE_PATTERN_NAMES = {
    name.lower(): name for name in _REDUCIBLE_RELATIONS
}
_DEFAULT_PATTERNS = ("Condition", "Response", "Exclude", "Include")


def _normalize_names(
        names: Iterable[str],
        valid_names: Mapping[str, str],
        parameter_name: str,
) -> Tuple[str, ...]:
    if isinstance(names, str):
        raise ValueError(f"{parameter_name} must be an iterable of names, not a string")
    try:
        normalized = []
        for name in names:
            canonical_name = valid_names.get(str(name).lower())
            if canonical_name is None:
                allowed = ", ".join(sorted(valid_names.values()))
                raise ValueError(f"Unknown {parameter_name} {name!r}; choose from {allowed}")
            if canonical_name not in normalized:
                normalized.append(canonical_name)
    except TypeError as error:
        raise ValueError(f"{parameter_name} must be an iterable of names") from error
    return tuple(normalized)


class DCRMiner(BaseMap[BEvent, DCRGraph]):
    """Discover DCR graphs from a stream of ``BEvent`` values.

    The event's trace name identifies its case and its event name is the DCR
    activity. Cases are partial traces: no explicit case-completion event is
    required. The least recently updated case is evicted when ``max_traces``
    is reached; each retained case can also be limited to its most recent
    ``max_events_per_trace`` events.
    """

    def __init__(
            self,
            model_update_frequency: int = 10,
            max_traces: Optional[int] = 20,
            max_events_per_trace: Optional[int] = 10,
            relations_threshold: int = 0,
            dcr_patterns: Iterable[str] = _DEFAULT_PATTERNS,
            transitive_reduction: Iterable[str] = ("Condition", "Response"),
    ) -> None:
        if (not isinstance(model_update_frequency, int)
                or isinstance(model_update_frequency, bool)
                or model_update_frequency < 1):
            raise ValueError("model_update_frequency must be a positive integer")
        self._validate_window_size("max_traces", max_traces)
        self._validate_window_size("max_events_per_trace", max_events_per_trace)
        if (not isinstance(relations_threshold, int)
                or isinstance(relations_threshold, bool)
                or relations_threshold < 0):
            raise ValueError("relations_threshold must be a nonnegative integer")

        self.dcr_patterns = _normalize_names(
            dcr_patterns, _PATTERN_NAMES, "dcr_patterns"
        )
        self.transitive_reduction = _normalize_names(
            transitive_reduction,
            _REDUCIBLE_PATTERN_NAMES,
            "transitive_reduction",
        )
        self.model_update_frequency = model_update_frequency
        self.max_traces = max_traces
        self.max_events_per_trace = max_events_per_trace
        self.relations_threshold = relations_threshold

        self._windowed_traces: "OrderedDict[object, List[str]]" = OrderedDict()
        self._observed_events = 0
        self._extended_dfg = ExtendedDFG(activities={}, relations={})
        self._current_model: Optional[DCRGraph] = None

    @staticmethod
    def _validate_window_size(name: str, value: Optional[int]) -> None:
        if value is not None and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 1):
            raise ValueError(f"{name} must be a positive integer or None")

    @property
    def observed_events(self) -> int:
        return self._observed_events

    @property
    def current_model(self) -> Optional[DCRGraph]:
        return self._current_model

    @property
    def windowed_traces(self) -> Dict[object, Tuple[str, ...]]:
        """A copy of the current case windows, ordered least to most recent."""
        return {case_id: tuple(trace) for case_id, trace in self._windowed_traces.items()}

    @property
    def extended_dfg(self) -> ExtendedDFG:
        """A copy of the current extended DFG statistics."""
        return ExtendedDFG(
            activities=dict(self._extended_dfg.activities),
            relations=dict(self._extended_dfg.relations),
        )

    @property
    def retained_event_count(self) -> int:
        return sum(len(trace) for trace in self._windowed_traces.values())

    def transform(self, event: BEvent) -> Optional[List[DCRGraph]]:
        if not isinstance(event, BEvent):
            raise TypeError(f"DCRMiner accepts BEvent values, got {type(event)}")

        case_id = event.get_trace_name()
        activity = event.get_event_name()
        try:
            hash(case_id)
        except TypeError as error:
            raise TypeError("BEvent trace names must be hashable") from error
        if not isinstance(activity, str):
            raise TypeError("BEvent activity names must be strings")

        if case_id in self._windowed_traces:
            trace = self._windowed_traces.pop(case_id)
        else:
            if self.max_traces is not None and len(self._windowed_traces) >= self.max_traces:
                self._windowed_traces.popitem(last=False)
            trace = []

        if (self.max_events_per_trace is not None
                and len(trace) >= self.max_events_per_trace):
            trace.pop(0)
        trace.append(activity)
        self._windowed_traces[case_id] = trace

        self._observed_events += 1
        self._refresh_extended_dfg()
        if self._observed_events % self.model_update_frequency == 0:
            self._current_model = self._discover_model()
            return [self._current_model]
        return None

    def _refresh_extended_dfg(self) -> None:
        counts = defaultdict(lambda: {
            "occurrences": 0,
            "index_sum": 0,
            "trace_appearances": 0,
            "first_occurrence_sum": 0,
        })
        relation_counts: Counter = Counter()

        for trace in self._windowed_traces.values():
            seen_in_trace: Set[str] = set()
            for index, activity in enumerate(trace):
                activity_counts = counts[activity]
                activity_counts["occurrences"] += 1
                activity_counts["index_sum"] += index
                if activity not in seen_in_trace:
                    seen_in_trace.add(activity)
                    activity_counts["trace_appearances"] += 1
                    activity_counts["first_occurrence_sum"] += index
                if index > 0:
                    relation_counts[(trace[index - 1], activity)] += 1

        activities = {}
        for activity, activity_counts in counts.items():
            activities[activity] = ActivityStatistics(
                occurrences=activity_counts["occurrences"],
                average_index=(
                    activity_counts["index_sum"] / activity_counts["occurrences"]
                ),
                trace_appearances=activity_counts["trace_appearances"],
                average_first_occurrence=(
                    activity_counts["first_occurrence_sum"]
                    / activity_counts["trace_appearances"]
                ),
            )

        self._extended_dfg = ExtendedDFG(
            activities=activities,
            relations=dict(relation_counts),
        )

    def _discover_model(self) -> DCRGraph:
        dfg = self._extended_dfg
        activities = set(dfg.activities)
        frequent_relations = {
            relation for relation, frequency in dfg.relations.items()
            if frequency > self.relations_threshold
        }
        requested_patterns = set(self.dcr_patterns)

        conditions: Set[DCRRelation] = set()
        if "Condition" in requested_patterns:
            for source, target in frequent_relations:
                source_stats = dfg.activities[source]
                target_stats = dfg.activities[target]
                if (source_stats.average_first_occurrence
                        < target_stats.average_first_occurrence
                        and source_stats.trace_appearances >= target_stats.trace_appearances):
                    conditions.add((source, target))

        responses: Set[DCRRelation] = set()
        if "Response" in requested_patterns:
            for source, target in frequent_relations:
                if dfg.activities[source].average_index < dfg.activities[target].average_index:
                    responses.add((source, target))

        includes: Set[DCRRelation] = set()
        excludes: Set[DCRRelation] = set()
        if requested_patterns.intersection({"Exclude", "Include"}):
            sequence_relations = self._mine_sequence_relations(activities, dfg.activities)
            includes, excludes = self._mine_include_exclude(
                activities,
                frequent_relations,
                sequence_relations,
                dfg.activities,
            )
            if "Include" not in requested_patterns:
                includes.clear()
            if "Exclude" not in requested_patterns:
                excludes.clear()

        relation_sets = {
            "conditions": conditions,
            "responses": responses,
            "includes": includes,
            "excludes": excludes,
        }
        for relation_name in self.transitive_reduction:
            attribute_name = _REDUCIBLE_RELATIONS[relation_name]
            relation_sets[attribute_name] = self._transitively_reduce(
                relation_sets[attribute_name]
            )

        return DCRGraph(
            activities=frozenset(activities),
            conditions=frozenset(relation_sets["conditions"]),
            responses=frozenset(relation_sets["responses"]),
            includes=frozenset(relation_sets["includes"]),
            excludes=frozenset(relation_sets["excludes"]),
        )

    @staticmethod
    def _mine_sequence_relations(
            activities: Set[str],
            statistics: Mapping[str, ActivityStatistics],
    ) -> Set[DCRRelation]:
        sequence_relations: Set[DCRRelation] = set()
        ordered_activities = sorted(activities)
        for index, first in enumerate(ordered_activities):
            first_stats = statistics[first]
            for second in ordered_activities[index + 1:]:
                second_stats = statistics[second]
                if (first_stats.average_first_occurrence
                        < second_stats.average_first_occurrence
                        and first_stats.average_index < second_stats.average_index):
                    sequence_relations.add((first, second))
                elif (first_stats.average_first_occurrence
                        > second_stats.average_first_occurrence
                        and first_stats.average_index > second_stats.average_index):
                    sequence_relations.add((second, first))
        return sequence_relations

    @classmethod
    def _mine_include_exclude(
            cls,
            activities: Set[str],
            dfg_relations: Set[DCRRelation],
            sequence_relations: Set[DCRRelation],
            statistics: Mapping[str, ActivityStatistics],
    ) -> Tuple[Set[DCRRelation], Set[DCRRelation]]:
        includes: Set[DCRRelation] = set()
        excludes: Set[DCRRelation] = set()

        # Self-exclusion captures activities observed at most once per trace.
        for activity in activities:
            activity_stats = statistics[activity]
            if (activity_stats.trace_appearances > 0
                    and activity_stats.occurrences == activity_stats.trace_appearances):
                excludes.add((activity, activity))

        # Precedence is derived from the mined sequence pattern.
        for source, target in sequence_relations:
            source_stats = statistics[source]
            target_stats = statistics[target]
            if source_stats.occurrences == target_stats.occurrences:
                excludes.add((target, target))
                includes.add((source, target))
            if (source, source) not in excludes:
                excludes.add((target, source))

        # Not-chain-succession excludes pairs without a direct-follow edge.
        reachability = cls._transitive_closure(activities, dfg_relations)
        for source in activities:
            for target in activities:
                if source == target or (source, target) in dfg_relations:
                    continue
                excludes.add((source, target))
                for intermediate in activities:
                    if (intermediate != source and intermediate != target
                            and intermediate in reachability[source]
                            and target in reachability[intermediate]):
                        includes.add((intermediate, target))

        # Remove exclude relations implied by a more specific sequence.
        for source, target in sequence_relations:
            between = {
                intermediate for intermediate in activities
                if (intermediate != source and intermediate != target
                    and intermediate in reachability[source]
                    and target in reachability[intermediate])
            }
            for sink in activities:
                if (source, sink) not in excludes or (target, sink) not in excludes:
                    continue
                if any((intermediate, sink) not in includes for intermediate in between):
                    excludes.discard((target, sink))

        return includes, excludes

    @staticmethod
    def _transitive_closure(
            activities: Set[str],
            relations: Set[DCRRelation],
    ) -> Dict[str, Set[str]]:
        reachable = {activity: set() for activity in activities}
        for source, target in relations:
            reachable[source].add(target)
        for intermediate in activities:
            for source in activities:
                if intermediate in reachable[source]:
                    reachable[source].update(reachable[intermediate])
        return reachable

    @classmethod
    def _transitively_reduce(
            cls, relations: Set[DCRRelation]
    ) -> Set[DCRRelation]:
        reduced = set(relations)
        for source, target in sorted(relations):
            candidate = (source, target)
            reduced.remove(candidate)
            reachable = cls._transitive_closure(
                {activity for relation in reduced for activity in relation},
                reduced,
            )
            if target not in reachable.get(source, set()):
                reduced.add(candidate)
        return reduced


def dcr_miner(
        model_update_frequency: int = 10,
        max_traces: Optional[int] = 20,
        max_events_per_trace: Optional[int] = 10,
        relations_threshold: int = 0,
        dcr_patterns: Iterable[str] = _DEFAULT_PATTERNS,
        transitive_reduction: Iterable[str] = ("Condition", "Response"),
) -> DCRMiner:
    """Create a ``BaseMap`` that periodically emits discovered DCR graphs."""
    return DCRMiner(
        model_update_frequency=model_update_frequency,
        max_traces=max_traces,
        max_events_per_trace=max_events_per_trace,
        relations_threshold=relations_threshold,
        dcr_patterns=dcr_patterns,
        transitive_reduction=transitive_reduction,
    )
