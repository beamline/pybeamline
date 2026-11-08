"""Streaming Split Miner operators.

The three operators share the Split Miner DFG filtering and BPMN construction
steps. They differ in the event summary they retain: a sliding event window,
Lossy Counting, or Lossy Counting with separate case and relation budgets.

The implementation follows the streaming Split Miner thesis and the Apache
2.0-licensed Beamline implementation:
https://github.com/beamline/discovery-splitminer
"""

from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from math import inf, isfinite
from typing import Deque, Dict, Iterable, List, Mapping, Optional, Set, Tuple

from pm4py.objects.bpmn.obj import BPMN

from pybeamline.bevent import BEvent
from pybeamline.stream.base_map import BaseMap


_Edge = Tuple[str, str]


@dataclass
class _DFG:
    activities: Set[str] = field(default_factory=set)
    sources: Set[str] = field(default_factory=set)
    sinks: Set[str] = field(default_factory=set)
    edges: Dict[_Edge, int] = field(default_factory=dict)
    self_loops: Set[_Edge] = field(default_factory=set)
    short_loops: Set[_Edge] = field(default_factory=set)
    concurrent_edges: Set[_Edge] = field(default_factory=set)
    infrequent_edges: Set[_Edge] = field(default_factory=set)


@dataclass
class _CaseSummary:
    starting_activity: str
    latest_activity: str
    second_to_last_activity: Optional[str] = None
    self_loops: Set[_Edge] = field(default_factory=set)
    short_loops: Set[_Edge] = field(default_factory=set)

    @classmethod
    def first(cls, activity: str) -> "_CaseSummary":
        return cls(starting_activity=activity, latest_activity=activity)

    def observe(self, activity: str) -> str:
        previous = self.latest_activity
        if self.second_to_last_activity == activity:
            self.short_loops.add((self.second_to_last_activity, previous))
        if previous == activity:
            self.self_loops.add((previous, activity))
        self.second_to_last_activity = previous
        self.latest_activity = activity
        return previous


class _LossyCounter:
    """Frequency counters using the standard Lossy Counting buckets."""

    def __init__(self) -> None:
        self.values: Dict[object, List[int]] = {}

    def add(self, key: object, current_bucket: int, initial_delta: Optional[int] = None) -> None:
        if key in self.values:
            self.values[key][0] += 1
        else:
            delta = current_bucket - 1 if initial_delta is None else initial_delta
            self.values[key] = [1, delta]

    def cleanup(self, current_bucket: int) -> None:
        expired = [
            key for key, (frequency, delta) in self.values.items()
            if frequency + delta < current_bucket
        ]
        for key in expired:
            del self.values[key]


class _BudgetCounter:
    """Lossy Counting with Budget counter, with automatic bucket cleanup."""

    def __init__(self, budget: int) -> None:
        self.budget = budget
        self.current_bucket = 0
        self.values: Dict[object, List[int]] = {}

    def add(self, key: object, initial_delta: Optional[int] = None) -> None:
        if key in self.values:
            self.values[key][0] += 1
            return
        if len(self.values) >= self.budget:
            self.cleanup()
        delta = self.current_bucket if initial_delta is None else initial_delta
        self.values[key] = [1, delta]

    def cleanup(self) -> None:
        self.current_bucket += 1
        expired = [
            key for key, (frequency, delta) in self.values.items()
            if frequency + delta < self.current_bucket
        ]
        if expired:
            for key in expired:
                del self.values[key]
            return

        if not self.values:
            return
        self.current_bucket = min(
            frequency + delta for frequency, delta in self.values.values()
        )
        self.cleanup()


class _SplitMinerBase(BaseMap[BEvent, BPMN]):
    def __init__(
            self,
            model_update_frequency: int,
            concurrency_threshold: float,
            frequency_percentile: float,
    ) -> None:
        self.model_update_frequency = _positive_integer(
            "model_update_frequency", model_update_frequency
        )
        self.concurrency_threshold = _unit_interval(
            "concurrency_threshold", concurrency_threshold
        )
        self.frequency_percentile = _unit_interval(
            "frequency_percentile", frequency_percentile
        )
        self.observed_events = 0
        self.current_model: Optional[BPMN] = None

    @staticmethod
    def _read_event(event: BEvent) -> Tuple[object, str]:
        if not isinstance(event, BEvent):
            raise TypeError(f"Split Miner accepts BEvent values, got {type(event)}")
        case_id = event.get_trace_name()
        activity = event.get_event_name()
        try:
            hash(case_id)
        except TypeError as error:
            raise TypeError("BEvent trace names must be hashable") from error
        if not isinstance(activity, str):
            raise TypeError("BEvent activity names must be strings")
        return case_id, activity

    def _emit_if_due(self, dfg: _DFG) -> Optional[List[BPMN]]:
        if self.observed_events % self.model_update_frequency != 0:
            return None
        self.current_model = _discover_bpmn(dfg, self.frequency_percentile)
        return [self.current_model]


class SlidingWindowSplitMiner(_SplitMinerBase):
    """Split Miner backed by a bounded queue of most recent stream events."""

    def __init__(
            self,
            model_update_frequency: int = 10,
            max_capacity: int = 100,
            concurrency_threshold: float = 0.4,
            frequency_percentile: float = 0.6,
    ) -> None:
        super().__init__(model_update_frequency, concurrency_threshold, frequency_percentile)
        self.max_capacity = _positive_integer("max_capacity", max_capacity)
        self._events: Deque[BEvent] = deque()

    @property
    def event_window(self) -> Tuple[BEvent, ...]:
        return tuple(self._events)

    @property
    def retained_event_count(self) -> int:
        return len(self._events)

    def transform(self, event: BEvent) -> Optional[List[BPMN]]:
        self._read_event(event)
        self.observed_events += 1
        if len(self._events) >= self.max_capacity:
            self._events.popleft()
        self._events.append(event)
        if self.observed_events % self.model_update_frequency != 0:
            return None
        dfg = _dfg_from_events(self._events, self.concurrency_threshold)
        return self._emit_if_due(dfg)


class LossyCountingSplitMiner(_SplitMinerBase):
    """Split Miner using Lossy Counting case and directly-follows summaries."""

    def __init__(
            self,
            model_update_frequency: int = 10,
            max_approx_error: float = 0.01,
            concurrency_threshold: float = 0.4,
            frequency_percentile: float = 0.6,
    ) -> None:
        super().__init__(model_update_frequency, concurrency_threshold, frequency_percentile)
        error = _finite_number("max_approx_error", max_approx_error)
        if not 0 < error <= 1:
            raise ValueError("max_approx_error must be in the range (0, 1]")
        self.max_approx_error = error
        self.bucket_width = max(1, int(1.0 / error))
        self._cases: Dict[object, List[object]] = {}
        self._relations = _LossyCounter()

    @property
    def tracked_case_count(self) -> int:
        return len(self._cases)

    @property
    def tracked_relation_count(self) -> int:
        return len(self._relations.values)

    def transform(self, event: BEvent) -> Optional[List[BPMN]]:
        case_id, activity = self._read_event(event)
        self.observed_events += 1
        current_bucket = self.observed_events // self.bucket_width

        if case_id in self._cases:
            summary, frequency, delta = self._cases[case_id]
            previous_activity = summary.observe(activity)
            self._cases[case_id] = [summary, frequency + 1, delta]
            self._relations.add((previous_activity, activity), current_bucket)
        else:
            self._cases[case_id] = [
                _CaseSummary.first(activity), 1, current_bucket - 1
            ]

        if self.observed_events % self.bucket_width == 0:
            self._cleanup(current_bucket)
        if self.observed_events % self.model_update_frequency != 0:
            return None
        return self._emit_if_due(self._dfg_from_summary())

    def _cleanup(self, current_bucket: int) -> None:
        expired_cases = [
            case_id for case_id, (_, frequency, delta) in self._cases.items()
            if frequency + delta < current_bucket
        ]
        for case_id in expired_cases:
            del self._cases[case_id]
        self._relations.cleanup(current_bucket)

    def _dfg_from_summary(self) -> _DFG:
        return _dfg_from_summaries(
            self._cases,
            {key: value[0] for key, value in self._relations.values.items()},
            self.concurrency_threshold,
        )


class LossyCountingBudgetSplitMiner(_SplitMinerBase):
    """Split Miner using separate budgets for case and DFG relation summaries."""

    def __init__(
            self,
            model_update_frequency: int = 10,
            cases_budget: int = 85,
            relations_budget: int = 15,
            concurrency_threshold: float = 0.4,
            frequency_percentile: float = 0.6,
    ) -> None:
        super().__init__(model_update_frequency, concurrency_threshold, frequency_percentile)
        self.cases_budget = _positive_integer("cases_budget", cases_budget)
        self.relations_budget = _positive_integer("relations_budget", relations_budget)
        self._cases: Dict[object, List[object]] = {}
        self._case_bucket = 0
        self._relations = _BudgetCounter(self.relations_budget)

    @property
    def tracked_case_count(self) -> int:
        return len(self._cases)

    @property
    def tracked_relation_count(self) -> int:
        return len(self._relations.values)

    def transform(self, event: BEvent) -> Optional[List[BPMN]]:
        case_id, activity = self._read_event(event)
        self.observed_events += 1

        if case_id in self._cases:
            summary, frequency, delta = self._cases[case_id]
            previous_activity = summary.observe(activity)
            self._cases[case_id] = [summary, frequency + 1, delta]
            self._relations.add((previous_activity, activity))
        else:
            if len(self._cases) >= self.cases_budget:
                self._cleanup_cases()
            self._cases[case_id] = [
                _CaseSummary.first(activity), 1, self._case_bucket - 1
            ]

        if self.observed_events % self.model_update_frequency != 0:
            return None
        return self._emit_if_due(self._dfg_from_summary())

    def _cleanup_cases(self) -> None:
        self._case_bucket += 1
        expired = [
            case_id for case_id, (_, frequency, delta) in self._cases.items()
            if frequency + delta < self._case_bucket
        ]
        if expired:
            for case_id in expired:
                del self._cases[case_id]
            return
        if not self._cases:
            return
        self._case_bucket = min(
            frequency + delta
            for _, frequency, delta in self._cases.values()
        )
        self._cleanup_cases()

    def _dfg_from_summary(self) -> _DFG:
        return _dfg_from_summaries(
            self._cases,
            {key: value[0] for key, value in self._relations.values.items()},
            self.concurrency_threshold,
        )


def split_miner_sliding_window(
        model_update_frequency: int = 10,
        max_capacity: int = 100,
        concurrency_threshold: float = 0.4,
        frequency_percentile: float = 0.6,
) -> SlidingWindowSplitMiner:
    """Create the sliding-window Split Miner ``BaseMap`` operator."""
    return SlidingWindowSplitMiner(
        model_update_frequency=model_update_frequency,
        max_capacity=max_capacity,
        concurrency_threshold=concurrency_threshold,
        frequency_percentile=frequency_percentile,
    )


def split_miner_lossy_counting(
        model_update_frequency: int = 10,
        max_approx_error: float = 0.01,
        concurrency_threshold: float = 0.4,
        frequency_percentile: float = 0.6,
) -> LossyCountingSplitMiner:
    """Create the Lossy Counting Split Miner ``BaseMap`` operator."""
    return LossyCountingSplitMiner(
        model_update_frequency=model_update_frequency,
        max_approx_error=max_approx_error,
        concurrency_threshold=concurrency_threshold,
        frequency_percentile=frequency_percentile,
    )


def split_miner_lossy_counting_budget(
        model_update_frequency: int = 10,
        cases_budget: int = 85,
        relations_budget: int = 15,
        concurrency_threshold: float = 0.4,
        frequency_percentile: float = 0.6,
) -> LossyCountingBudgetSplitMiner:
    """Create the Lossy Counting with Budget Split Miner ``BaseMap`` operator."""
    return LossyCountingBudgetSplitMiner(
        model_update_frequency=model_update_frequency,
        cases_budget=cases_budget,
        relations_budget=relations_budget,
        concurrency_threshold=concurrency_threshold,
        frequency_percentile=frequency_percentile,
    )


def _positive_integer(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _finite_number(name: str, value: float) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a finite number") from error
    if not isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def _unit_interval(name: str, value: float) -> float:
    number = _finite_number(name, value)
    if not 0 <= number <= 1:
        raise ValueError(f"{name} must be in the range [0, 1]")
    return number


def _dfg_from_events(events: Iterable[BEvent], threshold: float) -> _DFG:
    traces: Dict[object, List[str]] = defaultdict(list)
    activities: Set[str] = set()
    for event in events:
        case_id, activity = _SplitMinerBase._read_event(event)
        traces[case_id].append(activity)
        activities.add(activity)

    edges: Counter = Counter()
    sources: Set[str] = set()
    sinks: Set[str] = set()
    short_loops: Set[_Edge] = set()
    for trace in traces.values():
        if not trace:
            continue
        sources.add(trace[0])
        sinks.add(trace[-1])
        transitions = list(zip(trace, trace[1:]))
        edges.update(transitions)
        for first, second in zip(transitions, transitions[1:]):
            if first[0] == second[1]:
                short_loops.add(first)

    self_loops = {edge for edge in edges if edge[0] == edge[1]}
    return _complete_dfg(
        activities,
        sources,
        sinks,
        dict(edges),
        self_loops,
        short_loops,
        threshold,
    )


def _dfg_from_summaries(
        cases: Mapping[object, List[object]],
        edges: Mapping[_Edge, int],
        threshold: float,
) -> _DFG:
    activities: Set[str] = set()
    sources: Set[str] = set()
    sinks: Set[str] = set()
    self_loops: Set[_Edge] = set()
    short_loops: Set[_Edge] = set()
    for summary, _, _ in cases.values():
        activities.add(summary.starting_activity)
        activities.add(summary.latest_activity)
        sources.add(summary.starting_activity)
        sinks.add(summary.latest_activity)
        self_loops.update(summary.self_loops)
        short_loops.update(summary.short_loops)
    for source, target in edges:
        activities.update((source, target))
        if source == target:
            self_loops.add((source, target))
    return _complete_dfg(
        activities,
        sources,
        sinks,
        dict(edges),
        self_loops,
        short_loops,
        threshold,
    )


def _complete_dfg(
        activities: Set[str],
        sources: Set[str],
        sinks: Set[str],
        edges: Dict[_Edge, int],
        self_loops: Set[_Edge],
        short_loops: Set[_Edge],
        threshold: float,
) -> _DFG:
    edge_nodes = {activity for edge in edges for activity in edge}
    nodes = edge_nodes | set(activities)

    # Keep the known case starts and ends, and infer endpoints from the
    # retained relation set when a case summary was evicted.
    sources = {node for node in sources if node in nodes}
    sinks = {node for node in sinks if node in nodes}
    incoming = {target for _, target in edges}
    outgoing = {source for source, _ in edges}
    sources.update(nodes - incoming)
    sinks.update(nodes - outgoing)

    concurrent: Set[_Edge] = set()
    infrequent: Set[_Edge] = set()
    for edge, frequency in edges.items():
        source, target = edge
        reverse = (target, source)
        if edge in self_loops or edge in short_loops or reverse in short_loops:
            continue
        if reverse not in edges:
            continue
        reverse_frequency = edges[reverse]
        difference = frequency - reverse_frequency
        ratio = abs(difference) / float(frequency + reverse_frequency)
        if ratio < threshold:
            concurrent.add(edge)
        elif difference < 0:
            infrequent.add(edge)

    return _DFG(
        activities=nodes,
        sources=sources,
        sinks=sinks,
        edges=edges,
        self_loops=self_loops,
        short_loops=short_loops,
        concurrent_edges=concurrent,
        infrequent_edges=infrequent,
    )


def _discover_bpmn(dfg: _DFG, percentile: float) -> BPMN:
    pruned_edges = {
        edge: frequency for edge, frequency in dfg.edges.items()
        if edge not in dfg.self_loops
        and edge not in dfg.short_loops
        and (edge[1], edge[0]) not in dfg.short_loops
        and edge not in dfg.concurrent_edges
        and edge not in dfg.infrequent_edges
    }
    filtered = _filter_pdfg(
        dfg.activities,
        dfg.sources,
        dfg.sinks,
        pruned_edges,
        dfg.concurrent_edges,
        percentile,
    )
    return _filtered_graph_to_bpmn(filtered)


def _filter_pdfg(
        activities: Set[str],
        sources: Set[str],
        sinks: Set[str],
        edges: Dict[_Edge, int],
        concurrent_edges: Set[_Edge],
        percentile: float,
) -> _DFG:
    nodes = {node for edge in edges for node in edge}
    isolated = activities - nodes
    nodes.update(isolated)

    forward_capacity: Dict[str, float] = {source: inf for source in sources}
    best_incoming: Dict[str, _Edge] = {}
    queue = deque(sorted(sources))
    outgoing = _outgoing_edges(edges)
    while queue:
        node = queue.popleft()
        for edge in outgoing.get(node, ()):
            target = edge[1]
            candidate = min(forward_capacity[node], edges[edge])
            if candidate > forward_capacity.get(target, -1):
                forward_capacity[target] = candidate
                best_incoming[target] = edge
                queue.append(target)

    backward_capacity: Dict[str, float] = {sink: inf for sink in sinks}
    best_outgoing: Dict[str, _Edge] = {}
    incoming = _incoming_edges(edges)
    queue = deque(sorted(sinks))
    while queue:
        node = queue.popleft()
        for edge in incoming.get(node, ()):
            source = edge[0]
            candidate = min(backward_capacity[node], edges[edge])
            if candidate > backward_capacity.get(source, -1):
                backward_capacity[source] = candidate
                best_outgoing[source] = edge
                queue.append(source)

    frequencies: List[int] = []
    for node in nodes:
        frequencies.append(max((edges[e] for e in incoming.get(node, ())), default=0))
        frequencies.append(max((edges[e] for e in outgoing.get(node, ())), default=0))
    frequencies.sort()
    threshold_index = max(0, int(len(frequencies) * percentile) - 1)
    frequency_threshold = frequencies[threshold_index] if frequencies else 0

    selected = {
        edge for edge, frequency in edges.items()
        if (edge[0] in best_incoming or edge[0] in sources)
        and (edge in best_incoming.values()
             or edge in best_outgoing.values()
             or frequency > frequency_threshold)
    }
    filtered_nodes = {node for edge in selected for node in edge}
    filtered_nodes.update(node for node in isolated if node in sources or node in sinks)
    filtered_sources = {node for node in sources if node in filtered_nodes}
    filtered_sinks = {node for node in sinks if node in filtered_nodes}
    filtered_incoming = {target for _, target in selected}
    filtered_outgoing = {source for source, _ in selected}
    filtered_sources.update(filtered_nodes - filtered_incoming)
    filtered_sinks.update(filtered_nodes - filtered_outgoing)
    return _DFG(
        activities=filtered_nodes,
        sources=filtered_sources,
        sinks=filtered_sinks,
        edges={edge: edges[edge] for edge in selected},
        concurrent_edges=concurrent_edges,
    )


def _outgoing_edges(edges: Mapping[_Edge, int]) -> Dict[str, List[_Edge]]:
    result: Dict[str, List[_Edge]] = defaultdict(list)
    for edge in edges:
        result[edge[0]].append(edge)
    for node_edges in result.values():
        node_edges.sort()
    return result


def _incoming_edges(edges: Mapping[_Edge, int]) -> Dict[str, List[_Edge]]:
    result: Dict[str, List[_Edge]] = defaultdict(list)
    for edge in edges:
        result[edge[1]].append(edge)
    for node_edges in result.values():
        node_edges.sort()
    return result


def _filtered_graph_to_bpmn(graph: _DFG) -> BPMN:
    start = "__split_miner_start__"
    end = "__split_miner_end__"
    nodes = set(graph.activities) | {start, end}
    edges: Set[_Edge] = set(graph.edges)
    for source in graph.sources:
        edges.add((start, source))
    for sink in graph.sinks:
        edges.add((sink, end))

    split_counter = [0]
    _discover_splits(nodes, edges, graph.concurrent_edges, split_counter)
    _discover_joins(nodes, edges, start, split_counter)

    model = BPMN()
    node_objects = {}
    for node in sorted(nodes):
        if node == start:
            node_objects[node] = BPMN.StartEvent(id="start", name="Start")
        elif node == end:
            node_objects[node] = BPMN.EndEvent(id="end", name="End")
        elif node.startswith("AND:"):
            node_objects[node] = BPMN.ParallelGateway(
                id=node.split(":", 2)[2], name=""
            )
        elif node.startswith("XOR:"):
            node_objects[node] = BPMN.ExclusiveGateway(
                id=node.split(":", 2)[2], name=""
            )
        elif node.startswith("OR:"):
            node_objects[node] = BPMN.InclusiveGateway(
                id=node.split(":", 2)[2], name=""
            )
        else:
            node_objects[node] = BPMN.Task(id=_task_id(node), name=node)
        model.add_node(node_objects[node])

    for index, (source, target) in enumerate(sorted(edges)):
        model.add_flow(
            BPMN.SequenceFlow(
                node_objects[source],
                node_objects[target],
                id="flow_{}".format(index),
            )
        )
    return model


def _task_id(name: str) -> str:
    # Encode names so distinct activity labels always yield distinct BPMN IDs.
    return "task_" + "_".join("{:x}".format(ord(character)) for character in name)


def _new_gateway(gateway_type: str, category: str, counter: List[int]) -> str:
    counter[0] += 1
    return "{}:{}:gateway_{}".format(gateway_type, category, counter[0])


def _gateway_type(node: str) -> Optional[str]:
    if node.startswith("AND:"):
        return "AND"
    if node.startswith("XOR:"):
        return "XOR"
    if node.startswith("OR:"):
        return "OR"
    return None


def _discover_splits(
        nodes: Set[str],
        edges: Set[_Edge],
        concurrent_edges: Set[_Edge],
        gateway_counter: List[int],
) -> None:
    # Work on the original nodes. The new split gateways are already in final
    # position and must not themselves be treated as activity splits.
    original_nodes = sorted(nodes)
    for node in original_nodes:
        successors = {target for source, target in edges if source == node}
        if len(successors) <= 1:
            continue

        futures: Dict[str, Set[str]] = {}
        covers: Dict[str, Set[str]] = {}
        for first in successors:
            covers[first] = {first}
            futures[first] = {
                second for second in successors
                if second != first
                and ((first, second) in concurrent_edges
                     or (second, first) in concurrent_edges)
            }

        edges.difference_update({edge for edge in edges if edge[0] == node})
        direct = set(successors)

        while len(direct) > 1:
            found = False

            # XOR branches have the same concurrent future set.
            while True:
                group = _first_equal_group(direct, futures)
                if len(group) < 2:
                    break
                gateway = _new_gateway("XOR", "SPLIT", gateway_counter)
                nodes.add(gateway)
                edges.update((gateway, successor) for successor in group)
                combined_cover = set().union(*(covers[item] for item in group))
                shared_future = set(futures[min(group)])
                direct.difference_update(group)
                direct.add(gateway)
                covers[gateway] = combined_cover
                futures[gateway] = shared_future
                found = True

            # AND branches have equal combined cover/future sets.
            while True:
                group = _first_equal_union_group(direct, covers, futures)
                if len(group) < 2:
                    break
                gateway = _new_gateway("AND", "SPLIT", gateway_counter)
                nodes.add(gateway)
                edges.update((gateway, successor) for successor in group)
                combined_cover = set().union(*(covers[item] for item in group))
                common_future = set.intersection(*(set(futures[item]) for item in group))
                direct.difference_update(group)
                direct.add(gateway)
                covers[gateway] = combined_cover
                futures[gateway] = common_future
                found = True

            if not found:
                break

        if len(direct) > 1:
            gateway = _new_gateway("XOR", "SPLIT", gateway_counter)
            nodes.add(gateway)
            edges.update((gateway, successor) for successor in direct)
            direct = {gateway}

        if direct:
            edges.add((node, next(iter(direct))))


def _first_equal_group(values: Set[str], signatures: Mapping[str, Set[str]]) -> Set[str]:
    ordered = sorted(values)
    for first in ordered:
        group = {item for item in ordered if signatures[item] == signatures[first]}
        if len(group) > 1:
            return group
    return set()


def _first_equal_union_group(
        values: Set[str],
        covers: Mapping[str, Set[str]],
        futures: Mapping[str, Set[str]],
) -> Set[str]:
    signatures = {
        item: frozenset(covers[item] | futures[item]) for item in values
    }
    ordered = sorted(values)
    for first in ordered:
        group = {item for item in ordered if signatures[item] == signatures[first]}
        if len(group) > 1:
            return group
    return set()


def _discover_joins(
        nodes: Set[str],
        edges: Set[_Edge],
        start: str,
        gateway_counter: List[int],
) -> None:
    outgoing = _outgoing_edges({edge: 1 for edge in edges})
    incoming = _incoming_edges({edge: 1 for edge in edges})
    split_nodes = [
        node for node in sorted(nodes)
        if _gateway_type(node) in {"AND", "XOR"}
        and ":SPLIT:" in node
        and len(outgoing.get(node, ())) > 1
    ]

    requirements: Dict[str, List[str]] = defaultdict(list)
    for split in split_nodes:
        branches = [edge[1] for edge in outgoing[split]]
        paths = [_distances_from(branch, outgoing) for branch in branches]
        if not paths:
            continue
        common = set.intersection(*(set(path) for path in paths))
        candidates = []
        for candidate in common:
            if candidate == split or candidate == start:
                continue
            branch_sources = [
                source for source, target in incoming.get(candidate, ())
                if any(source in path for path in paths)
            ]
            if len(set(branch_sources)) > 1:
                distances = [path[candidate] for path in paths]
                candidates.append((max(distances), sum(distances), candidate))
        if candidates:
            _, _, candidate = min(candidates)
            requirements[candidate].append(_gateway_type(split) or "OR")

    # Rewire converging branches through the matching gateway. When the
    # evidence is mixed, preserve inclusive behavior with an OR join.
    for target in sorted(requirements):
        entering = {edge for edge in edges if edge[1] == target}
        if len(entering) < 2:
            continue
        types = set(requirements[target])
        gateway_type = next(iter(types)) if len(types) == 1 else "OR"
        gateway = _new_gateway(gateway_type, "JOIN", gateway_counter)
        nodes.add(gateway)
        edges.difference_update(entering)
        edges.update((source, gateway) for source, _ in entering)
        edges.add((gateway, target))

    # Any remaining multi-input activity needs an explicit join. OR is the
    # conservative default; a backedge denotes loop completion and is XOR.
    incoming = _incoming_edges({edge: 1 for edge in edges})
    for target in sorted(nodes):
        if _gateway_type(target) is not None:
            continue
        entering = set(incoming.get(target, ()))
        if len(entering) <= 1:
            continue
        backedge = any(_has_path(target, source, edges) for source, _ in entering)
        gateway_type = "XOR" if backedge else "OR"
        gateway = _new_gateway(gateway_type, "JOIN", gateway_counter)
        nodes.add(gateway)
        edges.difference_update(entering)
        edges.update((source, gateway) for source, _ in entering)
        edges.add((gateway, target))


def _distances_from(source: str, outgoing: Mapping[str, List[_Edge]]) -> Dict[str, int]:
    distances = {source: 0}
    queue = deque([source])
    while queue:
        node = queue.popleft()
        for _, target in outgoing.get(node, ()):
            if target not in distances:
                distances[target] = distances[node] + 1
                queue.append(target)
    return distances


def _has_path(source: str, target: str, edges: Set[_Edge]) -> bool:
    if source == target:
        return True
    outgoing = _outgoing_edges({edge: 1 for edge in edges})
    return target in _distances_from(source, outgoing)
