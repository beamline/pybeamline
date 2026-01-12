from typing import Dict, Set
import copy


class Pdfa:

    def __init__(self):
        self.nodes: Set[str] = set()
        self.edges: Dict[str, Dict[str, float]] = {}

    def __deepcopy__(self, memo) -> 'Pdfa':
        new_pdfa = type(self)()
        memo[id(self)] = new_pdfa
        new_pdfa.nodes = copy.deepcopy(self.nodes, memo)
        new_pdfa.edges = copy.deepcopy(self.edges, memo)
        return new_pdfa

    def deepcopy(self) -> 'Pdfa':
        return copy.deepcopy(self)

    def add_node(self, node_label: str) -> None:
        self.nodes.add(node_label)
        self.edges.setdefault(node_label, {})

    def add_edge(self, source: str, target: str, weight: float) -> None:
        self._check_nodes_error(source, target)
        self.edges[source][target] = weight

    def get_sequence_probability(self, source: str, target: str) -> float:
        if self._check_nodes(source, target):
            return self.edges[source].get(target, 0.0)
        return 0.0

    def _check_nodes(self, *nodes: str) -> bool:
        for node in nodes:
            if node not in self.nodes:
               return False
        return True

    def _check_nodes_error(self, *nodes: str) -> None:
        for node in nodes:
            if node not in self.nodes:
                raise KeyError(f'Node {node} not registered')

    def get_outgoing_edges(self, source: str) -> Dict[str, float]:
        if source not in self.edges:
            return dict()
        return self.edges[source]

