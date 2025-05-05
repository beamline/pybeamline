# Model class for Directly-Follows Multigraph (DFM)
from typing import Tuple, Dict


class DFM:
    """
    Directly-Follows Multigraph (DFM) across multiple object types.

    - Nodes are activity labels (strings).
    - Edges are triples: (source activity, object type, target activity).
    """
    def __init__(self):
        self.nodes: set[str] = set()
        self.edges: Dict[Tuple[str, str, str], int] = {} # (source, object_type, target) -> frequency

    def add_edge(self, source_activity: str, object_type: str, target_activity: str, frequency: int):
        """
        Add an edge to the DFM.
        Overwrites the frequency of the edge
        """
        self.edges[(source_activity, object_type, target_activity)] = frequency
        self.nodes.update([source_activity, target_activity])

    def get_edges(self) -> Dict[Tuple[str, str, str], int]:
        """
        Get all edges in the DFM.
        """
        return self.edges

    def __str__(self):
        """
        String representation of the DFM.
        """
        return "\n".join([f"{source} --({object_type})--> {target}" for source, object_type, target in self.edges])


