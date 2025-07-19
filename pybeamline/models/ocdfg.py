from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Tuple, Set, Any

@dataclass
class OCDFG:
    """
    Object-Centric Directly-Follows Graph (OCDFG) model.
    This data structure captures object-type-specific control flow in object-centric event logs.
    For each object type, it maintains a directed multigraph where nodes represent activities,
    and edges indicate directly-follows relations annotated with frequency counts.
        Attributes:
            activities (Set[str]):
                Set of all activity labels observed across all object types.
            object_types (Set[str]):
                Set of all object types contributing to the OCDFG.
            edges (Dict[str, Dict[Tuple[str, str], int]]):
                A mapping from object type to directly-follows edges between activities.
                Each edge is a tuple (activity1, activity2) with an associated frequency.
            start_activities (Dict[str, Set[str]]):
                A mapping from object type to the set of activities that are entry points
                (i.e., activities that have no predecessors in their object-type-specific subgraph).
            end_activities (Dict[str, Set[str]]):
                A mapping from object type to the set of activities that are terminal points
                (i.e., activities that have no successors in their object-type-specific subgraph).
        Methods:
            add_edge(source, object_type, target, frequency):
                Adds or updates a directly-follows edge for a given object type.
            add_start_activity(activity, object_type):
                Declares an activity as a start node for a specific object type.
            add_end_activity(activity, object_type):
                Declares an activity as an end node for a specific object type.
        Note:
        This implementation is a streaming-compatible adaptation of the OCDFG concept. It diverges from the formal definition
        in Berti & van der Aalst (2023) "OC-PM: analyzing object-centric event logs and process models"
        """

    activities: Set[str] = field(default_factory=set)
    object_types: Set[str] = field(default_factory=set)

    edges: Dict[str, Dict[Tuple[str,str], int]] = field(
    default_factory=lambda: defaultdict(dict)
    )
    start_activities: Dict[str, Set[str]] = field(
    default_factory=lambda: defaultdict(set)
    )
    end_activities: Dict[str, Set[str]] = field(
    default_factory=lambda: defaultdict(set)
    )

    def add_edge(self, source: str, object_type: str, target: str, frequency: int):
        # Record the activity and object type
        self.activities.update([source, target])
        self.object_types.add(object_type)

        # Update the edge frequency
        self.edges[object_type][(source, target)] = frequency

    def __str__(self):
        lines = ["OCDFG:"]
        for obj_type, transitions in self.edges.items():
            for (src, tgt), freq in transitions.items():
                lines.append(f"{src} --({obj_type})--> {tgt} [{freq}]")
            start = self.start_activities.get(obj_type, set())
            end = self.end_activities.get(obj_type, set())
            lines.append(f"Start activities for {obj_type}: {start}")
            lines.append(f"End activities for {obj_type}: {end}")
        return "\n".join(lines)

    def __repr__(self):
        return (
            f"OCDFG(activities={sorted(self.activities)}, "
            f"object_types={sorted(self.object_types)}, "
            f"edges={dict(self.edges)}, "
            f"start_activities={dict(self.start_activities)}, "
            f"end_activities={dict(self.end_activities)})"
        )

