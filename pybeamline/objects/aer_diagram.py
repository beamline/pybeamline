from dataclasses import dataclass, field
from typing import Dict, Tuple, Set
from collections import defaultdict
from pybeamline.utils.cardinality import Cardinality


@dataclass
class ActivityERDiagram:
    """
    Object-Centric Entity Relationship Diagram (AER).
    Tracks cardinalities between object types per activity.

    Attributes:
        activities: All activities observed.
        object_types: All object types seen across relations.
        relations: Maps activity → (source_type, target_type) → Cardinality.
        unary_participations: Maps activity → set of object types that appeared alone.
    """
    activities: Set[str] = field(default_factory=set)
    object_types: Set[str] = field(default_factory=set)

    relations: Dict[str, Dict[Tuple[str, str], Cardinality]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    unary_participations: Dict[str, Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def add_relation(self, activity: str, source: str, target: str, card: Cardinality):
        a, b = sorted([source, target])
        key = (a, b)
        self.activities.add(activity)
        self.object_types.update([a, b])
        self.relations[activity][key] = card

    def add_unary_participation(self, activity: str, obj_type: str):
        self.activities.add(activity)
        self.object_types.add(obj_type)
        self.unary_participations[activity].add(obj_type)

    def get_activities(self) -> Set[str]:
        return self.activities

    def get_relations(self, activity: str) -> Dict[Tuple[str, str], Cardinality]:
        return self.relations.get(activity, {})

    def get_unary_participation(self, activity: str) -> Set[str]:
        return self.unary_participations.get(activity, set())

    def get_unary_participations(self) -> Dict[str, Set[str]]:
        return self.unary_participations

    def __str__(self):
        lines = ["ActivityERDiagram:"]
        for act, rels in self.relations.items():
            for (src, tgt), card in rels.items():
                lines.append(f"{act}: {src} → {tgt} [{card.name}]")
        for act, unary in self.unary_participations.items():
            for ot in unary:
                lines.append(f"{act}: {ot} (unary)")
        return "\n".join(lines)

    def __repr__(self):
        return (
            f"ActivityERDiagram(activities={sorted(self.activities)}, "
            f"object_types={sorted(self.object_types)}, "
            f"relations={dict(self.relations)}, "
            f"unary_participation={dict(self.unary_participations)})"
        )
