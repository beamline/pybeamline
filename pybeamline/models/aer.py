from dataclasses import dataclass, field
from typing import Dict, Tuple, Set
from collections import defaultdict
from pybeamline.utils.cardinality import Cardinality


@dataclass
class AER:
    """
    Activity-Entity-Relationship Model (AER).
    Tracks cardinalities between object types per activity.
    Attributes:
        activities: All activities observed.
        object_types: Maps activity -> object types seen in relation.
        relations: Maps activity → (source_type, target_type) → Cardinality.
    """
    activities: Set[str] = field(default_factory=set)
    object_types: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    relations: Dict[str, Dict[Tuple[str, str], Cardinality]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    def add_object_types(self, activity: str, object_types: Set[str]):
        self.activities.add(activity)
        self.object_types[activity].update(object_types)

    def add_relation(self, activity: str, source: str, target: str, card: Cardinality):
        a, b = sorted([source, target])
        key = (a, b)
        self.activities.add(activity)
        self.object_types[activity].update([a, b])
        self.relations[activity][key] = card

    def add_activity(self, activity: str):
        self.activities.add(activity)

    def get_activities(self) -> Set[str]:
        return self.activities

    def get_relations(self, activity: str) -> Dict[Tuple[str, str], Cardinality]:
        return self.relations.get(activity, {})

    def get_object_types(self, activity: str) -> Set[str]:
        return self.object_types.get(activity, set())

    def __str__(self):
        lines = ["ActivityER:"]
        for act, rels in self.relations.items():
            for (src, tgt), card in rels.items():
                lines.append(f"{act}: {src} → {tgt} [{card.name}]")
        return "\n".join(lines)

    def __repr__(self):
        return (
            f"ActivityER(activities={sorted(self.activities)}, "
            f"object_types={dict(self.object_types)}, "
            f"relations={dict(self.relations)})"
        )
