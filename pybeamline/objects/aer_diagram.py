from typing import Dict, Tuple, Set
from pybeamline.utils.cardinality import Cardinality


class ActivityERDiagram:
    """
    Simple container for ER‐diagrams mined per activity.
    Attributes:
        relations:
            maps activity name → a dict of ((source_type, target_type) → cardinality)
        object_types:
            global set of all object types seen across all activities
    """
    def __init__(self):
        self.relations: Dict[str, Dict[Tuple[str, str], Cardinality]] = {}

    def add_relation(self,
                     activity: str,
                     source: str,
                     target: str,
                     card: Cardinality):
        """
        Record that, in `activity`, entities of type `source` relate to `target`
        with the given `cardinality.
        """

        # get or create the per-activity map
        rels = self.relations.setdefault(activity, {})
        key: Tuple[str, str] = (source, target)
        # add or update the relation
        rels[key] = card

    def get_activities(self) -> Set[str]:
        """All activity names under which we've recorded relations."""
        return set(self.relations.keys())

    def get_relations(self, activity: str) -> Dict[Tuple[str, str], Cardinality]:
        """The (source→target)->card map for one activity."""
        return self.relations.get(activity, {})

    def filter_by_object_types(self, object_types: Set[str]) -> 'ActivityERDiagram':
        new_diagram = ActivityERDiagram()
        for activity, rels in self.relations.items():
            for (source, target), card in rels.items():
                if source in object_types and target in object_types:
                    new_diagram.add_relation(activity, source, target, card)
        return new_diagram

    def __repr__(self):
        aer_dict = {
            activity: {
                (source, target): cardinality.name
                for (source, target), cardinality in rels.items()
            }
            for activity, rels in self.relations.items()
        }
        return str(aer_dict)

    def __str__(self):
        lines = [f"ActivityERDiagram({len(self.relations)} activities"]
        for act, rels in self.relations.items():
            lines.append(f"\nActivity: {act}")
            for (s, t), c in rels.items():
                lines.append(f"  {s} → {t} : {c.name}")
        return "\n".join(lines)
