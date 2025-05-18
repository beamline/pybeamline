from dataclasses import dataclass, field
from typing import List, Dict, Tuple
from pybeamline.utils.cardinality import Cardinality


@dataclass
class ObjectTypeEdge:
    source: str
    target: str
    cardinality: Cardinality

@dataclass
class ActivityRelation:
    activity: str
    object_types: List[str]
    edges: List[ObjectTypeEdge]

@dataclass
class ObjectRelationModel:
    activities: List[ActivityRelation]

    @classmethod
    def from_dict(cls, relation_data: dict) -> "ObjectRelationModel":
        if relation_data is None:
            raise ValueError("Relation data is None.")
        if "relations" not in relation_data:
            raise ValueError("Expected top-level 'relations' key.")

        activities = []
        for activity, content in relation_data["relations"].items():
            obj_types = list(content.get("object_types", []))
            rels = content.get("relations", {})
            edges = []

            for rel_key, counts in rels.items():
                most_common = max(counts.items(), key=lambda x: x[1])[0]
                src, tgt = rel_key.split("->")
                edges.append(ObjectTypeEdge(
                    source=src,
                    target=tgt,
                    cardinality=Cardinality(most_common)
                ))

            activities.append(ActivityRelation(
                activity=activity,
                object_types=obj_types,
                edges=edges
            ))

        return cls(activities=activities)
