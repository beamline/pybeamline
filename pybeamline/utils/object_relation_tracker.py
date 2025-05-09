from collections import defaultdict
from enum import Enum

from pybeamline.boevent import BOEvent

class Cardinality(Enum):
    ONE_TO_ONE = "1..1"
    ONE_TO_MANY = "1..*"
    MANY_TO_ONE = "*..1"
    MANY_TO_MANY = "*..*"

class ObjectTypeNode:
    def __init__(self, name: str):
        self.name = name
        self.relations = defaultdict(list)
        self.attributes = set()
        self.activities = set()
        self.total_occurrences = 0

    def update_relation(self, target_type: str, count: int):
        self.relations[target_type].append(count)

    def get_cardinalities(self):
        result = {}
        for target, counts in self.relations.items():
            unique_counts = set(counts)
            if unique_counts == {1}:
                result[target] = Cardinality.ONE_TO_ONE
            elif min(unique_counts) >= 1:
                result[target] = Cardinality.ONE_TO_MANY
            elif max(unique_counts) == 1:
                result[target] = Cardinality.MANY_TO_ONE
            else:
                result[target] = Cardinality.MANY_TO_MANY
        return result

    def __str__(self):
        rels = ", ".join([f"{target}: {counts}" for target, counts in self.relations.items()])
        acts = ", ".join(self.activities)
        return f"ObjectTypeNode({self.name}) - Activities: [{acts}] - Relations: [{rels}]"


class ObjectRelationTracker:
    def __init__(self):
        self.nodes = dict()

    def ingest_event(self, event: BOEvent):
        omap = event.get_omap()
        omap_types = omap.keys()

        # Iterate through the unique object types
        for obj_type in omap_types:
            # Create nodes for each unique type if not already present
            if obj_type not in self.nodes:
                self.nodes[obj_type] = ObjectTypeNode(obj_type)

            # Add activities to the node if not already present
            if event.get_event_name() not in self.nodes[obj_type].activities:
                self.nodes[obj_type].activities.add(event.get_event_name())

            # Add attributes to the node
            for attribute in event.get_vmap().keys():
                if attribute not in self.nodes[obj_type].attributes:
                    self.nodes[obj_type].attributes.add(attribute)

        # Infer relations and cardinalities
        for src_type in omap_types:
            for target_type in omap_types:
                if src_type == target_type:
                    continue
                self.nodes[src_type].update_relation(target_type, len(omap[target_type]))

    def shared_event(self, obj_type1: str, obj_type2: str, event_name: str) -> bool:
        """
            Check if two object types share an event and have a valid relationship.
        """
        if obj_type1 not in self.nodes or obj_type2 not in self.nodes:
            return False

        # Check if both have the activity
        shared_activity = (event_name in self.nodes[obj_type1].activities and
                           event_name in self.nodes[obj_type2].activities)

        # Check if a valid relationship (cardinality) exists
        valid_cardinality = obj_type2 in self.nodes[obj_type1].relations

        return shared_activity and valid_cardinality

    def get_relationships(self):
        rels = {}
        for src, node in self.nodes.items():
            rels[src] = node.get_cardinalities()
        return rels

    def get_snapshot(self):
        """
            Returns a snapshot of the current state of the relation tracker.
        """
        snapshot = ObjectRelationTracker()
        snapshot.nodes = {k: v for k, v in self.nodes.items()}
        return snapshot

    def __str__(self):
        representation = ["ObjectRelationTracker:"]
        for obj_type, node in self.nodes.items():
            representation.append(f"\n- {node.name}:")
            representation.append(f"  Activities: {', '.join(node.activities)}")
            representation.append("  Relations:")
            for target, card in node.get_cardinalities().items():
                representation.append(f"    -> {target}: {card.value}")
        return "\n".join(representation)
