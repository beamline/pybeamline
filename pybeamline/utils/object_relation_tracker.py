from collections import defaultdict
from enum import Enum

from pybeamline.boevent import BOEvent

class Cardinality(Enum):
    ONE_TO_ONE = "1..1"
    ONE_TO_MANY = "1..*"
    MANY_TO_ONE = "*..1"
    MANY_TO_MANY = "*..*"

class ObjectTypeNode:
    def __init__(self, name: str, max_approx_error: float = 0.1):
        self.name = name
        self.relations = defaultdict(list)
        self.attributes = set()
        self.activities = set()

        self.frequency = 0
        self.bucket = 0
        self.delta = 0

        self.bucket_width = int(1 / max_approx_error)
        self.current_bucket = 0

        # Attribute frequency tracking with cumulative count
        self.attribute_stats = defaultdict(lambda: {"count": 0, "cumulative_count": 0, "persistent": False})

    def update_relation(self, target_type: str, count: int):
        self.relations[target_type].append(count)

    def update_attributes(self,vmap: dict):
        for attribute in vmap.keys():
            self.attributes.add(attribute)  # Maintain the existing attribute set
            self.attribute_stats[attribute]["count"] += 1
            self.attribute_stats[attribute]["cumulative_count"] += 1

        self.current_bucket += 1
        if self.current_bucket >= self.bucket_width:
            self.cleanup_attributes()
            self.current_bucket = 0

    def cleanup_attributes(self, dominance_threshold: int = 90):
        """
        Remove non-dominant attributes based on the threshold.
        Dominance is determined by frequency of appearance.
        """
        if self.frequency == 0:
            return

        # Clear attributes set, we will re-populate it
        self.attributes.clear()

        # Calculate dominant attributes based on the cumulative count
        for attr, stats in list(self.attribute_stats.items()):
            percentage = (stats["cumulative_count"] / self.frequency) * 100

            if percentage >= dominance_threshold:
                stats["persistent"] = True
                self.attributes.add(attr)  # Only add dominant attributes
                if self.name == "SteelCoil":
                    print(f"Attribute '{attr}' is now dominant for {self.name} ({percentage:.2f}%).")
            else:
                # Mark as non-persistent
                stats["persistent"] = False
                if self.name == "SteelCoil":
                    print(f"Attribute '{attr}' lost dominance for {self.name} ({percentage:.2f}%).")

            # Reset count for next round, but keep cumulative count
            stats["count"] = 0


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
    def __init__(self, max_approx_error=0.01):
        self.nodes = dict()
        self.observed_events = 1
        self.bucket_width = int(1 / max_approx_error)  # set bucket width


    def ingest_event(self, event: BOEvent):
        current_bucket = int(self.observed_events / self.bucket_width)

        omap = event.get_omap()
        omap_types = omap.keys()

        # Iterate through the unique object types
        for obj_type in omap_types:
            # Create nodes for each unique type if not already present
            if obj_type not in self.nodes:
                self.nodes[obj_type] = ObjectTypeNode(obj_type)
                self.nodes[obj_type].bucket = current_bucket - 1
                self.nodes[obj_type].delta = current_bucket - 1

            # Increment frequency
            self.nodes[obj_type].frequency += 1

            # Add activities to the node if not already present
            if event.get_event_name() not in self.nodes[obj_type].activities:
                self.nodes[obj_type].activities.add(event.get_event_name())

            # Update attributes and their frequency
            self.nodes[obj_type].update_attributes(event.get_vmap())

        # Infer relations and cardinalities
        for src_type in omap_types:
            for target_type in omap_types:
                if src_type == target_type:
                    continue
                self.nodes[src_type].update_relation(target_type, len(omap[target_type]))

        if self.observed_events % self.bucket_width == 0:
            self._lossy_node_cleanup(current_bucket)

        self.observed_events += 1

    def _lossy_node_cleanup(self, current_bucket):
        to_delete = []

        for obj_type, node in self.nodes.items():
            if node.frequency + node.delta <= current_bucket:
                to_delete.append(obj_type)

        for obj_type in to_delete:
            #print("Cleaning up nodes----------------------------")
            #print(self.observed_events)
            #print(f"Deleting node: {obj_type}")
            del self.nodes[obj_type]

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
            if node.name == "SteelCoil":
                representation.append(f"\n- {node.name}:")
                representation.append(f"- {node.attributes}:")
        #    representation.append(f"  Activities: {', '.join(node.activities)}")
        #    representation.append("  Relations:")
        #    for target, card in node.get_cardinalities().items():
        #        representation.append(f"    -> {target}: {card.value}")
        return "\n".join(representation)
