# Class based on: Object-Centric Process Mining: Dealing with Divergence and Convergence in Event Data
# Handles the OCDFG for a single object type
from collections import defaultdict
from pybeamline.boevent import BOEvent


class OCDFG:
    def __init__(self, object_type: str):
        self.object_type = object_type  # e.g., "Customer", "Order"
        self.activities = set()  # Unique activities seen
        self.directly_follows = defaultdict(lambda: {"count": 0, "objects": set()})  # {(A, B): {count, objects}}
        self.activity_counts = defaultdict(int)  # {A: count}
        self.last_seen_activity = dict()
        self.first_seen_activity = dict()  # {object_id: activity}
        self.sources = defaultdict(set)  # {activity: set(object_id)}
        self.sinks = defaultdict(set)  # {activity: set(object_id)}

    def update(self, event: BOEvent):
        """
        Process a single event, updating the graph.
        """
        source_threshold = 0.8
        sink_threshold = 0.8


        current_activity = event.get_event_name()
        # Add activity to the set of unique activities
        if current_activity not in self.activities:
            self.activities.add(current_activity)

        # Update activity count
        self.activity_counts[current_activity] += 1

        # Process all object IDs of this type in the event
        for obj in event.get_object_refs():
            if obj["ocel:type"] != self.object_type:
                continue

            obj_id = obj["ocel:oid"]

            if obj_id not in self.first_seen_activity:
                self.first_seen_activity[obj_id] = current_activity
                self.last_seen_activity[obj_id] = current_activity

                if self.get_source_probability(current_activity) >= source_threshold:
                    self.sources[current_activity].add(obj_id)

                if self.get_sink_probability(current_activity) >= sink_threshold:
                    self.sinks[current_activity].add(obj_id)

                continue  # No causal link yet - potentially the source and sink of an object

            # Subsequent time → link previous to current, update sink
            prev_activity = self.last_seen_activity[obj_id]
            edge = (prev_activity, current_activity)
            self.directly_follows[edge]["count"] += 1
            self.directly_follows[edge]["objects"].add(obj_id)

            # Update sink: remove old sink reference, add new
            # Update sink
            self.sinks[prev_activity].discard(obj_id)
            if self.get_sink_probability(current_activity) >= sink_threshold:
                self.sinks[current_activity].add(obj_id)

            # Set/update the last seen activity for this object
            self.last_seen_activity[obj_id] = current_activity
        print(self)

    def get_source_probability(self, activity_name: str) -> float:
        incoming = 0
        outgoing = 0

        for (src, tgt), data in self.directly_follows.items():
            if tgt == activity_name:
                incoming += data["count"]
            if src == activity_name:
                outgoing += data["count"]

        total = incoming + outgoing
        return 1.0 if total == 0 else 1 - (incoming / total)

    def get_sink_probability(self, activity_name: str) -> float:
        incoming = 0
        outgoing = 0

        for (src, tgt), data in self.directly_follows.items():
            if tgt == activity_name:
                incoming += data["count"]
            if src == activity_name:
                outgoing += data["count"]

        total = incoming + outgoing
        return 1.0 if total == 0 else 1 - (outgoing / total)

    def get_sources(self, threshold=0.8):
        """
        Returns activities with P(source) >= threshold.
        """
        sources = {}
        for activity in self.activities:
            prob = self.get_source_probability(activity)
            if prob >= threshold:
                sources[activity] = prob
        return sources

    def get_sinks(self, threshold=0.8):
        """
        Returns activities with P(sink) >= threshold.
        """
        sinks = {}
        for activity in self.activities:
            prob = self.get_sink_probability(activity)
            if prob >= threshold:
                sinks[activity] = prob
        return sinks

    def filter_by_frequency(self, min_edge_freq=1, min_node_freq=1):
        """
        Remove edges or nodes below frequency thresholds.
        """
        pass

    def to_graph(self):
        """
        Return a graph representation (e.g., NetworkX or DOT format).
        """
        pass

    def __str__(self):
        """
            Return a human-readable string of the directly-follows relationships.
            """
        if not self.directly_follows:
            return f"OCDFG for '{self.object_type}': (empty)"

        lines = [f"OCDFG for '{self.object_type}':"]
        for (src, tgt), data in sorted(self.directly_follows.items(), key=lambda x: (-x[1]["count"], x[0])):
            count = data["count"]
            obj_count = len(data["objects"])
            lines.append(f"  {src} → {tgt}  [count: {count}, objects: {obj_count}]")

        lines.append("\nLikely Sources (P ≥ 0.8):")
        for activity, prob in self.get_sources().items():
            lines.append(f"  {activity}  [P(source) = {prob:.2f}]")

        lines.append("\nLikely Sinks (P ≥ 0.8):")
        for activity, prob in self.get_sinks().items():
            lines.append(f"  {activity}  [P(sink) = {prob:.2f}]")

        return "\n".join(lines)
