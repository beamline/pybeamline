from enum import Enum
from typing import Dict, Any, Tuple, Set, Callable
import copy
from reactivex import operators as ops, Observable
from reactivex import just, empty

from pybeamline.boevent import BOEvent
from pybeamline.objects.object_relation_model import ObjectRelationModel, ObjectTypeEdge, ActivityRelation
from pybeamline.utils.cardinality import Cardinality

def _infer_cardinality(count1: int, count2: int) -> Cardinality:
    if count1 == 1 and count2 == 1:
        return Cardinality.ONE_TO_ONE
    elif count1 == 1 and count2 > 1:
        return Cardinality.ONE_TO_MANY
    elif count1 > 1 and count2 == 1:
        return Cardinality.MANY_TO_ONE
    else:
        return Cardinality.MANY_TO_MANY

def object_relations_miner_lossy_counting(model_update_frequency=10, max_approx_error: float = 0.0001) -> Callable[
    [Observable[BOEvent]], Observable[Dict[str, Any]]]:
    """
    Object Relationship Miner using a lossy counting approach.
    :param model_update_frequency: Frequency of model updates
    :param max_approx_error: Maximum approximation error for the lossy counting on objects
    :return: Function to process BOEvent and return a dictionary with the model
    """
    obj_rel = ObjectRelationMinerLossyCounting(max_approx_error=max_approx_error)

    def miner(event: BOEvent) -> Observable[Dict[str, Any]]:
        if isinstance(event, BOEvent):
            obj_rel.ingest_event(event)
            if obj_rel.observed_events % model_update_frequency == 0:
                return just(obj_rel.get_model())
        return empty()

    return ops.flat_map(miner)


class ObjectRelationMinerLossyCounting:
    def __init__(self, max_approx_error: float = 0.001):
        self.__activity_object_relations: Dict[str, Dict[Tuple[str, str], Dict[Cardinality, int]]] = {}
        self.__activity_object_presence: Dict[str, Set[str]] = {}

        self.observed_events = 1
        self.bucket_width = int(1 / max_approx_error)

    def ingest_event(self, event: BOEvent):
        current_bucket = int(self.observed_events / self.bucket_width)
        activity = event.get_event_name()
        omap = event.get_omap()
        types = list(omap.keys())

        # Record presence of activity and object types
        if activity not in self.__activity_object_presence:
            self.__activity_object_presence[activity] = set()
        self.__activity_object_presence[activity].update(types)

        # Ensure the activity is initialized in the relations dictionary
        if activity not in self.__activity_object_relations:
            self.__activity_object_relations[activity] = {}

        # Record relations
        for i in range(len(types)):
            for j in range(i + 1, len(types)):
                type1, type2 = sorted([types[i], types[j]])
                count1 = len(omap[type1])
                count2 = len(omap[type2])
                cardinality = _infer_cardinality(count1, count2)
                key = (type1, type2)

                if key not in self.__activity_object_relations[activity]:
                    self.__activity_object_relations[activity][key] = {c: 0 for c in Cardinality}

                self.__activity_object_relations[activity][key][cardinality] += 1

        self.observed_events += 1

    def get_model(self) -> ObjectRelationModel:
        activities = []

        for activity, pairs in self.__activity_object_relations.items():
            object_types = list(self.__activity_object_presence.get(activity, []))
            edges = []

            for (type1, type2), counts in pairs.items():
                most_common_card = max(counts.items(), key=lambda x: x[1])[0]
                edges.append(ObjectTypeEdge(
                    source=type1,
                    target=type2,
                    cardinality=most_common_card
                ))

            activities.append(ActivityRelation(
                activity=activity,
                object_types=object_types,
                edges=edges
            ))
        return ObjectRelationModel(activities=activities)

    def __str__(self):
        lines = ["Activity-Object Type Relations:"]
        for activity, rels in self.__activity_object_relations.items():
            lines.append(f"\nActivity: {activity}")
            for (src, tgt), counter in rels.items():
                counts = ", ".join(f"{card.value}: {cnt}" for card, cnt in counter.items() if cnt > 0)
                lines.append(f"  {src} -> {tgt}: {counts}")
        return "\n".join(lines)
