from typing import Dict, Any, Tuple, Set, Callable, Optional, Union
from reactivex import operators as ops, Observable, from_iterable
from reactivex import just, empty

from pybeamline.algorithms.oc.object_lossy_counting_operator import Command
from pybeamline.boevent import BOEvent
from pybeamline.objects.aer_diagram import ActivityERDiagram
from pybeamline.utils.cardinality import infer_cardinality, Cardinality


def object_relations_miner_lossy_counting(model_update_frequency=10, max_approx_error: float = 0.01, control_flow: Optional[Set[str]] = None) -> Callable[
    [Observable[BOEvent]], Observable[Dict[str, Any]]]:
    """
    Object Relationship Miner using a lossy counting approach.
    :param control_flow:
    :param model_update_frequency: Frequency of model updates
    :param max_approx_error: Maximum approximation error for the lossy counting on objects
    :return: Function to process BOEvent and return a dictionary with the model
    """
    obj_rel = ObjectRelationMinerLossyCounting(max_approx_error=max_approx_error, control_flow=control_flow)

    def miner(event: Union[BOEvent,dict]) -> Observable[Dict[str, Any]]:
        if isinstance(event, BOEvent):
            obj_rel.ingest_event(event)
            if obj_rel.observed_events() % model_update_frequency == 0:
                return from_iterable([
                    {"type": "aer_diagram", "model": obj_rel.get_model()},
                    event
                ])
            return just(event)
        elif isinstance(event, dict) and event.get("command") == Command.DEREGISTER:
            obj_type = event.get("object_type")
            obj_rel.deregister_object_type(obj_type)
            return just(event)  # return here as well
        return just(event)

    return ops.flat_map(miner)


class ObjectRelationMinerLossyCounting:
    def __init__(self, max_approx_error: float = 0.001, control_flow: Optional[Set[str]] = None):
        self.__control_flow: Optional[Set[str]] = control_flow
        self.__activity_object_presence: Dict[str, Set[str]] = {}
        self.__relation_tracking: Dict[str, Dict[Tuple[str, str], Dict[Cardinality, Tuple[int, int]]]] = {} # {# activity: { (type1, type2): {Cardinality: (frequency, bucket)} }}
        self.__observed_events = 0
        self.__bucket_width = int(1 / max_approx_error)

    def ingest_event(self, event: BOEvent):
        activity = event.get_event_name()
        omap = event.get_omap()

        obj_types = [t for t in omap.keys() if not self.__control_flow or t in self.__control_flow]
        if not obj_types:
            return

        current_bucket = int(self.__observed_events / self.__bucket_width)

        if activity not in self.__activity_object_presence:
            self.__activity_object_presence[activity] = set()
        if activity not in self.__relation_tracking:
            self.__relation_tracking[activity] = {}

        self.__activity_object_presence[activity].update(obj_types)

        if len(obj_types) >= 2:
            for i in range(len(obj_types)):
                for j in range(i + 1, len(obj_types)):
                    type1, type2 = sorted([obj_types[i], obj_types[j]])
                    key = (type1, type2)

                    count1 = len(omap[type1])
                    count2 = len(omap[type2])
                    cardinality = infer_cardinality(count1, count2)

                    if key not in self.__relation_tracking[activity]:
                        self.__relation_tracking[activity][key] = {}
                    if cardinality not in self.__relation_tracking[activity][key]:
                        self.__relation_tracking[activity][key][cardinality] = (0, current_bucket)

                    freq, _ = self.__relation_tracking[activity][key][cardinality]
                    self.__relation_tracking[activity][key][cardinality] = (freq + 1, current_bucket)

        if self.__observed_events % self.__bucket_width == 0:
            self._cleanup(current_bucket)

        self.__observed_events += 1

    def _cleanup(self, current_bucket: int):
        for activity in list(self.__relation_tracking.keys()):
            relation_map = self.__relation_tracking[activity]
            to_remove_relation_keys = []

            for rel_key, card_map in relation_map.items():
                to_remove_cardinalities = []

                for cardinality, (freq, last_bucket) in card_map.items():
                    if freq + last_bucket <= current_bucket:
                        to_remove_cardinalities.append(cardinality)

                for cardinality in to_remove_cardinalities:
                    del card_map[cardinality]

                if not card_map:
                    to_remove_relation_keys.append(rel_key)

            for rel_key in to_remove_relation_keys:
                del relation_map[rel_key]

                type1, type2 = rel_key
                self.__activity_object_presence[activity].discard(type1)
                self.__activity_object_presence[activity].discard(type2)

            if not relation_map:
                del self.__relation_tracking[activity]
                self.__activity_object_presence.pop(activity, None)

    def get_model(self):
        """
        Builds the current Activity-Entity Relationship Diagram using the most
        frequent surviving cardinalities per (activity, obj_type1, obj_type2) relations
        """
        aer_diagram = ActivityERDiagram()
        for activity, rel_map in self.__relation_tracking.items():
            for (obj1, obj2), card_map in rel_map.items():
                if not card_map:
                    continue  # skip empty entries

                # Find the most frequent cardinality for this (activity, relation) pair
                most_common_card = max(card_map.items(), key=lambda kv: kv[1][0])[0]
                aer_diagram.add_relation(activity, obj1, obj2, most_common_card)

        return aer_diagram

    def observed_events(self) -> int:
        return self.__observed_events

    def deregister_object_type(self, obj_type):
        for activity in list(self.__relation_tracking.keys()):
            rel_map = self.__relation_tracking[activity]
            keys_to_remove = [k for k in rel_map if obj_type in k]
            for key in keys_to_remove:
                del rel_map[key]
            self.__activity_object_presence[activity].discard(obj_type)
            if not rel_map:
                del self.__relation_tracking[activity]
                self.__activity_object_presence.pop(activity, None)

