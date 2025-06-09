from typing import Dict, Any, Tuple, Set, Callable, Optional, Union
from reactivex import operators as ops, Observable, from_iterable
from reactivex import just, empty
from pybeamline.boevent import BOEvent
from pybeamline.objects.aer_diagram import ActivityERDiagram
from pybeamline.utils.cardinality import infer_cardinality, Cardinality


def activity_object_relations_miner_lossy_counting(model_update_frequency=10, max_approx_error: float = 0.01, control_flow: Optional[Set[str]] = None) -> Callable[
    [Observable[BOEvent]], Observable[ActivityERDiagram]]:
    """
    Object Relationship Miner using a lossy counting approach.
    :param control_flow:
    :param model_update_frequency: Frequency of model updates
    :param max_approx_error: Maximum approximation error for the lossy counting on objects
    :return: Function to process BOEvent and return a dictionary with the model
    """
    obj_rel = ActivityObjectRelationMinerLossyCounting(max_approx_error=max_approx_error, control_flow=control_flow)

    def miner(event: Union[BOEvent,dict]) -> Observable[ActivityERDiagram]:
        obj_rel.ingest_event(event)
        if obj_rel.observed_events() % model_update_frequency == 0:
             return just(obj_rel.get_model())
        else:
            return empty()

    return ops.flat_map(miner)


class ActivityObjectRelationMinerLossyCounting:
    def __init__(self, max_approx_error: float = 0.001, control_flow: Optional[Set[str]] = None):
        self.__control_flow: Optional[Set[str]] = control_flow
        self.__relation_tracking: Dict[str, Dict[Tuple[str, str], Dict[Cardinality, Tuple[int, int]]]] = {} # {# activity: { (type1, type2): {Cardinality: (frequency, bucket)} }}
        self.__unary_tracking: Dict[str, Dict[str, int]] = {}
        self.__observed_events = 0
        self.__bucket_width = int(1 / max_approx_error)

    def ingest_event(self, event: BOEvent):
        activity = event.get_event_name()
        omap = event.get_omap()
        current_bucket = int(self.__observed_events / self.__bucket_width)

        obj_types = [t for t in omap.keys() if not self.__control_flow or t in self.__control_flow]

        if activity not in self.__unary_tracking:
            self.__unary_tracking[activity] = {}

        # If unary (only one object type), track unary participation
        if len(obj_types) == 1:
            only_type = obj_types[0]
            self.__unary_tracking[activity][only_type] = self.__unary_tracking[activity].get(only_type, 0) + 1

        # If binary or more, remove unary participations for those types
        elif len(obj_types) >= 2:
            for t in obj_types:
                if t in self.__unary_tracking[activity]:
                    del self.__unary_tracking[activity][t]
            # Clean up empty unary tracking dict
            if not self.__unary_tracking[activity]:
                del self.__unary_tracking[activity]

        if activity not in self.__relation_tracking:
            self.__relation_tracking[activity] = {}

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
        # Iterate over all activities for which we have relation tracking
        for activity in list(self.__relation_tracking.keys()):
            relation_map = self.__relation_tracking[activity]
            to_remove_relation_keys = []

            # Go through each relation key (a pair of object types)
            for rel_key, card_map in relation_map.items():
                to_remove_cardinalities = []

                # Evaluate each observed cardinality for that object pair
                for cardinality, (freq, last_bucket) in card_map.items():
                    if freq + last_bucket <= current_bucket:
                        to_remove_cardinalities.append(cardinality)

                # Actually remove those outdated cardinalities
                for cardinality in to_remove_cardinalities:
                    del card_map[cardinality]

                # If no cardinalities remain for this relation, mark the relation key for removal
                if not card_map:
                    to_remove_relation_keys.append(rel_key)

            # Remove relation keys that no longer have valid cardinalities
            for rel_key in to_remove_relation_keys:
                del relation_map[rel_key]

            # If no relations are left for this activity, remove the whole activity
            if not relation_map:
                del self.__relation_tracking[activity]

    def get_model(self):
        """
        Builds the current Activity-Entity Relationship Diagram using the most
        frequent surviving cardinalities per (activity, obj_type1, obj_type2) relations
        """
        aer_diagram = ActivityERDiagram()
        for activity, rel_map in self.__relation_tracking.items():
            for (obj1, obj2), card_map in rel_map.items():

                # Find the most frequent cardinality for this (activity, relation) pair
                most_common_card = max(card_map.items(), key=lambda kv: kv[1][0])[0]
                aer_diagram.add_relation(activity, obj1, obj2, most_common_card)

        for activity, type_counts in self.__unary_tracking.items():
            for obj_type, count in type_counts.items():
                if count > 0:
                    aer_diagram.add_unary_participation(activity, obj_type)

        return aer_diagram

    def observed_events(self) -> int:
        return self.__observed_events


