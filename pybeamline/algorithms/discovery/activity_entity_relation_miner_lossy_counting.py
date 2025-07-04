import math
from typing import Dict, Any, Tuple, Set, Callable, Optional, Union
from reactivex import operators as ops, Observable, from_iterable
from reactivex import just, empty
from pybeamline.boevent import BOEvent
from pybeamline.models.aer import AER
from pybeamline.utils.cardinality import infer_cardinality, Cardinality


def activity_entity_relations_miner_lossy_counting(model_update_frequency=10, max_approx_error: float = 0.01, control_flow: Optional[Set[str]] = None) -> Callable[
    [Observable[BOEvent]], Observable[AER]]:
    """
    Entity Relationship Miner using a lossy counting approach.
    :param control_flow:
    :param model_update_frequency: Frequency of model updates
    :param max_approx_error: Maximum approximation error for the lossy counting on objects
    :return: Function to process BOEvent and return a dictionary with the model
    """
    obj_rel = ActivityEntityRelationMinerLossyCounting(max_approx_error=max_approx_error, control_flow=control_flow)

    def miner(event: Union[BOEvent,dict]) -> Observable[AER]:
        obj_rel.ingest_event(event)
        if obj_rel.observed_events() % model_update_frequency == 0:
             return just(obj_rel.get_model())
        else:
            return empty()

    return ops.flat_map(miner)

class ActivityEntityRelationMinerLossyCounting:
    def __init__(self, max_approx_error: float = 0.001, control_flow: Optional[Set[str]] = None):
        self.__control_flow = control_flow
        self.__D_C: Dict[str, Dict[Tuple[str, str], Dict[Cardinality, Tuple[int, int]]]] = {}
        self.__D_O: Dict[str, Set[str]] = {}
        self.__D_N: Dict[str, int] = {}
        self.__observed_events = 1
        self.__bucket_width = int(1 / max_approx_error)

    def ingest_event(self, event: BOEvent):
        activity = event.get_event_name()
        omap = event.get_omap()

        self.__D_N[activity] = self.__D_N.get(activity, 0) + 1

        current_bucket = math.ceil(self.__D_N[activity] / self.__bucket_width)
        obj_types = [t for t in omap if not self.__control_flow or t in self.__control_flow]

        self.__D_O.setdefault(activity, set()).update(obj_types)

        if len(obj_types) >= 2:
            rel_map = self.__D_C.setdefault(activity, {})
            for i in range(len(obj_types)):
                for j in range(i + 1, len(obj_types)):
                    type1, type2 = sorted([obj_types[i], obj_types[j]])
                    key = (type1, type2)
                    count1, count2 = len(omap[type1]), len(omap[type2])
                    card = infer_cardinality(count1, count2)

                    card_map = rel_map.setdefault(key, {})
                    if card in card_map:
                        freq, delta = card_map[card]
                        card_map[card] = (freq + 1, delta)
                    else:
                        card_map[card] = (1, current_bucket)

        if self.__D_N[activity] % self.__bucket_width == 0:
            self._cleanup(current_bucket, activity)

        self.__observed_events+= 1

    def _cleanup(self, current_bucket: int, activity: str):
        if activity not in self.__D_C:
            return
        rel_map = self.__D_C[activity]
        to_remove_keys = []

        for key, card_map in rel_map.items():
            to_remove_cards = [c for c, (freq, delta) in card_map.items() if freq + delta <= current_bucket]
            for c in to_remove_cards:
                del card_map[c]
            if not card_map:
                to_remove_keys.append(key)

        for key in to_remove_keys:
            if key in rel_map:
                del rel_map[key]

        if not rel_map:
            del self.__D_C[activity]

    def get_model(self) -> AER:
        diagram = AER()
        for activity, obj_types in self.__D_O.items():
            diagram.add_object_types(activity, obj_types)

        for activity, rel_map in self.__D_C.items():
            for (obj1, obj2), card_map in rel_map.items():
                most_common_card = max(card_map.items(), key=lambda kv: kv[1][0])[0]
                diagram.add_relation(activity, obj1, obj2, most_common_card)

        return diagram

    def observed_events(self) -> int:
        return self.__observed_events


