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
        self.__D_C: Dict[str, Dict[Any, Any]] = {}
        self.__observed_events = 0
        self.__bucket_width = int(1 / max_approx_error)

    def ingest_event(self, event: BOEvent):
        activity = event.get_event_name()
        omap = event.get_omap()
        current_bucket = math.ceil(self.__observed_events / self.__bucket_width)
        obj_types = [t for t in omap if not self.__control_flow or t in self.__control_flow]

        act_data = self.__D_C.setdefault(activity, {"object_types": set(), "relations": {}})
        act_data["object_types"].update(obj_types)

        if len(obj_types) >= 2:
            for i in range(len(obj_types)):
                for j in range(i + 1, len(obj_types)):
                    type1, type2 = sorted([obj_types[i], obj_types[j]])
                    key = (type1, type2)
                    count1, count2 = len(omap[type1]), len(omap[type2])
                    card = infer_cardinality(count1, count2)

                    rel_map = act_data["relations"].setdefault(key, {})
                    if card in rel_map:
                        freq, delta = rel_map[card]
                        rel_map[card] = (freq + 1, delta)
                    else:
                        rel_map[card] = (1, current_bucket)

        if self.__observed_events % self.__bucket_width == 0:
            self._cleanup(current_bucket)

        self.__observed_events += 1

    def _cleanup(self, current_bucket: int):
        for activity, act_data in list(self.__D_C.items()):
            relations = act_data.get("relations", {})
            to_delete_keys = []

            for key, card_map in relations.items():
                to_remove = [card for card, (freq, last) in card_map.items()
                             if freq + last <= current_bucket]
                for card in to_remove:
                    del card_map[card]
                if not card_map:
                    to_delete_keys.append(key)

            for key in to_delete_keys:
                del relations[key]

            if not relations and not act_data["object_types"]:
                del self.__D_C[activity]

    def get_model(self):
        diagram = AER()
        for activity, act_data in self.__D_C.items():
            diagram.add_object_types(activity, act_data.get("object_types", set()))
            for (obj1, obj2), card_map in act_data.get("relations", {}).items():
                most_common_card = max(card_map.items(), key=lambda kv: kv[1][0])[0]
                diagram.add_relation(activity, obj1, obj2, most_common_card)
        return diagram

    def observed_events(self) -> int:
        return self.__observed_events



