from pybeamline.bevent import BEvent
from typing import Tuple
from pybeamline.mappers.to_directly_follow_relations import ToDirectlyFollowRelations
from pybeamline.stream.base_map import BaseMap


def infinite_size_directly_follows_mapper() -> BaseMap[BEvent, Tuple[str, str]]:
    return ToDirectlyFollowRelations()