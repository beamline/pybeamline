from reactivex import Observable
from pybeamline.bevent import BEvent
from typing import Callable, Tuple
from pybeamline.mappers.to_directly_follow_relations import ToDirectlyFollowRelations


def infinite_size_directly_follows_mapper() -> Callable[[Observable[BEvent]], Observable[Tuple[str, str]]]:
    return ToDirectlyFollowRelations()