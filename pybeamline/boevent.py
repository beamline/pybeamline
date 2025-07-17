from datetime import datetime
from typing import List, Optional, Dict, Set
from pybeamline.abstractevent import AbstractEvent

DEFAULT_EVENT_ID = "ocel:eid"
DEFAULT_EVENT_ACTIVITY = "ocel:activity"
DEFAULT_EVENT_TIMESTAMP = "ocel:timestamp"
OCEL_OMAP_KEY = "ocel:omap"
DEFAULT_OBJECT_ID = "ocel:oid"
DEFAULT_OBJECT_TYPE = "ocel:type"
OCEL_VMAP_KEY = "ocel:vmap"


class BOEvent(AbstractEvent):

    def __init__(self, event_id: str, activity_name: str, omap: Dict[str, Set[str]], timestamp: Optional[datetime] = None, vmap: Optional[Dict[str, str]] = None):
        """
        Represents a single event aligned with OCEL specification.
        :param event_id: Unique event identifier (ocel:eid)
        :param activity_name: Name of the activity (ocel:activity)
        :param timestamp: Timestamp of the event (ocel:timestamp)
        :param omap: List of associated objects (ocel:omap)
        :param vmap: Additional event attributes (ocel:vmap)
        """
        self.event_id = event_id
        self.activity_name = activity_name
        self.timestamp = timestamp or datetime.now()
        self.omap = omap
        self.vmap = vmap or {}


    def get_event_id(self):
        return self.event_id

    def get_event_name(self):
        return self.activity_name

    def get_event_time(self):
        return self.timestamp

    def get_object_ids(self) -> List[str]:
        return [oid for ids in self.omap.values() for oid in ids]

    def get_omap_types(self):
        return list(self.omap.keys())

    def get_omap(self):
        return self.omap

    def get_vmap(self):
        return self.vmap

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            DEFAULT_EVENT_ID: self.event_id,
            DEFAULT_EVENT_ACTIVITY: self.activity_name,
            DEFAULT_EVENT_TIMESTAMP: self.timestamp,
            OCEL_OMAP_KEY: self.omap,
            OCEL_VMAP_KEY: self.vmap
        }