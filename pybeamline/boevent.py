from datetime import datetime
from pybeamline.abstractevent import AbstractEvent

DEFAULT_EVENT_ID = "ocel:eid"
DEFAULT_EVENT_ACTIVITY = "ocel:activity"
DEFAULT_EVENT_TIMESTAMP = "ocel:timestamp"
OCEL_OMAP_KEY = "ocel:omap"
DEFAULT_OBJECT_ID = "ocel:oid"
DEFAULT_OBJECT_TYPE = "ocel:type"

class BOEvent(AbstractEvent):

    def __init__(self, event_id, activity_name, timestamp=None, object_refs=None):
        self.event_attributes = {
            DEFAULT_EVENT_ID: event_id,
            DEFAULT_EVENT_ACTIVITY: activity_name,
            DEFAULT_EVENT_TIMESTAMP: datetime.now() if not timestamp else timestamp
        }
        self.ocel_omap = object_refs or []  # List of dicts: [{"id": "O-123", "type": "Order"}]

    def get_event_id(self):
        return self.event_attributes[DEFAULT_EVENT_ID]

    def get_event_name(self):
        return self.event_attributes[DEFAULT_EVENT_ACTIVITY]

    def get_event_time(self):
        return self.event_attributes[DEFAULT_EVENT_TIMESTAMP]

    def get_object_ids(self):
        return [obj['id'] for obj in self.ocel_omap]

    def get_object_types(self):
        return [obj['type'] for obj in self.ocel_omap]

    def __str__(self):
        return f"({self.get_event_id()}, {self.get_event_name()}, {self.get_event_time()}, {self.ocel_omap})"

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        """
        Convert the event to a dictionary representation.
        """
        return {
            DEFAULT_EVENT_ID: self.get_event_id(),
            DEFAULT_EVENT_ACTIVITY: self.get_event_name(),
            DEFAULT_EVENT_TIMESTAMP: self.get_event_time(),
            OCEL_OMAP_KEY: self.ocel_omap
        }