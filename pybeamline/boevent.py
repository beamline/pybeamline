from datetime import datetime

# These names are from pm4py.objects.log.util.xes which is not imported for performance reasons
DEFAULT_NAME_KEY = 'concept:name'
DEFAULT_TIMESTAMP_KEY = 'time:timestamp'
DEFAULT_TRACEID_KEY = 'concept:name'


class BOEvent:
    def __init__(self, activity_name, timestamp=None, object_refs=None):
        self.event_attributes = {
            DEFAULT_NAME_KEY: activity_name,
            DEFAULT_TIMESTAMP_KEY: datetime.now() if not timestamp else timestamp
        }
        self.object_references = object_refs or []  # List of dicts: [{"id": "O-123", "type": "Order"}]

    def get_event_name(self):
        return self.event_attributes[DEFAULT_NAME_KEY]

    def get_event_time(self):
        return self.event_attributes[DEFAULT_TIMESTAMP_KEY]

    def get_object_ids(self):
        return [obj['id'] for obj in self.object_references]

    def get_object_types(self):
        return [obj['type'] for obj in self.object_references]

    def __str__(self):
        return f"({self.get_event_name()}, {self.get_event_time()}, {self.object_references})"
