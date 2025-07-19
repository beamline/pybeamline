from datetime import datetime
from pybeamline.abstractevent import AbstractEvent

# These names are from pm4py.objects.log.util.xes which is not imported for performance reasons
DEFAULT_NAME_KEY = 'concept:name'
DEFAULT_TIMESTAMP_KEY = 'time:timestamp'
DEFAULT_TRACEID_KEY = 'concept:name'


class BEvent(AbstractEvent):

    def __init__(self, activity_name, case_id, process_name="ProcessName", event_time=None):
        self.process_attributes = dict()
        self.trace_attributes = dict()
        self.event_attributes = dict()

        self.process_attributes[DEFAULT_NAME_KEY] = process_name
        self.trace_attributes[DEFAULT_TRACEID_KEY] = case_id
        self.event_attributes[DEFAULT_NAME_KEY] = activity_name
        self.event_attributes[DEFAULT_TIMESTAMP_KEY] = datetime.now() if not event_time else event_time

    def get_process_name(self):
        return self.process_attributes[DEFAULT_NAME_KEY]

    def get_trace_name(self):
        return self.trace_attributes[DEFAULT_TRACEID_KEY]

    def get_event_name(self):
        return self.event_attributes[DEFAULT_NAME_KEY]

    def get_event_time(self):
        return self.event_attributes[DEFAULT_TIMESTAMP_KEY]

    def __str__(self) -> str:
        return "({}, {}, {}, {} - {} - {} - {})".format(
            self.get_event_name(),
            self.get_trace_name(),
            self.get_process_name(),
            str(self.get_event_time()),
            str({c: self.event_attributes[c] for c in self.event_attributes.keys() - {DEFAULT_NAME_KEY, DEFAULT_TIMESTAMP_KEY}}),
            str({c: self.trace_attributes[c] for c in self.trace_attributes.keys() - {DEFAULT_TRACEID_KEY}}),
            str({c: self.process_attributes[c] for c in self.process_attributes.keys() - {DEFAULT_NAME_KEY}})
        )

    def to_dict(self):
        return {
            DEFAULT_NAME_KEY: self.get_event_name(),
            DEFAULT_TIMESTAMP_KEY: self.get_event_time(),
            DEFAULT_TRACEID_KEY: self.get_trace_name(),
            "process_attributes": self.process_attributes,
            "trace_attributes": self.trace_attributes,
            "event_attributes": self.event_attributes
        }


