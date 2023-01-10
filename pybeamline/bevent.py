from pm4py.objects.log.util import xes
from datetime import datetime


class BEvent:
    def __init__(self, activity_name, case_id, process_name="ProcessName", event_time=datetime.now()):
        self.process_attributes = dict()
        self.trace_attributes = dict()
        self.event_attributes = dict()

        self.process_attributes[xes.DEFAULT_NAME_KEY] = process_name
        self.trace_attributes[xes.DEFAULT_NAME_KEY] = case_id
        self.event_attributes[xes.DEFAULT_NAME_KEY] = activity_name
        self.event_attributes[xes.DEFAULT_TIMESTAMP_KEY] = event_time

    def get_process_name(self):
        return self.process_attributes[xes.DEFAULT_NAME_KEY]

    def get_trace_name(self):
        return self.trace_attributes[xes.DEFAULT_NAME_KEY]

    def get_event_name(self):
        return self.event_attributes[xes.DEFAULT_NAME_KEY]

    def get_event_time(self):
        return self.event_attributes[xes.DEFAULT_TIMESTAMP_KEY]

    def __str__(self) -> str:
        return (
            "(" + self.get_event_name() + ", " + self.get_trace_name()
            + ", " + self.get_process_name() + ", " + str(self.get_event_time())
            + " - " +
            str({c: self.event_attributes[c] for c in self.event_attributes.keys() - {xes.DEFAULT_NAME_KEY, xes.DEFAULT_TIMESTAMP_KEY}})
            + " - " +
            str({c: self.trace_attributes[c] for c in self.trace_attributes.keys() - {xes.DEFAULT_NAME_KEY}})
            + " - " +
            + str({c: self.process_attributes[c] for c in self.process_attributes.keys() - {xes.DEFAULT_NAME_KEY}})
            + ")"
        )
