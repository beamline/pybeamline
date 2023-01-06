from pm4py.objects.log.util import xes
from datetime import datetime


class BEvent:
    def __init__(self, activityName, caseId, processName="ProcessName"):
        self.processAttributes = dict()
        self.traceAttributes = dict()
        self.eventAttributes = dict()

        self.processAttributes[xes.DEFAULT_NAME_KEY] = processName
        self.traceAttributes[xes.DEFAULT_NAME_KEY] = caseId
        self.eventAttributes[xes.DEFAULT_NAME_KEY] = activityName
        self.eventAttributes[xes.DEFAULT_TIMESTAMP_KEY] = datetime.now()

    def getProcessName(self):
        return self.processAttributes[xes.DEFAULT_NAME_KEY]

    def getTraceName(self):
        return self.traceAttributes[xes.DEFAULT_NAME_KEY]

    def getEventName(self):
        return self.eventAttributes[xes.DEFAULT_NAME_KEY]

    def getEventTime(self):
        return self.eventAttributes[xes.DEFAULT_TIMESTAMP_KEY]

    def __str__(self) -> str:
        return (
            "("
            + self.getEventName()
            + ", "
            + self.getTraceName()
            + ", "
            + self.getProcessName()
            + ", "
            + str(self.getEventTime())
            + ")"
        )
