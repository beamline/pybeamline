from pybeamline.sources.xes_log_source import xes_log_source_from_file, XesLogSource
from pybeamline.sources.string_test_source import string_test_source
from pybeamline.sources.mqttxes_source import mqttxes_source
from typing import Union
from pandas import DataFrame
from pm4py.objects.log.obj import EventLog
from pybeamline.bevent import BEvent
from pybeamline.stream.stream import Stream

def log_source(log: Union[EventLog, DataFrame, list, str]) -> Stream[BEvent]:
    if type(log) is EventLog or type(log) is DataFrame:
        return Stream.source(XesLogSource(log))
    if type(log) is list and len(log) > 1:
        return string_test_source(log)
    if type(log) is str:
        return xes_log_source_from_file(log)
    return Stream.empty()

