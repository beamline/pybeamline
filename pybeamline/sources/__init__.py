from pybeamline.sources.xes_log_source import xes_log_source, xes_log_source_from_file
from pybeamline.sources.string_test_source import string_test_source
from typing import Union
from pandas import DataFrame
from pm4py.objects.log.obj import EventLog
from reactivex import Observable
from pybeamline.bevent import BEvent


def log_source(log: Union[EventLog, DataFrame, list, str]) -> Observable[BEvent]:
    if type(log) is EventLog or type(log) is DataFrame:
        return xes_log_source(log)
    if type(log) is list and len(log) > 1:
        return string_test_source(log)
    if type(log) is str:
        return xes_log_source_from_file(log)
