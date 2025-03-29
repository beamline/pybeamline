from pm4py import OCEL, ocel_sort_by_additional_column

from pybeamline import *
import json
from pybeamline.sources import ocel_json_log_source
from pybeamline.sources.ocel_json_log_source import ocel_json_log_source_from_file

# Load the log from a file
ocel_json_log_source_from_file('tests/ocel.jsonocel') \
    .subscribe(lambda x: print(x))