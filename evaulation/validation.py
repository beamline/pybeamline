from collections import deque
import time
from typing import Tuple, Dict
from decorator import append
from pm4py.read import read_ocel2
from river.sketch import counter
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.models import ocdfg
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from reactivex import operators as ops, Subject
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from reactivex import zip as rx_zip

logs = { "Logistics": { "filename": "../tests/logistics.jsonocel"}}

def conform_ocdfg(ocdfg_pm4py) -> set[Tuple[str, str, str]]:
    """
    Convert PM4Py OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type in ocdfg_pm4py["edges"]["event_couples"].keys():
        for src, tgt in ocdfg_pm4py["edges"]["event_couples"][obj_type].keys():
            result.add((src, obj_type, tgt))
    return result

def conform_emit_ocdfg(ocdfg: OCDFG) -> set[Tuple[str, str, str]]:
    """
    Convert OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type, transitions in ocdfg.edges.items():
        for (src, tgt), freq in transitions.items():
            result.add((src, obj_type, tgt))
    return result

# Convert the PM4Py OCDFG to a set of edges
def edges_pr_object_type(ocdfg: set[Tuple[str,str,str]]) -> Dict[str, set[Tuple[str, str, str]]]:
    """
    Convert OCDFG to a dictionary of edges per object type.
    """
    result = {}
    for src, obj_type, tgt in ocdfg:
        if obj_type not in result:
            result[obj_type] = set()
        result[obj_type].add((src,obj_type, tgt))
    return result

def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

for log in logs:
    log_file = logs[log]["filename"]
    # Read the OCDFG from the log file
    ocdfg_pm4py = read_ocel2(log_file)
    ocdfg_offline_discovery = ocdfg_discovery.apply(ocdfg_pm4py)
    # Convert the PM4Py OCDFG to a set of edges
    ocdfg_edges_pm4py = conform_ocdfg(ocdfg_offline_discovery)
    logs[log]["pm4py"] = ocdfg_edges_pm4py


def handle_snapshot(snapshot: OCDFG, log_name: str):
    logs[log_name]["snapshots"].append(snapshot)



for log in logs:
    logs[log]["snapshots"] = []
    source = ocel_log_source_from_file(logs[log]["filename"])


    source.pipe(
        ops.take(75),
        oc_operator(),
        oc_merge_operator()
    ).subscribe(lambda snapshot: handle_snapshot(snapshot["ocdfg"], log))



for log in logs:
    logs[log]["jaccard_similarities"] = []



