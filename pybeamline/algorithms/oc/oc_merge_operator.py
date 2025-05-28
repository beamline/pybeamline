from collections import defaultdict
from typing import Callable, Optional, Dict, Any
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable

from pybeamline.algorithms.oc.object_lossy_counting_operator import Command
from pybeamline.objects.ocdfg import OCDFG

def oc_merge_operator() -> Callable[[Observable], Observable[OCDFG]]:
    manager = OCMergeOperator()
    def operator(stream):
        # Map each incoming message through the manager.process method
        return stream.pipe(
            ops.map(manager.process),
            ops.filter(lambda x: x is not None),
            ops.filter(lambda g: g is not None and bool(g.edges)),
        )
    return operator

class OCMergeOperator:
    """
    Stateful manager that keeps the latest DFG (HeuristicsNet) per object type,
    handles deregistration events, and reconstructs the merged OCDFG on demand.

    Attributes:
        _obj_dfg_repo (Dict[str, HeuristicsNet]):
            Maps object type -> its current DFG model.
    """
    def __init__(self):
        self._obj_dfg_repo: Dict[str, HeuristicsNet] = {}
        self._last_emit: Optional[OCDFG] = None


    def process(self, msg: Dict[str, Any]) -> OCDFG | None:
        """
                Handle an incoming message:
                - {"type": "model", "object_type": ..., "model": ...}
                - {"type": "deregister", "object_type": ...}
                """
        msg_type = msg.get("type")
        obj_type = msg.get("object_type")
        if msg_type == "model" and obj_type and isinstance(msg.get("model"), HeuristicsNet):
            self._obj_dfg_repo[obj_type] = msg["model"]


        elif msg_type == "command" and obj_type and msg.get("command") == Command.DEREGISTER:
            self._obj_dfg_repo.pop(obj_type, None)

        else:
            # Skip unknown or malformed messages
            return None

        ocdfg = self._build_ocdfg()
        return ocdfg

    def _build_ocdfg(self) -> OCDFG:
        """
        Construct a fresh OCDFG by iterating over all stored per-object DFGs.
        For each transition (A -> B) in a per-object DFG, add an edge
        A --(object_type)--> B in the global OCDFG, overwriting frequency.

        Also record start/end activities per object type using a simple heuristic.
        """
        ocdfg = OCDFG()
        # Rebuild the global OCDFG from all stored DFGs
        for obj_type, dfg_model in self._obj_dfg_repo.items():
            self_loops = set()
            sources, targets = set(), set()
            for (a1, a2), freq in dfg_model.dfg.items():
                ocdfg.add_edge(a1, obj_type, a2, freq)
                if a1 == a2:
                    self_loops.add(a1)
                sources.add(a1)
                targets.add(a2)

            isolated_self_loops = self_loops - sources - targets
            start_activities = (sources - targets) | isolated_self_loops
            end_activities = (targets - sources) | isolated_self_loops

            ocdfg.start_activities[obj_type] = start_activities
            ocdfg.end_activities[obj_type] = end_activities
        return ocdfg
