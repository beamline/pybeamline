from collections import defaultdict
from typing import Callable, Optional, Dict, Any
from reactivex import operators as ops, Observable
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import ObjectRelationMinerLossyCounting

def oc_merge_operator() -> Callable[[Observable], Observable]:
    # Object-Centric Model Manager
    manager = OCModelManager()
    def operator(stream):
        return stream.pipe(
            ops.map(manager.process)
        )
    return operator

# Class to manage the DFG and relations
class OCModelManager:
    """
    Maintains the latest object-centric DFG repository and the relations repository.
    Incrementally rebuilds the merged OCDFG on every update and emits model updates.
    """
    def __init__(self):
        self._dfg_repo = DFGRepository()
        self._relation_repo = RelationRepository()

    def process(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        if msg.get("relations") is not None:
            self._relation_repo.update(msg["relations"])
        elif msg.get("object_type") is not None and msg.get("model") is not None:
            self._dfg_repo.update(msg["object_type"], msg["model"])

        ocdfg = self._build_ocdfg()
        return {
            "ocdfg": ocdfg,
            "relations": self._relation_repo.all()
        }

    def _build_ocdfg(self) -> OCDFG:
        dfgs = self._dfg_repo.all()
        ocdfg = OCDFG()
        # Rebuild the global OCDFG from all stored DFGs
        for obj_type, dfg_model in dfgs.items():
            sources, targets = set(), set()
            for (a1, a2), freq in dfg_model.dfg.items():
                ocdfg.add_edge(a1, obj_type, a2, freq)
                sources.add(a1)
                targets.add(a2)

            # Heuristic: Start = nodes with no incoming edges; End = no outgoing
            start_activities = sources - targets  # Assumption: start activities are not targets
            end_activities = targets - sources  # Assumption: end activities are not sources
            ocdfg.start_activities[obj_type] = start_activities
            ocdfg.end_activities[obj_type] = end_activities
        return ocdfg


class DFGRepository:
    """ Holds the states of latest per-object-type DFGs seen """
    def __init__(self):
        self._dfgs: Dict[str, Any] = {}

    def update(self, object_type: str, dfg_model: Any):
        self._dfgs[object_type] = dfg_model

    def all(self) -> Dict[str, Any]:
        return dict(self._dfgs)

class RelationRepository:
    """Holds the latest object relations."""
    def __init__(self):
        self._relations: Dict[str, Any] = {}

    def update(self, relation_model: Any):
        self._relations = relation_model

    def all(self) -> Dict[str, Any]:
        return dict(self._relations)

