from collections import defaultdict
from typing import Callable, Optional, Dict, Any
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable
from pybeamline.objects.ocdfg import OCDFG

def oc_dfg_merge_operator(update_heuristic_func: Callable[[Optional[OCDFG], OCDFG], bool] = None ) -> Callable[[Observable], Observable[OCDFG]]:
    """
    Factory for an operator that merges per-object-type DFG updates
    into a single, global OCDFG on each incoming message.

    Returns:
        Callable[[Observable], Observable[OCDFG]]: a reactive operator
        that applies incoming model updates and deregistrations, then
        emits the rebuilt OCDFG.
    """
    manager = OCDFGManager(update_heuristic_func=update_heuristic_func)
    def operator(stream):
        # Map each incoming message through the manager.process method
        return stream.pipe(
            ops.map(manager.process),
            ops.filter(lambda x: x is not None),
            ops.do_action(lambda o: print(repr(o)))
        )
    return operator

class OCDFGManager:
    """
    Stateful manager that keeps the latest DFG (HeuristicsNet) per object type,
    handles deregistration events, and reconstructs the merged OCDFG on demand.

    Attributes:
        _obj_dfg_repo (Dict[str, HeuristicsNet]):
            Maps object type -> its current DFG model.
    """
    def __init__(self, update_heuristic_func: Callable[[Optional[OCDFG], OCDFG], bool]):
        self._obj_dfg_repo: Dict[str, HeuristicsNet] = {}
        self._update_heuristic = update_heuristic_func or None
        self._last_emit: Optional[OCDFG] = None


    def process(self, msg: Dict[str, Any]) -> OCDFG | None:
        """
        Handle a single incoming update or deregistration message, then
        rebuild and return the merged OCDFG.

        Args:
            msg (Dict[str, Any]):
                Expected keys:
                    - 'object_type' (str)
                    - 'model' (HeuristicsNet)
                    or
                    - 'deregister' (str) object type to remove

        Returns:
            OCDFG: the newly rebuilt object-centric DFG.
        """
        #print(f"Processing message: {msg}")
        # If new model is provided, update the repository
        if msg.get("object_type") is not None and msg.get("dfg") is not None:
            self._obj_dfg_repo[msg["object_type"]] = msg["dfg"]

        # If deregistration is requested, remove the object type
        if msg.get("deregister") is not None:
            print(f"Deregistering object type: {msg['deregister']}")
            # Deregister the object type if specified
            obj_type = msg["deregister"]
            if obj_type in self._obj_dfg_repo:
                del self._obj_dfg_repo[obj_type]

        ocdfg = self._build_ocdfg()

        if self._update_heuristic is None:
            # If no heuristic is provided, always emit the new OCDFG
            self._last_emit = ocdfg
            return ocdfg

        if self._last_emit is None or (self._update_heuristic is not None and self._update_heuristic(self._last_emit, ocdfg)):
            self._last_emit = ocdfg
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
