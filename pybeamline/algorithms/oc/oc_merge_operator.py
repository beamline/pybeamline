from typing import Callable, Optional, Dict, Any, Union
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from reactivex import operators as ops, Observable
from pybeamline.utils.commands import Command
from pybeamline.models.aer import AER
from pybeamline.models.ocdfg import OCDFG

def oc_merge_operator() -> Callable[[Observable], Observable[Dict[str,Union[OCDFG,AER]]]]:
    manager = OCMergeOperator()
    def operator(stream):
        # Map each incoming message through the manager.process method
        return stream.pipe(
            ops.map(manager.process),
            ops.filter(lambda e: e is not None),
        )
    return operator

class OCMergeOperator:
    """
    Stateful manager that keeps the latest DFG (HeuristicsNet) per object type,
    handles deregistration events, and reconstructs the merged OCDFG on demand.

    Attributes:
        _obj_dfg_repo (Dict[str, HeuristicsNet]):
            Maps object type -> its current DFG model.
        _aer_diagram (Optional[AER]):
            The latest ActivityERDiagram, which is updated with the latest relations
            and unary participations from the incoming messages.
    """
    def __init__(self):
        self._obj_dfg_repo: Dict[str, HeuristicsNet] = {}
        self._aer_diagram: Optional[AER] = None
        self._active_object_types: set[str] = set()

    def _handle_command(self, msg: Dict[str, Any], obj_type: str):
        """
        Handle a command message, which can be either; active or inactive.
        If the command is active, merged OCDFG contains that object type.
        """
        if msg["command"] == Command.ACTIVE:
            self._active_object_types.add(obj_type)
        elif msg["command"] == Command.INACTIVE:
            self._active_object_types.discard(obj_type)

    def process(self, msg: Dict[str, Any]) -> Dict[str,Union[OCDFG,AER]]:
        msg_type = msg.get("type")
        obj_type = msg.get("object_type")
        if msg_type == "model" and obj_type and isinstance(msg.get("model"), HeuristicsNet):
            self._obj_dfg_repo[obj_type] = msg["model"]

        if msg_type == "command" and obj_type and isinstance(msg.get("command"), Command):
            self._handle_command(msg, obj_type)

        elif msg_type == "aer_diagram" and isinstance(msg.get("model"), AER):
            # Overwrite the AER diagram with the latest one
            self._aer_diagram = msg["model"]


        ocdfg = self._build_ocdfg()
        aer_diagram = self._build_aer_diagram(ocdfg)
        return {"ocdfg": ocdfg, "aer_diagram": aer_diagram}

    def _build_aer_diagram(self, ocdfg: OCDFG) -> AER:
        if not self._aer_diagram:
            return AER()

        active_object_types = ocdfg.object_types
        activities = ocdfg.activities
        pruned_aer = AER()

        # Prune activities
        for activity in self._aer_diagram.activities:
            if activity not in activities:
                continue
            pruned_aer.add_activity(activity)

        # Prune entities
        for activity, types in self._aer_diagram.object_types.items():
            if activity not in activities:
                continue
            active_types = types.intersection(active_object_types)
            if active_types:
                pruned_aer.add_object_types(activity, active_types)

        # Prune relations
        for activity, rels in self._aer_diagram.relations.items():
            if activity not in activities:
                continue
            for (source, target), cardinality in rels.items():
                if source in active_object_types and target in active_object_types:
                    pruned_aer.add_relation(activity, source, target, cardinality)

        return pruned_aer


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
            if obj_type not in self._active_object_types:
                continue
            sources, targets = set(), set()
            for (a1, a2), freq in dfg_model.dfg.items():
                ocdfg.add_edge(a1, obj_type, a2, freq)
                if a1 != a2:
                    sources.add(a1)
                    targets.add(a2)

            start_activities = (sources - targets)
            end_activities = (targets - sources)

            ocdfg.start_activities[obj_type] = start_activities
            ocdfg.end_activities[obj_type] = end_activities
        return ocdfg
