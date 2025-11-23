from typing import Optional, Dict, Any, Union, List
from pm4py.objects.heuristics_net.obj import HeuristicsNet

from pybeamline.stream.base_map import BaseMap
from pybeamline.utils.commands import Command
from pybeamline.models.aer import AER
from pybeamline.models.ocdfg import OCDFG

def oc_merge_operator() -> BaseMap[Dict[str, Any], Dict[str, Union[OCDFG,AER]]]:
    """
    Factory function to create a reactive operator that merges multiple per-object-type
    Directly-Follows Graphs (DFGs) and a global Activity-Entity Relationship (AER) model
    into a unified, synchronised object-centric representation. This operator must be placed
    downstream of the OCOperator in the pipeline.

    The operator listens to a mixed stream of:
        - "dfg" messages, containing per-object-type control-flow models (HeuristicsNet)
        - "command" messages, which dynamically activate or deactivate object types
        - "aer" messages, representing the latest global AER model mined from events

    It keeps track of the currently active object types, updates internal DFG repositories,
    and reconstructs a global OCDFG accordingly. Additionally, it prunes the AER model
    to reflect only activities and relations relevant to the active OCDFG, thereby
    maintaining semantic consistency across views.

    :return:
        A callable operator that transforms a stream of mixed mining outputs into a stream
        of synchronised models: a dictionary with keys "ocdfg" (OCDFG) and "aer" (AER).
    """
    return OCMergeOperatorMapper()


class OCMergeOperatorMapper(BaseMap[Dict[str, Any], Dict[str,Union[OCDFG,AER]]]):

    def __init__(self):
        self.oc_operator = OCMergeOperator()

    def transform(self, value: Dict[str, Any]) -> Optional[List[Dict[str,Union[OCDFG,AER]]]]:
        return [self.oc_operator.process(value)]


class OCMergeOperator:
    """
    Stateful manager responsible for maintaining the latest per-object-type
    control-flow models (HeuristicsNet) and the global Activity-Entity Relationship (AER) model.
    It processes activation and deactivation commands to track which object types
    are currently active, constructs a merged OCDFG accordingly, and prunes the AER
    model to include only activities and object-type relations relevant to the current OCDFG.
    This ensures a synchronised and coherent representation of both control-flow
    and structural relationships in the process.
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

    def process(self, msg: Dict[str, Any]) -> Optional[Dict[str,Union[OCDFG,AER]]]:
        msg_type = msg.get("type")
        obj_type = msg.get("object_type")

        if msg_type == "dfg" and obj_type and isinstance(msg.get("model"), HeuristicsNet):
            # Overwrite the DFG for the object type
            self._obj_dfg_repo[obj_type] = msg["model"]
        if msg_type == "command" and obj_type and isinstance(msg.get("command"), Command):
            # Handle the command for the object type
            self._handle_command(msg, obj_type)
            return None # Suppress the command message
        if msg_type == "aer" and isinstance(msg.get("model"), AER):
            # Overwrite the AER diagram with the latest one
            self._aer_diagram = msg["model"]

        ocdfg = self._build_ocdfg()
        aer_diagram = self._build_aer_diagram(ocdfg)
        return {"ocdfg": ocdfg, "aer": aer_diagram}

    def _build_aer_diagram(self, ocdfg: OCDFG) -> AER:
        """
        Builds the Activity-Entity Relationship (AER) model reflecting behaviour
        of the current OCDFG.
        """
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
        Builds the global OCDFG from the stored DFGs per object type.
        Also record start/end activities per object type using a simple heuristic.
        Note:
        Construction is based on description of the OCDFG concept in
        Berti & van der Aalst (2023) "OC-PM: analyzing object-centric event logs and process models"
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

            # Simple heuristic to determine start and end activities
            start_activities = (sources - targets)
            end_activities = (targets - sources)

            ocdfg.start_activities[obj_type] = start_activities
            ocdfg.end_activities[obj_type] = end_activities
        return ocdfg
