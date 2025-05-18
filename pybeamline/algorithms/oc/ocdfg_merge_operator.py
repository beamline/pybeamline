from collections import defaultdict
from typing import Callable, Optional

from reactivex import operators as ops, Observable
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import ObjectRelationMinerLossyCounting
from reactivex import merge


def ocdfg_merge_operator() -> Callable[[Observable], Observable]:
    """"
    Reactive operator that:
    - Merges object-centric DFGs into OCDFG
    - Tracks relation snapshots separately
    - Emits either OCDFG or relation state as separate events
    """
    merger = OCDFGMerger()

    def operator(stream: Observable) -> Observable:
        return stream.pipe(
            ops.publish(lambda shared: merge(

                # Relation-only stream
                shared.pipe(
                    ops.filter(lambda x: "relations" in x),
                    ops.map(lambda x: {"type": "relations", "relations": x["relations"]})
                ),

                # Model-only stream (DFG mining)
                shared.pipe(
                    ops.filter(lambda x: "model" in x and x["model"] is not None),
                    ops.map(lambda x: merger.merge(
                        x["object_type"],
                        x["model"],
                    )),
                    ops.map(lambda merged: {
                        "type": "ocdfg",
                        "ocdfg": merged[0] if isinstance(merged, tuple) else merged,
                    })
                )
            ))
        )
    return operator

class OCDFGMerger:
    """
    Merges object-centric DFGs into a global OCDFG structure.
    """
    def __init__(self):
        self.__dfgs = defaultdict() # Dictionary of object type to DFG
        self.__ocdfg = OCDFG() # OCDFG (Object-Centric Directed Follows Graph)

    def merge(self, object_type: str, dfg, relation_tracker: Optional[ObjectRelationMinerLossyCounting] = None) -> OCDFG | tuple[OCDFG, ObjectRelationMinerLossyCounting]:
        # Store the latest DFG model
        self.__dfgs[object_type] = dfg

        # Create a new OCDFG from scratch
        self.__ocdfg = OCDFG()

        # Rebuild the global OCDFG from all stored DFGs
        for obj_type, dfg_model in self.__dfgs.items():
            sources, targets = set(), set()

            for (a1, a2), freq in dfg_model.dfg.items():

                self.__ocdfg.add_edge(a1, obj_type, a2, freq)
                sources.add(a1)
                targets.add(a2)

            # Add start and end activities
            start_activities = sources - targets #  Assumption: start activities are not targets
            end_activities = targets - sources # Assumption: end activities are not sources
            self.__ocdfg.start_activities[obj_type] = start_activities
            self.__ocdfg.end_activities[obj_type] = end_activities

        if relation_tracker:
            return self.__ocdfg, relation_tracker
        else:
             return self.__ocdfg


    def should_update(self, obj_type, new_model):
        """
        Checks if the model for the object type has changed.
        Returns True if different or new, False if identical.
        """
        # If its first time seeing this object type and its not empty — update
        new_keys = set(new_model.dfg.keys())
        if obj_type not in self.__dfgs:
            return bool(new_keys)

        # If keys are changed — update
        old_keys = set(self.__dfgs[obj_type].dfg.keys())
        return new_keys != old_keys
