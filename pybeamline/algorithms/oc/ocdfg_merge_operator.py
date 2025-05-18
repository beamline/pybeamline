from collections import defaultdict
from typing import Callable, Optional

from reactivex import operators as ops, Observable
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import ObjectRelationMinerLossyCounting
from reactivex import merge


def ocdfg_merge_operator() -> Callable[[Observable], Observable]:
    """
    Creates a reactive operator that:
    - Merges per-object-type DFGs into a unified OCDFG model.
    - Emits either an OCDFG update or a relation snapshot (if tracked).
    """
    merger = OCDFGMerger()

    def operator(stream: Observable) -> Observable:
        return stream.pipe(
            # Share the upstream stream to fan out to multiple consumers
            ops.publish(lambda shared: merge(

                # Relation-only stream
                shared.pipe(
                    ops.filter(lambda x: "relations" in x),
                    ops.map(lambda x: {"type": "relations", "relations": x["relations"]})
                ),

                # OCDFG stream
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
    Stores the DFG for a given object type, and rebuilds the full OCDFG.
    """
    def __init__(self):
        self.__dfgs = defaultdict() # Dictionary of object type to DFG
        self.__ocdfg = OCDFG() # OCDFG (Object-Centric Directed Follows Graph)

    def merge(self, object_type: str, dfg, relation_tracker: Optional[ObjectRelationMinerLossyCounting] = None) -> OCDFG:
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

            # Heuristic: Start = nodes with no incoming edges; End = no outgoing
            start_activities = sources - targets #  Assumption: start activities are not targets
            end_activities = targets - sources # Assumption: end activities are not sources
            self.__ocdfg.start_activities[obj_type] = start_activities
            self.__ocdfg.end_activities[obj_type] = end_activities

        return self.__ocdfg

