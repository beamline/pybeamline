from collections import defaultdict
from typing import Callable, Optional
from reactivex import operators as ops, Observable
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.utils.object_relation_tracker import ObjectRelationTracker


def ocdfg_merge_operator() -> Callable[[Observable], Observable]:
    """
    Reactive operator that merges incoming object-type DFG models
    into a ODFM structure, using OCDFGMerger.
    :return: RxPy operator (function) which is a MergedOCDFG
    """
    # Initialize the OCDFGMerger
    merger = OCDFGMerger()
    return lambda stream: stream.pipe(
        #ops.filter(lambda model_dict: merger.should_update(model_dict["object_type"], model_dict["model"])),
        ops.map(lambda model_dict: merger.merge(
            model_dict["object_type"],
            model_dict["model"],
            model_dict.get("relation", None)  # relation_tracker is optional
        ))
    )


class OCDFGMerger:
    """
    Merges object-centric DFGs into a global ODFM structure.
    """
    def __init__(self):
        self.__dfgs = defaultdict() # Dictionary of object type to DFG
        self.__ocdfg = OCDFG() # OCDFG (Object-Centric Directed Follows Graph)

    def merge(self,object_type: str, dfg, relation_tracker: Optional[ObjectRelationTracker] = None) -> tuple[OCDFG, ObjectRelationTracker] | OCDFG:
        """
        Merge a new model (object-type specific) into the global DFM structure.
        """
        # Overwrite old model
        self.__dfgs[object_type] = dfg

        # Reconstruct ODFG
        self.__ocdfg = OCDFG()
        for obj_type1, dfg_model in self.__dfgs.items():
            for (a1, a2) in dfg_model.dfg.keys():
                freq = dfg_model.dfg[(a1, a2)]
                # Traditional Union Merge based on similar activity names
                if relation_tracker is None:
                    self.__ocdfg.add_edge(a1, obj_type1, a2, freq)
                    continue

                # Relation-Aware merge: Only if the activities are shared
                # Check if two activities share an event
                for obj_type2 in self.__dfgs.keys():
                    if obj_type2 == obj_type1:
                        continue

                    if relation_tracker.shared_event(obj_type1, obj_type2, a1) or relation_tracker.shared_event(obj_type1, obj_type2, a2):
                        # Add edge to DFM
                        self.__ocdfg.add_edge(a1, obj_type1, a2, freq)

        # Print ODFM
        # print("\n[ODFM — Definition 8] - Object-Centric Process Mining: Dealing with Divergence and Convergence...")
        # for triple in self.odfm:
        #    print(f"{triple[0]} --({triple[1]})--> {triple[2]}")

        # Return merged model
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
