from collections import defaultdict
from typing import Callable
from reactivex import operators as ops, Observable
from pybeamline.objects.dfm import DFM
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
        ops.filter(lambda model_dict: merger.should_update(model_dict["object_type"], model_dict["model"])),
        ops.map(lambda model_dict: (
            merger.merge(
                model_dict["object_type"],
                model_dict["model"],
                model_dict["relation"]
                ),
            model_dict["relation"]
            )
        )
    )


class OCDFGMerger:
    """
    Merges object-centric DFGs into a global ODFM structure.
    """
    def __init__(self):
        self.dfgs = defaultdict() # Dictionary of object type to DFG
        self.dfm = DFM() # Directly-Follows Multigraph

    def merge(self,object_type: str, dfg, relation_tracker: ObjectRelationTracker) -> DFM:
        """
        Merge a new model (object-type specific) into the global DFM structure.
        """
        # Overwrite old model
        self.dfgs[object_type] = dfg

        # Reconstruct ODFG
        self.dfm = DFM()
        for obj_type1, dfg_model in self.dfgs.items():
            for (a1, a2) in dfg_model.dfg.keys():
                # Check if two activities share an event
                for obj_type2 in self.dfgs.keys():
                    if obj_type2 == obj_type1:
                        continue

                    if relation_tracker.shared_event(obj_type1, obj_type2, a1) or relation_tracker.shared_event(obj_type1, obj_type2, a2):
                        # Add edge to DFM
                        self.dfm.add_edge(a1, obj_type1, a2, dfg_model.dfg[(a1, a2)])

        # Print ODFM
        # print("\n[ODFM — Definition 8] - Object-Centric Process Mining: Dealing with Divergence and Convergence...")
        # for triple in self.odfm:
        #    print(f"{triple[0]} --({triple[1]})--> {triple[2]}")

        # Return merged model
        return self.dfm


    def should_update(self, obj_type, new_model):
        """
        Checks if the model for the object type has changed.
        Returns True if different or new, False if identical.
        """
        # If its first time seeing this object type and its not empty — update
        new_keys = set(new_model.dfg.keys())
        if obj_type not in self.dfgs:
            return bool(new_keys)

        # If keys are changed — update
        old_keys = set(self.dfgs[obj_type].dfg.keys())
        return new_keys != old_keys
