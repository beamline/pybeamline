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

    def merge(self, object_type: str, dfg, relation_tracker: Optional[ObjectRelationTracker] = None) -> OCDFG | tuple[OCDFG, ObjectRelationTracker]:
        # Store the latest DFG model
        self.__dfgs[object_type] = dfg

        # Create a new OCDFG from scratch
        self.__ocdfg = OCDFG()

        # Rebuild the global OCDFG from all stored DFGs
        for obj_type, dfg_model in self.__dfgs.items():
            sources, targets = set(), set()

            for (a1, a2), freq in dfg_model.dfg.items():
                if relation_tracker is None:
                    self.__ocdfg.add_edge(a1, obj_type, a2, freq)
                    sources.add(a1)
                    targets.add(a2)
                else:
                    # Only merge edges if activities are known to relate
                    for other_obj_type in self.__dfgs:
                        if other_obj_type == obj_type:
                            continue
                        if relation_tracker.shared_event(obj_type, other_obj_type, a1) or \
                           relation_tracker.shared_event(obj_type, other_obj_type, a2):
                            self.__ocdfg.add_edge(a1, obj_type, a2, freq)
                            break  # Once shared relation is confirmed, skip further checks

            # Add start and end activities
            start_activities = sources - targets
            end_activities = targets - sources
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
