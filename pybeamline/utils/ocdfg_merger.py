from collections import defaultdict
from typing import Any
from pybeamline.objects import dfm
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from pybeamline.objects.dfm import DFM


class OCDFGMerger:
    def __init__(self):
        self.oc_dfgs = defaultdict() # Dictionary of object type to DFG
        self.dfm = DFM() # Directly-Follows Multigraph

    def merge(self,object_type: str, dfg) -> DFM:
        """
        Merge a new model (object-type specific) into the global DFM structure.
        """
        # Overwrite old model
        self.oc_dfgs[object_type] = dfg

        # Reconstruct ODFG
        self.dfm = DFM()
        for ot, dfg_model in self.oc_dfgs.items():
            for (a1, a2) in dfg_model.dfg.keys():
                self.dfm.add_edge(a1, ot, a2)
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
        if obj_type not in self.oc_dfgs:
            return bool(new_keys)

        # If keys are changed — update
        old_keys = set(self.oc_dfgs[obj_type].dfg.keys())
        return new_keys != old_keys
