from pybeamline.boevent import BOEvent
from pybeamline.objects.dfg.ocdfg import OCDFG

class OCDFGManager:
    def __init__(self):
        self.graphs = {}

    # Updates the OCDFGs for each object type present in the event
    def update(self, event: BOEvent):
        # Iterate over each object references in the event
        for obj in event.get_object_refs():
            obj_type = obj["ocel:type"]
            obj_id = obj["ocel:oid"]
            if obj_type not in self.graphs:
                self.graphs[obj_type] = OCDFG(obj_type)
            self.graphs[obj_type].update(event)

    def get_combined_graph(self):
        pass
        # Merge all per-type graphs into one view (could be NetworkX MultiDiGraph)
        #return merge_graphs([g.to_graph() for g in self.graphs.values()])
