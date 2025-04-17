from collections import defaultdict

class OCDFGMerger:
    def __init__(self):
        self.oc_dfgs = defaultdict(lambda : defaultdict(int)) # {obj_type: {(A,B): Frequency}}

    def merge(self, model_dict):
        print("Merging...")
        print(model_dict['object_type'])

        object_type = model_dict['object_type']
        model = model_dict['model']
        for (a, b), frequency in model.dfg.items():
            self.oc_dfgs[object_type][(a, b)] += frequency

    def get_merged_dfg(self):
        return dict(self.oc_dfgs)
