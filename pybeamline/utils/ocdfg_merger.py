from collections import defaultdict

class OCDFGMerger:
    def __init__(self):
        self.oc_dfgs = defaultdict()
        self.odfm = defaultdict(lambda: defaultdict(int))
        self._on_change_callbacks = []

    def merge(self, model_dict):
        """
        Merges the model if it is new or has changed.
        Rebuilds the ODFM only if necessary.
        """
        obj_type = model_dict["object_type"]
        model = model_dict["model"]

        # Check if the model for the object type has changed
        if not self.should_update(obj_type, model):
            print(f"[SKIP] Model for '{obj_type}' unchanged — skipping ODFM rebuild.")
            return

        # Overwrite old model
        self.oc_dfgs[obj_type] = model

        # Reconstruct ODFM
        self.odfm = set()  # Use defaultdict for frequency if needed
        for ot, model in self.oc_dfgs.items():
            for (a1, a2), freq in model.dfg.items():
                self.odfm.add((a1, ot, a2))  # Explicit triple as per Definition 8

        # Emit change for callbacks
        self._emit_change()

        # Print ODFM
        print("\n[ODFM — Definition 8] - Object-Centric Process Mining: Dealing with Divergence and Convergence...")
        for triple in self.odfm:
            print(f"{triple[0]} --({triple[1]})--> {triple[2]}")

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

    def on_change(self, callback):
        """
        Sets a callback function to be called when the ODFM changes.
        """
        self._on_change_callbacks.append(callback)

    def _emit_change(self):
        for callback in self._on_change_callbacks:
            callback(self.odfm)

    def get_odfm(self):
        return self.odfm