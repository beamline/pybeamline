import os
import webbrowser
from PIL import Image
from attr import attributes
from graphviz import Digraph, Graph
import random
from graphviz import Graph
from pybeamline.objects.object_relation_model import ObjectRelationModel
from pybeamline.objects.ocdfg import OCDFG

class Visualizer:
    def __init__(self):
        self.object_type_colors = {}
        self.counter = 0
        self.snapshots_dfm = []
        self.snapshots_uml = []
        self.current_index = 0
        self.snapshot_dir = os.path.join(os.getcwd(), "snapshots")

    def _get_color(self, obj_type: str):
        if obj_type not in self.object_type_colors:
            color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            while color in self.object_type_colors.values():
                color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            self.object_type_colors[obj_type] = color
        return self.object_type_colors[obj_type]

    def draw_ocdfg(self, ocdfg: OCDFG) -> Digraph:
        """
        Draws an OCDFG using Graphviz with clearly marked start and end nodes per object type.
        :param dfm: OCDFG instance
        :return: Digraph object
        """
        dot = Digraph(format="png")

        for obj_type, transitions in ocdfg.edges.items():
            color = self._get_color(obj_type)

            # Draw edges with frequency labels
            for (a1, a2), freq in transitions.items():
                dot.edge(a1, a2, label=str(freq), color=color)

            # Start and end activities (explicitly stored)
            start_node = f"__start__{obj_type}__"
            end_node = f"__end__{obj_type}__"

            dot.node(start_node, label=f"Start ({obj_type})", shape="ellipse", style="filled", fillcolor=color)
            dot.node(end_node, label=f"End ({obj_type})", shape="ellipse", style="filled", fillcolor=color)

            for act in ocdfg.start_activities.get(obj_type, set()):
                dot.edge(start_node, act, style="dashed", color=color)

            for act in ocdfg.end_activities.get(obj_type, set()):
                dot.edge(act, end_node, style="dashed", color=color)

        return dot

    def save(self, ocdfg: OCDFG):
        ocdfg_dot = self.draw_ocdfg(ocdfg)
        #uml_dot = self.draw_uml(uml)

        # Save ocdfg
        ocdfg_path = os.path.join(self.snapshot_dir, f"ocdfg_snapshot_{self.counter}")

        ocdfg_dot.render(ocdfg_path, cleanup=True, format="png")
        #uml_dot.render(uml_path, cleanup=True, format="png")

        self.snapshots_dfm.append(ocdfg_path)
        #self.snapshots_uml.append(uml_path)
        self.counter += 1

    def draw_relation(self, model: ObjectRelationModel) -> Graph:
        dot = Graph(name="ObjectCentricRelations", format="png")
        dot.attr(rankdir="LR")  # top-bottom stacking
        dot.attr(center="true")  # center horizontally
        dot.attr(compound="true")
        dot.attr(ranksep="1.2", nodesep="0.8")
        for i, activity in enumerate(model.activities):
            with dot.subgraph(name=f"cluster_{i}") as sub:
                sub.attr(label=activity.activity)
                sub.attr(style="dashed")
                sub.attr(rank="same")
                sub.attr(fontsize="16")

                for obj_type in activity.object_types:
                    node_id = f"{activity.activity}_{obj_type}"
                    sub.node(node_id, label=obj_type, shape="ellipse")

                for edge in activity.edges:
                    src_id = f"{activity.activity}_{edge.source}"
                    tgt_id = f"{activity.activity}_{edge.target}"
                    sub.edge(src_id, tgt_id, label=edge.cardinality.value, fontname="Courier", fontsize="20")

        return dot

    def save_relation(self, relation_model: ObjectRelationModel):
        relation_dot = self.draw_relation(relation_model)
        relation_path = os.path.join(self.snapshot_dir, f"relation_snapshot_{self.counter}")

        relation_dot.render(relation_path, cleanup=True, format="png")
        self.snapshots_dfm.append(relation_path)
        self.counter += 1


    #def draw_relation(self, relation: ObjectRelation) -> Digraph:



    def generate_side_by_side_gif(self, out_file="dfm_uml_evolution.gif", duration=1500, size=(2500, 1200)):
        if not self.snapshots_dfm or not self.snapshots_uml:
            print("No snapshots to include in GIF.")
            return

        combined_images = []

        for dfm_img_path, uml_img_path in zip(self.snapshots_dfm, self.snapshots_uml):
            dfm_img = Image.open(dfm_img_path+".png")
            uml_img = Image.open(uml_img_path+".png")

            # Create a combined image
            combined_width = 1200
            combined_height = 1200
            combined = Image.new("RGB", (combined_width, combined_height), (255, 255, 255))

            # Paste the two images side by side
            combined.paste(dfm_img, (0, 0))
            combined.paste(uml_img, (dfm_img.width, 0))

            combined_images.append(combined)

        # Save the combined images as a GIF
        combined_images[0].save(
            out_file,
            save_all=True,
            append_images=combined_images[1:],
            duration=duration,
            loop=0
        )
        print(f"[GIF] Saved to {out_file}")