import os
import webbrowser
from PIL import Image
from attr import attributes
from graphviz import Digraph
import random
from pybeamline.objects.ocdfg import OCDFG
from pybeamline.utils.object_relation_tracker import ObjectRelationTracker

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

    def draw_dfg(self, dfm: OCDFG) -> Digraph:
        """
        :param dfm: Directly-Follows Multigraph (DFM) object
        :return: Graphviz Digraph object
        """
        dot = Digraph(format="png")
        all_sources_by_type = {}
        all_targets_by_type = {}
        all_activities_by_type = {}

        for a1, obj_type, a2 in dfm.get_edges():
            color = self._get_color(obj_type)
            freq = str(dfm.edges[(a1, obj_type, a2)])
            dot.edge(a1, a2, label=freq, color=color)
            all_sources_by_type.setdefault(obj_type, set()).add(a1)
            all_targets_by_type.setdefault(obj_type, set()).add(a2)
            all_activities_by_type.setdefault(obj_type, set()).update([a1, a2])

        for obj_type in all_activities_by_type:
            color = self._get_color(obj_type)
            sources = all_sources_by_type.get(obj_type, set())
            targets = all_targets_by_type.get(obj_type, set())

            initial_activities = sources - targets
            final_activities = targets - sources

            start_node = f"__start__{obj_type}__"
            end_node = f"__end__{obj_type}__"

            dot.node(start_node, label=f"Start ({obj_type})", shape="ellipse", style="filled", fillcolor=color)
            dot.node(end_node, label=f"End ({obj_type})", shape="ellipse", style="filled", fillcolor=color)

            for act in initial_activities:
                dot.edge(start_node, act, style="dashed", color=color)
            for act in final_activities:
                dot.edge(act, end_node, style="dashed", color=color)

        return dot

    def draw_uml(self, uml: ObjectRelationTracker, size=(1000, 700)) -> Digraph:
        """
        Draws the UML-style diagram of object relations and cardinalities.

        :param uml: ObjectRelationTracker instance
        :param size: Size of the UML diagram (width, height)
        :return: Graphviz Digraph object
        """
        dot = Digraph(format="png")
        dot.attr(rankdir='BT', size=f"{size[0] / 100},{size[1] / 100}", dpi="150")  # Fixed size

        # Draw object type nodes with attributes and activities
        for obj_type, node in uml.nodes.items():
            color = self._get_color(obj_type)
            attributes = "\\l".join(sorted(node.attributes)) if node.attributes else ""
            activities = "\\l".join(sorted(node.activities)) if node.activities else ""

            # Separate sections for attributes and activities in the UML node
            label = f"{{ {obj_type} | + Attributes:\\l{attributes}\\l | + Activities:\\l{activities}\\l }}"

            # Create the UML node with the specified label and color
            dot.node(
                obj_type,
                label=label,
                shape="record",
                style="filled",
                fillcolor="#e1e1e1",  # Background color white for contrast
                color=color,
            )

        # Draw relations with cardinalities
        for obj_type, node in uml.nodes.items():
            for target_type, cardinality in node.get_cardinalities().items():
                dot.edge(obj_type, target_type, label=cardinality.value)

        return dot

    def save(self, dfm: OCDFG, uml: ObjectRelationTracker):
        dfm_dot = self.draw_dfg(dfm)
        #uml_dot = self.draw_uml(uml)

        # Save both DFM and UML
        dfm_path = os.path.join(self.snapshot_dir, f"dfm_snapshot_{self.counter}")
        #uml_path = os.path.join(self.snapshot_dir, f"uml_snapshot_{self.counter}")

        dfm_dot.render(dfm_path, cleanup=True, format="png")
        #uml_dot.render(uml_path, cleanup=True, format="png")

        self.snapshots_dfm.append(dfm_path)
        #self.snapshots_uml.append(uml_path)
        self.counter += 1

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