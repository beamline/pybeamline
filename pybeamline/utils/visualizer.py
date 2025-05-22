import os
from PIL import Image
from graphviz import Digraph, Graph
import random
from graphviz import Graph

from pybeamline.objects.aer_diagram import ActivityERDiagram
from pybeamline.objects.ocdfg import OCDFG

class Visualizer:
    def __init__(self):
        self.object_type_colors = {}
        self.counter = 0
        self.snapshots_ocdfg = []
        self.snapshots_relation = []
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
        # Save ocdfg
        ocdfg_path = os.path.join(self.snapshot_dir, f"ocdfg_snapshot_{self.counter}")

        ocdfg_dot.render(ocdfg_path, cleanup=True, format="png")
        self.snapshots_ocdfg.append(ocdfg_path)
        self.counter += 1

    def draw_relation(self, model: ActivityERDiagram, max_activities_per_column=5) -> Graph:
        dot = Graph(name="ObjectCentricRelations", format="png")
        dot.attr(compound="true", fontsize="14")

        col_idx = 0
        act_count = 0

        # iterate activities in insertion order
        for idx, (activity_name, edges) in enumerate(model.relations.items()):
            # wrap to next column?
            if act_count >= max_activities_per_column:
                col_idx += 1
                act_count = 0
            act_count += 1

            # cluster per activity
            with dot.subgraph(name=f"cluster_col_{col_idx}_{idx}") as sub:
                sub.attr(
                    label=activity_name,
                    style="dashed",
                    rank="same",
                    fontsize="16"
                )

                # collect all entity types in this activity
                obj_types = {t for (s, t) in edges.keys()} | {s for (s, t) in edges.keys()}

                # draw each node
                for obj in sorted(obj_types):
                    node_id = f"{activity_name}__{obj}"
                    sub.node(node_id, label=obj, shape="ellipse")

                # draw each edge with its cardinality label
                for (src, tgt), card in edges.items():
                    src_id = f"{activity_name}__{src}"
                    tgt_id = f"{activity_name}__{tgt}"
                    sub.edge(
                        src_id,
                        tgt_id,
                        label=card.value,
                        fontname="Courier",
                        fontsize="20"
                    )

        # overall left-to-right layout
        dot.attr(rankdir="LR", nodesep="1.0", ranksep="1.0")
        return dot

    def save_aer_diagram(self, relation_model: ActivityERDiagram):
        relation_dot = self.draw_relation(relation_model)
        relation_path = os.path.join(self.snapshot_dir, f"relation_snapshot_{self.counter}")

        relation_dot.render(relation_path, cleanup=True, format="png")
        self.snapshots_relation.append(relation_path)
        self.counter += 1

    def generate_ocdfg_gif(self, out_file="ocdfg_evolution.gif", duration=1500):
        if not self.snapshots_ocdfg:
            print("No OCDFG snapshots to include in GIF.")
            return

        images = [Image.open(path + ".png") for path in self.snapshots_ocdfg]
        canvas_size = self._get_max_canvas_size(images)

        padded_images = [self._center_on_canvas(img, canvas_size) for img in images]
        padded_images[0].save(
            out_file,
            save_all=True,
            append_images=padded_images[1:],
            duration=duration,
            loop=0
        )
        print(f"[GIF] Saved to {out_file}")

    def generate_relation_gif(self, out_file="relation_evolution.gif", duration=1500):
        if not self.snapshots_relation:
            print("No relation snapshots to include in GIF.")
            return

        images = [Image.open(path + ".png") for path in self.snapshots_relation]
        canvas_size = self._get_max_canvas_size(images)

        padded_images = [self._center_on_canvas(img, canvas_size) for img in images]
        padded_images[0].save(
            out_file,
            save_all=True,
            append_images=padded_images[1:],
            duration=duration,
            loop=0
        )
        print(f"[GIF] Saved to {out_file}")

    def _get_max_canvas_size(self, images):
        max_width = max(img.width for img in images)
        max_height = max(img.height for img in images)
        return (max_width, max_height)

    def _center_on_canvas(self, img, canvas_size):
        canvas = Image.new("RGB", canvas_size, (255, 255, 255))
        x_offset = (canvas_size[0] - img.width) // 2
        y_offset = (canvas_size[1] - img.height) // 2
        canvas.paste(img, (x_offset, y_offset))
        return canvas

    def _center_vertically(self, img, target_height):
        canvas = Image.new("RGB", (img.width, target_height), (255, 255, 255))
        y_offset = (target_height - img.height) // 2
        canvas.paste(img, (0, y_offset))
        return canvas