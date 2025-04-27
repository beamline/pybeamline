import os
import webbrowser
from PIL import Image

from graphviz import Digraph
import random
from pybeamline.objects.dfm import DFM

class OCDFGVisualizer:
    def __init__(self):
        self.object_type_colors = {}
        self.counter = 0
        self.snapshots = []
        self.current_index = 0
        self.snapshot_dir = os.path.join(os.getcwd(), "snapshots")

    def _get_color(self, obj_type: str):
        if obj_type not in self.object_type_colors:
            color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            while color in self.object_type_colors.values():
                color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
            self.object_type_colors[obj_type] = color
        return self.object_type_colors[obj_type]

    def draw(self, dfm: DFM) -> Digraph:
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
            dot.edge(a1, a2, label=obj_type, color=color)
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

    def render(self, dfm: DFM):
        """
        Renders the ODFM using Graphviz.
        :param dfm:
        :return: Rendered Graphviz Digraph object
        """
        dot = self.draw(dfm)
        dot.view(cleanup=True, filename="DFM-" + str(self.counter))
        self.counter += 1

    def save(self, dfm: DFM, filename, format="png"):
        """
        Saves the ODFM to a file using Graphviz.
        :param dfm:
        :param odfm:
        :param filename:
        :param format: Format of the output file (e.g., 'png', 'pdf')
        :return: saved file
        """
        dot = self.draw(dfm)
        filepath = os.path.join(self.snapshot_dir, f"{filename}_{self.counter}")
        rendered = dot.render(filepath, cleanup=True, view=False)
        self.snapshots.append(rendered)
        self.snapshots.append(rendered)
        self.counter += 1

    def next_slide(self):
        if self.current_index < len(self.snapshots) - 1:
            self.current_index += 1
            self._open_current()

    def previous_slide(self):
        if self.current_index > 0:
            self.current_index -= 1
            self._open_current()

    def _open_current(self):
        path = self.snapshots[self.current_index]
        print(f"[Slide {self.current_index + 1}/{len(self.snapshots)}] {path}")
        if os.path.exists(path):
            webbrowser.open(path)

    def generate_html_slideshow(self, output_file="ocdfg_slideshow"):
        """
        Exports the saved OCDFG snapshots to an HTML slideshow.
        :param output_file:
        :return: HTML file path
        """
        if ".html" not in output_file:
            output_file += ".html"
        with open(output_file, "w") as f:
            f.write("<html><head><title>OCDFG Slideshow</title>\n")
            f.write("""
            <style>
            body { text-align: center; font-family: sans-serif; background: #f8f8f8; }
            img { max-width: 90%; max-height: 80vh; display: none; margin-top: 20px; }
            #controls { margin-top: 20px; }
            </style>
            <script>
            let current = 0;
            function show(index) {
                let imgs = document.querySelectorAll('img');
                imgs.forEach((img, i) => img.style.display = i === index ? 'block' : 'none');
            }
            function next() { current = (current + 1) % document.images.length; show(current); }
            function prev() { current = (current - 1 + document.images.length) % document.images.length; show(current); }
            window.onload = () => show(current);
            </script>
            </head><body>
            <h1>OCDFG Slideshow</h1>
            """)

            for path in self.snapshots:
                f.write(f'<img src="{path}" alt="{path}"/>\n')

            f.write("""
            <div id="controls">
            <button onclick="prev()">Previous</button>
            <button onclick="next()">Next</button>
            </div>
            </body></html>
            """)
            webbrowser.open(output_file)

    def export_gif(self,out_file="ocdfg_evolution.gif", duration=1500, size=(1000,700)):
        """
            Exports the saved PNG snapshots as an animated GIF.

            :param out_file: The output GIF filename
            :param duration: Frame duration in milliseconds
            :param size: Size of the GIF (width, height)
        """
        if not self.snapshots:
            print("No snapshots to include in GIF.")
            return

        images = []
        for f in self.snapshots:
            img = Image.open(f)

            # Create canvas
            canvas = Image.new("RGB", size, "white")

            # Resize to fit (keep aspect ratio)
            img.thumbnail((size[0] * 0.9, size[1] * 0.9))  # 90% of full size

            # Center the image
            x = (size[0] - img.width) // 2
            y = (size[1] - img.height) // 2
            canvas.paste(img, (x, y))

            images.append(canvas)

        images[0].save(
            out_file,
            save_all=True,
            append_images=images[1:],
            optimize=False,
            duration=duration,
            loop=0
        )
        print(f"[GIF] Saved to {out_file}")