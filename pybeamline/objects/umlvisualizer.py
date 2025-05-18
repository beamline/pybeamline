import os
import webbrowser
from graphviz import Digraph
from pybeamline.algorithms.discovery.object_relation_miner_lossy_counting import ObjectRelationMinerLossyCounting


class UMLVisualizer:
    def __init__(self, snapshot_dir="uml_snapshots"):
        self.counter = 0
        self.snapshots = []
        self.snapshot_dir = os.path.join(os.getcwd(), snapshot_dir)
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.current_index = 0

    def draw_uml(self, tracker: ObjectRelationMinerLossyCounting) -> Digraph:
        """
        Generates a UML diagram for the object relations stored in the tracker.

        :param tracker: ObjectRelationTracker instance
        :return: Graphviz Digraph object
        """
        dot = Digraph(format="png")
        dot.attr(rankdir='BT')  # Bottom-to-top for a clearer hierarchy

        # Draw object type nodes
        for obj_type, node in tracker.nodes.items():
            attributes = "\\n".join(node.attributes) if node.attributes else ""
            activities = "\\n".join(node.activities) if node.activities else ""

            label = f"{obj_type} | {attributes}\\l | {activities}\\l"
            dot.node(obj_type, label=f"{{ {label} }}", shape="record")

        # Draw relations with cardinalities
        for obj_type, node in tracker.nodes.items():
            for target_type, cardinality in node.get_cardinalities().items():
                dot.edge(obj_type, target_type, label=cardinality.value)

        return dot

    def render(self, tracker: ObjectRelationMinerLossyCounting, view=True):
        """
        Renders the UML diagram using Graphviz and optionally opens it.

        :param tracker: ObjectRelationTracker instance
        :param view: If True, opens the rendered image
        """
        dot = self.draw_uml(tracker)
        filename = os.path.join(self.snapshot_dir, f"UML-{self.counter}")
        dot.render(filename, view=view, cleanup=True)
        self.snapshots.append(filename + ".png")
        self.counter += 1

    def save(self, tracker: ObjectRelationMinerLossyCounting, filename: str):
        """
        Saves the UML diagram to a file.

        :param tracker: ObjectRelationTracker instance
        :param filename: The base name of the output PNG file (no extension)
        """
        dot = self.draw_uml(tracker)
        filepath = os.path.join(self.snapshot_dir, filename)
        dot.render(filepath, view=False, cleanup=True)
        self.snapshots.append(filepath + ".png")
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

    def generate_html_slideshow(self, output_file="uml_slideshow.html"):
        """
        Exports the saved UML snapshots to an HTML slideshow.

        :param output_file: The HTML output filename
        :return: HTML file path
        """
        if ".html" not in output_file:
            output_file += ".html"
        with open(output_file, "w") as f:
            f.write("<html><head><title>UML Slideshow</title>\n")
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
            <h1>UML Slideshow</h1>
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
