from graphviz import Digraph
import random

def visualize_ocdfg_structure(ocdfg_edges):
    """
    Visualizes a structurally merged OCDFG.
    :param ocdfg_edges: Set of (activity1, object_type, activity2) tuples
    :return: Rendered Graphviz Digraph object
    """
    dot = Digraph(format='png')
    object_types = {edge[1] for edge in ocdfg_edges}

    # Assign unique colors per object type
    def generate_color():
        return "#{:06x}".format(random.randint(0, 0xFFFFFF))

    assigned_colors = {}
    for ot in object_types:
        color = generate_color()
        while color in assigned_colors.values():
            color = generate_color()
        assigned_colors[ot] = color

    # Add edges
    for a1, obj_type, a2 in ocdfg_edges:
        color = assigned_colors[obj_type]
        dot.edge(a1, a2, color=color, label=obj_type)

    return dot
