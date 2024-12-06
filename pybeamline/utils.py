def dfg_to_graphviz(dfg):
    graphviz = """
    digraph G {

    ranksep = 0.5
    fontsize = 9
    remincross = true
    margin = "0.0,0.0"
    outputorder = "edgesfirst"

    node[
        shape = box
        height = 0.23
        width = 1.2
        style = "rounded,filled"
        fontname = "Arial"
    ]
    edge[
        decorate = false
        fontsize = 8
        arrowsize = 0.5
        fontname = Arial
        tailclip = false
    ]
    
    start [
        shape = circle
        style = filled
        fillcolor = "#CED6BD"
        gradientangle = 270
        color = "#595F45"
        height = 0.13
        width = 0.13
        label = ""
    ]
    end [
        shape = circle
        style = filled
        fillcolor = "#D8BBB9"
        gradientangle = 270
        color = "#614847"
        height = 0.13
        width = 0.13
        label = ""
    ]
    """
    # Add all regular edges
    for (source, target), weight in dfg.items():
        graphviz += f'"{source}" -> "{target}" [penwidth={ max(0.5, min(5 * weight, 5)) },label="{weight}"];\n'

    # Add edges to start and end nodes for all nodes that have no incoming or outgoing edges
    start_nodes = {source for source, target in dfg.keys()}
    end_nodes = {target for source, target in dfg.keys()}
    for (source, target) in dfg.keys():
        if source in end_nodes:
            end_nodes.remove(source)
        if target in start_nodes:
            start_nodes.remove(target)
    for node in start_nodes:
        graphviz += f'start -> "{node}" [penwidth = 2, style = dashed, color = "#ACB89C"];\n'
    for node in end_nodes:
        graphviz += f'"{node}" -> end [penwidth = 2, style = dashed, color = "#C2B0AB"];\n'

    graphviz += "}"
    return graphviz
