import networkx as nx
from matplotlib import pyplot as plt
from _common import _common as _common_

# nx.draw(t_task.tasks, with_labels=True)

@_common_.exception_handler
def display_node(graph: nx.DiGraph) -> None:
    """display job dependency

    Args:
        graph: job dependency graph

    Returns:
        None

    """

    pos = nx.spring_layout(graph)
    labels = {}
    for idx, node in enumerate(graph.nodes()):
        labels[node] = node.description

    nx.draw_networkx_nodes(graph, pos)
    nx.draw_networkx_edges(graph, pos)
    nx.draw_networkx_labels(graph, pos, labels, font_size=16)

    plt.show()