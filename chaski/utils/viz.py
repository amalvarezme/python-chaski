import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from typing import List
from chaski.node import ChaskiNode
import seaborn as sns


# ----------------------------------------------------------------------
def display_graph(nodes: List[ChaskiNode]) -> None:
    """
    Display the network graph and latency statistics.

    Parameters
    ----------
    nodes : List[Node]
        A list of nodes, each containing name and server pairs.

    Returns
    -------
    None
    """
    G = nx.Graph()

    # Prepare nodes for graph representation
    nodes_ = [{'name': node.name, 'server_pairs': {v.name: v.latency for v in node.server_pairs}} for node in nodes]

    # Add edges to the graph
    for node in nodes_:
        for neighbor, latency in node["server_pairs"].items():
            G.add_edge(node["name"], neighbor, weight=latency)

    # Graph display options
    options = {
        "with_labels": True,
        "node_color": '#6DA58A',
        "edge_color": "#B3B3BD",
        "width": 3,
        "node_size": 1500,
        "font_color": '#ffffff',
        "font_family": 'Noto Sans',
        "font_size": 11,
        "pos": nx.circular_layout(G),
    }

    # Create the plot
    plt.figure(figsize=(16, 9), dpi=90)

    ax1 = plt.subplot2grid((3, 5), (0, 0), colspan=4, rowspan=3)
    nx.draw(G, ax=ax1, **options)

    ax2 = plt.subplot2grid((3, 5), (2, 4), colspan=1)

    # Collect latencies for statistics
    latencies = [peer.latency for node in nodes for peer in node.server_pairs]

    # Log statistics
    log = f"""
    nodes: {len(nodes)}
    ready: {sum(node.ready() for node in nodes)}/{len(nodes)}
    connections: {0.5 * sum(len(node.server_pairs) for node in nodes): .0f}
    max(latency): {np.max(latencies): .3f} ms
    min(latency): {np.min(latencies): .3f} ms
    mean(latency): {np.mean(latencies): .3f} ms
    std(latency): {np.std(latencies): .3f} ms
    """

    # Display log statistics
    font_options = {
        "fontsize": 12,
        "fontfamily": 'Noto Sans Mono',
        "fontweight": 'normal',
        "ha": 'left',
        "color": "#0B5D37",
    }
    ax2.text(0, 1, log, **font_options)
    ax2.axis('off')

    plt.show()


# ----------------------------------------------------------------------
def display_heatmap(nodes: List[ChaskiNode], show=True) -> None:
    """
    Display a heatmap of latencies between nodes.

    Parameters
    ----------
    nodes : List[Node]
        A list of Node objects, each containing server pairs with latency information.

    Returns
    -------
    None
    """
    # Create a graph dictionary from nodes
    graph = {}
    for node in nodes:
        row = {}
        for peer in node.server_pairs:
            row[peer.name] = peer.latency
        graph[node.name] = row

    # Extract node names and initialize a latency matrix
    nodes_ = list(graph.keys())
    n = len(nodes_)
    latency_matrix = np.zeros((n, n))

    # Map node names to matrix indices
    node_index = {node: idx for idx, node in enumerate(nodes_)}

    # Fill the latency matrix with values from the graph dictionary
    for node, neighbors in graph.items():
        for neighbor, latency in neighbors.items():
            i, j = node_index[node], node_index[neighbor]
            latency_matrix[i, j] = latency

    if not show:
        return latency_matrix, nodes_

    # Plot the heatmap
    plt.figure(figsize=(8, 6), dpi=90)
    sns.heatmap(latency_matrix, xticklabels=nodes_, yticklabels=nodes_, annot=True, fmt=".2f", cmap="GnBu", cbar=True, linewidths=0.5)
    plt.title('Latency Heatmap')
    # plt.xlabel('Nodes')
    # plt.ylabel('Nodes')

    plt.show()


