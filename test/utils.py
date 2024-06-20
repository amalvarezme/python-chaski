from chaski.node import ChaskiNode
from typing import List, Optional
import asyncio
from string import ascii_uppercase

PORT = 65432


# ----------------------------------------------------------------------
async def _create_nodes(n: int, host: str = '127.0.0.1', port: int = PORT, subscriptions: Optional[List[str]] = None) -> List[ChaskiNode]:
    """
    Create a list of ChaskiNode instances.

    Parameters
    ----------
    n : int
        Number of nodes to create.
    subscriptions : list of str, optional
        List of subscription topics for the nodes. If None, the first `n` letters
        of the alphabet will be used.

    Returns
    -------
    list of ChaskiNode
        List of created nodes.
    """
    if subscriptions is None:
        subscriptions = list(ascii_uppercase)[:n]

    nodes = [ChaskiNode(
        host=host,
        port=port + i,
        name=f'Node{i}',
        subscriptions=[sub],
        run=True,
        ttl=15,
        root=(i == 0)
    ) for i, sub in enumerate(subscriptions)]
    port += len(subscriptions) + 1

    await asyncio.sleep(0.5)
    return nodes
