from chaski.node import ChaskiNode
from typing import List, Union
import asyncio
from string import ascii_uppercase

PORT = 65432


# ----------------------------------------------------------------------
async def create_nodes(subscriptions: Union[int, List[str]], host: str = '127.0.0.1', port: int = PORT) -> List[ChaskiNode]:
    """
    Create a list of ChaskiNode instances.

    This function generates a list of ChaskiNode instances with the given number of nodes
    or subscriptions. If an integer is provided for subscriptions, the first `n` letters
    of the alphabet will be used as default subscription topics. Each node will run on
    a sequentially incremented port starting from the given port number.

    Parameters
    ----------
    subscriptions : Union[int, List[str]]
        The number of nodes to create if an integer is provided. If a list of strings is
        provided, each string represents a subscription topic for a node.
    host : str, optional
        The host IP address or hostname where the nodes will bind to, by default '127.0.0.1'.
    port : int, optional
        The starting port number for the nodes, by default PORT.

    Returns
    -------
    List[ChaskiNode]
        A list of ChaskiNode instances.
    """
    if isinstance(subscriptions, int):
        subscriptions = list(ascii_uppercase)[:subscriptions]

    nodes = [ChaskiNode(
        host=host,
        port=port + i,
        name=f'Node{i}',
        subscriptions=sub,
        run=True,
        ttl=15,
        root=(i == 0)
    ) for i, sub in enumerate(subscriptions)]
    port += len(subscriptions) + 1

    await asyncio.sleep(0.5)
    return nodes
