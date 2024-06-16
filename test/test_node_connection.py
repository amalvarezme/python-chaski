import unittest
from chaski.node import ChaskiNode
from string import ascii_uppercase
import asyncio
from copy import copy
import subprocess

HOST = '127.0.0.1'  # IPV4
PORT = 65432


# ----------------------------------------------------------------------
async def _nodes(n, subs=None):
    """"""
    global PORT
    if subs is None:
        subs = list(ascii_uppercase)[:n]

    nodes = [ChaskiNode(HOST,
                        PORT + i,
                        name=f'Node{i}',
                        suscriptions=[sub],
                        run=True,
                        ttl=15,
                        root=(i == 0))
             for i, sub in enumerate(subs)]
    PORT += n + 1
    await asyncio.sleep(0.3)
    return nodes


########################################################################
class TestConnections(unittest.IsolatedAsyncioTestCase):

    # ----------------------------------------------------------------------
    async def test_single_connections(self):
        """"""
        nodes = await _nodes(4)
        await nodes[0].connect_to_peer(nodes[1])
        await nodes[2].connect_to_peer(nodes[3])
        await asyncio.sleep(0.3)

        for i in range(4):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")

    # ----------------------------------------------------------------------
    async def test_multiple_connections(self):
        """"""
        nodes = await _nodes(5)
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])
        await nodes[3].connect_to_peer(nodes[0])
        await nodes[4].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")
        self.assertEqual(len(nodes[0].server_pairs), 4, "Node.4 conections Failed")

    # ----------------------------------------------------------------------
    async def test_disconnection(self):
        """"""
        nodes = await _nodes(5)
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])
        await nodes[3].connect_to_peer(nodes[0])
        await nodes[4].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        await nodes[0].stop()
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].server_pairs), 0, "Node.4 conections Failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 0, f"Node.{i} conections Failed")

    # ----------------------------------------------------------------------
    async def test_edges_disconnection(self):
        """"""
        nodes = await _nodes(6)
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])
        await nodes[3].connect_to_peer(nodes[0])
        await nodes[4].connect_to_peer(nodes[0])
        await nodes[1].connect_to_peer(nodes[5])
        await nodes[2].connect_to_peer(nodes[5])
        await nodes[3].connect_to_peer(nodes[5])
        await nodes[4].connect_to_peer(nodes[5])
        await asyncio.sleep(0.3)

        edges = copy(nodes[0].server_pairs)

        for i, edge in enumerate(edges):
            await nodes[0].close_connection(edge)
            await asyncio.sleep(0.3)
            self.assertEqual(len(nodes[0].server_pairs), 3 - i, f"Node.0 conections Failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")

    # ----------------------------------------------------------------------
    async def test_edges_client_orphan(self):
        """"""
        nodes = await _nodes(5)
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])
        await nodes[3].connect_to_peer(nodes[0])
        await nodes[4].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].server_pairs), 4, f"Node.0 conections Failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")

        for i in range(1, 5):
            await nodes[i].close_connection(nodes[i].server_pairs[0])

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 4, f"Node.0 conections Failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")

    # ----------------------------------------------------------------------
    async def test_edges_server_orphan(self):
        """"""
        nodes = await _nodes(5)
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])
        await nodes[3].connect_to_peer(nodes[0])
        await nodes[4].connect_to_peer(nodes[0])
        await asyncio.sleep(0.5)

        self.assertEqual(len(nodes[0].server_pairs), 4, f"Node.0 conections Failed PRE")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed PRE")

        for edge in nodes[0].server_pairs:
            await nodes[0].close_connection(edge)

        await asyncio.sleep(1)
        self.assertEqual(len(nodes[0].server_pairs), 4, f"Node.0 conections Failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node.{i} conections Failed")


if __name__ == '__main__':
    unittest.main()
