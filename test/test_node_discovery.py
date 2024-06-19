import unittest
import asyncio
from string import ascii_uppercase
from typing import List, Optional
from chaski.node import ChaskiNode

from .test_node_connection import _create_nodes


########################################################################
class TestDiscovery(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for testing connections between ChaskiNode instances.
    """
    host = '127.0.0.1'

    # ----------------------------------------------------------------------
    def assertConnection(self, node1, node2, msg=None):
        """"""
        conn = node1.is_connected_to(node2) and node2.is_connected_to(node1)
        return self.assertTrue(conn, msg)

    # ----------------------------------------------------------------------
    async def test_no_discovery(self):
        """"""
        nodes = await _create_nodes(2, self.host, subscriptions=list('AB'))
        await nodes[0].connect_to_peer(nodes[1])

        await asyncio.sleep(0.3)
        await nodes[1].discovery()

        for i, node in enumerate(nodes):
            self.assertEqual(len(node.server_pairs), 1, f"Node {i} discovery failed")
        self.assertConnection(*nodes, "The nodes are not connected to each other")

    # ----------------------------------------------------------------------
    async def test_single_server_connect_discovery(self):
        """"""
        nodes = await _create_nodes(3, self.host, subscriptions=list('ABB'))
        await nodes[0].connect_to_peer(nodes[1])
        await nodes[0].connect_to_peer(nodes[2])

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 2, f"Node 0 discovery failed")
        self.assertEqual(len(nodes[1].server_pairs), 1, f"Node 1 discovery failed")
        self.assertEqual(len(nodes[2].server_pairs), 1, f"Node 2 discovery failed")

        await nodes[1].discovery(on_pair='none', timeout=10)
        await nodes[2].discovery(on_pair='none', timeout=10)

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 2, f"Node 0 discovery failed after discovery")
        self.assertEqual(len(nodes[1].server_pairs), 2, f"Node 1 discovery failed after discovery")
        self.assertEqual(len(nodes[2].server_pairs), 2, f"Node 2 discovery failed after discovery")

        self.assertConnection(nodes[0], nodes[1], "The node 0 is not connected to node 1")
        self.assertConnection(nodes[0], nodes[2], "The node 0 is not connected to node 2")
        self.assertConnection(nodes[1], nodes[2], "The node 1 is not connected to node 2")

    # ----------------------------------------------------------------------
    async def test_single_discovery(self):
        """"""
        nodes = await _create_nodes(3, self.host, subscriptions=list('ABB'))
        await nodes[1].connect_to_peer(nodes[0])
        await nodes[2].connect_to_peer(nodes[0])

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 2, f"Node 0 discovery failed")
        self.assertEqual(len(nodes[1].server_pairs), 1, f"Node 1 discovery failed")
        self.assertEqual(len(nodes[2].server_pairs), 1, f"Node 2 discovery failed")

        await nodes[1].discovery(on_pair='none', timeout=10)
        await nodes[2].discovery(on_pair='none', timeout=10)

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 2, f"Node 0 discovery failed after discovery")
        self.assertEqual(len(nodes[1].server_pairs), 2, f"Node 1 discovery failed after discovery")
        self.assertEqual(len(nodes[2].server_pairs), 2, f"Node 2 discovery failed after discovery")

        self.assertConnection(nodes[0], nodes[1], "The node 0 is not connected to node 1")
        self.assertConnection(nodes[0], nodes[2], "The node 0 is not connected to node 2")
        self.assertConnection(nodes[1], nodes[2], "The node 1 is not connected to node 2")


if __name__ == '__main__':
    unittest.main()
