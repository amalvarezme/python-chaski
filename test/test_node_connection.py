"""
==========================
Connection Handling Module
==========================

This module provides functionality for handling connections between ChaskiNode instances.
It includes test classes for IPv4 and IPv6 connections.

Classes:
    - TestConnections: Base class for connection tests.
    - TestConnectionsForIPv4: Tests for IPv4 connections.
    - TestConnectionsForIPv6: Tests for IPv6 connections.
"""

import unittest
import asyncio
from string import ascii_uppercase
from typing import List, Optional
from chaski.node import ChaskiNode

PORT = 65432


# ----------------------------------------------------------------------
async def _create_nodes(n: int, host: str = '127.0.0.1', subscriptions: Optional[List[str]] = None) -> List[ChaskiNode]:
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
    global PORT
    if subscriptions is None:
        subscriptions = list(ascii_uppercase)[:n]

    nodes = [ChaskiNode(
        host=host,
        port=PORT + i,
        name=f'Node{i}',
        subscriptions=[sub],
        run=True,
        ttl=15,
        root=(i == 0)
    ) for i, sub in enumerate(subscriptions)]
    PORT += n + 1
    await asyncio.sleep(0.3)
    return nodes


########################################################################
class TestConnections:
    """
    Unit tests for testing connections between ChaskiNode instances.
    """

    # ----------------------------------------------------------------------
    async def test_single_connections(self):
        """
        Test single connections between pairs of nodes.
        """
        nodes = await _create_nodes(4, self.host)
        await nodes[0].connect_to_peer(nodes[1])
        await nodes[2].connect_to_peer(nodes[3])
        await asyncio.sleep(0.3)

        for i in range(4):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connection failed")

    # ----------------------------------------------------------------------

    async def test_multiple_connections(self):
        """
        Test multiple connections to a single node.
        """
        nodes = await _create_nodes(5, self.host)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i}'s connection to Node 0 failed")
        self.assertEqual(len(nodes[0].server_pairs), 4, f"Node 0 failed to establish all connections")

    # ----------------------------------------------------------------------
    async def test_disconnection(self):
        """
        Test disconnection of nodes.
        """
        nodes = await _create_nodes(5, self.host)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        await nodes[0].stop()
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].server_pairs), 0, "Node 0 not disconnected")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 0, f"Node {i} not disconnected")

    # ----------------------------------------------------------------------
    async def test_edges_disconnection(self):
        """
        Test disconnection of edge nodes.
        """
        nodes = await _create_nodes(6, self.host)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[5])

        await asyncio.sleep(0.3)
        for i in range(4):
            await nodes[0].close_connection(nodes[0].server_pairs[0])
            await asyncio.sleep(0.3)
            self.assertEqual(len(nodes[0].server_pairs), max(3 - i, 1), "Node 0 connections failed")

        await asyncio.sleep(0.3)
        for i in range(1, 4):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connections failed")
        self.assertEqual(len(nodes[4].server_pairs), 2, "Node 4 connections failed")

    # ----------------------------------------------------------------------
    async def test_edges_client_orphan(self):
        """
        Test edge nodes becoming orphaned on the client side.
        """
        nodes = await _create_nodes(5, self.host)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].server_pairs), 4, "Node 0 connections failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connections failed")

        for i in range(1, 5):
            await nodes[i].close_connection(nodes[i].server_pairs[0])

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 4, "Node 0 connections failed after orphan detection")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connections failed after orphan detection")

    # ----------------------------------------------------------------------
    async def test_edges_server_orphan(self):
        """
        Test edge nodes becoming orphaned on the server side.
        """
        nodes = await _create_nodes(5, self.host)
        for i in range(1, 5):
            await nodes[i].connect_to_peer(nodes[0])
        await asyncio.sleep(0.5)

        self.assertEqual(len(nodes[0].server_pairs), 4, "Node 0 connections failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connections failed")

        for edge in nodes[0].server_pairs:
            await nodes[0].close_connection(edge)

        await asyncio.sleep(0.3)
        self.assertEqual(len(nodes[0].server_pairs), 4, "Node 0 connections failed after orphan detection")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].server_pairs), 1, f"Node {i} connections failed after orphan detection")


########################################################################
class Test_Connections_for_IPv4(TestConnections, unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for testing connections between ChaskiNode instances using IPv4.
    """

    # ----------------------------------------------------------------------
    async def asyncSetUp(self) -> None:
        """
        Set up the test environment for IPv4 connections.
        """
        self.host = '127.0.0.1'
        await asyncio.sleep(0)  # PSS: Added await keyword for asyncio.sleep


########################################################################
class Test_Connections_for_IPv6(unittest.IsolatedAsyncioTestCase, TestConnections):
    """
    Unit tests for testing connections between ChaskiNode instances using IPv6.
    """

    # ----------------------------------------------------------------------
    async def asyncSetUp(self) -> None:
        """
        Set up the test environment for IPv6 connections.
        """
        self.host = '::1'
        await asyncio.sleep(0)  # PSS: Added await keyword for asyncio.sleep


if __name__ == '__main__':
    unittest.main()
