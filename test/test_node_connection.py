"""
===========================
Chaski Test Node Connection
===========================

This module provides test cases for verifying the connection-related
functionality of ChaskiNode instances within a distributed network.
It includes test classes and utility methods that ensure nodes can
establish, maintain, and disconnect peer-to-peer connections effectively.

Classes
-------
TestConnections:
    Base class containing utility methods and asynchronous test methods
    to verify connections between ChaskiNode instances.

Test_Connections_for_IPv4:
    Derived class that extends TestConnections for testing IPv4 connections
    specifically.

Test_Connections_for_IPv6:
    Derived class that extends TestConnections for testing IPv6 connections
    specifically.
"""

import unittest
import asyncio
from chaski.utils.auto import create_nodes
from typing import Optional


########################################################################
class TestConnections:
    """
    Base class for testing connection-related functionality between ChaskiNode instances.

    This class provides utility methods and asynchronous test methods to verify the
    ability of ChaskiNodes to establish, maintain, and disconnect peer-to-peer
    connections. Derived classes can utilize these tests to validate specific
    communication protocols, such as IPv4 and IPv6.
    """

    # ----------------------------------------------------------------------
    async def _close_nodes(self, nodes: list['ChaskiNode']):
        """
        Close all ChaskiNode instances in the provided list.

        This method iterates through each ChaskiNode instance in the given list and
        stops their operation by invoking the `stop` method on each node.

        Parameters
        ----------
        nodes : list of ChaskiNode
            A list containing instances of ChaskiNode that need to be stopped.
        """
        for node in nodes:
            await node.stop()

    # ----------------------------------------------------------------------
    def assertConnection(
        self, node1: 'ChaskiNode', node2: 'ChaskiNode', msg: Optional[str] = None
    ):
        """
        Assert that two ChaskiNodes are connected to each other.

        This method checks if `node1` is connected to `node2` and vice versa.
        It raises an assertion error if the connection is not established in
        both directions.

        Parameters
        ----------
        node1 : ChaskiNode
            The first ChaskiNode to check connection from.
        node2 : ChaskiNode
            The second ChaskiNode to check connection to.
        msg : str, optional
            An optional message to include in the assertion error if the
            nodes are not connected.

        Raises
        ------
        AssertionError
            If `node1` is not connected to `node2` or `node2` is not connected to `node1`.
        """
        conn = node1.is_connected_to(node2) and node2.is_connected_to(node1)
        return self.assertTrue(conn, msg)

    # ----------------------------------------------------------------------
    async def test_single_connections(self):
        """
        Test single connections between ChaskiNodes.

        This asynchronous method tests the ability of ChaskiNode instances to
        establish individual peer-to-peer connections.

        Steps:
        1. Create 4 nodes.
        2. Connect Node 0 to Node 1 and Node 2 to Node 3.
        3. Verify that each node has established 1 connection.
        4. Close all nodes.
        5. Repeat steps 1-4 to ensure consistency.

        Raises
        ------
        AssertionError
            If any node fails to establish the expected number of connections.
        """
        nodes = await create_nodes(4, self.ip)
        await nodes[0]._connect_to_peer(nodes[1])
        await nodes[2]._connect_to_peer(nodes[3])
        await asyncio.sleep(3)

        for i in range(4):
            self.assertEqual(len(nodes[i].edges), 1, f"Node {i} connection failed")

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_multiple_connections(self):
        """
        Test multiple connections to a single ChaskiNode.

        This asynchronous method tests the ability of a single ChaskiNode to
        handle multiple peer-to-peer connections simultaneously.

        Steps:
        1. Create 5 nodes.
        2. Connect Node 1, Node 2, Node 3, and Node 4 to Node 0.
        3. Verify that Nodes 1-4 each have 1 connection.
        4. Verify that Node 0 has 4 connections.
        5. Close all nodes.
        6. Repeat steps 1-5 to ensure consistency.

        Raises
        ------
        AssertionError
            If any node fails to establish the expected number of connections.
        """
        nodes = await create_nodes(5, self.ip)
        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        for i in range(1, 5):
            self.assertEqual(
                len(nodes[i].edges), 1, f"Node {i}'s connection to Node 0 failed"
            )
        self.assertEqual(
            len(nodes[0].edges), 4, f"Node 0 failed to establish all connections"
        )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_disconnection(self):
        """
        Test disconnection of nodes.

        This method tests the ability of nodes to handle disconnection events.
        It establishes connections between nodes, stops one node, and verifies
        if all nodes properly reflect the disconnection.

        Steps:
        1. Create 5 nodes.
        2. Connect Node 1, Node 2, Node 3, and Node 4 to Node 0.
        3. Stop Node 0.
        4. Verify that Node 0 and Nodes 1-4 have no active connections.
        5. Close all nodes.

        Raises
        ------
        AssertionError
            If any node fails to properly disconnect or maintain the expected
            state after disconnection.
        """
        nodes = await create_nodes(5, self.ip)
        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        await nodes[0].stop()
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].edges), 0, "Node 0 not disconnected")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].edges), 0, f"Node {i} not disconnected")

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_edges_disconnection(self):
        """
        Test progressive disconnection of nodes from edge nodes.

        This method is designed to evaluate the behavior and stability of ChaskiNodes
        when edge nodes selectively disconnect from the network. It ensures the nodes
        handle partial disconnections without compromising the remaining connections.

        Steps:
        1. Create 6 nodes.
        2. Connect Node 1 through Node 4 to Node 0.
        3. Connect Node 1 through Node 4 to Node 5.
        4. Sequentially disconnect Node 0's connections.
        5. Verify the connection count after each disconnection.
        6. Verify that Node 1 through Node 4 remain connected to Node 5.
        7. Close all nodes.

        Raises
        ------
        AssertionError
            If any node fails to properly manage connections or disconnections.
        """
        nodes = await create_nodes(6, self.ip)

        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[5])

        await asyncio.sleep(0.3)
        for i in range(4):
            await nodes[0].close_connection(nodes[0].edges[0])
            await asyncio.sleep(0.3)
            self.assertEqual(
                len(nodes[0].edges), max(3 - i, 1), "Node 0 connections failed"
            )

        await asyncio.sleep(0.3)
        for i in range(1, 4):
            self.assertEqual(len(nodes[i].edges), 1, f"Node {i} connections failed")
        self.assertEqual(len(nodes[4].edges), 2, "Node 4 connections failed")

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_edges_client_orphan(self):
        """
        Test when client-edge nodes become orphaned.

        This method assesses how the system handles the scenario where edge nodes acting as clients
        get disconnected and consequently become orphans.

        Steps:
        1. Create 5 nodes.
        2. Connect Node 1 through Node 4 to Node 0.
        3. Verify initial connections.
        4. Close client connections.
        5. Verify connection states after disconnection.
        6. Close all nodes.

        Raises
        ------
        AssertionError
            If the connection management does not reflect expected states after disconnections.
        """
        nodes = await create_nodes(5, self.ip)
        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.3)

        self.assertEqual(len(nodes[0].edges), 4, "Node 0 connections failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].edges), 1, f"Node {i} connections failed")

        for i in range(1, 5):
            await nodes[i].close_connection(nodes[i].edges[0])

        await asyncio.sleep(0.3)
        self.assertEqual(
            len(nodes[0].edges), 4, "Node 0 connections failed after orphan detection"
        )
        for i in range(1, 5):
            self.assertEqual(
                len(nodes[i].edges),
                1,
                f"Node {i} connections failed after orphan detection",
            )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_edges_server_orphan(self):
        """
        Test when server-edge nodes become orphaned.

        This method evaluates how the system handles scenarios where server-edge nodes get disconnected,
        leading them to become orphans. This is crucial to ensure that nodes properly manage connections
        and maintain expected states after disconnections.

        Steps:
        1. Create 5 nodes.
        2. Connect Node 1 through Node 4 to Node 0.
        3. Verify initial connections.
        4. Close server connections.
        5. Verify connection states after disconnection.
        6. Close all nodes.

        Raises
        ------
        AssertionError
            If connection management does not reflect expected states after disconnections.
        """
        nodes = await create_nodes(5, self.ip)
        for i in range(1, 5):
            await nodes[i]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.5)

        self.assertEqual(len(nodes[0].edges), 4, "Node 0 connections failed")
        for i in range(1, 5):
            self.assertEqual(len(nodes[i].edges), 1, f"Node {i} connections failed")

        for edge in nodes[0].edges:
            await nodes[0].close_connection(edge)

        await asyncio.sleep(0.3)
        self.assertEqual(
            len(nodes[0].edges), 4, "Node 0 connections failed after orphan detection"
        )
        for i in range(1, 5):
            self.assertEqual(
                len(nodes[i].edges),
                1,
                f"Node {i} connections failed after orphan detection",
            )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_response_udp(self):
        """
        Test the UDP response mechanism of ChaskiNodes.

        This asynchronous method checks the ability of the ChaskiNode instances
        to handle UDP requests and provide the correct responses.

        Steps:
        1. Create 2 nodes.
        2. Connect Node 1 to Node 0.
        3. Send a generic UDP request from Node 0 to Node 1.
        4. Verify the response data matches the sent data.
        5. Close all nodes.

        Raises
        ------
        AssertionError
            If the response data does not match the sent data.
        """
        nodes = await create_nodes(2, self.ip)
        await nodes[1]._connect_to_peer(nodes[0])
        await asyncio.sleep(0.5)

        dummy_data = {
            nodes[0].uuid(): nodes[0].uuid(),
            nodes[0].uuid(): nodes[0].uuid(),
            nodes[0].uuid(): nodes[0].uuid(),
        }
        response_data = await nodes[0]._test_generic_request_udp(dummy_data)
        self.assertEqual(
            dummy_data, response_data, 'Mismatch between sent and received data'
        )

        await self._close_nodes(nodes)


########################################################################
class Test_Connections_for_IPv4(TestConnections, unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for testing connections between ChaskiNode instances using IPv4.

    This class extends the `TestConnections` base class and utilizes the
    asynchronous test case capabilities provided by `unittest.IsolatedAsyncioTestCase`.
    It specifically tests connections over IPv4.

    Attributes
    ----------
    ip : str
        The IPv4 address used for creating and connecting nodes, set to '127.0.0.1'.

    Methods
    -------
    asyncSetUp()
        Sets up the test environment for IPv4 connections by initializing the IPv4 address.

    async test_single_connections()
        Tests the ability of ChaskiNode instances to establish single peer-to-peer connections over IPv4.

    async test_multiple_connections()
        Tests the ability of a single ChaskiNode to handle multiple peer-to-peer connections over IPv4.

    async test_disconnection()
        Tests the ability of nodes to handle disconnection events over IPv4.

    async test_edges_disconnection()
        Evaluates the behavior and stability of ChaskiNodes when edge nodes disconnect progressively over IPv4.

    async test_edges_client_orphan()
        Assesses how the system handles edge nodes acting as clients becoming orphaned over IPv4.

    async test_edges_server_orphan()
        Evaluates the handling of server-edge nodes becoming orphaned over IPv4.
    """

    # ----------------------------------------------------------------------
    async def asyncSetUp(self) -> None:
        """
        Initialize the test environment for IPv4 connections.

        This method sets up the testing environment before the execution of each asynchronous test.
        It initializes the ip address to the local IPv4 address '127.0.0.1'.

        Notes
        -----
        This method is automatically invoked by the testing framework and typically does not need to be called explicitly.
        """
        self.ip = '127.0.0.1'
        await asyncio.sleep(0)

    async def test_single_connections(self):
        return await super().test_single_connections()

    async def test_multiple_connections(self):
        return await super().test_single_connections()

    async def test_disconnection(self):
        return await super().test_single_connections()

    async def test_edges_disconnection(self):
        return await super().test_single_connections()

    async def test_edges_client_orphan(self):
        return await super().test_single_connections()

    async def test_edges_server_orphan(self):
        return await super().test_single_connections()

    async def test_response_udp(self):
        return await super().test_response_udp()


########################################################################
class Test_Connections_for_IPv6(unittest.IsolatedAsyncioTestCase, TestConnections):
    """
    Unit tests for testing connections between ChaskiNode instances using IPv6.

    This class extends the `TestConnections` base class and utilizes the
    asynchronous test case capabilities provided by `unittest.IsolatedAsyncioTestCase`.
    It specifically tests connections over IPv6.

    Attributes
    ----------
    ip : str
        The IPv6 address used for creating and connecting nodes, set to '::1'.

    Methods
    -------
    asyncSetUp()
        Sets up the test environment for IPv6 connections by initializing the ip address.

    async test_single_connections()
        Tests the ability of ChaskiNode instances to establish single peer-to-peer connections over IPv6.

    async test_multiple_connections()
        Tests the ability of a single ChaskiNode to handle multiple peer-to-peer connections over IPv6.

    async test_disconnection()
        Tests the ability of nodes to handle disconnection events over IPv6.

    async test_edges_disconnection()
        Evaluates the behavior and stability of ChaskiNodes when edge nodes disconnect progressively over IPv6.

    async test_edges_client_orphan()
        Assesses how the system handles edge nodes acting as clients becoming orphaned over IPv6.

    async test_edges_server_orphan()
        Evaluates the handling of server-edge nodes becoming orphaned over IPv6.
    """

    # ----------------------------------------------------------------------
    async def asyncSetUp(self) -> None:
        """
        Initialize the test environment for IPv6 connections.

        This method sets up the testing environment before the execution of each asynchronous test.
        It initializes the ip address to the local IPv6 address '::1'.

        Notes
        -----
        This method is automatically invoked by the testing framework and typically does not need to be called explicitly.
        """
        self.ip = '::1'
        await asyncio.sleep(0)

    async def test_single_connections(self):
        return await super().test_single_connections()

    async def test_multiple_connections(self):
        return await super().test_single_connections()

    async def test_disconnection(self):
        return await super().test_single_connections()

    async def test_edges_disconnection(self):
        return await super().test_single_connections()

    async def test_edges_client_orphan(self):
        return await super().test_single_connections()

    async def test_edges_server_orphan(self):
        return await super().test_single_connections()

    async def test_response_udp(self):
        return await super().test_response_udp()


if __name__ == '__main__':
    unittest.main()
