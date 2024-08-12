"""
=========================
Test Subscriptions Module
=========================

This module contains the TestSubscriptions class designed to assert and validate
the connection functionalities of ChaskiNode instances based on their subscription topics.
The tests are structured to ensure that nodes with matching subscriptions establish
connections correctly and can communicate effectively.

Functions
---------
- _close_nodes(nodes) : Close all instances of ChaskiNode in the provided list.
- assertConnection(node1, node2, msg) : Assert that two ChaskiNode instances are connected.
- assertNoConnection(node1, node2, msg) : Assert that two ChaskiNode instances are not connected.
- test_single_subscription_no_disconnect() : Test single subscription connections between ChaskiNodes without disconnecting other nodes.
- test_single_subscription_with_disconnect() : Test single subscription connections between ChaskiNodes with node disconnection during pairing.
"""

import unittest
import asyncio
from typing import Optional
from chaski.utils.auto import create_nodes


########################################################################
class TestSubscriptions(unittest.IsolatedAsyncioTestCase):
    """
    Test case for testing a single subscription scenario in ChaskiNodes.

    This test case defines the infrastructure to create multiple ChaskiNode instances
    and validate the correct establishment of connections based on their subscription topics.
    """

    host = '127.0.0.1'

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
    def assertNoConnection(
        self, node1: 'ChaskiNode', node2: 'ChaskiNode', msg: Optional[str] = None
    ):
        """
        Assert that two ChaskiNodes are not connected to each other.

        This method checks if `node1` is connected to `node2` and vice versa.
        It raises an assertion error if the connection is established in
        either direction.

        Parameters
        ----------
        node1 : ChaskiNode
            The first ChaskiNode to check connection from.
        node2 : ChaskiNode
            The second ChaskiNode to check connection to.
        msg : str, optional
            An optional message to include in the assertion error if the
            nodes are connected.

        Raises
        ------
        AssertionError
            If `node1` is connected to `node2` or `node2` is connected to `node1`.
        """
        conn = node1.is_connected_to(node2) and node2.is_connected_to(node1)
        return self.assertFalse(conn, msg)

    # ----------------------------------------------------------------------
    async def test_single_subscription_no_disconnect(self):
        """
        Test single subscription connections between ChaskiNodes without disconnecting other nodes.

        This test method performs the following steps to ensure nodes with matching subscriptions
        connect correctly:

        1. Create nodes with designated subscriptions: ['A', 'B', 'C', 'A', 'B', 'C'].
        2. Initiate connections from each subsequent node to the first node.
        3. Trigger a discovery phase to find and pair peers based on subscriptions.
        4. Immediately disconnect nodes after they are paired.
        5. Validate if nodes are connected based on their subscription topics.
        6. Close all node instances upon completion.

        Assertions
        ----------
        AssertionError
            Raised if the nodes do not pair correctly according to their subscription topics.
        """
        nodes = await create_nodes(['A', 'B', 'C', 'A', 'B', 'C'])
        for node in nodes[1:]:
            await node._connect_to_peer(nodes[0])

        await asyncio.sleep(0.3)
        for node in nodes[1:]:
            await node.discovery(on_pair='none')

        await asyncio.sleep(0.3)
        self.assertConnection(
            nodes[0],
            nodes[3],
            "Node 0 should be connected to Node 3 because both nodes are subscribed to the topic 'A'.",
        )
        self.assertConnection(
            nodes[1],
            nodes[4],
            "Node 1 should be connected to Node 4 because both nodes are subscribed to the topic 'B'.",
        )
        self.assertConnection(
            nodes[2],
            nodes[5],
            "Node 2 should be connected to Node 5 because both nodes are subscribed to the topic 'C'.",
        )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_single_subscription_with_disconnect(self):
        """"""
        nodes = await create_nodes(['A', 'B', 'C', ['A', 'C'], ['B', 'A'], 'C'])
        for node in nodes[1:]:
            await node._connect_to_peer(nodes[0])

        await asyncio.sleep(0.3)
        for node in nodes[1:]:
            await node.discovery(on_pair='disconnect')

        await asyncio.sleep(0.3)
        self.assertConnection(
            nodes[0],
            nodes[3],
            "Node 0 must connect to Node 3 since both are subscribed to 'A'.",
        )
        self.assertConnection(
            nodes[0],
            nodes[4],
            "Node 0 must connect to Node 4 since they both subscribe to 'A' and 'B'.",
        )
        self.assertConnection(
            nodes[1],
            nodes[4],
            "Node 1 must connect to Node 4 since both are subscribed to 'B'.",
        )
        self.assertConnection(
            nodes[2],
            nodes[5],
            "Node 2 must connect to Node 5 since both are subscribed to 'C'.",
        )
        self.assertNoConnection(
            nodes[0],
            nodes[5],
            "Node 0 must not connect to Node 5 since their subscriptions do not overlap.",
        )

        await self._close_nodes(nodes)


if __name__ == '__main__':
    unittest.main()
