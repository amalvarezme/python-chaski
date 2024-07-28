import unittest
import asyncio
from chaski.node import Message
from chaski.utils.auto import create_nodes
from typing import Optional


########################################################################
class TestFunctions(unittest.IsolatedAsyncioTestCase):
    """"""

    ip = '127.0.0.1'

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
    async def test_ping(self):
        """"""
        nodes = await create_nodes(3, self.ip)
        await nodes[1].connect(nodes[0])
        await nodes[2].connect(nodes[0])
        await asyncio.sleep(0.3)

        await nodes[0].ping(nodes[0].edges[0])
        await asyncio.sleep(1)
        await nodes[0].ping(nodes[0].edges[1], size=100000)
        await asyncio.sleep(1)

        self.assertGreater(
            nodes[0].edges[1].latency,
            nodes[0].edges[0].latency,
            "Latency of the second edge should be greater than the latency of the first edge",
        )

        nodes[0].edges[0].reset_latency()
        nodes[0].edges[1].reset_latency()

        self.assertEqual(
            nodes[0].edges[1].latency,
            nodes[0].edges[0].latency,
            "Latencies of the two edges should be equal after resetting",
        )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_address(self):
        """"""
        nodes = await create_nodes(2, self.ip)
        await nodes[1].connect(nodes[0])
        await asyncio.sleep(0.3)

        self.assertEqual(
            nodes[0].edges[0].address[0],
            self.ip,
            "The address of the edge should match the provided IP",
        )

        self.assertEqual(
            nodes[0].edges[0].local_address[1],
            65432,
            "Local address of the edge should be 65432",
        )

        await self._close_nodes(nodes)

    # ----------------------------------------------------------------------
    async def test_message(self):
        """"""
        message = Message('command', ttl=10)
        message.decrement_ttl()
        message.decrement_ttl()

        self.assertEqual(
            message.ttl,
            8,
            "The TTL should be decremented by 2 from the initial value of 10",
        )


if __name__ == '__main__':
    unittest.main()
