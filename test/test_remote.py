"""
===========================================================
TestRemote: Unit Test for `ChaskiRemote` Class Functionality
===========================================================

This module contains unit tests for the `ChaskiRemote` class, which is part of the Chaski framework
for distributed systems.

Classes
-------
TestRemote : unittest.IsolatedAsyncioTestCase
    A test case for testing the `ChaskiRemote` class functionality.
"""

import unittest
import os
import asyncio
from chaski.remote import ChaskiRemote
import numpy as np

import logging

logging.basicConfig(level=logging.DEBUG)


########################################################################
class TestRemote(unittest.IsolatedAsyncioTestCase):
    """
    A test case for testing the ChaskiRemote class functionality.

    This test case uses the unittest.IsolatedAsyncioTestCase to facilitate
    asynchronous tests. It covers different scenarios to ensure the
    ChaskiRemote class behaves as expected.
    """

    # ----------------------------------------------------------------------
    async def test_module_no_available_register(self):
        """
        Test absence of module registration.

        This test verifies that when a server is set up without any available
        modules, a client cannot access any unregistered module.

        Steps:
        1. Create a `ChaskiRemote` server with no available modules.
        2. Initialize the client and connect it to the server.
        3. Attempt to proxy the 'os' module on the client.
        4. Ensure that accessing attributes of the 'os' module raises an exception.

        Raises
        ------
        AssertionError
            If the client manages to access the module's attributes or
            if the connection steps fail.
        """
        server = ChaskiRemote(
            port=65432,
            available=[],
            reconnections=None,
        )
        await asyncio.sleep(0.3)

        client = ChaskiRemote(
            port=65433,
            reconnections=None,
        )
        await client.connect(server.address)
        await asyncio.sleep(0.3)

        os_remote = client.proxy('os')
        await asyncio.sleep(0.3)

        try:
            os_remote.name
            self.fail('bad, the module has access')
        except:
            self.assertTrue(True, 'ok, the module has not access')

        await server.stop()
        await client.stop()

    # ----------------------------------------------------------------------
    async def test_module_register(self):
        """
        Test the registration and remote access of a specified module.

        This test method performs the following steps:
        1. Sets up a ChaskiRemote server with the 'os' module available for proxying.
        2. Connects a client to the server.
        3. Proxies the 'os' module on the client.
        4. Verifies that the 'os' module's 'listdir' method works correctly when called remotely.
        5. Confirms that the name attribute of the 'os' module matches between the client and server.

        Raises
        ------
        AssertionError
            If the proxied 'os' module's method call results do not match the expected values, or
            if any exceptions are encountered during the test steps.
        """
        server = ChaskiRemote(
            port=65434,
            available=['os'],
            reconnections=None,
        )
        await asyncio.sleep(0.3)

        client = ChaskiRemote(
            port=65435,
            reconnections=None,
        )
        await client.connect(server.address)
        await asyncio.sleep(0.3)

        os_remote = client.proxy('os')
        await asyncio.sleep(0.3)

        self.assertIsInstance(os_remote.listdir('.'), list)
        self.assertEqual(str(os_remote.name), os.name)

        await server.stop()
        await client.stop()

    # ----------------------------------------------------------------------
    async def test_secuential_calls(self):
        """"""
        server = ChaskiRemote(
            port=65434,
            available=['os'],
            reconnections=None,
        )
        await asyncio.sleep(0.3)

        client = ChaskiRemote(
            port=65435,
            reconnections=None,
        )
        await client.connect(server.address)
        await asyncio.sleep(0.3)

        os_remote = client.proxy('os')
        await asyncio.sleep(0.3)

        for _ in range(10):
            self.assertIsInstance(os_remote.listdir('.'), list)
            self.assertEqual(str(os_remote.name), os.name)
            self.assertEqual(os_remote.name._, os.name)

        await server.stop()
        await client.stop()

    # ----------------------------------------------------------------------
    async def test_numpy_calls(self):
        """"""
        server = ChaskiRemote(
            port=65434,
            available=['numpy'],
            reconnections=None,
        )
        await asyncio.sleep(0.3)

        client = ChaskiRemote(
            port=65435,
            reconnections=None,
        )
        await client.connect(server.address)
        await asyncio.sleep(0.3)

        np_remote = client.proxy('numpy')
        await asyncio.sleep(0.3)

        self.assertIsInstance(np_remote.pi._, float)
        self.assertEqual(
            np_remote.random.normal(0, 1, size=(2, 2)).shape, (2, 2)
        )
        self.assertEqual(
            np_remote.random.normal(0, 1, size=(4, 4)).shape, (4, 4)
        )
        self.assertAlmostEqual(np_remote.pi._, 3.141592653589793)

        state = np_remote.random.get_state()
        seed = state[1][0]
        self.assertIsInstance(seed, np.uint32)

        await server.stop()
        await client.stop()


if __name__ == '__main__':
    unittest.main()
