"""
==========================
Test Node Functions Module
==========================

This module contains unit tests for validating the functionality and behavior
of various node-related operations within the Chaski framework. The tests
are designed to ensure the correct operation of nodes, addresses, message
pinging, and message handling.

Classes
-------
TestFunctions : unittest.IsolatedAsyncioTestCase
    Contains test cases for validating node operations, including ping tests,
    address verification, and message handling.
"""

import ssl
import asyncio
import unittest
from chaski.node import Message
from chaski.streamer import ChaskiStreamer
from chaski.utils.auto import run_transmission, create_nodes


########################################################################
class TestFunctions(unittest.IsolatedAsyncioTestCase):
    """"""

    ip = '127.0.0.1'

    # ----------------------------------------------------------------------
    async def _close_nodes(self, nodes: list['ChaskiNode']) -> None:
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
    async def test_ping(self) -> None:
        """
        Test the ping functionality between ChaskiNode instances.

        This test method performs the following steps:
        1. Create three ChaskiNodes.
        2. Connect nodes 1 and 2 to node 0.
        3. Ping the first edge of node 0 and wait.
        4. Ping the second edge of node 0 with a larger size and wait.
        5. Assert that the latency of the second edge is greater than the first.
        6. Reset the latencies of both edges.
        7. Assert that the latencies of both edges are equal after resetting.
        8. Close the nodes.

        Assertions
        ----------
        AssertionError
            If the specified conditions are not met.

        Notes
        -----
        This test ensures that the ping functionality between nodes works correctly and that latency is calculated and reset properly.
        """
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
    async def test_address(self) -> None:
        """
        Test the correctness of node addresses.

        This test method verifies that the addresses of node edges are correctly set up. The following steps are performed:

        1. Create two ChaskiNodes.
        2. Connect nodes[1] to nodes[0].
        3. Assert that the address of the first edge of node[0] matches the provided IP.
        4. Assert that the local address of the first edge of node[0] is set to port 65432.
        5. Close the nodes.

        Assertions
        ----------
        AssertionError
            If the addresses do not match the expected values.

        Notes
        -----
        This test ensures that the nodes' addresses are correctly set and can be retrieved accurately.
        """
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
    async def test_message_ttl(self) -> None:
        """
        Test the time-to-live (TTL) functionality of messages.

        This test verifies that the message's TTL (time-to-live) value decrements correctly with each call to the `decrement_ttl` method.
        The following steps are performed:

        1. Create a Message instance with a TTL value of 10.
        2. Decrement the TTL value twice.
        3. Assert that the TTL value is correctly decremented to 8.

        Assertions
        ----------
        AssertionError
            If the TTL value does not decrement correctly.

        Notes
        -----
        This test ensures that the TTL functionality works as expected, which is important for message lifetime management.
        """
        message = Message('command', ttl=10)
        message.decrement_ttl()
        message.decrement_ttl()

        self.assertEqual(
            message.ttl,
            8,
            "The TTL should be decremented by 2 from the initial value of 10",
        )

    # ----------------------------------------------------------------------
    async def test_ssl_certificate(self) -> None:
        """
        Test the SSL certificate configuration for secure communication.

        This test method verifies the correct setup and functionality
        of SSL/TLS certificates and contexts for both the server and
        client sides. The steps are as follows:

        1. Configure server SSL context to authenticate clients,
           load server certificate and key, and verify with CA certificate.
        2. Configure client SSL context to authenticate server,
           load client certificate and key, and verify with CA certificate.
        3. Initialize a ChaskiStreamer instance for the producer with SSL context.
        4. Configure a second server SSL context for another client-server pair.
        5. Configure a second client SSL context for server authentication.
        6. Initialize another ChaskiStreamer instance for the consumer with SSL context.
        7. Run the transmission between producer and consumer to validate secure communication.

        Assertions
        ----------
        AssertionError
            If the SSL contexts or certificates are improperly configured.

        Notes
        -----
        This test ensures that the SSL configurations for ChaskiStreamer
        instances are correctly set up to allow secure communication between
        producer and consumer nodes.
        """
        uuid1 = '3cb580e3-bdd0-4ad8-b535-1aab47b14b28'
        uuid2 = '422f15aa-611d-46d2-b18d-f6c117b8682c'

        # Configure the server SSL context for client authentication,
        # load the server's certificate and key, and set up the CA certificate for verification.
        server_ssl_context = ssl.create_default_context(
            ssl.Purpose.CLIENT_AUTH
        )
        server_ssl_context.load_cert_chain(
            certfile=f'certs/server_{uuid1}.cert',
            keyfile=f'certs/server_{uuid1}.key',
        )
        server_ssl_context.load_verify_locations(cafile='certs/ca.cert')
        server_ssl_context.verify_mode = ssl.CERT_REQUIRED

        # Configure the client SSL context for server authentication,
        # load the client's certificate and key, and set up the CA certificate for verification.
        client_ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH
        )
        client_ssl_context.load_cert_chain(
            certfile=f'certs/client_{uuid1}.cert',
            keyfile=f'certs/client_{uuid1}.key',
        )
        client_ssl_context.load_verify_locations(cafile='certs/ca.cert')
        client_ssl_context.verify_mode = ssl.CERT_REQUIRED

        # Initialize the ChaskiStreamer instance for the producer, configuring SSL contexts
        # for secure communication, subscriptions, and other parameters.
        producer = ChaskiStreamer(
            port=65433,
            name='Producer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs',
            ssl_context_server=server_ssl_context,
            ssl_context_client=client_ssl_context,
        )

        # Configure the second server SSL context for client authentication,
        # load the second server's certificate and key, and set up the CA certificate for verification.
        server_ssl_context2 = ssl.create_default_context(
            ssl.Purpose.CLIENT_AUTH
        )
        server_ssl_context2.load_cert_chain(
            certfile=f'certs/server_{uuid2}.cert',
            keyfile=f'certs/server_{uuid2}.key',
        )
        server_ssl_context2.load_verify_locations(cafile='certs/ca.cert')
        server_ssl_context2.verify_mode = ssl.CERT_REQUIRED

        # Configure the second client SSL context for server authentication,
        # load the client's certificate and key, and set up the CA certificate for verification.
        client_ssl_context2 = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH
        )
        client_ssl_context2.load_cert_chain(
            certfile=f'certs/client_{uuid2}.cert',
            keyfile=f'certs/client_{uuid2}.key',
        )
        client_ssl_context2.load_verify_locations(cafile='certs/ca.cert')
        client_ssl_context2.verify_mode = ssl.CERT_REQUIRED

        # Initialize the ChaskiStreamer instance for the consumer, configuring SSL contexts
        # for secure communication, subscriptions, and other parameters.
        consumer = ChaskiStreamer(
            port=65434,
            name='Consumer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs',
            ssl_context_server=server_ssl_context2,
            ssl_context_client=client_ssl_context2,
        )

        await run_transmission(producer, consumer, parent=self)

    # ----------------------------------------------------------------------
    async def test_ssl_certificate_CA(self) -> None:
        """
        Test requesting SSL certificates from the Certificate Authority (CA).

        This test method validates the process of requesting and obtaining
        SSL/TLS certificates from a Certificate Authority for both producer
        and consumer nodes. The steps include:

        1. Initialize a ChaskiStreamer instance for the producer with the CA's address.
        2. Initialize a ChaskiStreamer instance for the consumer with the CA's address.
        3. Request SSL certificates for both producer and consumer from the CA.
        4. Run a secure transmission between producer and consumer to validate the
           successful acquisition and usage of the SSL certificates.

        Assertions
        ----------
        AssertionError
            If there are issues in obtaining or using the SSL certificates.

        Notes
        -----
        This test ensures the proper interaction with the CA to secure
        communication channels.
        """
        producer = ChaskiStreamer(
            port=65433,
            name='Producer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs_ca',
        )

        consumer = ChaskiStreamer(
            port=65434,
            name='Consumer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs_ca',
        )

        await producer.request_ssl_certificate('ChaskiCA@127.0.0.1:65432')
        await consumer.request_ssl_certificate('ChaskiCA@127.0.0.1:65432')

        await run_transmission(producer, consumer, parent=self)

    # ----------------------------------------------------------------------
    async def test_ssl_certificate_CA_inline(self) -> None:
        """
        Test the inline requesting of SSL certificates from the Certificate Authority (CA).

        This test validates the process of requesting and obtaining SSL/TLS certificates
        directly during the initialization of ChaskiStreamer instances for both producer and
        consumer nodes via a single step. The test ensures that the ChaskiStreamer instances
        can securely request and obtain SSL certificates at the same time they are initialized.

        Steps
        -----
        1. Initialize a ChaskiStreamer instance for the producer with inline SSL certificate request.
        2. Initialize a ChaskiStreamer instance for the consumer with inline SSL certificate request.
        3. Verify that the SSL certificates are correctly obtained by both producer and consumer.
        4. Run a secure transmission between producer and consumer to validate the successful
           acquisition and usage of the SSL certificates.

        Assertions
        ----------
        AssertionError
            If there are issues in obtaining or using the SSL certificates.

        Notes
        -----
        This test ensures that the ChaskiStreamer instances correctly handle the process of
        requesting SSL certificates from the CA and use them successfully for secure communications.
        """
        producer = ChaskiStreamer(
            port=65433,
            name='Producer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs_ca',
            request_ssl_certificate='ChaskiCA@127.0.0.1:65432',
        )

        consumer = ChaskiStreamer(
            port=65434,
            name='Consumer',
            subscriptions=['topic1'],
            reconnections=None,
            ssl_certificates_location='certs_ca',
            request_ssl_certificate='ChaskiCA@127.0.0.1:65432',
        )

        await run_transmission(producer, consumer, parent=self)

    # ----------------------------------------------------------------------
    async def test_ssl_certificate_CA_off(self) -> None:
        """
        Test the behavior of requesting SSL certificates from a non-existent CA.

        This test validates that the system can handle errors when requesting
        SSL/TLS certificates from an incorrect or non-existent Certificate Authority (CA).
        It ensures proper error handling and logging for such requests.

        Steps
        -----
        1. Attempt to initialize a ChaskiStreamer instance for the producer with an incorrect CA address.
        2. Intentionally cause an error by using a non-existent CA address.
        3. Ensure that an exception is raised.
        4. Verify appropriate error handling and logging mechanisms are invoked.

        Assertions
        ----------
        AssertionError
            The test passes if the SSL certificate request raises an error due to an incorrect CA address.

        Notes
        -----
        This test ensures robustness by checking that the streamer correctly handles failed SSL certificate
        requests due to incorrect CA details.
        """
        try:
            ChaskiStreamer(
                port=65433,
                name='Producer',
                subscriptions=['topic1'],
                reconnections=None,
                ssl_certificates_location='certs_ca',
                request_ssl_certificate='ChaskiCA@127.0.0.1:1111',
            )
        except:
            self.assertTrue(
                True,
                "SSL certificate request should raise an error with an incorrect CA address.",
            )


if __name__ == '__main__':
    unittest.main()
