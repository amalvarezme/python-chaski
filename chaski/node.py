"""
=========================================================
ChaskiNode: Distributed Node Communication and Management
=========================================================

This module defines the `ChaskiNode` class and its associated classes for managing network communication
between distributed nodes. It provides a framework for creating Nodes which can connect to each other
over TCP/IP, handle messaging and serialization of data, and perform network-wide functions, such as
discovery and pairing of nodes based on shared subscriptions.

Classes
=======
    - *MessagesPool*: Manages a collection of messages with a fixed maximum size, ensuring thread safety.
    - *Edge*: Represents a connection to a peer in the network, managing the input/output streams and storing
      metadata such as latency, jitter, and subscription topics.
    - *Message*: A container class for messages, packing together the command, data, and timestamp information.
    - *UDPProtocol*: An asyncio protocol class for handling UDP packets, interfacing with the asyncio Datagram Protocol.
    - *ChaskiNode*: The main class representing a node in the network, which can initiate connections, handle incoming
      requests, and orchestrate network-wide actions.
"""

import os
import re
import ssl
import uuid
import time
import pickle
import asyncio
import logging
import socket
import ipaddress
import traceback
from datetime import datetime
from functools import cached_property
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

try:
    from chaski.utils.certificate_authority import CertificateAuthority
except ImportError as e:
    import logging

    logging.warning(f"Failed to import CertificateAuthority: {e}")

from chaski.utils.debug import styled_logger


# Initialize loggers for the main node operations, edge connections, and UDP protocol
logger_main = styled_logger(logging.getLogger("ChaskiNode"))
logger_edge = styled_logger(logging.getLogger("ChaskiNodeEdge"))
logger_udp = styled_logger(logging.getLogger("ChaskiNodeUDP"))


# List of default ports the ChaskiNode will attempt to use for establishing connections
FAVORITE_PORTS = [
    8511,
    8512,
    8513,
    8514,
    8515,
    8516,
]


########################################################################
class MessagesPool:
    """
    A pool for managing a collection of messages with a fixed maximum size.

    The `MessagesPool` class is designed to provide a thread-safe environment for storing,
    retrieving, and managing messages in a fixed-size list. When the pool exceeds its
    maximum size, the oldest message is removed to accommodate new ones.
    """

    # ----------------------------------------------------------------------
    def __init__(self, maxzise: int = 128):
        """
        Initialize a new MessagesPool instance.

        Parameters
        ----------
        maxzise : int, optional
            The maximum size of the message pool. If the pool exceeds this size,
            the oldest messages will be removed to make room for new ones. The default
            maximum size is 128.
        """
        self.pool = []
        self.maxzise = maxzise
        self.lock = asyncio.Lock()

    # ----------------------------------------------------------------------
    async def append(self, item: Any) -> None:
        """
        Append an item to the message pool.

        This coroutine safely appends an item to the message pool, ensuring that the pool size
        does not exceed the defined maximum size (`self.maxzise`). If the pool is full, the
        oldest item in the pool is removed to make space for the new item. If the item already
        exists in the pool, it is moved to the end of the pool.

        Parameters
        ----------
        item : Any
            The item to append to the message pool. This can be any object that is comparable
            to the items stored in the pool.
        """
        async with self.lock:
            if item in self.pool:
                self.pool.remove(item)
            else:
                if len(self.pool) > self.maxzise:
                    self.pool.pop(0)
            self.pool.append(item)

    # ----------------------------------------------------------------------
    async def contains(self, item: Any) -> bool:
        """
        Check if the specified item is present in the message pool.

        This coroutine acquires a lock to ensure thread safety while checking
        the presence of an item in the pool. It returns `True` if the item
        is found, otherwise `False`.

        Parameters
        ----------
        item : Any
            The item to check for presence in the pool. This can be any
            object that is comparable to the items stored in the pool.

        Returns
        -------
        bool
            `True` if the item is present in the pool, `False` otherwise.
        """
        async with self.lock:
            return item in self.pool


########################################################################
@dataclass
class Edge:
    """
    Represents a connection to a peer node, encompassing essential communication features and performance metrics.

    The `Edge` class is designed to manage and provide detailed insights into network connections between nodes.
    This class focuses on TCP connections, offering methods for performance evaluation, address management,
    and connection properties.

    Attributes
    ----------
    writer : asyncio.StreamWriter
        StreamWriter for sending data to the connected peer node.
    reader : asyncio.StreamReader
        StreamReader for reading data from the connected peer node.
    latency : float, optional
        Current latency in the connection, default is 0 milliseconds.
    jitter : float, optional
        Variation in latency, default is 0 milliseconds.
    name : str, optional
        The name identifier of the edge, typically used for logging and monitoring.
    ip : str, optional
        The IP of the connected peer node.
    port : int, optional
        The port number of the connected peer node.
    subscriptions : set, optional
        Set of topics this edge subscribes to.
    ping_in_progress : bool, optional
        A flag to indicate if a ping operation is in progress, default is False.
    paired : bool, optional
        A flag to indicate if the node is paired, default is False.
    """

    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    latency: float = 0
    jitter: float = 0
    name: str = ""
    ip: str = ""
    port: int = 0
    subscriptions: set = field(default_factory=set)
    ping_in_progress: bool = False
    paired: bool = False

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Return a string representation of the Edge.

        Generates a human-readable string that includes the Edge's name, latency,
        jitter, ip, and port. The string format highlights the state of the Edge
        in terms of network performance and connection details.

        Returns
        -------
        str
            A formatted string characterizing the Edge instance with details
            like name, latency (in milliseconds), jitter (in milliseconds),
            IP, and port.
        """
        return f"{self.name}: N({self.latency: .0f}, {self.jitter: .0f}) {self.ip}: {self.port}"

    # ----------------------------------------------------------------------
    @cached_property
    def address(self) -> tuple[str, int]:
        """
        Retrieve the address of the connected remote peer.

        This method returns the remote address to which the edge's writer is connected to. It
        extracts the 'peername' information from the underlying socket associated with the
        StreamWriter instance held by the edge.

        Returns
        -------
        tuple[str, int]
            A tuple of two elements where the first element is the IP of
            the remote peer as a string, and the second element is the port number as an integer.
        """
        return self.writer.get_extra_info("peername")

    # ----------------------------------------------------------------------
    @cached_property
    def local_address(self) -> Tuple[str, int]:
        """
        Retrieve the local address to which the edge's writer is connected.

        This cached property returns a tuple containing the local IP address or hostname, and the local port number, obtained from the writer socket's information. It represents the local end of the connection managed by the edge.

        Returns
        -------
        Tuple[str, int]
            A tuple containing the local address of the writer socket. The first element is the IP address or hostname as a string, and the second element is the port number as an integer.
        """
        return self.writer.get_extra_info("sockname")

    # ----------------------------------------------------------------------
    def reset_latency(self) -> None:
        """
        Reset the latency and jitter values for the edge.

        This function resets the latency and jitter values to their default initial
        state, which is 0 for latency and 100 for jitter. This is usually called to
        clear any existing latency and jitter measurements and start fresh, typically
        before starting a new set of latency tests or after a significant network event.
        """
        logger_edge.debug("Reset latency and jitter for the edge.")
        self.latency = 0
        self.jitter = 100

    # ----------------------------------------------------------------------
    def update_latency(self, new_latency: float) -> None:
        """
        Update the edge latency based on a new latency measurement.

        This method updates the edge latency statistics by combining the new latency value with
        the existing latency and jitter information. It uses a simple Bayesian update approach
        to compute a new posterior mean and variance for the edge latency, representing the
        updated belief about the edge's latency characteristics given the new data.

        Parameters
        ----------
        new_latency : float
            The new latency measurement to incorporate into the edge's latency statistics.
        """
        if self.jitter == 0:
            self.latency = new_latency
            self.jitter = 100
        else:
            prior_mean = self.latency
            prior_variance = self.jitter**2

            # Update the posterior parameters using Bayesian approach
            likelihood_mean = new_latency
            likelihood_variance = (
                100**2
            )  # Assume a fixed variance for the new measurement
            posterior_mean = (
                prior_mean / prior_variance
                + likelihood_mean / likelihood_variance
            ) / (1 / prior_variance + 1 / likelihood_variance)
            posterior_variance = 1 / (
                1 / prior_variance + 1 / likelihood_variance
            )

            self.latency = posterior_mean
            self.jitter = (
                posterior_variance**0.5
            )  # Take the square root to return to standard deviation.

            logger_edge.debug(
                f"Updated latency: {self.latency: .2f}, jitter: {self.jitter: .2f}."
            )

    # ----------------------------------------------------------------------
    def __eq__(self, other):
        if isinstance(other, Edge):
            return (self.writer == other.writer) and (
                self.reader == other.reader
            )
        return False


########################################################################
@dataclass
class Message:
    """
    A class to represent a message with a specific command and associated data, along with a timestamp indicating when it was created, a topic for categorization, a TTL (time-to-live) value, and a unique identifier (UUID).

    This class is designed to encapsulate all necessary details of a message within a network communication context.
    Each message carries a command that indicates the action to be performed, the data required to execute the action,
    the time at which the message was instantiated, an optional topic to categorize the message, a TTL value
    to indicate the lifespan of the message in hops, and a UUID to uniquely identify the message.

    Parameters
    ----------
    command : str
        The specific command or instruction that this message signifies. Commands are typically predefined and
        understood by both the sender and receiver in the communication protocol being implemented.
    topic : str, optional
        An optional topic string used to categorize the message. Defaults to an empty string.
    data : Any, optional
        The payload of the message containing the data that the command operates on. This can be any type of data
        struct, such as a string, dictionary, or a custom object, and its structure depends on the specific needs
        of the command. Defaults to None.
    timestamp : datetime, optional
        The exact date and time when the message was created, represented as a datetime object. The timestamp provides
        chronological context for the message's creation, aiding in message tracking, ordering, and latency calculations.
        Defaults to 0.
    ttl : int, optional
        Time-to-live value for the message, indicating the maximum number of hops the message can take. Defaults to 64.
    uuid : str, optional
        A unique identifier for the message, represented as a string. Defaults to None.
    """

    command: str
    topic: str = ''
    data: Any = None
    timestamp: datetime = 0
    ttl: int = 64
    uuid: str = None

    # ----------------------------------------------------------------------
    def decrement_ttl(self) -> None:
        """"""
        self.ttl -= 1


########################################################################
@dataclass
class UDPProtocol(asyncio.DatagramProtocol):
    """
    An asyncio protocol class for processing UDP packets.

    This class defines a custom protocol to handle UDP communications for a node. It outlines
    methods providing core functionality for sending, receiving, and effectively managing
    UDP connections.
    """

    node: 'ChaskiNode'
    on_message_received: Awaitable

    # ----------------------------------------------------------------------
    def datagram_received(
        self, message: bytes, addr: tuple[str, int]
    ) -> None:
        """
        Handle incoming datagram messages and dispatch them for processing.

        This method is invoked automatically whenever a UDP packet is received. It is responsible for
        creating a coroutine that will handle the incoming message asynchronously. This allows the event loop
        to continue handling other tasks while the message is processed.

        Parameters
        ----------
        message : bytes
            The datagram message received from the sender. The content is raw bytes and is expected to be
            deserialized and processed by the designated handler.
        addr : tuple[str, int]
            The sender's address where the first element is a string representing the IP address or hostname
            of the sender and the second element is an integer representing the port number.
        """
        asyncio.create_task(self.on_message_received(message, addr))

    # ----------------------------------------------------------------------
    def error_received(self, exc: Optional[Exception]) -> None:
        """
        Handle any errors received during the UDP transaction.

        This method is called automatically when an error is encountered during the UDP communication.
        It logs the error using the UDP-specific logger. The method is a part - of the asyncio protocol and provides
        a standardized interface for error handling in asynchronous UDP operations.

        Parameters
        ----------
        exc : Optional[Exception]
            The exception that occurred during UDP operations, if any. It is None if the error was triggered by something
            other than an Exception, such as a connection problem.
        """
        logger_udp.error(f"UDP error received: {exc}")

    # ----------------------------------------------------------------------
    def connection_lost(self, exc: Optional[Exception]) -> None:
        """
        Respond to a lost connection or the closing of the UDP endpoint.

        This event handler is called when the UDP connection used by the protocol is no longer connected or has been explicitly closed. Connection loss could be due to a variety of reasons, such as network issues, or the remote end closing the connection. If the connection is closed because of an error, the exception will be passed to this handler. Otherwise, the handler is called with None if the closing was clean.

        Parameters
        ----------
        exc : Optional[Exception]
            The exception object if the connection was lost due to an error, or None if the connection was closed cleanly.
        """
        logger_udp.info(f"UDP connection closed: {exc}")


########################################################################
class ChaskiNode:
    """
    Represents a ChaskiNode for distributed network communication.

    The ChaskiNode class orchestrates the management of network communication
    between distributed nodes. It can initiate, handle incoming requests, and
    manage connections. The node is capable of:

    - Creating TCP and UDP endpoints.
    - Performing message serialization and deserialization.
    - Implementing automatic network discovery and pairing based on subscriptions.
    """

    # ----------------------------------------------------------------------
    def __init__(
        self,
        ip: str = '127.0.0.1',
        port: int = 0,
        serializer: Callable[[Any], bytes] = pickle.dumps,
        deserializer: Callable[[bytes], Any] = pickle.loads,
        name: Optional[str] = None,
        subscriptions: Union[str, List[str]] = [],
        run: bool = True,
        ttl: int = 64,
        root: bool = False,
        paired: bool = False,
        max_connections: int = 5,
        reconnections: int = 32,
        messages_pool_maxzise: int = 128,
        message_propagation: bool = False,
        ssl_context_client: Optional[ssl.SSLContext] = None,
        ssl_context_server: Optional[ssl.SSLContext] = None,
        ssl_certificate_attributes: Optional[dict] = {},
        ssl_certificates_location: Optional[str] = '.',
        request_ssl_certificate: Optional[str] = None,
    ) -> None:
        """
        Represent a ChaskiNode, which handles various network operations and manages connections.

        ChaskiNode is responsible for creating TCP and UDP endpoints, handling incoming connections,
        and executing network commands. It manages a list of edges, which are connections to other nodes,
        and performs message serialization and deserialization for network communication. The node can also
        participate in network-wide actions like discovery, to find and connect with nodes sharing similar
        subscriptions.

        Parameters
        ----------
        ip : str
            The IP address to listen on or bind to.
        ip : int
            The port number to listen on or bind to.
        serializer : Callable[[Any], bytes], optional
            The function to serialize data before sending it over the network. Defaults to `pickle.dumps`.
        deserializer : Callable[[bytes], Any], optional
            The function to deserialize received data. Defaults to `pickle.loads`.
        name : Optional[str], optional
            The name of the node, used for identification and logging purposes. Defaults to `None`.
        subscriptions : Union[str, List[str]], optional
            A string or list of subscription topic strings this node is interested in. Defaults to an empty list.
        run : bool, optional
            A flag determining whether the TCP and UDP servers should start immediately upon the node's
            initialization. Defaults to `False`.
        ttl : int, optional
            Time-to-live value for discovery messages. Defaults to `64`.
        root : bool, optional
            Flag to indicate whether this node is the root in the network topology. Defaults to `False`.
        reconnections : int, optional
            The number of reconnection attempts to make if a node loses connection. Defaults to `32`.
        messages_pool_maxzise : int, optional
            The maximum size allowed for the message pool, determining how many messages can be stored in the pool before it starts removing the oldest ones. Defaults to `128`.
        message_propagation: bool, optional
            Flag to indicate whether the node should propagate received messages to other nodes.
            If set to `True`, messages received by this node will be forwarded to other connected
            nodes except for the edge it received the message from, helping in message dissemination
            across the network. Defaults to False.
        ssl_context_client: Optional[ssl.SSLContext], optional
        ssl_context_server: Optional[ssl.SSLContext], optional
        ssl_certificate_attributes: dict, optional
            A dictionary containing attributes to use when generating an SSL certificate,
            such as 'Country Name', 'State or Province Name', 'Locality Name', and others.
            These attributes provide metadata for the SSL certificate, ensuring that it is
            correctly identified and validated within the network. Defaults to an empty dictionary.
        ssl_certificates_location : Optional[str] = '.',
            Location of the directory where SSL/TLS certificates are stored.
            This directory should include the necessary certificate files for establishing secure
            connections using the configured SSL context. If not specified, defaults to the current directory ('.').

        Notes
        -----
        The combination of the `root` and `port` parameters in the configuration of a `ChaskiNode` determines how and on which port the node attempts to connect or listen.

        - `root=True`, `port` specified:
            - The node is initialized as a root node and uses the specified port to establish connections.

        - `root=True`, `port` not specified (None or 0):
            - A port will be chosen from the `FAVORITE_PORTS` list that is available. If no port is found, a free port will be dynamically assigned.

        - `root=False`, `port` specified:
            - The node is initialized as a non-root node and listens on the specified port.

        - `root=False`, `port` not specified (None or 0):
            - The node is not a root node and if no port is specified, a free port is dynamically assigned using `self.port = self.get_free_port()`.
        """
        # Initialize node parameters
        self.ip = ip
        self.port = int(port)
        self.serializer_ = serializer
        self.deserializer_ = deserializer
        self.server = None
        self.ttl = ttl
        self.max_connections = max_connections
        self.id = self.uuid()
        self.name = f"{name}" if name else self.id
        self.root = root
        self.reconnections = reconnections
        self.message_propagation = message_propagation
        self.ssl_context_client = ssl_context_client
        self.ssl_context_server = ssl_context_server
        self.ssl_certificates_location = ssl_certificates_location

        # If root and no specific port is set, select one from favorite ports that is available
        if root and not self.port:
            for port in FAVORITE_PORTS:
                if self.is_port_available(port):
                    self.port = port

        # If port is 0, dynamically assign a free port
        elif self.port == 0:
            self.port = self.get_free_port()

        # Convert subscriptions to a set if provided as a string
        if isinstance(subscriptions, str):
            self.subscriptions = set([subscriptions])
        elif subscriptions is None:
            self.subscriptions = []
        else:
            self.subscriptions = set(subscriptions)

        # Initialize asyncio locks for concurrency control
        self.lock = asyncio.Lock()
        self.lock_pair = asyncio.Lock()
        self.lock_disconnect = asyncio.Lock()
        self.lock_propagate = asyncio.Lock()

        # Initialize the node's connection and event tracking structures
        self.edges = []
        self.ping_events = {}
        self.handshake_events = {}
        self.synchronous_udp = {}
        self.synchronous_udp_events = {}
        self.reconnecting = asyncio.Event()

        # Initialize paired_event dictionary with asyncio Events for each subscription
        self.paired_event = {}
        for subscription in subscriptions:
            self.paired_event[subscription] = asyncio.Event()
            if paired:
                self.paired_event[subscription].set()

        # Initialize the pool for storing messages with a maximum size
        self.messages_pool = MessagesPool(maxzise=messages_pool_maxzise)

        # Initialize the list of commands that should be propagated to other nodes in the network.
        self.propagation_command_list = []

        # Initialize SSL certificate attributes for secure communication
        self.ssl_certificate_attributes = {
            'Country Name': "CO",
            'Locality Name': "Manizales",
            'Organization Name': "DunderLab",
            'State or Province Name': "Caldas",
            'Common Name': "Chaski-Confluent",
        }
        self.ssl_certificate_attributes.update(ssl_certificate_attributes)

        # If the run flag is set to True, create and start the main event loop task for the node
        if run:
            asyncio.create_task(self.run())

        # Request an SSL certificate for secure communication if specified
        if request_ssl_certificate:
            asyncio.sleep(1)
            asyncio.create_task(
                self.request_ssl_certificate(request_ssl_certificate)
            )

    #             if os.path.exists(self.ssl_context_client,
    #                               self.ssl_context_server)

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Represent a node in a network graph.

        This class represents an Edge in a network graph, which is part of a ChaskiNode.
        It encapsulates the necessary properties and methods for managing the state and
        behavior of a network connection. Edges track connection details like latency
        and jitter, and they store information about the IP, port, and name of the
        connection, as well as the subscriptions of topics of interest. Furthermore,
        an edge provides functionality for sending pings to measure latency, and it
        can reset its performance statistics.
        """
        h = '*' if self.paired else ''
        return f"{h}ChaskiNode@{self.ip}:{self.port}"

    # ----------------------------------------------------------------------
    def serializer(self, obj: Any) -> bytes:
        """
        Serialize the provided object using the configured serializer.

        This method converts a Python object into a bytes representation using the
        serializer function specified at node initialization. This is typically used
        for preparing data to be sent over the network.

        Parameters
        ----------
        obj : Any
            The Python object to be serialized. This can be any serializable object,
            such as dictionaries, lists, or custom objects.

        Returns
        -------
        bytes
            The serialized representation of the input object as bytes.

        Notes
        -----
        If the serialization process fails, this method returns the string representation
        of the object as a fallback.
        """
        try:
            return self.serializer_(obj)
        except Exception as e:
            raise Exception(f"Serialization error: {str(e)}")

    # ----------------------------------------------------------------------
    def deserializer(self, data: bytes) -> Any:
        """
        Deserialize the provided data using the configured deserializer.

        This method attempts to convert a bytes object back into its original form
        (such as an object, string, or other data structures) using the
        deserializer function specified at the node's initialization.

        Parameters
        ----------
        data : bytes
            The serialized data as a bytes object that needs to be deserialized.

        Returns
        -------
        Any
            The deserialized data, converted back to its original form. This can be
            any data type that was originally serialized.

        Notes
        -----
        If the deserialization process fails, this method returns the raw data
        as a fallback.
        """
        try:
            return self.deserializer_(data)
        except:
            return data

    # ----------------------------------------------------------------------
    async def run(self) -> None:
        """
        Launch TCP and UDP servers for the node.

        This coroutine starts the TCP and UDP server tasks to listen for incoming connections and handle UDP datagrams. It is an essential part of the node's operation, enabling it to accept connections from other nodes and exchange messages over the network.
        """
        self.server_closing = False
        await asyncio.gather(
            self._start_tcp_server(), self._start_udp_server()
        )

    # ----------------------------------------------------------------------
    async def stop(self) -> None:
        """
        Stop all activities of the node, ensuring proper cleanup.

        This coroutine is responsible for gracefully stopping all network services of the node. It closes both TCP and
        UDP connections, cancels background tasks such as keep-alive checks, and finalizes any pending operations.
        After invoking this function, the node will no longer serve as part of the network until restarted.

        """
        self.server_closing = True

        # Close all connections gracefully
        for edge in self.edges:
            await self.close_connection(edge)

        # Close the UDP transport if it exists
        if hasattr(self, 'udp_transport'):
            self.udp_transport.close()

        # Cancel the keep-alive task if it exists
        if hasattr(self, '_keep_alive_task'):
            self._keep_alive_task.cancel()

        # Attempt to gracefully shut down the server if it exists
        if hasattr(self, 'server'):
            self.server.close()
            try:
                await asyncio.wait_for(self.server.wait_closed(), timeout=5)
            except asyncio.TimeoutError:
                logger_main.warning("Timeout waiting for server to close.")

    # ----------------------------------------------------------------------
    async def _connect_to_peer(
        self,
        node: 'ChaskiNode',
        peer_port: Optional[int] = None,
        paired: bool = False,
        data: dict = {},
    ) -> None:
        """
        Asynchronously establish a TCP connection to a peer node.

        Initiate a TCP connection to the specified peer node. If a connection is already established, or if
        the target node is the same as the current one, the function will produce a warning and not proceed
        further. This function also supports marking a connection as 'paired', updating corresponding
        state information about the peer node.

        Parameters
        ----------
        node : 'ChaskiNode'
            The target node instance or the ip string to connect to.
        peer_port : Optional[int]
            The port number of the target node if the `node` parameter is not a `ChaskiNode` instance.
        paired : bool
            Flag indicating whether the connection should be marked as 'paired'.
        data : dict
            Additional data to include in the `report_paired` command if the connection is paired.
        """
        if hasattr(node, "ip"):
            peer_ip, peer_port = node.ip, node.port
        else:
            peer_ip, peer_port = node, int(peer_port)

        # # Check if the node is trying to connect to itself
        # if self.ip == peer_ip and self.port == peer_port:
        # logger_main.warning(f"{self.name}: Impossible to connect a node to itself.")
        # return False

        # Check if a connection to the peer ip and port already exists
        if (peer_ip, peer_port, False) in [
            (edge.ip, edge.port, edge.writer.is_closing())
            for edge in self.edges
        ]:
            logger_main.warning(
                f"{self.name}: Already connected with this node."
            )
            return self.get_edge(peer_ip, peer_port)

        # Resolve address info for the specified ip and port
        addr_info = socket.getaddrinfo(
            peer_ip, peer_port, socket.AF_UNSPEC, socket.SOCK_DGRAM
        )
        if not addr_info:
            raise ValueError(f"Cannot resolve address: {self.ip}")
        family, socktype, proto, canonname, sockaddr = addr_info[0]

        # Establish a TCP connection to the peer node
        reader, writer = await asyncio.open_connection(
            peer_ip,
            peer_port,
            family=family,
            ssl=self.ssl_context_client,
        )

        edge = Edge(writer=writer, reader=reader)

        # Check if the connection should be marked as paired
        if paired:
            data['paired'] = paired
            edge.paired = True
            await self._write(
                command="report_paired",
                data=data,
                edge=edge,
            )

        # Log new connection
        logger_main.debug(
            f"{self.name}: New connection with {edge.address}."
        )
        asyncio.create_task(self._reader_loop(edge))
        await self.handshake(edge, response=True)
        # await self.ping(edge)
        return edge

    # ----------------------------------------------------------------------
    def enable_message_propagation(self) -> None:
        """
        Enable the message propagation feature for the node.

        When this method is called, it sets the `propagate` attribute to `True`.
        This allows the node to forward received messages to other connected nodes.
        By enabling message propagation, the node helps in disseminating messages
        across the network, except for the edge from which the message was received.
        """
        self.message_propagation = True

    # ----------------------------------------------------------------------
    def disable_message_propagation(self) -> None:
        """
        Disable the message propagation feature for the node.

        This method turns off the node's ability to forward received messages
        to other connected nodes. Once disabled, the node will stop relaying
        messages but will continue to receive and process incoming messages.
        """
        self.message_propagation = False

    # ----------------------------------------------------------------------
    def add_propagation_command(self, command: str) -> None:
        """
        Add a command to the list of commands that should be propagated.

        This method appends a given command to the internal list of commands
        that are allowed to be propagated to other nodes. It ensures that each
        command is unique within the list by converting it to a set and back to a list.

        Parameters
        ----------
        command : str
            The command to add to the propagation list. This command should be a string
            representing a specific type of message that can be propagated to other nodes.
        """
        self.propagation_command_list.append(command)
        self.propagation_command_list = list(
            set(self.propagation_command_list)
        )

    # ----------------------------------------------------------------------
    def remove_propagation_command(self, command: str) -> None:
        """
        Remove a command from the list of commands that should be propagated.

        This method removes a given command from the internal list of commands
        that are allowed to be propagated to other nodes. It ensures that the
        command is removed if it exists in the list. If the command does not
        exist in the list, the method completes silently without any changes.

        Parameters
        ----------
        command : str
            The command to remove from the propagation list. This command should
            be a string representing a specific type of message that was previously
            added for propagation to other nodes.
        """
        if command in self.propagation_command_list:
            self.propagation_command_list.remove(command)

    # ----------------------------------------------------------------------
    async def connect(
        self,
        address_or_ip_or_node: Union[str, 'ChaskiNode'],
        port: Optional[int] = None,
    ) -> None:
        """
        Establish a connection to the specified node or address.

        This method initiates a TCP connection to a specified node or an IP address and port.
        It leverages the `_connect_to_peer` method to create the connection. The input can be an
        instance of `ChaskiNode`, a string representing the IP address, or an address string in
        the format "ip:port" or "[ipv6]:port".

        Parameters
        ----------
        address_or_ip_or_node : Union[str, ChaskiNode]
            The target node instance or IP string to connect to. Acceptable formats include:
            - ChaskiNode instance
            - IP address string (e.g., "192.168.1.1")
            - Address string with port (e.g., "192.168.1.1:65432" or "[2001:db8::1]:65432")
        port : Optional[int]
            The port number of the target node if an IP address string is provided. Ignored if `address_or_ip_or_node`
            includes port information or is a `ChaskiNode` instance.

        Raises
        ------
        ValueError
            If the address cannot be resolved.

        """
        if port:
            ip, port = address_or_ip_or_node, port

        elif hasattr(address_or_ip_or_node, "ip"):
            ip, port = address_or_ip_or_node.ip, address_or_ip_or_node.port

        else:
            pattern = r"(?:(?:\*?\w+@)?(\d{1,3}(?:\.\d{1,3}){3})|(?:\*?\w+@)?\[((?:[0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4})\]):(\d+)"
            ipv4, ipv6, port = re.findall(pattern, address_or_ip_or_node)[0]
            ip = ipv4 + ipv6

        return await self._connect_to_peer(ip, port)

    # ----------------------------------------------------------------------
    async def discovery(
        self,
        node: Optional['ChaskiNode'] = None,
        on_pair: Union[str, Literal['none', 'disconnect']] = 'none',
        timeout: int = 10,
    ) -> None:
        """
        Conducts a network-wide discovery process.

        Executes a discovery process across the network to find and potentially connect with other ChaskiNodes. This function is used to find nodes with overlapping subscriptions to establish a peer-to-peer connection. It allows the node to expand its network by connecting to more nodes, which may be of interest based on the subscriptions. Depending on the 'on_pair' setting, nodes may connect permanently or just acknowledge the presence of each other.

        Parameters
        ----------
        node : Optional['ChaskiNode'], optional
            A reference to a ChaskiNode instance to start the discovery process from.
            If None, discovery will be attempted using the current node's server pairs.
            Defaults to None.
        on_pair : Union[str, Literal['none', 'disconnect']], optional
            The action to take when a peer is discovered. 'none' means no action is taken,
            while 'disconnect' causes the node to disconnect after pairing. Defaults to 'none'.
        timeout : int, optional
            The maximum time in seconds to wait for the discovery process to complete before
            considering the node as paired. Defaults to 10 seconds.
        """
        # Clear the paired_event flag for each subscription
        for subscription in self.subscriptions:
            self.paired_event[subscription].clear()

        # Check if there are no edges present
        if not self.edges:
            logger_main.warning(
                f"{self.name}: No connection to perform discovery."
            )
            return

        # Check if the node is not provided and there are no edges
        if (node is None) and (len(self.edges) == 0):
            logger_main.warning(
                f"{self.name}: Unable to discover new nodes no 'Node' or 'Edge' available."
            )
            return

        # Iterate over edges to identify unpaired subscriptions and check pairing status.
        for edge in self.edges:
            unpaired_subscription = []
            for subscription in self.subscriptions:
                if subscription in edge.subscriptions:
                    logger_main.warning(
                        f"{self.name}: The node is already paired for suscription {subscription}."
                    )
                    edge.paired = True
                    self.paired_event[subscription].set()
                else:
                    unpaired_subscription.append(subscription)

        # If no specific node is provided for discovery, default to the first edge in the current list of edges.
        if not node:
            node = self.edges[0]

        # Iterate over the unpaired subscriptions to perform discovery
        for subscription in unpaired_subscription:

            data = {
                "origin_ip": self.ip,
                "origin_port": self.port,
                "origin_name": self.name,
                "previous_node": self.name,
                "visited": set([self.name]),
                "on_pair": on_pair,
                "root_ip": node.ip,
                "root_port": node.port,
                "origin_subscription": subscription,
                "ttl": self.ttl,
            }

            # Write discovery command to the node's writer
            await self._write(
                command="discovery",
                data=data,
                edge=node,
            )

            # Start a timer for the discovery process
            t0 = time.time()
            while time.time() < t0 + timeout:
                await asyncio.sleep(0.1)
                if self.paired_event[subscription].is_set():
                    break
            if time.time() > t0 + timeout:
                logger_main.debug(
                    f"{self.name}: Timeout reached during discovery process for subscription {subscription}, node is considered paired."
                )
            self.paired_event[subscription].set()

    # ----------------------------------------------------------------------
    async def close_connection(
        self, edge: Edge, port: Optional[int] = None
    ) -> None:
        """
        Close the connection associated with a given edge, optionally specifying a port.

        This coroutine handles the termination of a network connection that corresponds to the provided edge. If a port number
        is specified, the connection to that port will be closed. All resources associated with the connection, such as stream
        writers, are properly finalized. If the current node ends up without any connections, a warning is logged, and an
        attempt to reconnect is made.

        Parameters
        ----------
        edge : Edge
            The edge object representing the network connection to be closed. If a port is specified, only the connection
            to that port is closed.
        port : Optional[int]
            An optional port number to specifically close the connection to. If None, all connections associated with the
            edge are closed.
        """
        # Begin block to handle disconnection with lock
        async with self.lock_disconnect:

            # Check if a specific port is provided to close a particular connection
            if port:
                edge = self.get_edge(edge, port)

            # Initiate a handshake process with the specified edge to ensure proper connectivity update.
            # This line is crucial because it updates the edge connection on the other node's end before closing it.
            # By doing this, we ensure that any ongoing data transfer or network communication with the edge is properly
            # accounted for and that the connection closure does not interrupt important activities. This practice helps
            # in maintaining network stability, reduces the risk of leaving connections in a half-open state, and provides
            # an opportunity to gather latency metrics which can be useful for debugging or optimizing network performance.
            await self.handshake(edge, response=True)

            # Verify that provided edge instance is valid
            if not isinstance(edge, Edge):
                logger_main.warning(
                    f"{self.name}: The provided object '{edge}' is not a valid 'Edge' instance."
                )
                return

            logger_main.debug(
                f"{self.name}: The connection with {edge} will be removed."
            )
            logger_main.debug(
                f"{self.name}: Closing connection to {edge.address}."
            )

            # Closing the writer stream if it is not already closing
            if not edge.writer.is_closing():
                edge.writer.close()
                try:
                    await asyncio.wait_for(edge.writer.wait_closed(), 1)
                except asyncio.TimeoutError:
                    logger_main.debug(
                        f"{self.name}: Timeout occurred while closing connection to {edge}."
                    )
                except Exception as e:
                    e

            # Remove the closed connection from the edge list
            async with self.lock:
                self.edges = [edge_ for edge_ in self.edges if edge_ != edge]

            logger_main.debug(
                f"{self.name}: Connection to {edge} has been closed and removed."
            )

    # ----------------------------------------------------------------------
    def get_edge(self, ip: str, port: int) -> Optional[Edge]:
        """
        Retrieve an existing edge by its ip and port.

        This method looks up and returns an `Edge` instance that matches the given ip
        and port. If no such edge is found, it returns `None`.

        Parameters
        ----------
        ip : str
            The IP address of the edge to find.
        port : int
            The port number of the edge to find.

        Returns
        -------
        Optional[Edge]
            The `Edge` instance with the specified ip and port if found, else `None`.
        """
        for edge_ in self.edges:
            if (edge_.ip == ip) and (edge_.port == port):
                return edge_

    # ----------------------------------------------------------------------
    def get_edge_by_name(self, node_name: str) -> Optional[Edge]:
        """
        Retrieve an existing edge by its node name.

        This method looks up and returns an `Edge` instance that matches the given node name.
        If no such edge is found, it returns `None`.

        Parameters
        ----------
        node_name : str
            The name of the node to find.

        Returns
        -------
        Optional[Edge]
            The `Edge` instance with the specified node name if found, else `None`.
        """
        for edge_ in self.edges:
            if edge_.name == node_name:
                return edge_

    # ----------------------------------------------------------------------
    async def _connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Handle an incoming TCP connection.

        This coroutine is called when a new TCP connection is established. It will create a new Edge
        instance representing this connection and start listening to incoming messages from the peer.

        Parameters
        ----------
        reader : asyncio.StreamReader
            The StreamReader object to read data from the connection.
        writer : asyncio.StreamWriter
            The StreamWriter object to write data to the connection.
        """
        edge = Edge(writer=writer, reader=reader)

        logger_main.debug(
            f"{self.name}: Accepted connection from {writer.get_extra_info('peername')}."
        )
        logger_main.debug(
            f"{self.name}: New connection with {edge.address}."
        )
        asyncio.create_task(self._reader_loop(edge))

        # # If there are no edges (connections) yet, designate this node as the root node
        # if not self.edges:
        #     self.root = True

        # Check if a connection to the peer ip and port already exists
        if (edge.ip, edge.port, False) in [
            (edge_.ip, edge_.port, edge_.writer.is_closing())
            for edge_ in self.edges
        ]:
            logger_main.debug(
                f"{self.name}: Already connected with this node."
            )
            await self.close_connection(edge)
            return

    # ----------------------------------------------------------------------
    async def _reader_loop(self, edge: Edge) -> None:
        """
        Listen and process messages from a given edge in the network.

        This asynchronous method is the main loop for managing incoming messages
        from the connected peer node represented by the provided 'edge'. It constantly
        reads data from the StreamReader of the edge until the connection is closed, or
        an error is encountered. It handles framing, deserialization, and dispatching
        of the messages using the '_process_message' coroutine for further handling.

        Parameters
        ----------
        edge : Edge
            The network edge (connection) object from which the messages are read and
            processed. It contains the StreamReader and StreamWriter for network I/O.
        """
        try:
            # Reading data in chunks and processing messages
            while True:
                # Read the length of data (4 bytes)
                try:
                    length_data_bin = await edge.reader.readexactly(4)
                    length_topic_bin = await edge.reader.readexactly(4)
                except:
                    await asyncio.sleep(0.1)
                    continue

                # Check if the length of data is zero or missing
                if not length_data_bin:
                    await asyncio.sleep(0.1)
                    continue

                # Convert the first 4 bytes to integer representing data length and topic length in bytes
                length_data = int.from_bytes(
                    length_data_bin, byteorder="big"
                )
                length_topic = int.from_bytes(
                    length_topic_bin, byteorder="big"
                )

                # Read the topic from the edge reader exactly matching the topic length
                topic = await edge.reader.readexactly(length_topic)

                topic = self.deserializer(topic)
                # Check if the topic is "All" or if the topic is in the node's subscriptions
                if (
                    (topic == "All")
                    or (topic in self.subscriptions)
                    or self.root
                ):
                    data = await edge.reader.readexactly(length_data)
                    # Deserialize the received data into a message object
                    message = self.deserializer(data)
                    logger_main.debug(
                        f"{self.name}: Received a message of size {length_data} bytes."
                    )

                    # Check if the command in the message should be propagated
                    if message.command in self.propagation_command_list:

                        if (  # If the message propagation is enabled and the message hasn't been processed already, propagate the message.
                            self.message_propagation
                            and not await self.messages_pool.contains(
                                message.uuid
                            )
                        ):
                            # Propagate the received message if it meets the criteria and is not already in the message pool
                            async with self.lock_propagate:
                                await self._loop_message(message, edge)
                                await self.propagate(message, edge)
                    else:
                        # Process the message based on its command using the appropriate handler function.
                        await self._loop_message(message, edge)

                else:
                    # Read exactly the specified length of data from the edge reader
                    await edge.reader.readexactly(length_data)

                # # Propagate the received message to other nodes or peers.
                # if self.enable_propagation:
                #     async with self.lock_propagate:
                #         await self.propagate(message, edge)

        except ConnectionResetError as e:
            logger_main.debug(
                f"{self.name}: Connection reset by peer at {edge.address}: {str(e)}."
            )
            logger_main.debug(
                f"{self.name}: An exception occurred: \n{traceback.format_exc()}"
            )

        except asyncio.IncompleteReadError:
            logger_main.debug(
                f"{self.name}: Connection closed while reading from {edge.address}."
            )
            logger_main.debug(
                f"{self.name}: An exception occurred: \n{traceback.format_exc()}"
            )

        except Exception as e:
            logger_main.debug(
                f"{self.name}: Error in reader_loop for {edge.address}: {e}."
            )
            logger_main.debug(
                f"{self.name}: An exception occurred: \n{traceback.format_exc()}"
            )

        finally:
            # Close connection with the edge
            logger_main.debug(f"{self.name}: Closing connection with {edge}")
            logger_main.debug(
                f"{self.name}: An exception occurred: \n{traceback.format_exc()}"
            )
            await self._remove_closing_connection()
            await self.close_connection(edge)

            # Attempting to reconnect with the edge after a connection loss
            logger_main.debug(
                f"{self.name}: attempting to reconnect with {edge}"
            )
            await self.try_to_reconnect(edge)

    # ----------------------------------------------------------------------
    async def _loop_message(self, message: Message, edge: Edge) -> None:
        """
        Asynchronously process a received message and invoke the appropriate handler.

        This method acts as a dispatcher, delegating the received message to a specific method
        based on the command of the message. It utilizes dynamic method resolution to determine
        the handler for each command. If no specific handler is found for the command, a warning
        is logged indicating the missing processor.

        Parameters
        ----------
        message : Message
            The received message containing a command, associated data, and a timestamp.
        edge : Edge
            The network edge (connection) associated with the message source.
        """
        if processor := getattr(self, f"_process_{message.command}", None):
            # Process the received message command
            logger_main.debug(
                f"{self.name}: Processing the '{message.command}' command."
            )
            await processor(message, edge)
        else:
            logger_main.warning(
                f"{self.name}: No processor available for the command '{message.command}'."
            )

    # ----------------------------------------------------------------------
    async def _process_report_paired(
        self, message: Message, edge: Edge
    ) -> None:
        """
        Process a 'report_paired' network message.

        This method gets executed when a 'report_paired' command is received, indicating that a pairing action has occurred. Depending on the 'on_pair' behavior specified in the message, the node may disconnect after pairing or take no action.

        Parameters
        ----------
        message : Message
            The message instance containing the 'report_paired' command and associated data, such as pairing status and actions to take upon pairing.
        edge : Edge
            The edge from which the 'report_paired' message was received. It provides context for where to apply the action specified in the message data.
        """

        edge.paired = True
        # await self._handshake(edge, response=True, delay=0.1, back_delay=1)

        async with self.lock_pair:
            subscription = message.data["paired"]

            # Check if the node is already paired for the given subscription
            if self.paired_event[subscription].is_set():
                logger_main.debug(
                    f"{self.name}: Node is already paired, closing connection"
                )
                await self.close_connection(edge)
                return

            match message.data['on_pair']:
                case 'none':
                    pass
                case 'disconnect':
                    # Handling node disconnection after pairing
                    logger_main.debug(
                        f"{self.name}: Disconnected after pairing with {message.data['root_ip']} {message.data['root_port']}."
                    )
                    edge = self.get_edge(
                        message.data['root_ip'], message.data['root_port']
                    )
                    if edge and not edge.paired:
                        await self.close_connection(edge)

            # Set the paired event for the given subscription to indicate successful pairing
            logger_main.debug(f"{self.name}: Node is successfully paired.")
            self.paired_event[subscription].set()

    # ----------------------------------------------------------------------
    async def _start_tcp_server(self) -> None:
        """
        Configure and start the asyncio TCP server.

        A coroutine that sets up and starts the asyncio TCP server on the ip and port attributes of the ChaskiNode instance.
        The server will handle incoming client connections using the 'connected' coroutine as the protocol factory. In addition,
        a background keep-alive task is started to manage node heartbeat and connectivity. The server will run until explicitly
        stopped or an unhandled exception occurs.
        """
        self.server = await asyncio.start_server(
            self._connected,
            self.ip,
            self.port,
            ssl=self.ssl_context_server,
            reuse_address=True,
            reuse_port=True,
            # reuse_address=False,
            # reuse_port=False,
        )

        # Logging the server address and starting keep-alive task
        addr = self.server.sockets[0].getsockname()
        logger_main.debug(f"{self.name}: Serving at address {addr}.")
        self._keep_alive_task = asyncio.create_task(self._keep_alive())

        # Start serving TCP connections forever
        async with self.server:
            await self.server.serve_forever()

    # ----------------------------------------------------------------------
    async def _write(
        self,
        command: str,
        data: Any,
        edge: Optional[Edge] = None,
        topic: str = 'All',
    ) -> None:
        """
        Write data to the specified writer or all connected peers.

        Sends a packaged message with a particular command and associated data to either a single specified writer or broadcast it to all connected server peers. The message includes the command name and data, which gets serialized before being sent. This method ensures the data is properly framed with its length for transmission over TCP.

        Parameters
        ----------
        command : str
            The name of the command or type of the message to be sent.
        data : Any
            The payload of the message, which may consist of any type of data compatible with the serializer.
        writer : Optional[asyncio.StreamWriter], optional
            The stream writer to which the message should be sent. If None, the message will be sent to all server pairs. Defaults to None.
        """
        message = Message(
            command=command,
            topic=topic,
            data=data,
            timestamp=datetime.now(),
            ttl=self.ttl,
            uuid=self.uuid(),
        )
        # Serialize the Message instance into a bytes object for transmission
        data = self.serialize_message(message)

        # Call the _write_data method to send the serialized message data to the specified edge(s).
        await self._write_data(data, edges=[edge])

    # ----------------------------------------------------------------------
    def serialize_message(self, message: Message) -> bytes:
        """
        Serialize a Message instance into a bytes object.

        This method takes a Message instance and converts it into a bytes object
        that can be transmitted over the network. It first serializes the message
        and its topic, and then prepends the lengths of these serialized components
        in bytes for proper framing.

        Parameters
        ----------
        message : Message
            The message object to be serialized. It contains the command, topic,
            data, timestamp, TTL, and UUID associated with the message.

        Returns
        -------
        bytes
            A bytes object that represents the serialized message. The format
            ensures both the message and its topic can be accurately reconstructed
            on the receiving end.
        """
        # Serialize the Message instance and the topic string before sending over the network.
        data = self.serializer(message)
        topic = self.serializer(message.topic)

        # Convert the lengths of the data and topic to 4-byte representations.
        length = len(data).to_bytes(4, byteorder="big")
        length_topic = len(topic).to_bytes(4, byteorder="big")

        # Concatenate lengths and serialized data to form the final message frame
        data = length + length_topic + topic + data
        return data

    # ----------------------------------------------------------------------
    async def _write_data(
        self,
        data: bytes,
        edges: Optional[List[Edge]] = None,
        excluded_edges: List[Edge] = [],
    ) -> None:
        """
        Send serialized data to specified edges or all connected edges, excluding optional edges.

        This method writes serialized data to the provided list of edges or all connected edges.
        It ensures that data is only sent to edges that are not closing their connections and
        have not been explicitly excluded.

        Parameters
        ----------
        data : bytes
            The serialized data to be sent to the edges.
        edges : Optional[List[Edge]], optional
            A list of Edge instances to which the data should be sent. If None, the data is sent to all edges.
            Defaults to None.
        excluded_edges : List[Edge], optional
            A list of Edge instances to exclude from data transmission. Defaults to an empty list.

        Raises
        ------
        ConnectionResetError
            If an edge connection is lost while writing data, an attempt to reconnect is made.
        """
        # If no specific edges are provided, default to all connected edges
        if (edges is None) or (edges == [None]):
            edges = self.edges

        for edge in edges:

            # Skip edges that are in the excluded_edges list
            if edge in excluded_edges:
                continue

            # Ensure the server edge is not closing before writing data
            if not edge.writer.is_closing():
                edge.writer.write(data)
                try:
                    # Ensure the write buffer is flushed
                    await edge.writer.drain()
                except ConnectionResetError:
                    logger_main.warning(
                        f"{self.name}: Connection lost while writing to {edge.address}."
                    )
                    # Attempt to reconnect with the edge if the connection was lost.
                    # Attempt to reconnect only if the connection is not closing or closed.
                    if not edge.writer.is_closing():
                        await self.try_to_reconnect(edge)
                    # Remove the connection from the edges list if it is closing or closed.
                    elif edge.writer.is_closing():
                        await self._remove_closing_connection()

    # ----------------------------------------------------------------------
    async def ping(
        self,
        server_edge: Optional[Edge] = None,
        size: int = 0,
        repeat: int = 30,
    ) -> None:
        """
        Send ping messages to one or all connected edges.

        This method sends a ping message either to a specified edge or broadcasts it to all
        connected edges in the server_pairs list. It is used to measure network latency and can
        be used to ensure connectivity. The method allows specifying the size of each ping
        message and the number of times it should be repeated.

        Parameters
        ----------
        server_edge : Optional[Edge], optional
            The specific edge to which the ping message should be sent. If None, the ping
            message is sent to all edges in the server_pairs list. Defaults to None.
        size : int, optional
            The size of the dummy data to be sent with the ping message in bytes. This
            allows simulating payload sizes and their effect on latency. Defaults to 0.
        repeat : int, optional
            The number of ping messages to send. This can be used to perform repeated latency
            tests. Defaults to 30.
        """
        for _ in range(repeat):
            if server_edge is None:
                for edge in self.edges:
                    await self._ping(edge, size=size)
            else:
                await self._ping(server_edge, size=size)

    # ----------------------------------------------------------------------
    async def _ping(
        self,
        server_edge: Edge,
        delay: float = 0,
        latency_update: bool = True,
        size: int = 0,
    ) -> None:
        """
        Send a ping message to measure latency and connectivity.

        This method sends a single ping message to a specified edge or to all server pairs if no edge is specified. It also allows for setting a size for the payload in bytes and a delay before sending the ping. If the response option is true, a pong message will be sent back immediately after receiving a ping.

        Parameters
        ----------
        server_edge : Edge, optional
            The edge (network connection) to ping. If provided, the ping will be sent only to this edge. If None, pings will be sent to all server_pairs.
        delay : float, optional
            The delay in seconds before sending the ping message. Defaults to 0 seconds.
        latency_update : bool, optional
            If True, the latency information for the edge will be updated based on the ping response. Defaults to True.
        size : int, optional
            The size of the dummy payload data in bytes to be included in the ping message. Defaults to 0 bytes, meaning no additional data is sent.
        """
        await asyncio.sleep(delay)
        id_ = self.uuid()
        self.ping_events[id_] = server_edge

        await self._write(
            command='ping',
            data={
                "ping_id": id_,
                'latency_update': latency_update,
                'dummy_data': os.urandom(size),
                'size': size,
            },
            edge=server_edge,
        )

    # ----------------------------------------------------------------------
    async def _process_ping(self, message: Message, edge: Edge) -> None:
        """
        Handle incoming ping messages and optionally send a pong response.

        When a ping message is received, this method processes the message and sends
        a pong response back to the sender if requested. The method updates the edge's
        latency measurements based on the round trip time of the ping-pong exchange if
        the latency_update flag in the message is set to True. It also sets the edge's
        name, ip, port, and subscriptions based on the information received in the
        pong message.

        Parameters
        ----------
        message : Message
            The incoming ping message containing the timestamp and data needed to send
            a pong response.
        edge : Edge
            The edge associated with the incoming ping message.
        """
        data = {
            "source_timestamp": message.timestamp,
            "name": self.name,
            "ip": self.ip,
            "port": self.port,
            "subscriptions": self.subscriptions,
            "ping_id": message.data["ping_id"],
            "latency_update": message.data["latency_update"],
            "dummy_data": message.data["dummy_data"],
        }

        await self._write(command="pong", data=data, edge=edge)

    # ----------------------------------------------------------------------
    async def _process_pong(self, message: Message, edge: Edge) -> None:
        """
        Process a pong message and update edge latency measurements.

        This coroutine is triggered when a pong message is received in response to a ping request.
        It uses the time difference between the pong message's timestamp and the current time
        to calculate the round-trip latency. If the 'latency_update' flag in the message
        data is True, this latency value will be used to update the edge's latency statistics.
        Additionally, the edge's identifying information such as name, ip, and port is updated
        based on the pong message data.

        Parameters
        ----------
        message : Message
            The incoming pong message containing the original timestamp, sender's name,
            ip, port, and subscription information, as well as a unique identifier
            for the ping event.
        edge : Edge
            The edge object representing the connection to the sender of the pong message.
        """
        # Pop the ping event for the given ping_id
        server_edge = self.ping_events.pop(message.data["ping_id"])
        if message.data["latency_update"]:
            server_edge.update_latency(
                (
                    datetime.now() - message.data["source_timestamp"]
                ).total_seconds()
                * 500
            )

        # Update the edge information with the details from the pong message
        server_edge.name = message.data["name"]
        server_edge.ip = message.data["ip"]
        server_edge.port = message.data["port"]
        server_edge.subscriptions = message.data["subscriptions"]

        await asyncio.sleep(0)

    # ----------------------------------------------------------------------
    async def handshake(
        self,
        server_edge: Edge,
        delay: float = 0,
        back_delay=0,
        response: bool = False,
    ):
        """
        Initiate or respond to a handshake with the given edge.

        This method sends a handshake message to the specified edge and optionaly awaits for a handshake response.
        It is used to initiate or confirm a connection establishment between two ChaskiNodes.

        Parameters
        ----------
        server_edge : Edge
            The edge instance to which the handshake message is to be sent.
        delay : float, optional
            The amount of time (in seconds) to wait before sending the handshake message.
        back_delay : TODO
        response : bool, optional
            Indicates whether a response is expected. Set to True if waiting for a handshake back.
        """
        await asyncio.sleep(delay)

        id_ = self.uuid()
        self.handshake_events[id_] = server_edge

        await self._write(
            command='handshake',
            data={
                "handshake_id": id_,
                'response': response,
                'back_delay': back_delay,
            },
            edge=server_edge,
        )

    # ----------------------------------------------------------------------
    async def _process_handshake(self, message: Message, edge: Edge) -> None:
        """
        Process a handshake command received from a peer node.

        This coroutine is triggered upon receiving a handshake message, indicating an
        initiation of communication protocol by another ChaskiNode. It prepares and sends
        a handshake response back to the origin node to acknowledge the handshake and
        completes the two-way communication setup.

        Parameters
        ----------
        message : Message
            The handshake message received, containing the timestamp and data that
            includes the peer's name, ip, port, and subscription information.
        edge : Edge
            The edge associated with the peer node from which the handshake message was
            received, representing the communication connection to the peer.
        """
        data = {
            "name": self.name,
            "ip": self.ip,
            "port": self.port,
            "subscriptions": self.subscriptions,
            "handshake_id": message.data["handshake_id"],
        }

        # Check if a handshake response is expected.
        if message.data["response"]:
            await self.handshake(edge, delay=message.data["back_delay"])

        # Respond with handshake acknowledgement
        await self._write(command="handshake_back", data=data, edge=edge)

    # ----------------------------------------------------------------------
    async def _process_handshake_back(
        self, message: Message, edge: Edge
    ) -> None:
        """
        Handle a handshake response (back) from a peer node after an initial handshake.

        This coroutine is invoked upon receiving a handshake response from a peer node
        in the network. It updates the edge information with the name, ip, port, and
        subscriptions of the responding node and adds the edge to the server's active
        connections list.

        Parameters
        ----------
        message : Message
            The incoming handshake message containing peer information and a unique
            handshake identifier.
        edge : Edge
            The edge associated with the peer node that responded to the handshake, representing
            the communication link with the peer.
        """

        # Update server edge details after receiving handshake back
        server_edge = self.handshake_events.pop(message.data["handshake_id"])
        server_edge.name = message.data["name"]
        server_edge.ip = message.data["ip"]
        server_edge.port = message.data["port"]
        server_edge.subscriptions = message.data["subscriptions"]

        # Adding the server edge to the list of edges
        async with self.lock:
            self.edges.append(server_edge)

        # Ensure the coroutine yields control back to the event loop
        await asyncio.sleep(0)

    # ----------------------------------------------------------------------
    async def _process_discovery(
        self, message: Message, edge: Optional[Edge] = None
    ) -> None:
        """
        Processes a network discovery message and propagates it if necessary.

        This method is responsible for processing discovery messages as part of a network-wide search
        for ChaskiNodes with matching subscriptions. The method checks if the message should be
        propagated based on the TTL and visited nodes. If the current node's subscriptions match the
        origin's, a connection is attempted. Otherwise, the discovery message is forwarded to other
        ChaskiNodes, avoiding nodes that have already been visited.

        Parameters
        ----------
        message : Message
            The discovery message containing details about the discovery process, including the
            sender's information, visited nodes, and TTL.
        edge : Optional[Edge], optional
            The edge where the discovery message was received from. It may be used to avoid
            sending the discovery message back to the sender. Defaults to None.
        """
        subscription = message.data["origin_subscription"]

        # Check if all subscriptions are paired
        if not self.paired:
            return

        # Check the status of the origin node
        status = await self._request_status(
            message.data["origin_ip"],
            message.data["origin_port"],
        )

        # Check if the original node is already paired
        if status.data["paired"][subscription]:
            logger_main.debug(
                f"{self.name}: Node is already paired with another branch."
            )
            return

        # Check if TTL (Time-to-Live) has reached zero
        if message.data["ttl"] == 0:
            logger_main.debug(
                f"{self.name}: Discovery time-to-live (TTL) reached 0."
            )
            return

        # Check if the node can accommodate more edges and the subscription matches
        if (len(self.edges) < self.max_connections) and (
            subscription in self.subscriptions
        ):

            # Attempt connection to peer node with the given subscription
            await self._connect_to_peer(
                message.data["origin_ip"],
                message.data["origin_port"],
                paired=subscription,
                data=message.data,
            )
        else:
            new_data = message.data.copy()
            new_data["previous_node"] = self.name
            new_data["ttl"] = message.data["ttl"] - 1

            # Check if the current node is already in the list of visited nodes
            if self.name in message.data['visited']:
                logger_main.debug(
                    f"{self.name}: This branch has already been visited: {message.data['visited']}."
                )
                return

            # Add the current node's name to the set of visited nodes
            new_data["visited"].add(self.name)

            # Iterate through edges and forward the discovery message to peer nodes that have not been visited.
            for server_edge in self.edges:
                if not server_edge.name in [
                    message.data["previous_node"],
                    message.data["origin_name"],
                ]:
                    await self._write(
                        command="discovery",
                        data=new_data,
                        edge=server_edge,
                    )

    # ----------------------------------------------------------------------
    async def _remove_closing_connection(self) -> None:
        """
        Identify and remove server pairs that have closed connections.

        This coroutine iterates through the server pairs of the ChaskiNode instance
        and filters out any edges where the StreamWriter's associated connection is
        determined to be closed. This serves to maintain an accurate list of active
        connections on the server and ensures that operations are not attempted on
        closed connections.
        """

        n = len(self.edges)
        async with self.lock:
            self.edges = [
                edge for edge in self.edges if not edge.writer.is_closing()
            ]
        logger_main.debug(
            f"{self.name}: Removed a closing connection, {n - len(self.edges)} total edges disconnected."
        )

    # ----------------------------------------------------------------------
    async def _start_udp_server(self) -> None:
        """
        Start an asyncio UDP server to handle incoming datagrams.

        This coroutine is responsible for creating and binding a UDP socket to listen for incoming datagram messages.
        It then creates a UDP protocol endpoint, providing mechanics for handling UDP communications. The protocol handler,
        defined by the UDPProtocol class, specifies how incoming datagrams and error events are processed.

        Raises
        ------
        ValueError
            If the address provided for the UDP socket can't be resolved.
        """
        # Start the asyncio event loop to handle incoming UDP datagrams
        loop = asyncio.get_running_loop()

        # Resolve address info for the specified ip and port
        addr_info = socket.getaddrinfo(
            self.ip, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM
        )
        if not addr_info:
            raise ValueError(f"Cannot resolve address: {self.ip}")
        family, socktype, proto, canonname, sockaddr = addr_info[0]
        sock = socket.socket(family, socktype, proto)

        # Set socket options to allow address and port reuse
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        sock.bind((self.ip, self.port))
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self, self._process_udp_message), sock=sock
        )

        # Initialize UDP transport and request-response multiplexers
        self.udp_transport = transport
        self.request_response_multiplexer = {}
        self.request_response_multiplexer_events = {}

    # ----------------------------------------------------------------------
    async def _send_udp_message(
        self, command: str, message: Any, dest_ip: str, dest_port: int
    ) -> None:
        """
        Send a UDP message to the specified destination ip and port.

        This coroutine sends a pre-formatted message over UDP to a given destination ip and port. It serializes the
        message, which includes a command and its associated data, before transmission. This method is utilized for
        communication protocols that require UDP for message passing, like status checks or discovery procedures.

        Parameters
        ----------
        command : str
            The command type that dictates the kind of operation to perform, included in the message.
        message : Any
            The payload associated with the command that contains data necessary for carrying out the operation.
        dest_ip : str
            The destination IP address to which the message will be sent.
        dest_port : int
            The port number on the destination host to which the message should be directed.
        """
        message = Message(
            command=command, data=message, timestamp=datetime.now()
        )
        data = self.serializer(message)
        self.udp_transport.sendto(data, (dest_ip, dest_port))

    # ----------------------------------------------------------------------
    async def _process_udp_message(
        self, data: bytes, addr: Tuple[str, int]
    ) -> None:
        """
        Process incoming UDP messages routed to this node's UDP server.

        This asynchronous handler is called when the UDP server receives a
        new message. It deserializes the received bytes back into a message object
        and processes it according to the command it contains. The method handles
        'status' and 'response' commands used for node status checks and responses.

        Parameters
        ----------
        data : bytes
            The raw bytes received from the UDP client.
        addr : Tuple[str, int]
            A tuple containing the sender's IP address as a string and the port
            number as an integer.

        Raises
        ------
        ValueError
            If the received message cannot be processed or contains an invalid
            command not supported by the node.
        """
        message = self.deserializer(data)

        # Process the "status" command received via UDP
        match message.command:

            # Process the "status" command received via UDP
            case "status":
                data = self._get_status(id=message.data["id"])
                await self._send_udp_message("response", data, *addr[:2])

            # Process the "response" command
            case "response":
                self.request_response_multiplexer[message.data["id"]] = (
                    message
                )
                if (
                    message.data["id"]
                    in self.request_response_multiplexer_events
                ):
                    self.request_response_multiplexer_events[
                        message.data["id"]
                    ].set()

    # ----------------------------------------------------------------------
    async def _request_status(self, dest_ip: str, dest_port: int) -> Message:
        """
        Request the status of a node via UDP and wait for a response.

        This asynchronous method sends a UDP message to the target ip and port,
        requesting its status. It generates a unique identifier for the request, sends
        the message, and then waits for a response that matches the identifier. Once
        the response is received, it is returned as a Message object.

        Parameters
        ----------
        dest_ip : str
            The IP address of the destination node to query for status.
        dest_port : int
            The port number of the destination node to communicate the status request.

        Returns
        -------
        Message
            The status response message from the destination node, containing information
            such as whether it is paired and actively serving.
        """
        # Generate a unique identifier for the ping event
        id_ = self.uuid()

        # Store an asyncio event for the current request-response pairing
        self.request_response_multiplexer_events[id_] = asyncio.Event()

        # Prepare the data with the unique identifier for the request
        data = {"id": id_}

        # Send the status request message
        await self._send_udp_message("status", data, dest_ip, dest_port)

        # Wait for the response to be received
        await self.request_response_multiplexer_events[id_].wait()

        # Retrieve the status message from the request_response_multiplexer using the generated ID
        status = self.request_response_multiplexer[id_]

        # Removing the ID from multiplexer events and multiplexers.
        self.request_response_multiplexer_events.pop(id_)
        self.request_response_multiplexer.pop(id_)
        return status

    # ----------------------------------------------------------------------
    def uuid(self) -> str:
        """
        Generate a unique identifier for the node.

        This method generates and returns a universally unique identifier (UUID)
        as a string. The UUID is used to uniquely identify objects and events
        within the network communication context, ensuring that each identifier
        is distinct across the distributed node system.

        Returns
        -------
        str
            A string representation of a UUID, which can be used to uniquely identify
            an object, event, or message within the node network.

        Notes
        -----
        The UUID generation is based on Python's `uuid` module, which provides
        a way to create universally unique identifiers compliant with RFC 4122.

        See Also
        --------
        uuid.uuid4 : Generates a random UUID.

        References
        ----------
        .. [1] UUID documentation in Python library:
               https://docs.python.org/3/library/uuid.html
        """
        return str(uuid.uuid4())

    # ----------------------------------------------------------------------
    async def _keep_alive(self, interval: int = 7) -> None:
        """"""
        return

    # ----------------------------------------------------------------------
    async def remove_duplicated_connections(self) -> None:
        """
        Remove duplicate connections from the server pairs.

        Iterates over the list of server pairs and closes connections that have
        the same ip and port as an already seen connection. This ensures that each
        peer is connected to the node only once, avoiding redundant connections.

        """
        # Initialize an empty set to track seen connections
        seen_connections = set()

        for edge in self.edges:

            # Check if both edge.ip and edge.port are available
            if not (edge.ip and edge.port):
                continue

            # Check for duplicates and remove if found
            connection = (edge.ip, edge.port)
            if connection not in seen_connections:
                seen_connections.add(connection)
            else:
                await self.close_connection(edge)
                logger_main.debug(
                    f"{self.name}: Closed a duplicate connection to {connection}."
                )

    # ----------------------------------------------------------------------
    def is_connected_to(self, node: 'ChaskiNode') -> bool:
        """
        Check if this node is connected to another specified node.

        Determines whether the current ChaskiNode instance has an established TCP connection
        with the given node. It checks the server pairs list for a matching ip and port
        pair to confirm connectivity.

        Parameters
        ----------
        node : ChaskiNode
            The node to check for connectivity with the current node.

        Returns
        -------
        bool
            `True` if the current node is connected to the specified node; otherwise, `False`.

        """
        return (node.ip, node.port) in [
            (edge.ip, edge.port) for edge in self.edges
        ]

    # ----------------------------------------------------------------------
    def _get_status(self, **kwargs) -> dict:
        """
        Retrieve the status of the node.

        This method compiles and returns a dictionary containing the current status
        details of the node. The status includes information about the node's
        paired events for each subscription, whether the server is closing,
        and whether the node is in the process of attempting to reconnect.

        Parameters
        ----------
        **kwargs : dict, optional
            Additional status information that can be passed as key-value pairs and
            will be included in the returned status dictionary.

        Returns
        -------
        dict
            A dictionary containing the status details of the node. The keys include:
            - 'paired': A dictionary where keys are subscription topics and values are boolean
                        indicating whether the node is paired for that subscription.
            - 'serving': Boolean value indicating whether the server is closing (`False`) or not (`True`).
            - 'reconnecting': Boolean value indicating whether the node is currently attempting to
                              reconnect to a peer (`True`) or not (`False`).
        """
        return {
            "name": self.name,
            # Get the status of paired events for each subscription
            "paired": {
                sub: self.paired_event[sub].is_set()
                for sub in self.subscriptions
            },
            # Check if the server is closing; 'True' means it's still serving.
            "serving": not self.server_closing,
            # Check if the node's reconnecting event is currently set, implying it is trying to reconnect to a peer.
            "reconnecting": self.reconnecting.is_set(),
            **kwargs,
        }

    # ----------------------------------------------------------------------
    @property
    def status(self) -> dict:
        """
        Retrieve the current status of the ChaskiNode.

        This property compiles and returns a dictionary containing the current status
        details of the node. The status includes information about the node's
        paired events for each subscription, whether the server is closing,
        and whether the node is in the process of attempting to reconnect.

        Returns
        -------
        dict
            A dictionary containing the status details of the node. The keys include:
            - 'paired': A dictionary where keys are subscription topics and values are boolean
                        indicating whether the node is paired for that subscription.
            - 'serving': Boolean value indicating whether the server is closing (`False`) or not (`True`).
            - 'reconnecting': Boolean value indicating whether the node is currently attempting to
                              reconnect to a peer (`True`) or not (`False`).
        """
        return self._get_status()

    # ----------------------------------------------------------------------
    async def try_to_reconnect(self, edge: Edge) -> None:
        """
        Continuously attempt to reconnect to a given edge.

        This coroutine will retry to establish a TCP connection with the specified edge in case the initial connection
        has been lost. The attempts will be made at 1-second intervals until a successful connection is established
        or the coroutine is explicitly cancelled. This method is useful for maintaining persistent connections in a
        network of ChaskiNodes.

        Parameters
        ----------
        edge : Edge
            The edge to which the reconnection attempts will be made. This represents the lost connection that needs
            to be restored.
        """
        # If the reconnection attempt limit is set to zero, skip reconnection
        if not self.reconnections:
            self.reconnecting.clear()
            return

        attempt = 0
        self.reconnecting.set()
        while True:
            attempt += 1
            # Pause execution for 5 seconds before the next reconnection attempt
            await asyncio.sleep(5)
            try:
                logger_main.debug(f"{self.name}: Reconnecting with {edge}")
                await self._connect_to_peer(edge)
                break
            except Exception as e:
                logger_main.debug(
                    f"{self.name}: Reconnection attempt {attempt + 1} failed: {e}"
                )

            if attempt >= self.reconnections:
                break  # Stop attempting to reconnect after reaching the maximum allowed reconnections

        # Clear the 'reconnecting' event, indicating that reconnection attempts are complete
        self.reconnecting.clear()

    # ----------------------------------------------------------------------
    def get_free_port(self) -> int:
        """
        Get a free port for the node to use.

        This method creates a temporary socket to bind to a port with value 0, which
        causes the operating system to allocate an available port. Once the socket is bound,
        the port number assigned by the operating system is retrieved and the socket is
        closed. This port number can be used for subsequent network operations requiring
        a free and available port.

        Returns
        -------
        int
            The port number assigned by the operating system that is free and available for use.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Bind the socket to an empty string and port 0 to let the OS automatically select a free port.
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port

    # ----------------------------------------------------------------------
    def is_port_available(self, port: int) -> bool:
        """
        Check if a specific port is available for use.

        This method attempts to bind to a given port to determine if it is available
        for use. It creates a temporary socket and tries to bind it to the specified
        port on the current node's ip. If the binding is successful, the port is
        considered available. Otherwise, it is in use.

        Parameters
        ----------
        port : int
            The port number to check for availability.

        Returns
        -------
        bool
            `True` if the port is available; `False` if the port is already in use.

        Raises
        ------
        OSError
            If the port binding operation encounters an error other than the port being in use.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((self.ip, port))
                s.listen(
                    1
                )  # Start listening for incoming connections on the assigned port with a backlog of 1
                return True
            except OSError:
                return False

    # ----------------------------------------------------------------------
    @property
    def paired(self) -> bool:
        """
        Check if the node is paired for all its subscriptions.

        This property returns a boolean value indicating whether the node is paired
        for all the subscriptions it subscribes to.

        Returns
        -------
        bool
            `True` if the node is paired for all subscriptions, otherwise `False`.
        """
        return all(
            [self.paired_event[sub].is_set() for sub in self.subscriptions]
        )

    # ----------------------------------------------------------------------
    async def _generic_request_udp(
        self,
        callback: str,
        kwargs: dict[str, Any] = {},
        edge: Optional['Edge'] = None,
    ) -> Any:
        """
        Make a generic UDP request to a peer node and await the response.

        This method sends a UDP request with a specified callback function and additional
        keyword arguments. It generates a unique identifier for the request, sends the message,
        and waits for a response from the peer node. The response is then retrieved and returned
        as the result of the method call.

        Parameters
        ----------
        callback : str
            The name of the method to call on the peer node.
        kwargs : dict[str, Any], optional
            A dictionary of keyword arguments to pass to the callback method on the peer node.

        Returns
        -------
        Any
            The response data received from the peer node.

        Raises
        ------
        asyncio.TimeoutError
            If the response is not received within a specified timeout period.

        Notes
        -----
        This method is typically used for internal communication between nodes in the network.
        It helps in sending requests and receiving responses asynchronously over UDP.
        """
        # Generates a unique identifier and assigns it to id_.
        id_ = self.uuid()

        # Prepare the data for the UDP request, including a unique request ID, the callback function to invoke, and any additional arguments.
        data_ = {"id": id_}
        data_['callback'] = callback
        data_['kwargs'] = kwargs

        # Create an event to synchronize the request and response flow
        self.synchronous_udp_events[id_] = asyncio.Event()

        # Send the UDP request and wait for the response event to be set
        await self._write('request_udp', data_, edge)
        await self.synchronous_udp_events[id_].wait()

        # Retrieve the response data for the given request ID from the synchronous_udp dictionary
        response = self.synchronous_udp[id_]

        # Remove the processed request ID from the UDP events and responses dictionaries.
        self.synchronous_udp_events.pop(id_)
        self.synchronous_udp.pop(id_)

        return response

    # ----------------------------------------------------------------------
    async def _process_response_udp(
        self, message: Message, edge: Edge
    ) -> None:
        """
        Process a response to a UDP request.

        This method is invoked when a response to a UDP request is received.
        It extracts the corresponding request ID from the message, retrieves the
        response data, and sets the corresponding event to indicate that
        the response has been processed.

        Parameters
        ----------
        message : Message
            The received message containing the response data.
        edge : Edge
            The edge from which the response was received.
        """
        # Extract the response data and set the event for synchronous handling
        data = message.data
        id_ = data['id']

        # Store the response data and set the event to signal that the response has been processed
        self.synchronous_udp[id_] = data['response']
        self.synchronous_udp_events[id_].set()

        await asyncio.sleep(0)

    # ----------------------------------------------------------------------
    async def _process_request_udp(
        self, message: Message, edge: Edge
    ) -> None:
        """
        Process an incoming UDP request and dispatch a response.

        This method handles the reception of a UDP request message. It uses the callback function
        specified in the message to generate a response and then sends this response back to the
        requester.

        Parameters
        ----------
        message : Message
            The received UDP request message containing the required callback and arguments.
        edge : Edge
            The edge representing the connection from which the request was received.
        """
        data = message.data

        # Execute the callback method specified in the request data and store its result in the response field.
        data['response'] = await getattr(self, data['callback'])(
            **data['kwargs']
        )

        await self._write('response_udp', data, edge)

    # ----------------------------------------------------------------------
    async def _test_generic_request_udp(
        self, echo_data: dict[str, Any] = {}
    ) -> Any:
        """
        Send a test UDP request and await the response.

        This method constructs and sends a generic UDP request with the provided `echo_data` to a peer node
        and waits for the response. It is useful for testing the UDP communication mechanism between nodes.

        Parameters
        ----------
        echo_data : dict, optional
            A dictionary of data to be included in the UDP request. The default is an empty dictionary.

        Returns
        -------
        Any
            The response data received from the peer node.
        """
        return await self._generic_request_udp(
            '_test_generic_response_udp', echo_data
        )

    # ----------------------------------------------------------------------
    async def _test_generic_response_udp(self, **echo_data: Any) -> Any:
        """
        Respond to a test UDP request by echoing the received data.

        This coroutine processes a generic UDP request for testing purposes.
        It simply echoes back the provided `echo_data` to the requester.
        This method can be used to verify the correct handling of UDP requests
        and responses between nodes.

        Parameters
        ----------
        **echo_data : dict
            A dictionary of data received in the UDP request. This data will be
            echoed back in the response.

        Returns
        -------
        dict
            The `echo_data` dictionary received in the request, echoed back to
            the requester.
        """
        await asyncio.sleep(0)
        return echo_data

    # ----------------------------------------------------------------------
    async def propagate(self, message: Message, source_edge: Edge) -> None:
        """
        Propagate a message to other connected edges, excluding the source edge.

        This method is used to propagate messages received by a node to other connected
        edges in the network, excluding the edge from which the message was received.
        This helps in disseminating the message across the network while preventing
        immediate looping of the message back to its origin.

        Parameters
        ----------
        message : Message
            The message to be propagated. This contains the command, topic, data,
            timestamp, TTL, and UUID of the message.
        source_edge : Edge
            The edge from which the message was originally received. This edge will
            be excluded from the propagation to prevent looping.

        Notes
        -----
        - The message's TTL (Time-to-Live) will be decremented by one. If the TTL
          reaches zero or less, the message will not be propagated further.
        - The message's UUID will be appended to the message pool to track its
          propagation and avoid duplicate processing.
        """
        # Decrement the Time-to-Live (TTL) value of the message.
        # If the TTL value reaches zero or less, the message will no longer be propagated.
        message.decrement_ttl()
        if message.ttl <= 0:
            return

        # Append the message's UUID to the pool to track it and avoid duplicate processing.
        await self.messages_pool.append(message.uuid)

        # Propagate the message to other edges, excluding the source edge to prevent immediate loops.
        await self._write_data(
            self.serialize_message(message), excluded_edges=[source_edge]
        )

    # ----------------------------------------------------------------------
    async def request_ssl_certificate(self, ca_address: str) -> None:
        """
        Request an SSL certificate from a Certificate Authority (CA).

        This coroutine initiates a request for an SSL certificate from the specified
        Certificate Authority (CA) address. It generates a Certificate Signing Request (CSR),
        sends it to the CA, and waits for the CA to sign the CSR and return the signed certificate.
        Additionally, it retrieves the CA's certificate and updates the node's SSL context.

        Parameters
        ----------
        ca_address : str
            The address of the Certificate Authority (CA) node to request the SSL certificate from.

        Raises
        ------
        Exception
            If there is an error during the SSL certificate request or signing process.

        Notes
        -----
        The method performs the following steps:
        1. Generates a CSR locally.
        2. Establishes a connection with the CA node.
        3. Sends the CSR to the CA for signing.
        4. Receives the signed certificate and CA's certificate from the CA.
        5. Writes the signed certificate and CA's certificate to disk.
        6. Updates the SSL context of the node with the new certificate.
        7. Closes the connection with the CA node.
        8. Restarts the node with the newly updated SSL context.
        """

        if self.edges:
            raise Exception(
                "Cannot request an SSL certificate while there are active connections. Please close all connections before proceeding."
            )

        pattern = r"(?:(?:\*?\w+@)?(\d{1,3}(?:\.\d{1,3}){3})|(?:\*?\w+@)?\[((?:[0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4})\]):(\d+)"
        ipv4, ipv6, port = re.findall(pattern, ca_address)[0]

        if ipv4:
            ip_address = ipaddress.IPv4Address(self.ip)
        elif ipv6:
            ip_address = ipaddress.IPv6Address(self.ip)

        # Initialize the CertificateAuthority object to manage SSL certificates.
        ca = CertificateAuthority(
            self.id,
            ip_address,
            ssl_certificates_location=self.ssl_certificates_location,
            ssl_certificate_attributes=self.ssl_certificate_attributes,
        )
        # Generate the keys and the Certificate Signing Request (CSR).
        ca.generate_key_and_csr()

        # Load the certificate from the specified certificate path
        csr_data_client = ca.load_certificate(ca.certificate_paths['client'])
        csr_data_server = ca.load_certificate(ca.certificate_paths['server'])

        # Establish a connection with the Certificate Authority (CA) node to request SSL certification.
        try:
            ca_edge = await self.connect(ca_address)
        except:
            raise Exception(
                "The Certification Authority is not operational."
            )

        # Prepare the data for the Certificate Signing Request (CSR) including
        # the CSR data and the node's unique identifier (node_id). Then,
        # request the CA to sign the CSR using a generic UDP request.
        data = {
            'csr_data_client': csr_data_client,
            'csr_data_server': csr_data_server,
            'node_id': self.id,
        }
        data_response = await self._generic_request_udp(
            callback='sign_csr',
            kwargs=data,
            edge=ca_edge,
        )

        signed_csr_client = data_response['signed_csr_client']
        signed_csr_server = data_response['signed_csr_server']
        ca_certificate_path = data_response['ca_certificate_path']

        ca.ca_certificate_path = os.path.join(
            ca.ssl_certificates_location, 'ca.cert'
        )
        # Write the signed certificate and the CA's certificate to their respective paths.
        ca.write_certificate(
            ca.certificate_signed_paths['client'], signed_csr_client
        )
        ca.write_certificate(
            ca.certificate_signed_paths['server'], signed_csr_server
        )
        ca.write_certificate(ca.ca_certificate_path, ca_certificate_path)

        logger_main.debug(
            f"{self.name}: SSL certificate request process completed successfully."
        )
        # Load a new SSL context with the generated root CA's certificate and signed certificate.
        self.ssl_context_client, self.ssl_context_server = ca.get_context()

        # Close the connection with the Certificate Authority (CA) node after obtaining the signed certificate.
        await self.close_connection(ca_edge)

        # Restart the node by stopping the current event loop and then creating a new event loop task to run the node.
        await self.stop()
        asyncio.create_task(self.run())

        if not (self.ssl_context_client and self.ssl_context_server):
            raise Exception("Failed to create SSL contexts")
