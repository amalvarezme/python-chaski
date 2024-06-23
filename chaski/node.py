"""
==========
ChaskiNode
==========

"""

import asyncio
import logging
import os
import pickle
import random
import socket
import traceback
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from string import ascii_letters

from typing import Any, Optional, List, Callable, Awaitable, Tuple, Literal, Union

logger_main = logging.getLogger("ChaskiNode")
logger_edge = logging.getLogger("ChaskiNodeEdge")
logger_udp = logging.getLogger("ChaskiNodeUDP")


########################################################################
@dataclass
class Edge:
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    latency: float = 0
    jitter: float = 0
    name: str = ""
    host: str = ""
    port: int = 0
    subscriptions: set = field(default_factory=set)
    ping_in_progress: bool = False

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Return a string representation of the Edge.

        Generates a human-readable string that includes the Edge's name, latency,
        jitter, host, and port. The string format highlights the state of the Edge
        in terms of network performance and connection details.

        Returns
        -------
        str
            A formatted string characterizing the Edge instance with details
            like name, latency (in milliseconds), jitter (in milliseconds),
            host, and port.
        """
        return f"{self.name}: N({self.latency: .0f}, {self.jitter: .0f}) {self.host}: {self.port}"

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
            A tuple of two elements where the first element is the host (IP address or hostname) of
            the remote peer as a string, and the second element is the port number as an integer.
        """
        return self.writer.get_extra_info("peername")

    # ----------------------------------------------------------------------
    @cached_property
    def local_address(self) -> Tuple[str, int]:
        """
        Retrieve the local address to which the edge's writer is connected.

        This cached property returns a tuple containing the local host IP address or hostname, and the local port number, obtained from the writer socket's information. It represents the local end of the connection managed by the edge.

        Returns
        -------
        Tuple[str, int]
            A tuple containing the local address of the writer socket. The first element is the host IP address or hostname as a string, and the second element is the port number as an integer.
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
            prior_variance = self.jitter ** 2

            likelihood_mean = new_latency
            likelihood_variance = 100 ** 2  # Assume a fixed variance for the new measurement
            posterior_mean = (
                prior_mean / prior_variance + likelihood_mean / likelihood_variance
            ) / (1 / prior_variance + 1 / likelihood_variance)
            posterior_variance = 1 / (1 / prior_variance + 1 / likelihood_variance)

            self.latency = posterior_mean
            self.jitter = posterior_variance ** 0.5  # Take the square root to return to standard deviation.

            logger_edge.debug(f"Updated latency: {self.latency: .2f}, jitter: {self.jitter: .2f}.")


########################################################################
@dataclass
class Message:
    """
    A class to represent a message with a specific command and associated data, along with a timestamp indicating when it was created.

    This class is designed to encapsulate all necessary details of a message within a network communication context.
    Each message carries a command that indicates the action to be performed, the data required to execute the action,
    and the time at which the message was instantiated. The timestamp is particularly useful for logging and
    debugging purposes, as it helps determine when the message was generated relative to other events.

    Parameters
    ----------
    command : str

        The specific command or instruction that this message signifies. Commands are typically predefined and
        understood by both the sender and receiver in the communication protocol being implemented.
    data : Any
        The payload of the message containing the data that the command operates on. This can be any type of data
        struct, such as a string, dictionary, or a custom object, and its structure depends on the specific needs
        of the command.
    timestamp : datetime
        The exact date and time when the message was created, represented as a datetime object. The timestamp provides
        chronological context for the message's creation, aiding in message tracking, ordering, and latency calculations.

    """
    command: str
    data: Any
    timestamp: datetime


########################################################################
class UDPProtocol(asyncio.DatagramProtocol):
    """
    An asyncio protocol class for processing UDP packets.

    This class defines a custom protocol to handle UDP communications for a node. It outlines
    methods providing core functionality for sending, receiving, and effectively managing
    UDP connections.
    """

    # ----------------------------------------------------------------------
    def __init__(self, node: 'ChaskiNode', on_message_received: Awaitable[None]):
        """
        Initialize the UDP protocol with a reference to the parent node and the message receive callback.

        Provides initialization for the UDPProtocol instance by setting up a reference to the ChaskiNode
        which uses the protocol, and setting the callback function that is called upon message reception.
        This method is critical for establishing the protocol's behavior in the context of the node it serves.

        Parameters
        ----------
        node : 'ChaskiNode'
            The ChaskiNode instance that this protocol is associated with. It acts as the parent node
            which contains all the relevant context and state necessary for handling UDP communication.
        on_message_received : Callable[[bytes, Tuple[str, int]], Awaitable[None]]
            A callback function that is invoked when a new message is received via the UDP protocol.
            The function should be an asynchronous function that takes two parameters: a bytes object
            containing the raw message data and a tuple containing the sender's address as a string
            and the port number as an integer.

        """
        self.node = node
        self.on_message_received = on_message_received

    # ----------------------------------------------------------------------
    def datagram_received(self, message: bytes, addr: tuple[str, int]) -> None:
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
    """"""

    # ----------------------------------------------------------------------
    def __init__(
        self,
        host: str,
        port: int,
        serializer: Callable[[Any], bytes] = pickle.dumps,
        deserializer: Callable[[bytes], Any] = pickle.loads,
        name: Optional[str] = None,
        subscriptions: List[str] = [],
        run: bool = False,
        ttl: int = 64,
        root: bool = False,
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
        host : str
            The hostname or IP address to listen on or bind to.
        port : int
            The port number to listen on or bind to.
        serializer : Callable[[Any], bytes], optional
            The function to serialize data before sending it over the network. Defaults to `pickle.dumps`.
        deserializer : Callable[[bytes], Any], optional
            The function to deserialize received data. Defaults to `pickle.loads`.
        name : Optional[str], optional
            The name of the node, used for identification and logging purposes. Defaults to `None`.
        subscriptions : List[str], optional
            A list of subscription topic strings this node is interested in. Defaults to an empty list.
        run : bool, optional
            A flag determining whether the TCP and UDP servers should start immediately upon the node's
            initialization. Defaults to `False`.
        ttl : int, optional
            Time-to-live value for discovery messages. Defaults to `64`.
        root : bool, optional
            Flag to indicate whether this node is the root in the network topology. Defaults to `False`.

        """
        self.host = host
        self.port = port
        self.serializer = serializer
        self.deserializer = deserializer
        self.server = None
        self.ttl = ttl
        self.suscriptions = set(subscriptions)
        self.name = f"{name}"
        self.parent_node = None
        self.server_pairs = []
        self.paired_event = asyncio.Event()
        self.lock = asyncio.Lock()
        self.ping_events = {}

        if root:
            self.paired_event.set()

        if run:
            asyncio.create_task(self.run())

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Represent a node in a network graph.

        This class represents an Edge in a network graph, which is part of a ChaskiNode. It encapsulates the necessary properties and methods for managing the state and behavior of a network connection. Edges track connection details like latency and jitter, and they store information about the host, port, and name of the connection, as well as the subscriptions of topics of interest. Furthermore, an edge provides functionality for sending pings to measure latency, and it can reset its performance statistics.
        """
        return f"{{{self.name}: {self.port}}}"

    # ----------------------------------------------------------------------
    async def run(self) -> None:
        """
        Launch TCP and UDP servers for the node.

        This coroutine starts the TCP and UDP server tasks to listen for incoming connections and handle UDP datagrams. It is an essential part of the node's operation, enabling it to accept connections from other nodes and exchange messages over the network.
        """
        self.server_closing = False
        await asyncio.gather(
            self._start_tcp_server(),
            self._start_udp_server()
        )

    # ----------------------------------------------------------------------
    async def stop(self) -> None:
        """"""
        self.server_closing = True

        for edge in self.server_pairs:
            await self.close_connection(edge)

        if hasattr(self, 'udp_transport'):
            self.udp_transport.close()

        if hasattr(self, '_keep_alive_task'):
            self._keep_alive_task.cancel()

        if hasattr(self, 'server'):
            self.server.close()
            try:
                await asyncio.wait_for(self.server.wait_closed(), timeout=5)
            except asyncio.TimeoutError:
                logger_main.warning("Timeout waiting for server to close.")

    # ----------------------------------------------------------------------
    async def connect_to_peer(
        self,
        node: 'ChaskiNode',
        peer_port: Optional[int] = None,
        paired: bool = False,
        data: dict = {}
    ) -> None:
        """"""
        if hasattr(node, "host"):
            peer_host, peer_port = node.host, node.port
        else:
            peer_host, peer_port = node, peer_port

        if self.host == peer_host and self.port == peer_port:
            logger_main.warning(f"{self.name}: Impossible to connect a node to itself.")
            return

        await self.wait_for_all_edges_ready()

        if (peer_host, peer_port, False) in [(edge.host, edge.port, edge.writer.is_closing()) for edge in self.server_pairs]:
            logger_main.warning(f"{self.name}: Already connected with this node.")
            return

        addr_info = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM)
        if not addr_info:
            raise ValueError(f"Cannot resolve address: {self.host}")
        family, socktype, proto, canonname, sockaddr = addr_info[0]

        reader, writer = await asyncio.open_connection(peer_host, peer_port, family=family)
        edge = Edge(writer=writer, reader=reader)

        async with self.lock:
            self.server_pairs.append(edge)

        if paired:
            data['paired'] = paired
            await self._write(
                command="report_paired",
                data=data,
                writer=writer,
            )

        logger_main.debug(f"{self.name}: New connection with {edge.address}.")
        asyncio.create_task(self._reader_loop(edge))
        await self._ping(edge, response=True, latency_update=False)

    # ----------------------------------------------------------------------
    async def wait_for_all_edges_ready(self) -> None:
        """"""
        logger_main.debug(f"Waiting for all connections to node {self.name} to be ready.")
        if not self.server_pairs:
            return
        while True:
            all_ready = all(edge.host and edge.port and edge.name for edge in self.server_pairs)
            if all_ready:
                logger_main.debug(f"Node {self.name} is ready.")
                break
            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    async def wait_for_ready(self) -> None:
        """"""
        while True:
            if self.ready():
                break
            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    async def discovery(self, node: Optional['ChaskiNode'] = None, on_pair: Union[str, Literal['none', 'disconnect']] = 'none', timeout: int = 10) -> None:
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
        if not self.server_pairs:
            logger_main.warning(f"{self.name}: No connection to perform discovery.")
            return

        for edge in self.server_pairs:
            if edge.subscriptions.intersection(self.suscriptions):
                logger_main.warning(f"{self.name}: The node is already paired.")
                self.paired_event.set()
                return

        if (node is None) and (len(self.server_pairs) == 0):
            logger_main.warning(f"{self.name}: Unable to discover new nodes no 'Node' or 'Edge' available.")
            return

        if not node:
            node = self.server_pairs[0]

        data = {
            "origin_host": self.host,
            "origin_port": self.port,
            "origin_name": self.name,
            "previous_node": self.name,
            "visited": set([self.name]),
            "on_pair": on_pair,
            "root_host": node.host,
            "root_port": node.port,
            "origin_suscriptions": self.suscriptions,
            "ttl": self.ttl,
        }

        await self._write(
            command="discovery",
            data=data,
            writer=node.writer,
        )

        await asyncio.sleep(timeout)
        logger_main.warning(f"{self.name}: Timeout reached during discovery process node is considered paired.")
        self.paired_event.set()

    # ----------------------------------------------------------------------
    async def close_connection(self, edge: Edge, port: Optional[int] = None) -> None:
        """"""
        # self.wait_for_all_edges_ready()
        if port:
            for edge_ in self.server_pairs:
                if (edge_.host == edge) and (edge_.port == port):
                    edge = edge_
                    break

        if not isinstance(edge, Edge):
            logger_main.warning(f"{self.name}: The provided object '{edge}' is not a valid 'Edge' instance.")
            return
        logger_main.debug(f"{self.name}: The connection with {edge} will be removed.")

        logger_main.debug(f"{self.name}: Closing connection to {edge.address}.")
        if not edge.writer.is_closing():
            edge.writer.close()
            try:
                await asyncio.wait_for(edge.writer.wait_closed(), 1)
            except asyncio.TimeoutError:
                logger_main.debug(f"{self.name}: Timeout occurred while closing connection to {edge}.")

        async with self.lock:
            self.server_pairs = [
                edge_ for edge_ in self.server_pairs if edge_ != edge
            ]

        if len(self.server_pairs) == 0 and not self.server_closing:
            logger_main.warning(f"{self.name}: Orphan node detected.")
            logger_main.warning(f"{self.name}: Retrying connection.")

            status = await self._request_status(edge.host, edge.port)
            if status.data['serving']:
                try:
                    await self.connect_to_peer(edge)
                except ConnectionRefusedError:
                    logger_main.warning(f"{self.name}: Unable to reconnect with {edge}")

        logger_main.debug(f"{self.name}: Connection to {edge} has been closed and removed.")

    # ----------------------------------------------------------------------
    async def connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """"""
        edge = Edge(writer=writer, reader=reader)

        logger_main.debug(f"{self.name}: Accepted connection from {writer.get_extra_info('peername')}.")
        logger_main.debug(f"{self.name}: New connection with {edge.address}.")
        asyncio.create_task(self._reader_loop(edge))

        await self.wait_for_all_edges_ready()

        if (edge.host, edge.port, False) in [(edge_.host, edge_.port, edge_.writer.is_closing()) for edge_ in self.server_pairs]:
            logger_main.debug(f"{self.name}: Already connected with this node.")
            await self.close_connection(edge)
            return

        async with self.lock:
            self.server_pairs.append(edge)

    # ----------------------------------------------------------------------
    async def _reader_loop(self, edge: Edge) -> None:
        """"""
        try:
            while True:
                length_data = await edge.reader.readexactly(4)
                if not length_data:
                    return None
                length = int.from_bytes(length_data, byteorder="big")
                data = await edge.reader.readexactly(length)
                if not data:
                    raise ConnectionResetError("Connection closed by peer")

                message = self.deserializer(data)
                logger_main.debug(f"{self.name}: Received a message of size {length} bytes.")
                await self._process_message(message, edge)

        except ConnectionResetError as e:
            logger_main.warning(
                f"{self.name}: Connection reset by peer at {edge.address}: {str(e)}."
            )
            logger_main.error(f"{self.name}: An exception occurred: \n{traceback.format_exc()}")
        except asyncio.IncompleteReadError:
            logger_main.warning(
                f"{self.name}: Connection closed while reading from {edge.address}."
            )
            logger_main.error(f"{self.name}: An exception occurred: \n{traceback.format_exc()}")
        except Exception as e:
            logger_main.warning(
                f"{self.name}: Error in reader_loop for {edge.address}: {e}."
            )
            logger_main.error(f"{self.name}: An exception occurred: \n{traceback.format_exc()}")
        finally:
            logger_main.warning(f"{self.name}: Closing connection with {edge}")
            logger_main.error(f"{self.name}: An exception occurred: \n{traceback.format_exc()}")
            await self.close_connection(edge)

    # ----------------------------------------------------------------------
    async def _process_message(self, message: Message, edge: Edge) -> None:
        """"""
        if processor := getattr(self, f"_process_{message.command}", None):
            logger_main.debug(f"{self.name}: Processing the '{message.command}' command.")
            await processor(message, edge)
        else:
            logger_main.warning(f"{self.name}: No processor available for the command '{message.command}'.")

    # ----------------------------------------------------------------------
    async def _process_report_paired(self, message: Message, edge: Edge) -> None:
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
        if message.data["paired"]:

            match message.data['on_pair']:
                case 'none':
                    pass
                case 'disconnect':
                    logger_main.debug(f"{self.name}: Disconnected after pairing with {message.data['root_host']} {message.data['root_port']}.")
                    await self.close_connection(message.data['root_host'], message.data['root_port'])

            logger_main.debug(f"{self.name}: Node is successfully paired.")
            self.paired_event.set()

    # ----------------------------------------------------------------------
    async def _start_tcp_server(self) -> None:
        """
        Configure and start the asyncio TCP server.

        A coroutine that sets up and starts the asyncio TCP server on the host and port attributes of the ChaskiNode instance.
        The server will handle incoming client connections using the 'connected' coroutine as the protocol factory. In addition,
        a background keep-alive task is started to manage node heartbeat and connectivity. The server will run until explicitly
        stopped or an unhandled exception occurs.
        """
        self.server = await asyncio.start_server(
            self.connected,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True,
        )
        addr = self.server.sockets[0].getsockname()
        logger_main.debug(f"{self.name}: Serving at address {addr}.")
        self._keep_alive_task = asyncio.create_task(self._keep_alive())

        async with self.server:
            await self.server.serve_forever()

    # ----------------------------------------------------------------------
    async def _write(self, command: str, data: Any, writer: Optional[asyncio.StreamWriter] = None) -> None:
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
        message = Message(command=command, data=data, timestamp=datetime.now())
        data = self.serializer(message)

        length = len(data).to_bytes(4, byteorder="big")
        data = length + data

        if writer is None:
            for server_edge in self.server_pairs:
                if not server_edge.writer.is_closing():
                    server_edge.writer.write(data)
                    try:
                        await server_edge.writer.drain()
                    except ConnectionResetError:
                        logger_main.warning(
                            f"{self.name}: Connection lost while writing to {server_edge.address}."
                        )
                        await self.close_connection(server_edge)
        else:
            writer.write(data)
            try:
                await writer.drain()
            except ConnectionResetError:
                logger_main.warning(
                    f"{self.name}: Connection lost while attempting to write to {writer.get_extra_info('peername')}."
                )
                await self._remove_closing_connection()

    # ----------------------------------------------------------------------
    async def ping(self, server_edge: Optional[Edge] = None, size: int = 0, repeat: int = 30) -> None:
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
                for edge in self.server_pairs:
                    await self._ping(edge, size=size)
            else:
                await self._ping(server_edge, size=size)

    # ----------------------------------------------------------------------
    async def _ping(self, server_edge: Edge, delay: float = 0, response: bool = False, latency_update: bool = True, size: int = 0) -> None:
        """
        Send a ping message to measure latency and connectivity.

        This method sends a single ping message to a specified edge or to all server pairs if no edge is specified. It also allows for setting a size for the payload in bytes and a delay before sending the ping. If the response option is true, a pong message will be sent back immediately after receiving a ping.

        Parameters
        ----------
        server_edge : Edge, optional
            The edge (network connection) to ping. If provided, the ping will be sent only to this edge. If None, pings will be sent to all server_pairs.
        delay : float, optional
            The delay in seconds before sending the ping message. Defaults to 0 seconds.
        response : bool, optional
            If True, the method sends a pong response immediately after receiving a ping request. Defaults to False.
        latency_update : bool, optional
            If True, the latency information for the edge will be updated based on the ping response. Defaults to True.
        size : int, optional
            The size of the dummy payload data in bytes to be included in the ping message. Defaults to 0 bytes, meaning no additional data is sent.
        """
        await asyncio.sleep(delay)
        id_ = self._gen_id()
        self.ping_events[id_] = server_edge

        await self._write(
            command='ping',
            data={
                "ping_id": id_,
                'response': response,
                'latency_update': latency_update,
                'dummy_data': os.urandom(size),
                'size': size,
            },
            writer=server_edge.writer,
        )

    # ----------------------------------------------------------------------
    async def _process_ping(self, message: Message, edge: Edge) -> None:
        """
        Handle incoming ping messages and optionally send a pong response.

        When a ping message is received, this method processes the message and sends
        a pong response back to the sender if requested. The method updates the edge's
        latency measurements based on the round trip time of the ping-pong exchange if
        the latency_update flag in the message is set to True. It also sets the edge's
        name, host, port, and subscriptions based on the information received in the
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
            "host": self.host,
            "port": self.port,
            "suscriptions": self.suscriptions,
            "ping_id": message.data["ping_id"],
            "response": message.data["response"],
            "latency_update": message.data["latency_update"],
            "dummy_data": message.data["dummy_data"],
        }

        if message.data["response"]:
            await self._ping(edge, delay=0.1,
                             latency_update=message.data["latency_update"],
                             size=message.data["size"],
                             )
        await self._write(command="pong", data=data, writer=edge.writer)

    # ----------------------------------------------------------------------
    async def _process_pong(self, message: Message, edge: Edge) -> None:
        """
        Process a pong message and update edge latency measurements.

        This coroutine is triggered when a pong message is received in response to a ping request. It uses the time difference between the pong message's timestamp and the current time to calculate the round-trip latency. If the 'latency_update' flag in the message data is True, this latency value will be used to update the edge's latency statistics. Additionally, the edge's identifying information such as name, host, and port is updated based on the pong message data.

        Parameters
        ----------
        message : Message
            The incoming pong message containing the original timestamp, sender's name,
            host, port, and subscription information, as well as a unique identifier
            for the ping event.
        edge : Edge
            The edge object representing the connection to the sender of the pong message.
        """
        server_edge = self.ping_events.pop(message.data["ping_id"])
        if message.data["latency_update"]:
            server_edge.update_latency(
                (datetime.now() - message.data["source_timestamp"]).total_seconds() * 500
            )
        server_edge.name = message.data["name"]
        server_edge.host = message.data["host"]
        server_edge.port = message.data["port"]
        server_edge.subscriptions = message.data["suscriptions"]

        await asyncio.sleep(0)

    # ----------------------------------------------------------------------
    async def _process_discovery(self, message: Message, edge: Optional[Edge] = None) -> None:
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
        status = await self._request_status(
            message.data["origin_host"],
            message.data["origin_port"],
        )
        if status.data["paired"]:
            logger_main.debug(f"{self.name}: Node is already paired with another branch.")
            return

        if message.data["ttl"] == 0:
            logger_main.debug(f"{self.name}: Discovery time-to-live (TTL) reached 0.")
            return

        if message.data["origin_suscriptions"].intersection(self.suscriptions):
            await self.connect_to_peer(
                message.data["origin_host"],
                message.data["origin_port"],
                paired=True,
                data=message.data,
            )
        else:
            new_data = message.data.copy()
            new_data["previous_node"] = self.name
            new_data["ttl"] = message.data["ttl"] - 1

            await self.wait_fo_ready()
            if self.name in message.data['visited']:
                logger_main.debug(f"{self.name}: This branch has already been visited: {message.data['visited']}.")
                return

            new_data["visited"].add(self.name)

            for server_edge in self.server_pairs:
                if not server_edge.name in [
                    message.data["previous_node"],
                    message.data["origin_name"],
                ]:
                    await self._write(
                        command="discovery",
                        data=new_data,
                        writer=server_edge.writer,
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
        async with self.lock:
            self.server_pairs = [
                edge for edge in self.server_pairs if not edge.writer.is_closing()
            ]
        logger_main.debug(f"{self.name}: Removed a closing connection.")

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
        loop = asyncio.get_running_loop()

        addr_info = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socket.SOCK_DGRAM)
        if not addr_info:
            raise ValueError(f"Cannot resolve address: {self.host}")
        family, socktype, proto, canonname, sockaddr = addr_info[0]
        sock = socket.socket(family, socktype, proto)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((self.host, self.port))
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self, self._process_udp_message), sock=sock
        )
        self.udp_transport = transport
        self.request_response_multiplexer = {}
        self.request_response_multiplexer_events = {}

    # ----------------------------------------------------------------------
    async def _send_udp_message(self, command: str, message: Any, dest_host: str, dest_port: int) -> None:
        """
        Send a UDP message to the specified destination host and port.

        This coroutine sends a pre-formatted message over UDP to a given destination host and port. It serializes the
        message, which includes a command and its associated data, before transmission. This method is utilized for
        communication protocols that require UDP for message passing, like status checks or discovery procedures.

        Parameters
        ----------
        command : str
            The command type that dictates the kind of operation to perform, included in the message.
        message : Any
            The payload associated with the command that contains data necessary for carrying out the operation.
        dest_host : str
            The destination host's IP address or hostname to which the message will be sent.
        dest_port : int
            The port number on the destination host to which the message should be directed.
        """
        message = Message(command=command, data=message, timestamp=datetime.now())
        data = self.serializer(message)
        self.udp_transport.sendto(data, (dest_host, dest_port))

    # ----------------------------------------------------------------------
    async def _process_udp_message(self, data: bytes, addr: Tuple[str, int]) -> None:
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
        if message.command == "status":
            data = {
                "id": message.data["id"],
                "paired": self.paired_event.is_set(),
                "serving": not self.server_closing,
            }
            await self._send_udp_message("response", data, *addr[:2])

        elif message.command == "response":
            self.request_response_multiplexer[message.data["id"]] = message

            if message.data["id"] in self.request_response_multiplexer_events:
                self.request_response_multiplexer_events[message.data["id"]].set()

        # elif message.command == "discovery":
        # await self._process_discovery(message)

    # ----------------------------------------------------------------------
    async def _request_status(self, dest_host: str, dest_port: int) -> Message:
        """
        Request the status of a node via UDP and wait for a response.

        This asynchronous method sends a UDP message to the target host and port,
        requesting its status. It generates a unique identifier for the request, sends
        the message, and then waits for a response that matches the identifier. Once
        the response is received, it is returned as a Message object.

        Parameters
        ----------
        dest_host : str
            The hostname or IP address of the destination node to query for status.
        dest_port : int
            The port number of the destination node to communicate the status request.

        Returns
        -------
        Message
            The status response message from the destination node, containing information
            such as whether it is paired and actively serving.
        """
        id_ = self._gen_id()
        self.request_response_multiplexer_events[id_] = asyncio.Event()
        data = {"id": id_}
        await self._send_udp_message("status", data, dest_host, dest_port)
        await self.request_response_multiplexer_events[id_].wait()
        status = self.request_response_multiplexer[id_]
        self.request_response_multiplexer_events.pop(id_)
        self.request_response_multiplexer.pop(id_)
        return status

    # ----------------------------------------------------------------------
    def _gen_id(self, size: int = 32) -> str:
        """
        Generate a unique identifier string.

        This method produces a random string composed of ASCII letters. It is used where a unique ID is required,
        such as in identifying messages in a network protocol. The default length of the generated identifier is 32
        characters, but it can be customized by specifying a different size.

        Parameters
        ----------
        size : int, optional
            The number of characters in the generated identifier. The default size is 32 characters.

        Returns
        -------
        str
            A randomly generated identifier string of length `size`.
        """
        return "".join([random.choice(ascii_letters) for _ in range(size)])

    # ----------------------------------------------------------------------
    async def _keep_alive(self, interval: int = 7) -> None:
        """"""
        return
        # while True:
            # try:
                # logging.debug("Performing keep-alive check")
                # for edge in self.server_pairs:
                    # await self._ping(edge)
                    # logging.debug(f"{self.name}: Ping to {edge.name}")

                # if self.paired_event.is_set():
                    # self.paired_event.clear()
                    # logging.debug(f"{self.name}: Removing connection")
                    # await self.close_connection(self.server_pairs[0])

                # # await self.remove_duplicated_connections()

            # except Exception as e:
                # logging.error(f"{self.name}: Error during keep-alive check: {e}")

            # await asyncio.sleep(interval)

    # ----------------------------------------------------------------------
    async def remove_duplicated_connections(self) -> None:
        """"""
        seen_connections = set()
        for edge in self.server_pairs:

            if not (edge.host and edge.port):
                continue

            connection = (edge.host, edge.port)
            if connection not in seen_connections:
                seen_connections.add(connection)
            else:
                await self.close_connection(edge)
                logger_main.debug(f"{self.name}: Closed a duplicate connection to {connection}.")

    # ----------------------------------------------------------------------
    def ready(self) -> bool:
        """"""
        all_ready = all(edge.host and edge.port and edge.name for edge in self.server_pairs)
        paired = self.paired_event.is_set()

        return (all_ready and paired)

    # ----------------------------------------------------------------------
    def is_connected_to(self, node: 'ChaskiNode') -> bool:
        """"""
        return (node.host, node.port) in [(edge.host, edge.port) for edge in self.server_pairs]

