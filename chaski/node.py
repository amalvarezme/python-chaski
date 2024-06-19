import pickle
import random
import socket
import asyncio
import logging
import traceback
from datetime import datetime
from string import ascii_letters
from dataclasses import dataclass, field
from collections import namedtuple
from functools import cached_property


@dataclass
########################################################################
class Edge:
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    latency: float = 0
    jitter: float = 0
    name: str = ""
    host: str = ""
    port: str = ""
    subscriptions: set = field(default_factory=set)
    ping_in_progress: bool = False

    # ----------------------------------------------------------------------
    def __repr__(self):
        """"""
        return f"{self.name}: N({self.latency: .0f}, {self.jitter: .0f}) {self.host}: {self.port}"

    # ----------------------------------------------------------------------
    @cached_property
    def address(self):
        """"""
        return self.writer.get_extra_info("peername")

    # ----------------------------------------------------------------------
    @cached_property
    def local_address(self):
        """"""
        return self.writer.get_extra_info("sockname")

    # ----------------------------------------------------------------------
    def update_latency(self, new_latency):
        """"""
        if self.jitter == 0:
            self.latency = new_latency
            self.jitter = 100
        else:
            prior_mean = self.latency
            prior_variance = self.jitter

            likelihood_mean = new_latency
            likelihood_variance = prior_variance
            posterior_mean = (
                prior_mean / prior_variance + likelihood_mean / likelihood_variance
            ) / (1 / prior_variance + 1 / likelihood_variance)
            posterior_variance = 1 / (1 / prior_variance + 1 / likelihood_variance)

            self.latency = posterior_mean
            self.jitter = posterior_variance


Message = namedtuple("Message", "command data timestamp")


########################################################################
class UDPProtocol(asyncio.DatagramProtocol):

    # ----------------------------------------------------------------------
    def __init__(self, node, on_message_received):
        """"""
        self.node = node
        self.on_message_received = on_message_received

    # ----------------------------------------------------------------------
    def datagram_received(self, message, addr):
        """"""
        asyncio.create_task(self.on_message_received(message, addr))

    # ----------------------------------------------------------------------
    def error_received(self, exc):
        """"""
        logging.error(f"UDP error received: {exc}")

    # ----------------------------------------------------------------------
    def connection_lost(self, exc):
        """"""
        logging.info(f"UDP connection closed: {exc}")


########################################################################
class ChaskiNode:

    # ----------------------------------------------------------------------
    def __init__(
        self,
        host,
        port,
        serializer=pickle.dumps,
        deserializer=pickle.loads,
        name=None,
        subscriptions=[],
        run=False,
        ttl=64,
        root=False,
    ):
        """"""
        self.host = host
        self.port = port
        self.serializer = serializer
        self.deserializer = deserializer
        self.server = None
        self.ttl = ttl
        self.suscriptions = set(subscriptions)
        self.name = f"{name}: {self.suscriptions}"
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
    def __repr__(self):
        """"""
        return f"{{{self.name}: {self.port}}}"

    # ----------------------------------------------------------------------
    async def run(self):
        """"""
        self.server_closing = False
        await asyncio.gather(
            self._start_tcp_server(),
            self._start_udp_server()
        )

    # ----------------------------------------------------------------------
    async def stop(self):
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
                await asyncio.wait_for(self.server.wait_closed(), timeout=5.0)
            except asyncio.TimeoutError:
                logging.warning("Timeout waiting for server to close")

    # ----------------------------------------------------------------------
    async def connect_to_peer(
        self,
        node,
        peer_port=None,
        paired=False,
        data={}
    ):
        """"""
        if hasattr(node, "host"):
            peer_host, peer_port = node.host, node.port
        else:
            peer_host, peer_port = node, peer_port

        if self.host == peer_host and self.port == peer_port:
            logging.warning(f"{self.name}: Impossible to connect to the same node")
            return

        await self.wait_for_all_edges_ready()

        if (peer_host, peer_port, False) in [(edge.host, edge.port, edge.writer.is_closing()) for edge in self.server_pairs]:
            logging.warning(f"{self.name}: Already connected with this node")
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

        logging.debug(f"{self.name}: New connection with {edge.address}")
        asyncio.create_task(self._reader_loop(edge))
        await self._ping(edge)

    # ----------------------------------------------------------------------
    async def wait_for_all_edges_ready(self):
        if not self.server_pairs:
            return
        while True:
            all_ready = all(edge.host and edge.port and edge.name for edge in self.server_pairs)
            if all_ready:
                break
            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    async def wait_fo_ready(self):
        while True:
            if self.ready():
                break
            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    async def discovery(self, node=None, on_pair='none', timeout=10):
        """"""
        if not self.server_pairs:
            logging.warning(f"{self.name}: No connection to perform discovery.")
            return

        for edge in self.server_pairs:
            if edge.subscriptions.intersection(self.suscriptions):
                logging.warning(f"{self.name}: Already paired.")
                self.paired_event.set()
                return

        if (node is None) and (len(self.server_pairs) == 0):
            logging.warning(f"{self.name}: Impossible to discover new nodes, no 'Node' or 'Edge' available.")
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
        logging.warning(f"{self.name}: Timeout!! then paired")
        self.paired_event.set()

    # ----------------------------------------------------------------------
    async def close_connection(self, edge, port=None):
        """"""
        await self.wait_for_all_edges_ready()

        if port:
            for edge_ in self.server_pairs:
                if (edge_.host == edge) and (edge_.port == port):
                    edge = edge_
                    break

        if not isinstance(edge, Edge):
            logging.warning(f"{self.name}: {edge} is not a valid edge")
            return
        logging.debug(f"{self.name}: conection with {edge} will be removed")

        logging.debug(f"{self.name}: Closing connection to {edge}")
        if not edge.writer.is_closing():
            edge.writer.close()
            try:
                await asyncio.wait_for(edge.writer.wait_closed(), 1)
            except asyncio.TimeoutError:
                logging.debug(f"{self.name}: Timeout closing connection to {edge}")

        async with self.lock:
            self.server_pairs = [
                edge_ for edge_ in self.server_pairs if edge_ != edge
            ]

        if len(self.server_pairs) == 0 and not self.server_closing:
            logging.warning(f"{self.name}: Orphan node!!")
            logging.warning(f"{self.name}: Retring conection")

            status = await self._request_status(edge.host, edge.port)
            if status.data['serving']:
                await self.connect_to_peer(edge)

        logging.debug(f"{self.name}: Connection to {edge} closed and removed")

    # ----------------------------------------------------------------------
    async def connected(self, reader, writer):
        """"""
        edge = Edge(writer=writer, reader=reader)

        logging.warning(f"{self.name}: Accepted connection from {writer.get_extra_info('peername')}")
        logging.warning(f"{self.name}: X New connection with {edge.address}: {edge.host}")
        asyncio.create_task(self._reader_loop(edge))

        await self.wait_for_all_edges_ready()

        if (edge.host, edge.port, False) in [(edge_.host, edge_.port, edge_.writer.is_closing()) for edge_ in self.server_pairs]:
            logging.warning(f"{self.name}: X Already connected with this node")
            await self.close_connection(edge)
            return

        async with self.lock:
            self.server_pairs.append(edge)

    # ----------------------------------------------------------------------
    async def _reader_loop(self, edge):
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
                await self._process_message(message, edge)

        except ConnectionResetError as e:
            logging.warning(
                f"{self.name}: Connection reset by peer {edge.address}: {e}"
            )
            # logging.error(traceback.format_exc())
        except asyncio.IncompleteReadError:
            logging.warning(
                f"{self.name}: Connection closed while reading from {edge.address}"
            )
            # logging.error(traceback.format_exc())
        except Exception as e:
            logging.warning(
                f"{self.name}: Error in reader_loop for {edge.address}: {e}"
            )
            # logging.error(traceback.format_exc())
        finally:
            logging.warning(f"{self.name}: Clossing conection with {edge}")
            # logging.error(traceback.format_exc())
            await self.close_connection(edge)

    # ----------------------------------------------------------------------

    async def _process_message(self, message, edge):
        """"""
        if processor := getattr(self, f"_process_{message.command}", None):
            logging.warning(f"{self.name}: Processing {message.command}")
            await processor(message, edge)
        else:
            logging.warning(f"{self.name}: No processor for {message.command}")

    # ----------------------------------------------------------------------
    async def _process_report_paired(self, message, edge):
        """"""
        if message.data["paired"]:

            match message.data['on_pair']:
                case 'none':
                    pass
                case 'disconnect':
                    logging.warning(f"{self.name}: Diconnected after pair {message.data['root_host']} {message.data['root_port']}")
                    # await asyncio.sleep(3)
                    await self.close_connection(message.data['root_host'], message.data['root_port'])

            logging.warning(f"{self.name}: Paired!!")
            self.paired_event.set()

    # ----------------------------------------------------------------------
    async def _start_tcp_server(self):
        """"""
        self.server = await asyncio.start_server(
            self.connected,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True,
        )
        addr = self.server.sockets[0].getsockname()
        logging.debug(f"{self.name}: Serving on {addr}")
        self._keep_alive_task = asyncio.create_task(self._keep_alive())

        async with self.server:
            await self.server.serve_forever()

    # ----------------------------------------------------------------------
    async def _write(self, command, data, writer=None):
        """"""
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
                        logging.warning(
                            f"{self.name}: Connection lost while writing to {server_edge.name}"
                        )
                        await self.close_connection(server_edge)
        else:
            writer.write(data)
            try:
                await writer.drain()
            except ConnectionResetError:
                logging.warning(
                    f"{self.name}: Connection lost while writing to {writer.get_extra_info('peername')}"
                )
                await self._remove_closing_connection()

    # ----------------------------------------------------------------------
    async def _ping(self, server_edge, delay=0, data={'ttl': 0}):
        """"""
        await asyncio.sleep(delay)
        id_ = self._gen_id()
        self.ping_events[id_] = server_edge

        data['ping_id'] = id_
        await self._write(
            command="ping",
            data=data,
            writer=server_edge.writer,
        )

    # ----------------------------------------------------------------------
    async def _process_ping(self, message, edge):
        """"""
        data = {
            "source_timestamp": message.timestamp,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "suscriptions": self.suscriptions,
            "ping_id": message.data["ping_id"],
        }

        if message.data["ttl"] == 0:
            await self._ping(edge, delay=0.1, data={'ttl': message.data["ttl"] - 1, })

        await self._write(command="rping", data=data, writer=edge.writer)

    # ----------------------------------------------------------------------
    async def _process_rping(self, message, edge):
        """"""
        server_edge = self.ping_events.pop(message.data["ping_id"])
        server_edge.update_latency(
            (datetime.now() - message.data["source_timestamp"]).total_seconds() * 500
        )
        server_edge.name = message.data["name"]
        server_edge.host = message.data["host"]
        server_edge.port = message.data["port"]
        server_edge.subscriptions = message.data["suscriptions"]

        await asyncio.sleep(0)

    # ----------------------------------------------------------------------
    async def _process_discovery(self, message, edge=None):
        """"""
        status = await self._request_status(
            message.data["origin_host"],
            message.data["origin_port"],
        )
        if status.data["paired"]:
            logging.warning(f"{self.name}: Already paired in other branch.")
            return

        if message.data["ttl"] == 0:
            logging.warning(f"{self.name}: Discovery TTL to 0")
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
                logging.warning(f"{self.name}: Branch already visited: {message.data['visited']}")
                return

            new_data["visited"].add(self.name)
            logging.warning(f"{self.name}: NO visited {message.data['visited']}")

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
    async def _remove_closing_connection(self):
        """"""
        async with self.lock:
            self.server_pairs = [
                edge for edge in self.server_pairs if not edge.writer.is_closing()
            ]
        logging.debug(f"{self.name}: Removed closing connection")

    # ----------------------------------------------------------------------
    async def _start_udp_server(self):
        """"""
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
    async def _send_udp_message(self, command, message, dest_host, dest_port):
        """"""
        message = Message(command=command, data=message, timestamp=datetime.now())
        data = self.serializer(message)
        self.udp_transport.sendto(data, (dest_host, dest_port))

    # ----------------------------------------------------------------------
    async def _process_udp_message(self, data, addr):
        """"""
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
            self.request_response_multiplexer_events[message.data["id"]].set()

        # elif message.command == "discovery":
        # await self._process_discovery(message)

    # ----------------------------------------------------------------------
    async def _request_status(self, dest_host, dest_port):
        """"""
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
    def _gen_id(self, size=32):
        """"""
        return "".join([random.choice(ascii_letters) for _ in range(size)])

    # ----------------------------------------------------------------------
    async def _keep_alive(self, interval=7):
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
    async def remove_duplicated_connections(self):
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
                logging.warning(f"{self.name}: Closed duplicate connection to {connection}")

    # ----------------------------------------------------------------------
    def ready(self):
        """"""
        all_ready = all(edge.host and edge.port and edge.name for edge in self.server_pairs)
        paired = self.paired_event.is_set()

        return (all_ready and paired)

    # ----------------------------------------------------------------------
    def is_connected_to(self, node):
        """"""
        return (node.host, node.port) in [(edge.host, edge.port) for edge in self.server_pairs]




