import asyncio
from asyncio import DatagramProtocol
from datetime import datetime, timedelta
import logging
import pickle

from string import ascii_letters
import random
from dataclasses import dataclass
from collections import namedtuple
import socket


# header = '@'
# command = '$'
# separator = '#'


@dataclass
class Edge:
    """"""

    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    latency: float = 0
    name: str = ""

    # ----------------------------------------------------------------------
    def __repr__(self):
        """"""
        return f"{self.name}: {self.latency}"

    # ----------------------------------------------------------------------

    def write(self, data):
        """"""
        return self.writer.write(data)

    # ----------------------------------------------------------------------
    def read(self, buffer):
        """"""
        return self.reader.read(buffer)


Message = namedtuple("Message", "command data timestamp")


########################################################################
class UDPProtocol(DatagramProtocol):
    def __init__(self, node, on_message_received):
        self.node = node
        self.on_message_received = on_message_received

    def datagram_received(self, message, addr):
        asyncio.create_task(self.on_message_received(message, addr))

    def error_received(self, exc):
        logging.error(f"UDP error received: {exc}")

    def connection_lost(self, exc):
        logging.info("UDP connection closed")


########################################################################
class ChaskiNode:

    # ----------------------------------------------------------------------
    def __init__(
        self,
        host,
        port,
        serializer=pickle.dumps,
        deserializer=pickle.loads,
        buffer=1024,
        name=None,
        suscriptions=[],
    ):
        """"""
        self.host = host
        self.port = port
        self.serializer = serializer
        self.deserializer = deserializer
        self.buffer = buffer
        self.server = None
        self._reader_loop = True
        self.suscriptions = set(suscriptions)

        self.server_pairs = []
        self.client_pairs = []

        self.paired = False

        self.name = f"{name}: {self.suscriptions}"

        self.server_read_event = asyncio.Event()
        self.server_read_event.set()

    # ----------------------------------------------------------------------

    async def connected(self, client_reader, client_writer):
        """"""
        addr = client_writer.get_extra_info("peername")
        self.client_pairs.append(Edge(writer=client_writer, reader=client_reader))
        await self.reader_loop(client_reader, client_writer, addr)

    # ----------------------------------------------------------------------
    async def reader_loop(self, client_reader, client_writer, addr):
        try:
            while True:
                await self.server_read_event.wait()
                data = await client_reader.read(self.buffer)
                if not data:
                    raise ConnectionResetError("Connection closed by peer")
                message = self.deserializer(data)
                await self.process_message(message, client_writer)
        except Exception as e:
            logging.error(f"ReaderLoop: Error in reader_loop for {addr}: {e}")
        finally:
            logging.warning(f"ReaderLoop: Closing connection to {addr}")
            client_writer.close()
            await client_writer.wait_closed()
            self.remove_dead_connection(client_writer)

    # ----------------------------------------------------------------------
    def pause_reading(self):
        self.server_read_event.clear()

    # ----------------------------------------------------------------------
    def resume_reading(self):
        self.server_read_event.set()

    # ----------------------------------------------------------------------
    async def process_message(self, message, client_writer):
        """"""
        if processor := getattr(self, f"process_{message.command}", None):
            logging.warning(f"Processing {message.command}")
            await processor(message, client_writer)
        else:
            logging.warning(f"No processor for {message.command}")

    # ----------------------------------------------------------------------
    async def process_graph(self, message, client_writer):
        """"""
        data = {"name": self.name}
        await self.write(command="rgraph", data=data, writer=client_writer)

    # ----------------------------------------------------------------------
    async def process_connect_back(self, message, client_writer):
        """"""
        client_host, client_port = message.data["host"], message.data["port"]

        if message.data['paired']:
            logging.warn(f'{self.name}: Paired! removing {self.client_pairs}')
            await self.close_connection(self.client_pairs[0])

        await self.connect_to_peer(
            client_host,
            client_port,
            back_connect=False,
        )

    # ----------------------------------------------------------------------
    async def start_server(self):
        """"""
        self.server = await asyncio.start_server(
            self.connected,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True,
        )
        addr = self.server.sockets[0].getsockname()
        logging.debug(f"Serving on {addr}")
        async with self.server:
            await self.server.serve_forever()

    # ----------------------------------------------------------------------
    async def write(self, command, data, writer=None):
        message = Message(command=command, data=data, timestamp=datetime.now())
        data = self.serializer(message)
        if writer is None:
            for server_edge in self.server_pairs:
                if not server_edge.writer.is_closing():
                    server_edge.writer.write(data)
                    try:
                        await server_edge.writer.drain()
                    except ConnectionResetError:
                        logging.error(
                            f"Connection lost while writing to {
                                server_edge.name}"
                        )
                        self.remove_dead_connection(server_edge.writer)
        else:
            writer.write(data)
            try:
                await writer.drain()
            except ConnectionResetError:
                logging.error(
                    f"Connection lost while writing to {
                        writer.get_extra_info('peername')}"
                )
                self.remove_dead_connection(writer)

    # ----------------------------------------------------------------------
    async def connect_to_peer(
        self,
        node,
        peer_port=None,
        back_connect=False,
        paired=False,
    ):
        """"""
        if hasattr(node, "host"):
            peer_host, peer_port = node.host, node.port
        else:
            peer_host, peer_port = node, peer_port

        reader, writer = await asyncio.open_connection(peer_host, peer_port)

        edge = Edge(writer=writer, reader=reader)
        self.server_pairs.append(edge)

        if back_connect:
            data = {
                "host": self.host,
                "port": self.port,
                'paired': paired,
            }
            await self.write(command="connect_back", data=data, writer=writer)

        await asyncio.sleep(1)
        logging.warning(
            f"{self.name}: Ping {self.host}:{self.port} -> {peer_host}:{peer_port}"
        )
        await self.ping(edge)

    # ----------------------------------------------------------------------
    async def ping(self, server_edge):
        """"""
        await self.write(command="ping", data={}, writer=server_edge.writer)
        try:
            response = await asyncio.wait_for(server_edge.read(self.buffer), 5)

            if not response:
                logging.warning(f"No RPing!!")
                self.remove_dead_connection(server_edge.writer)
                return

            response = self.deserializer(response)
            server_edge.latency = (
                datetime.now() - response.data["source_timestamp"]
            ).total_seconds() * 500
            server_edge.name = response.data["name"]

        except asyncio.TimeoutError:
            logging.warning(f"Ping to {server_edge} timed out")
            self.remove_dead_connection(server_edge.writer)

    # ----------------------------------------------------------------------
    async def process_ping(self, message, client_writer):
        """"""
        data = {
            "source_timestamp": message.timestamp,
            "name": self.name,
        }
        await self.write(command="rping", data=data, writer=client_writer)

    # ----------------------------------------------------------------------
    async def graph(self):
        """"""
        for server_edge in self.server_pairs:
            await self.write(command="graph", data={}, writer=server_edge.writer)
            response = await asyncio.wait_for(server_edge.reader.read(self.buffer), 1)
            response = self.deserializer(response)

    # ----------------------------------------------------------------------
    async def forward(self):
        """"""
        data = {
            "origin_host": self.host,
            "origin_port": self.port,
            "origin_name": self.name,
            "previous_node": self.name,
            "origin_suscriptions": self.suscriptions,
        }

        for server_edge in self.server_pairs:
            # if self.paired:
            #     return
            await self.write(command="forward", data=data, writer=server_edge.writer)

    # ----------------------------------------------------------------------
    async def process_forward(self, message, client_writer):
        """"""
        status = await self.request_status(
            message.data["origin_host"],
            message.data["origin_port"],
        )
        if status.data['paired']:
            return

        if message.data["origin_suscriptions"].intersection(self.suscriptions):

            await self.connect_to_peer(
                message.data["origin_host"],
                message.data["origin_port"],
                back_connect=True,
                paired=True,
            )

        else:

            for server_edge in self.server_pairs:

                if server_edge.name != message.data["previous_node"]:
                    new_data = message.data.copy()
                    new_data["previous_node"] = self.name
                    await self.write(
                        command="forward",
                        data=new_data,
                        writer=server_edge.writer,
                    )

    # ----------------------------------------------------------------------
    def __eq__(self, node):
        """"""
        return (node.host == self.host) and (node.port == self.port)

    # ----------------------------------------------------------------------
    def __gt__(self, node):
        """"""
        return node.port > self.port

    # ----------------------------------------------------------------------
    def __lt__(self, node):
        """"""
        return node.port < self.port

    # ----------------------------------------------------------------------
    def __hash__(self):
        """"""
        return hash((self.host, self.port))

    # ----------------------------------------------------------------------
    def __repr__(self):
        """"""
        if self.name:
            return f"{self.name}: {self.port}"
        else:
            return f"CSK{{{self.port}}}"

    # ----------------------------------------------------------------------
    def remove_dead_connection(self, writer):
        self.server_pairs = [
            edge for edge in self.server_pairs if edge.writer != writer
        ]
        self.client_pairs = [
            edge for edge in self.client_pairs if edge.writer != writer
        ]
        # logging.warning(
        #     f"Removed dead connection {
        #         writer.get_extra_info('peername')}"
        # )

    # # ----------------------------------------------------------------------
    # async def close_connection(self, edge):
    #     """Close a specific connection and remove it from pairs."""
    #     logging.warning(f"Closing connection to {edge}")
    #     edge.writer.close()
    #     await asyncio.wait_for(edge.writer.wait_closed(), 1)
    #     self.remove_dead_connection(edge.writer)
    #     logging.warning(f"Connection to {edge} closed and removed")

    # ----------------------------------------------------------------------
    async def close_connection(self, edge):
        """Close a specific connection and remove it from pairs."""
        logging.warning(f"Closing connection to {edge}")
        if not edge.writer.is_closing():
            edge.writer.close()
            try:
                await asyncio.wait_for(edge.writer.wait_closed(), 1)
            except asyncio.TimeoutError:
                logging.warning(f"Timeout closing connection to {edge}")
        self.remove_dead_connection(edge.writer)
        logging.warning(f"Connection to {edge} closed and removed")

    # ----------------------------------------------------------------------
    async def start_udp_server(self):
        """Start a UDP server to listen for incoming messages."""
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((self.host, self.port))

        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self, self.process_udp_message), sock=sock
        )
        self.udp_transport = transport
        self.request_response_multiplexer = {}
        self.request_response_multiplexer_events = {}

    # ----------------------------------------------------------------------
    async def send_udp_message(self, command, message, dest_host, dest_port):
        """Send a UDP message to another node."""
        message = Message(command=command, data=message, timestamp=datetime.now())
        data = self.serializer(message)
        self.udp_transport.sendto(data, (dest_host, dest_port))

    # ----------------------------------------------------------------------
    async def process_udp_message(self, data, addr):
        """Process incoming UDP message."""
        message = self.deserializer(data)
        if message.command == 'request':
            data = {
                'id': message.data["id"],
                'paired': self.paired,
            }
            await self.send_udp_message('response', data, *addr)

        elif message.command == 'response':
            self.request_response_multiplexer[message.data["id"]] = message
            self.request_response_multiplexer_events[message.data["id"]].set()

    # ----------------------------------------------------------------------
    async def request_status(self, dest_host, dest_port):
        """"""
        id_ = self.gen_id()
        self.request_response_multiplexer_events[id_] = asyncio.Event()
        data = {
            'id': id_,
        }
        await self.send_udp_message('request', data, dest_host, dest_port)
        await self.request_response_multiplexer_events[id_].wait()
        return self.request_response_multiplexer[id_]

    # ----------------------------------------------------------------------
    def gen_id(self, size=32):
        """"""
        return ''.join([random.choice(ascii_letters) for _ in range(size)])
