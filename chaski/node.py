import asyncio
from asyncio import DatagramProtocol
from datetime import datetime
import logging
import pickle
from string import ascii_letters
import random
from dataclasses import dataclass
from collections import namedtuple
import socket


@dataclass
########################################################################
class Edge:
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    latency: float = 0
    name: str = ""
    lock: asyncio.Lock = asyncio.Lock()
    ping_in_progress: bool = False

    # ----------------------------------------------------------------------
    def __repr__(self):
        return f"{self.name}: {self.latency}"

    # ----------------------------------------------------------------------
    def write(self, data):
        return self.writer.write(data)

    # ----------------------------------------------------------------------
    async def read(self, buffer):
        async with self.lock:
            return await self.reader.read(buffer)


Message = namedtuple("Message", "command data timestamp")


########################################################################
class UDPProtocol(DatagramProtocol):

    # ----------------------------------------------------------------------
    def __init__(self, node, on_message_received):
        self.node = node
        self.on_message_received = on_message_received

    # ----------------------------------------------------------------------
    def datagram_received(self, message, addr):
        asyncio.create_task(self.on_message_received(message, addr))

    # ----------------------------------------------------------------------
    def error_received(self, exc):
        logging.error(f"UDP error received: {exc}")

    # ----------------------------------------------------------------------
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

        self.name = f"{name}: {self.suscriptions}"

        self.server_read_event = asyncio.Event()
        self.server_read_event.set()

        self.paired_event = asyncio.Event()

        self.lock = asyncio.Lock()

        self.ping_events = {}

    # ----------------------------------------------------------------------
    async def connected(self, reader, writer):
        """"""
        async with self.lock:
            edge = Edge(writer=writer, reader=reader)
            self.server_pairs.append(edge)
        asyncio.create_task(self.reader_loop(edge))

    # ----------------------------------------------------------------------
    async def reader_loop(self, edge):
        """"""
        try:
            addr = edge.writer.get_extra_info("peername")
            while True:
                await self.server_read_event.wait()
                data = await edge.reader.read(self.buffer)
                if not data:
                    raise ConnectionResetError("Connection closed by peer")
                message = self.deserializer(data)
                await self.process_message(message, edge)
        except Exception as e:
            logging.error(f"{self.name}: ReaderLoop: Error in reader_loop for {addr}: {e}")

        finally:
            logging.warning(f"ReaderLoop: Closing connection to {addr}")
            await self.close_connection(edge)

    # ----------------------------------------------------------------------
    def pause_reading(self):
        """"""
        self.server_read_event.clear()

    # ----------------------------------------------------------------------
    def resume_reading(self):
        """"""
        self.server_read_event.set()

    # ----------------------------------------------------------------------
    async def process_message(self, message, edge):
        """"""
        if processor := getattr(self, f"process_{message.command}", None):
            logging.warning(f"{self.name}: Processing {message.command}")
            await processor(message, edge)
        else:
            logging.warning(f"No processor for {message.command}")

    # ----------------------------------------------------------------------
    async def process_report_paired(self, message, edge):
        """"""
        if message.data['paired']:
            self.paired_event.set()

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

        asyncio.create_task(self.keep_alive())

        async with self.server:
            await self.server.serve_forever()

    # ----------------------------------------------------------------------
    async def write(self, command, data, writer=None):
        """"""
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
                            f"Connection lost while writing to {server_edge.name}"
                        )
                        await self.close_connection(server_edge)

        else:
            writer.write(data)
            try:
                await writer.drain()
            except ConnectionResetError:
                logging.error(
                    f"Connection lost while writing to {writer.get_extra_info('peername')}"
                )
                self.remove_closing_connection()

            except Exception as e:
                e

    # ----------------------------------------------------------------------
    async def connect_to_peer(
        self,
        node,
        peer_port=None,
        paired=False,
    ):
        """"""
        if hasattr(node, "host"):
            peer_host, peer_port = node.host, node.port
        else:
            peer_host, peer_port = node, peer_port

        reader, writer = await asyncio.open_connection(peer_host, peer_port)

        edge = Edge(writer=writer, reader=reader)

        async with self.lock:
            self.server_pairs.append(edge)

        if paired:
            await self.write(command="report_paired",
                             data={
                                 'paired': paired,
                             },
                             writer=writer)

        logging.warning(
            f"{self.name}: Ping {self.host}: {self.port} -> {peer_host}: {peer_port}"
        )

        asyncio.create_task(self.reader_loop(edge))
        await self.ping(edge)

    # ----------------------------------------------------------------------
    async def ping(self, server_edge, timeout=5):
        """"""
        id_ = self.gen_id()
        self.ping_events[id_] = server_edge
        await self.write(command="ping", data={'ping_id': id_, }, writer=server_edge.writer)

    # ----------------------------------------------------------------------
    async def process_ping(self, message, edge):
        """"""
        data = {
            "source_timestamp": message.timestamp,
            "name": self.name,
            'ping_id': message.data['ping_id'],
        }
        await self.write(command="rping", data=data, writer=edge.writer)

    # ----------------------------------------------------------------------
    async def process_rping(self, message, edge):
        """"""
        server_edge = self.ping_events[message.data['ping_id']]
        server_edge.latency = (
            datetime.now() - message.data["source_timestamp"]
        ).total_seconds() * 500
        server_edge.name = message.data["name"]
        logging.warning(f"Getting name: {server_edge.name}")

        await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    async def process_forward(self, message, edge=None):
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
        return f"{self.name}: {self.port}" if self.name else f"CSK{{{self.port}}}"

    # ----------------------------------------------------------------------
    async def remove_closing_connection(self):
        """"""
        async with self.lock:
            self.server_pairs = [
                edge for edge in self.server_pairs if not edge.writer.is_closing()
            ]
        logging.warning(f"Removed closing connection")

    # ----------------------------------------------------------------------
    async def close_connection(self, edge):
        """"""
        logging.warning(f"Closing connection to {edge}")
        if not edge.writer.is_closing():
            edge.writer.close()
            try:
                await asyncio.wait_for(edge.writer.wait_closed(), 1)
            except asyncio.TimeoutError:
                logging.warning(f"Timeout closing connection to {edge}")

        async with self.lock:
            self.server_pairs = [
                edge_ for edge_ in self.server_pairs if edge_.writer != edge.writer
            ]
        logging.warning(f"Connection to {edge} closed and removed")

    # ----------------------------------------------------------------------
    async def start_udp_server(self):
        """"""
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
        """"""
        message = Message(command=command, data=message, timestamp=datetime.now())
        data = self.serializer(message)
        self.udp_transport.sendto(data, (dest_host, dest_port))

    # ----------------------------------------------------------------------
    async def process_udp_message(self, data, addr):
        """"""
        message = self.deserializer(data)
        if message.command == 'request':
            data = {
                'id': message.data["id"],
                'paired': self.paired_event.is_set(),
            }
            await self.send_udp_message('response', data, *addr)

        elif message.command == 'response':
            self.request_response_multiplexer[message.data["id"]] = message
            self.request_response_multiplexer_events[message.data["id"]].set()

        elif message.command == 'forward':
            await self.process_forward(message)

    # ----------------------------------------------------------------------
    async def request_status(self, dest_host, dest_port):
        """"""
        id_ = self.gen_id()
        self.request_response_multiplexer_events[id_] = asyncio.Event()
        data = {'id': id_}
        await self.send_udp_message('request', data, dest_host, dest_port)
        await self.request_response_multiplexer_events[id_].wait()
        return self.request_response_multiplexer[id_]

    # ----------------------------------------------------------------------
    async def request_forward(self, node, peer_port=None):
        """"""
        if hasattr(node, "host"):
            dest_host, dest_port = node.host, node.port
        else:
            dest_host, dest_port = node, peer_port

        data = {
            "origin_host": self.host,
            "origin_port": self.port,
            "origin_name": self.name,
            "previous_node": self.name,
            "origin_suscriptions": self.suscriptions,
        }

        await self.send_udp_message('forward', data, dest_host, dest_port)

    # ----------------------------------------------------------------------
    def gen_id(self, size=32):
        """"""
        return ''.join([random.choice(ascii_letters) for _ in range(size)])

    # ----------------------------------------------------------------------
    async def keep_alive(self, interval=7):
        """"""

        while True:
            try:
                logging.warning("Performing keep-alive check")

                for edge in self.server_pairs:
                    await self.ping(edge)
                    logging.warning(f"{self.name} ping to {edge.name}")

                if self.paired_event.is_set():
                    self.paired_event.clear()
                    await self.close_connection(self.server_pairs[0])
                    logging.warning(f"self.paired_event.clear()")

            except Exception as e:
                logging.error(f"Error during keep-alive check: {e}")

            await asyncio.sleep(interval)
