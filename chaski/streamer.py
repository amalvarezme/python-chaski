"""
=========================================================================
ChaskiStreamer: Asynchronous Message Streaming with a Distributed Network
=========================================================================

The `ChaskiStreamer` module provides the functionality to stream messages
asynchronously within a distributed network environment. It leverages the
base class `ChaskiNode` and extends its capabilities to handle an internal
message queue for efficient and scalable message processing.

Classes
=======

    - *ChaskiStreamer*: Extends ChaskiNode to provide asynchronous message streaming capabilities.
"""

import os
import asyncio
import hashlib
from asyncio import Queue
from typing import Generator
from chaski.node import ChaskiNode


########################################################################
class ChaskiStreamer(ChaskiNode):
    """
    Stream messages with ChaskiStreamer.

    The ChaskiStreamer class inherits from ChaskiNode and provides an implementation
    to handle asynchronous message streaming within a distributed network. It sets up
    an internal message queue to manage incoming messages, processes these messages,
    and allows the asynchronous sending of messages to designated topics.
    """

    # ----------------------------------------------------------------------
    def __init__(
        self,
        destination_folder: str = '.',
        chunk_size: int = 8192,
        file_handling_callback: callable = None,
        allow_incoming_files: bool = False,
        *args: tuple,
        **kwargs: dict,
    ):
        """
        Initialize a new instance of ChaskiStreamer.

        Parameters
        ----------
        destination_folder : str, optional
            The folder where the processed files will be stored. Defaults to the current directory ('.').
        chunk_size : int, optional
            The size of the chunks in which the files will be processed. Defaults to 1024 bytes.
        file_handling_callback : callable, optional
            A callback function to handle file inputs. This function should accept `name`, `path`, and `hash` as arguments.
        allow_incoming_files : bool, optional
            Flag to enable or disable processing of incoming file chunks. Defaults to False.
        *args : tuple
            Additional positional arguments to pass to the superclass initializer.
        **kwargs : dict
            Additional keyword arguments to pass to the superclass initializer.
        """
        super().__init__(*args, **kwargs)
        self.message_queue = Queue()
        self.chunk_size = chunk_size
        self.destination_folder = destination_folder
        self.file_handling_callback = file_handling_callback
        self.allow_incoming_files = allow_incoming_files
        self.terminate_stream_flag = False

        self.enable_message_propagation()
        self.add_propagation_command('ChaskiMessage')

    # ----------------------------------------------------------------------
    def __repr__(self):
        """
        Provide a string representation of the ChaskiStreamer instance.

        This method returns a string that includes the class name and network information
        such as the IP address and port. If the instance is a root node, it prepends an
        asterisk (*) to the string.
        """
        h = '*' if self.paired else ''
        return h + self.address

    # ----------------------------------------------------------------------
    @property
    def address(self) -> str:
        """
        Get the address of the ChaskiStreamer instance.

        This property returns the address of the ChaskiStreamer in the format
        "ChaskiStreamer@ip:port".

        Returns
        -------
        str
            A string representation of the ChaskiStreamer address.
        """
        return f"ChaskiStreamer@{self.ip}:{self.port}"

    # ----------------------------------------------------------------------
    @classmethod
    def get_hash(cls, file: str, algorithm: str = 'sha256') -> str:
        """
        Compute the hash of a file using the specified algorithm.

        This method reads the file in chunks and computes the hash digest
        using the provided hashing algorithm. The default algorithm is SHA-256.

        Parameters
        ----------
        file : str
            The path to the file for which to compute the hash.
        algorithm : str, optional
            The hashing algorithm to use. Defaults to 'sha256'.
            Other common algorithms include 'md5', 'sha1', 'sha512', etc.

        Returns
        -------
        str
            The hexadecimal hash digest of the file.
        """
        hash_func = hashlib.new(algorithm)
        with open(file, 'rb') as f:
            while chunk := f.read(8192):  # Read the file in 8192 byte blocks
                hash_func.update(chunk)
        return hash_func.hexdigest()

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
            # Get the status of paired events for each subscription
            "paired": {
                sub: self.paired_event[sub].is_set()
                for sub in self.subscriptions
            },
            # Check if the server is closing; 'True' means it's still serving.
            "serving": not self.server_closing,
            # Check if the node's reconnecting event is currently set, implying it is trying to reconnect to a peer.
            "reconnecting": self.reconnecting.is_set(),
            "allow_incoming_files": self.allow_incoming_files,
            **kwargs,
        }

    # ----------------------------------------------------------------------
    async def __aenter__(self) -> Generator['Message', None, None]:
        """
        Enter the asynchronous context for streaming messages.

        This method is called when entering the asynchronous context using the `async with` statement.
        It returns the message stream generator which will yield messages asynchronously from the
        internal message queue.

        Returns
        -------
        Generator[Message, None, None]
            A generator that yields `Message` objects as they arrive in the message queue.
        """
        return self.message_stream()

    # ----------------------------------------------------------------------
    async def __aexit__(
        self,
        exception_type: type,
        exception_value: BaseException,
        exception_traceback: 'TracebackType',
    ) -> None:
        """
        Exit the runtime context related to this object and stop the streamer.

        This method is invoked to exit the asynchronous runtime context, typically
        used in conjunction with an asynchronous context manager. It ensures that
        any resources or operations related to this object are properly cleaned
        up and stopped.

        Parameters
        ----------
        exception_type : type, optional
            The exception type if an exception was raised, otherwise None.
        exception_value : BaseException, optional
            The exception instance if an exception was raised, otherwise None.
        exception_traceback : TracebackType, optional
            The traceback object if an exception was raised, otherwise None.

        Notes
        -----
        This method ensures that the streamer is stopped and any pending
        messages are handled gracefully. It is intended to be used within an
        asynchronous context that supports the asynchronous context manager
        protocol.
        """
        await self.stop()

    # ----------------------------------------------------------------------
    async def push(self, topic: str, data: bytes = None) -> None:
        """
        Write a message to the specified topic.

        This method allows the asynchronous sending of messages to a designated topic. The message data, if provided, is encapsulated in a `ChaskiMessage` and dispatched to the relevant subscribers within the network.

        Parameters
        ----------
        topic : str
            The topic to which the message is to be sent. Each message is delivered exclusively to the nodes subscribing to this topic.
        data : bytes, optional
            The byte-encoded data to be sent with the message. This could be any binary payload that subscribers are expected to process.
        """
        await self._write('ChaskiMessage', data=data, topic=topic)

    # ----------------------------------------------------------------------
    async def _process_ChaskiMessage(
        self, message: 'Message', edge: 'Edge'
    ) -> None:
        """
        Process an incoming Chaski message and place it onto the message queue.

        This method is responsible for handling Chaski messages received from the network.
        Upon receiving a message, it places the message into the internal message queue for
        further processing.

        Parameters
        ----------
        message : Message
            The Chaski message received that needs to be processed. It contains the command,
            data, and several other attributes.
        edge : Edge
            The network edge (connection) from which the message was received.

        Notes
        -----
        This method operates asynchronously to ensure non-blocking behavior.
        The received message is added to the internal message queue using the `put` method.
        Once placed in the queue, the message can be retrieved and processed by other
        components of the application.
        """
        await self.message_queue.put(message)

    # ----------------------------------------------------------------------
    def activate_file_transfer(self) -> None:
        """
        Enable the processing of incoming file chunks.

        This method sets the `allow_incoming_files` flag to `True`, allowing the
        `ChaskiStreamer` to process incoming file chunks. When enabled, the
        `ChaskiStreamer` can receive and handle file transfers as messages
        containing file chunks are received.
        """
        self.allow_incoming_files = True

    # ----------------------------------------------------------------------
    def deactivate_file_transfer(self) -> None:
        """
        Disable the processing of incoming file chunks.

        This method sets the `allow_incoming_files` flag to `False`, preventing the
        `ChaskiStreamer` from processing any incoming file chunks until re-enabled.
        """
        self.allow_incoming_files = False

    # ----------------------------------------------------------------------
    async def message_stream(self) -> Generator['Message', None, None]:
        """
        Asynchronously generate messages from the message queue.

        This coroutine listens for incoming messages on the internal message queue
        and yields each message as it arrives. This method is intended to be used
        within an asynchronous context, allowing the consumer to retrieve messages
        in a non-blocking manner.

        Yields
        ------
        Message
            A `Message` object retrieved from the message queue.

        Notes
        -----
        This method runs indefinitely until the message queue is exhausted or the
        coroutine is explicitly stopped. Ensure proper cancellation to avoid
        hanging coroutines.
        """
        while True:
            message = await self.message_queue.get()
            if self.terminate_stream_flag:
                break
            yield message
        await self.stop()
        self.terminate_stream_flag = True

    # ----------------------------------------------------------------------
    def terminate_stream(self) -> None:
        """
        Terminate the message streaming process.

        This method sets the `terminate_stream_flag` to `True`, signaling the
        `message_stream` coroutine to stop generating further messages. This
        is typically used to gracefully shut down the message streaming process.
        """
        self.terminate_stream_flag = True

    # ----------------------------------------------------------------------
    async def push_file(
        self,
        topic: str,
        file: 'IOBase',
        filename: str = None,
        data: dict = {},
    ):
        """
        Asynchronously sends a file chunk by chunk to the specified topic.

        This method reads the file in chunks and sends each chunk as a separate message
        to the specified topic. The file's metadata, including filename and hash,
        can also be sent along with the chunks. This allows for efficient and scalable
        file transfer in a distributed network.

        Parameters
        ----------
        topic : str
            The topic to which the file chunks are to be sent. Nodes subscribing to this
            topic will receive the file chunks.
        file : IOBase
            A file-like object (must support `read` method). The file from which the data is read
            and sent in chunks.
        filename : str, optional
            The name of the file being sent. If not provided, the name attribute of the file object is used.

        Notes
        -----
        This method uses asynchronous I/O to read the file in chunks and send each chunk
        without blocking the event loop. It ensures that the entire file is processed and sent
        even if the process involves multiple chunks.
        """
        size = 0
        # Initialize a SHA-256 hash function for computing the hash digest of the file chunks
        hash_func = hashlib.new('sha256')
        while True:
            # Read the next chunk of data from the file up to the specified chunk size
            chunk = file.read(self.chunk_size)
            # Increment the size by the length of the current chunk
            size += len(chunk)
            # Update the hash function with the current chunk of data.
            hash_func.update(chunk)
            # Package the chunked file data along with metadata such as filename, hash, and chunk size
            package_data = {
                'filename': (
                    filename if filename else os.path.split(file.name)[-1]
                ),
                'chunk': chunk,
                'hash': hash_func.hexdigest(),
                'data': data,
                'chunk_size': self.chunk_size,
                'size': size,
            }

            # Send the chunked file data as a message to the specified topic and yield control to the event loop
            await self._write('ChaskiFile', data=package_data, topic=topic)
            await asyncio.sleep(0)  # very important sleep

            # If no more chunks are available to read, the file transfer is complete
            if not chunk:
                break

    # ----------------------------------------------------------------------
    async def _process_ChaskiFile(
        self, message: 'Message', edge: 'Edge'
    ) -> None:
        """
        Process an incoming ChaskiFile message and append each chunk of data to the target file.

        This method handles the processing of incoming file chunks sent over the network within a ChaskiFile message.
        Each chunk is appended to the file specified by the filename in the message data. When all chunks have been received,
        the callback function specified by file_input_callback is invoked.

        Parameters
        ----------
        message : Message
            The Chaski message that contains the file chunk data. This message includes attributes such as the filename,
            chunk data, and file hash.
        edge : Edge
            The network edge (connection) from which the message was received. This can be used for additional context
            about the sender.

        Notes
        -----
        This method performs asynchronous file I/O using the `open` function with the 'ab' mode to append each chunk of
        data. It checks if the chunk data is empty, indicating that all chunks have been received, and then invokes the
        file_input_callback function, if provided.
        """
        # Check if the processing of incoming file chunks is allowed.
        if not self.allow_incoming_files:
            return

        # Append incoming file chunk data to the target file in append-binary mode
        if chunk := message.data.pop('chunk'):
            with open(
                os.path.join(
                    self.destination_folder, message.data['filename']
                ),
                'ab',
            ) as file:
                # Write the current chunk to the target file in append-binary mode
                file.write(chunk)

        else:
            # Invoke the file input callback if it is callable, passing message data and destiny folder
            if callable(self.file_handling_callback):
                # If a file input callback is defined, call it with message data and destiny folder
                self.file_handling_callback(
                    **{
                        **message.data,
                        'destiny_folder': self.destination_folder,
                    }
                )
