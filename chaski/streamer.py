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
    def __init__(self, *args: tuple, **kwargs: dict):
        """
        Initialize a new instance of ChaskiStreamer.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments to pass to the superclass initializer.
        **kwargs : dict
            Additional keyword arguments to pass to the superclass initializer.

        Notes
        -----
        This constructor initializes the ChaskiStreamer by invoking the
        superclass constructor with any additional arguments provided.
        It also sets up an internal message queue for handling incoming messages.
        """
        super().__init__(*args, **kwargs)
        self.message_queue = Queue()

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
        self.stop()

    # ----------------------------------------------------------------------
    async def _process_ChaskiMessage(self, message: 'Message', edge: 'Edge') -> None:
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
            yield message
        self.stop()
