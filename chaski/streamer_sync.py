"""
=========================================================================
ChaskiStreamerSync: Synchronous Wrapper for ChaskiStreamer
=========================================================================

The `ChaskiStreamerSync` module provides a synchronous interface for the
asynchronous `ChaskiStreamer`. It enables users to stream messages and perform
operations in a threaded environment without managing event loops directly.

Key Functionalities
-------------------
- Provides synchronous access to the asynchronous streaming functionalities of `ChaskiStreamer`.
- Manages an event loop running in a background thread.
- Facilitates graceful shutdown and resource cleanup.

"""

import asyncio
import threading
import inspect
import typing
import logging

from chaski.streamer import ChaskiStreamer


########################################################################
class ChaskiStreamerSync:
    """
    ChaskiStreamerSync: Synchronous Wrapper for ChaskiStreamer.

    The ChaskiStreamerSync class provides a synchronous interface for the
    asynchronous ChaskiStreamer, allowing users to stream messages and manage
    operations in a threaded environment, without dealing directly with event loops.

    Attributes
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop instance for managing asynchronous tasks.
    streamer : ChaskiStreamer
        The underlying ChaskiStreamer instance for message handling.
    thread : threading.Thread
        The thread that runs the event loop.

    Methods
    -------
    close() -> None:
        Stops the ChaskiStreamer gracefully and shuts down the event loop.
    __getattr__(attr: str) -> typing.Any:
        Retrieves an attribute from the ChaskiStreamer instance,
        allowing access to its methods and properties.
    """

    # ----------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """Initialize a new instance of ChaskiStreamerSync.

        This constructor sets up an asynchronous event loop running in a separate thread
        and initializes an instance of the underlying ChaskiStreamer for message streaming.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments to pass to the ChaskiStreamer initializer.
        **kwargs : dict
            Additional keyword arguments to configure the ChaskiStreamer instance.

        Notes
        -----
        The ChaskiStreamerSync instance must be closed properly using the `close` method
        to ensure that resources are released and the event loop is shut down cleanly.
        """

        self.loop = asyncio.new_event_loop()

        # This function sets the given event loop as the current event loop and starts it to run indefinitely, enabling asynchronous tasks.
        asyncio.set_event_loop(self.loop)

        self.streamer = ChaskiStreamer(*args, **kwargs, run=False)

        # Start a new thread that runs the event loop, allowing for asynchronous operations to occur in parallel.
        self.thread = threading.Thread(
            target=self._start_loop, args=(self.loop,)
        )

        self.thread.start()  # Start the thread that runs the event loop
        asyncio.run_coroutine_threadsafe(
            self.streamer.run(), self.loop
        )  # Schedule the streamer to run in the event loop

    # ----------------------------------------------------------------------
    def __repr__(self):
        """
        Provide a string representation of the ChaskiStreamerSync instance.

        This method returns a string that includes the class name and network information
        such as the IP address and port. If the instance is a root node, it prepends an
        asterisk (*) to the string.
        """
        h = '*' if self.paired else ''
        return h + self.address.replace(
            'ChaskiStreamer', 'ChaskiStreamerSync'
        )

    # ----------------------------------------------------------------------
    def _start_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the event loop.

        This method sets the given event loop as the current event loop
        and executes it indefinitely. It is designed to run in a separate
        thread, enabling asynchronous tasks to operate concurrently with
        other operations.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop instance to be run in the thread.

        Returns
        -------
        None
            This method does not return any value.

        Notes
        -----
        Ensure that the event loop is properly configured before calling
        this method. The loop will run indefinitely until stopped by
        an external signal.
        """
        # Set the given event loop as the current event loop and start running it indefinitely.
        asyncio.set_event_loop(loop)
        loop.run_forever()

    # ----------------------------------------------------------------------
    def close(self) -> None:
        """Close the ChaskiStreamerSync instance.

        This method stops the ChaskiStreamer gracefully, ensuring that
        all ongoing operations are completed and the event loop is shut down
        cleanly. It joins the thread running the event loop to ensure that
        all resources are released properly before the instance is discarded.

        Returns
        -------
        None
            This method does not return any value.
        """
        # Stops the ChaskiStreamer gracefully.
        self.streamer.stop()

        # Schedule the event loop to stop and wait for the thread to finish execution.
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()

    # ----------------------------------------------------------------------
    def __getattr__(self, attr: str) -> typing.Any:
        """Retrieve an attribute from the ChaskiStreamer instance.

        This method overrides the default behavior of attribute access,
        allowing for dynamic handling of attributes that are not directly defined.

        If the attribute is callable and a coroutine function, it wraps the
        call to execute it within the event loop and retrieves the result.

        Parameters
        ----------
        attr : str
            The name of the attribute to retrieve from the ChaskiStreamer instance.

        Returns
        -------
        typing.Any
            The value of the requested attribute, or a callable wrapper if it's a coroutine.

        Raises
        ------
        AttributeError
            If the requested attribute does not exist in the ChaskiStreamer instance.
        """
        # Retrieve an attribute from the streamer object and check if it's callable.
        object_ = getattr(self.streamer, attr)

        # Check if the object is callable and if it's a coroutine function
        if callable(object_) and inspect.iscoroutinefunction(object_):

            def wrapp(*args, **kwargs):
                # Execute the coroutine in the event loop and retrieve the result
                coro = object_(*args, **kwargs)
                future = asyncio.run_coroutine_threadsafe(coro, self.loop)
                return future.result()

            return wrapp

        else:
            return object_

    def get_message(self, timeout):
        # Obtener un mensaje de 'message_queue' de forma síncrona
        coro = self.streamer.message_queue.get()
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    # ----------------------------------------------------------------------
    def message_stream(
        self, timeout=None
    ) -> typing.Generator['Message', None, None]:
        """"""
        while True:
            try:
                message = self.get_message(timeout)
                message
            except TimeoutError:
                return None
            except Exception as e:
                continue

            # if self.terminate_stream_flag:
            #     break

            yield message

        # self.terminate_stream_flag = True
