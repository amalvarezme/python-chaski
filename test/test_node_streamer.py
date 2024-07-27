"""
=====================================================
Unit Tests for ChaskiStreamer Functionality in Python
=====================================================

This module provides unit tests for the ChaskiStreamer class, which is
part of the Chaski framework. The ChaskiStreamer is designed to facilitate
streaming of messages between producers and consumers within a distributed system.

"""

import unittest
import asyncio
import os
from chaski.streamer import ChaskiStreamer


########################################################################
class TestStreamer(unittest.IsolatedAsyncioTestCase):
    """
    This class contains unit tests for the ChaskiStreamer functionality.

    The TestStreamer class is designed to test the ChaskiStreamer's streaming capabilities
    using asynchronous methods. The tests simulate the behavior of a producer and a consumer
    to ensure that messages are correctly streamed and received.
    """

    # ----------------------------------------------------------------------
    async def test_stream(self) -> None:
        """
        Test the streaming functionality between a producer and a consumer.

        This test method covers the following operations:
        1. Initialize a producer and a consumer with the same subscription topic.
        2. Establish a connection between the producer and consumer.
        3. Push messages from the producer to the consumer and validate the received data.

        The test ensures that the streamed data is received correctly by the consumer.

        Raises
        ------
        AssertionError
            If the received data does not match the expected values.
        """
        producer = ChaskiStreamer(
            port=8511,
            name='Producer',
            subscriptions=['topic1'],
        )

        consumer = ChaskiStreamer(
            port=8512,
            name='Consumer',
            subscriptions=['topic1'],
        )

        await asyncio.sleep(0.3)
        await producer.connect(consumer.address)

        await asyncio.sleep(0.3)
        await producer.push(
            'topic1',
            {
                'data': 'test0',
            },
        )

        count = 0
        async with consumer as message_queue:
            async for incoming_message in message_queue:

                self.assertEqual(f'test{count}', incoming_message.data['data'])

                if count >= 5:
                    return

                count += 1
                await producer.push(
                    'topic1',
                    {
                        'data': f'test{count}',
                    },
                )

        await asyncio.sleep(1)
        await consumer.stop()
        await producer.stop()

    # ----------------------------------------------------------------------
    async def test_file_transfer(self) -> None:
        """
        Test the file transfer functionality between a producer and a consumer.

        This test method covers the following operations:
        1. Initialize a producer and a consumer with the same subscription topic for file transfer.
        2. Establish a connection between the producer and consumer.
        3. Push files from the producer to the consumer and validate the received file data including size and hash.

        This test ensures that files are transferred correctly between the producer and the consumer.

        Raises
        ------
        AssertionError
            If the received file data (size or hash) does not match the expected values.
        """

        def new_file_event(**kwargs):
            size = kwargs['data']['size']
            self.assertEqual(
                size,
                kwargs['size'],
                f"File {kwargs['filename']} no match size of {kwargs['filename'][6:-5]}",
            )
            hash = ChaskiStreamer.get_hash(
                os.path.join(
                    kwargs['destiny_folder'],
                    kwargs['filename'],
                )
            )
            self.assertEqual(
                hash,
                kwargs['hash'],
                "The hash of the received file does not match the expected hash.",
            )

        producer = ChaskiStreamer(
            port=8513,
            name='Producer',
            subscriptions=['topicF'],
            #
            # File transfer
            allow_incoming_files=True,
            file_input_callback=new_file_event,
            destiny_folder=os.path.join('testdir', 'output'),
        )

        consumer = ChaskiStreamer(
            port=8514,
            name='Consumer',
            subscriptions=['topicF'],
            #
            # File transfer
            allow_incoming_files=True,
            file_input_callback=new_file_event,
            destiny_folder=os.path.join('testdir', 'output'),
        )

        await asyncio.sleep(0.3)
        await producer.connect(consumer.address)
        await asyncio.sleep(0.3)

        for filename, size in [
            ('dummy_1KB.data', 1e3),
            ('dummy_10KB.data', 10e3),
            ('dummy_100KB.data', 100e3),
            ('dummy_1MB.data', 1e6),
            ('dummy_10MB.data', 10e6),
            ('dummy_100MB.data', 100e6),
            # ('dummy_500MB.data', 500e6),
            # ('dummy_1000MB.data', 1000e6),
            # ('dummy_1500MB.data', 1500e6),
        ]:
            if os.path.exists(os.path.join('testdir', 'output', filename)):
                os.remove(os.path.join('testdir', 'output', filename))

            with open(os.path.join('testdir', 'input', filename), 'rb') as file:
                await producer.push_file(
                    'topicF',
                    file,
                    data={
                        'size': size,
                    },
                )

        await asyncio.sleep(1)
        await consumer.stop()
        await producer.stop()

    # ----------------------------------------------------------------------
    async def test_file_dissable_transfer(self) -> None:
        """
        Test the file transfer functionality when the consumer has file transfer disabled.

        This test method covers the following operations:
        1. Initialize a producer and a consumer with the same subscription topic for file transfer.
        2. Establish a connection between the producer and consumer.
        3. Attempt to push files from the producer to the consumer and verify transfer failure.

        This test ensures that the file transfer fails if the consumer has file transfer disabled.

        Raises
        ------
        AssertionError
            If the file transfer does not fail as expected.
        """

        producer = ChaskiStreamer(
            port=8515,
            name='Producer',
            subscriptions=['topicF'],
            #
            # File transfer
            allow_incoming_files=True,
            destiny_folder=os.path.join('testdir', 'output'),
        )

        consumer = ChaskiStreamer(
            port=8516,
            name='Consumer',
            subscriptions=['topicF'],
            #
            # File transfer
            allow_incoming_files=False,
            destiny_folder=os.path.join('testdir', 'output'),
        )

        await asyncio.sleep(0.3)
        await producer.connect(consumer.address)
        await asyncio.sleep(0.3)

        filename = 'dummy_1KB.data'

        if os.path.exists(os.path.join('testdir', 'output', filename)):
            os.remove(os.path.join('testdir', 'output', filename))

        with open(os.path.join('testdir', 'input', filename), 'rb') as file:
            done = await producer.push_file('topicF', file)
            self.assertFalse(
                done, 'File transfer should fail as consumer has file transfer disabled'
            )

        await asyncio.sleep(1)
        await consumer.stop()
        await producer.stop()


if __name__ == '__main__':
    unittest.main()
