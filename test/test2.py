import sys
import time
import asyncio
from datetime import datetime
from chaski.core2 import ChaskiNode


async def main():
    host = '::1'
    node1 = ChaskiNode(host, 65433)

    # Start the server as a task to allow the execution to proceed.
    server_task = asyncio.create_task(node1.start_server())
    await asyncio.sleep(1)  # Give the server a moment to start

    # Connect to peer and send messages
    await node1.connect_to_peer(host, 65432)

    # Send messages periodically
    while True:
        await node1.write_message(f'NODE2: Message at {datetime.now()}')
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
