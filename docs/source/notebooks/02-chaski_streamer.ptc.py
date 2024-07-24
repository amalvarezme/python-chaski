# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% nbsphinx="hidden"
import sys

sys.path.append('../../..')

# %% [markdown]
# # ChaskiStreamer: Scalable Message Streaming in Distributed Networks
#
# The `ChaskiStreamer` class is designed to efficiently stream messages across a distributed network of nodes.
# It offers scalable solutions for handling large volumes of data and ensures timely delivery across the network.
#
# **Key Features:**
#
#  - **High Throughput:** Optimized for streaming large volumes of messages.
#  - **Low Latency:** Ensures minimal delay in message delivery across distributed nodes.
#  - **Fault Tolerance:** Robust mechanisms to handle node failures and network issues.
#  - **Dynamic Scaling:** Automatically adjusts to the number of nodes and message load.
#  - **Subscription-based Streaming:** Allows nodes to subscribe to specific topics of interest.
#
# ## Create Streamer
#
# To create a `ChaskiStreamer`, you need to instantiate it with the appropriate parameters. Here’s an example to guide you:
#

# %%
from chaski.streamer import ChaskiStreamer
import pickle

streamer = ChaskiStreamer(
    ip='127.0.0.1',  # The IP address for the node to bind to.
    port=65432,  # The port number for the node to listen on.
    serializer=pickle.dumps,  # Function to serialize data before sending.
    deserializer=pickle.loads,  # Function to deserialize received data.
    name='Node',  # The name for the node.
    subscriptions=['topic1', 'topic2'],  # List of topics the node is interested in.
    run=True,  # Flag to start the servers immediately on initialization.
    ttl=64,  # Time-to-live value for discovery messages.
    root=False,  # Flag to indicate if the node is a root node.
    max_connections=5,  # Maximum number of connections the node can handle.
    reconnections=32,  # Number of reconnection attempts if a connection is lost.
)

# %% [markdown]
# The `address` property provides a string representation of the node's network address in the format
# `ChaskiStreamer@<IP>:<port>`, which other nodes use to establish a connection.

# %%
streamer.address

# %% [markdown]
# ## Streaming Messages
#
# The `ChaskiStreamer` efficiently streams messages to various nodes in the network, ensuring high
# throughput and minimal latency for a seamless data transfer experience. Here’s an example:

# %%
producer = ChaskiStreamer(
    port=8511,
    name='Producer',
    subscriptions=['topic1'],
)
producer

# %% [markdown]
# The `producer` node is set up to publish messages on `topic1`, while the `consumer` node
# subscribes to this topic to receive messages.
#
#   - The `producer` connects to the `consumer` using the `connect` method.
#   - Messages are then sent using the `push` method on the `producer`.
#

# %%
await producer.connect('*ChaskiStreamer@127.0.0.1:8511')

# %%
message = {'data': 'Hello, World!'}

# Stream a message to all subscribed nodes
await streamer.push('topic1', message)

# %% [markdown]
# The `ChaskiStreamer` ensures messages are streamed efficiently, maintaining high
# throughput and low latency. This mechanism seamlessly handles data transfer in the network.

# %% [markdown]
# ## Receiving Messages
#
# The `consumer` node listens for messages on the topics it subscribes to, processing
# and printing each received message asynchronously, enabling real-time message handling
# and processing within the distributed network.
#

# %%
consumer = ChaskiStreamer(
    port=8512,
    name='Consumer',
    subscriptions=['topic1'],
    root=True,
)

consumer

# %% [markdown]
# ### Receiving Messages using 'async with'
#
# The `ChaskiStreamer` allows for another method to consume messages using asynchronous context managers.
# With the `async with` statement, you can handle incoming messages in a more streamlined way:
#
#   - Use `async with consumer` to enter the asynchronous context.
#   - Iterate over `message_queue` to process each incoming message asynchronously.
#
# This ensures all resources are properly managed and released when done, providing cleaner and more efficient code.

# %%
async with consumer as message_queue:
    async for incoming_message in message_queue:
        print("Received message:", incoming_message)

# %% [markdown]
# ### Receiving Messages using explicit close
#
# Apart from `async with`, `ChaskiStreamer` also provides a way to consume messages
# using an explicit close operation. This method gives you more control over the
# streaming process and is useful in scenarios where the context manager approach is
# not suitable.
#
# Here’s how to use it:
#
#   - Iterate over the `message_stream()` to process each incoming message asynchronously.
#   - Explicitly call the `stop()` method to close the streamer and release resources.
#

# %%
async for incoming_message in consumer.message_stream():
    print("Received message:", incoming_message)

# %% [markdown]
# Close the consumer to release resources.

# %%
consumer.stop()
