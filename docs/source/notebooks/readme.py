# ---
# jupyter:
#   jupytext:
#     formats: py:light,ipynb
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Chaski Confluent

# Chaski Confluent is a sophisticated distributed node communication and management system that leverages
# TCP/IP for seamless inter-node connections. It enables efficient message handling, serialization, discovery,
# and pairing of nodes based on shared subscriptions, fostering dynamic network topologies and robust data
# exchange capabilities.

# ## Main Features of Chaski Confluent
#
# The Chaski Confluent framework provides various powerful features that make it suitable for managing distributed systems. Here are some of the key features:
#
# **1. TCP and UDP Communication:**
# Chaski Confluent supports both TCP and UDP protocols, allowing for reliable and timely message delivery between nodes. The framework ensures efficient data transfer irrespective of the underlying network conditions.
#
# **2. Node Discovery and Pairing:**
# Automatic discovery of nodes based on shared subscription topics is a crucial feature. Chaski Confluent facilitates the pairing of nodes with common interests, making it easy to build dynamic and scalable network topologies.
#
# **3. Ping and Latency Management:**
# The framework includes built-in mechanisms for measuring latency between nodes through ping operations. This helps in maintaining healthy connections and ensures that communication within the network is optimal.
#
# **4. Subscription Management:**
# Nodes can subscribe to specific topics, and messages are routed efficiently based on these subscriptions. This allows for effective communication and data exchange only with relevant nodes.
#
# **5. Keep-alive and Disconnection Handling:**
# Chaski Confluent ensures that connections between nodes remain active by implementing keep-alive checks. If a connection is lost, the framework handles reconnection attempts gracefully to maintain network integrity.


# ## Chaski Node
#
# The Chaski Node is an essential component of the Chaski Confluent system. It is responsible for initiating and managing
# network communication between distributed nodes. This class handles functions such as connection establishment,
# message passing, node discovery, and pairing based on shared subscriptions.

# ## Chaski Streamer
#
# The Chaski Streamer extends the functionality of Chaski Node by introducing asynchronous message streaming capabilities.
# It sets up an internal message queue to manage incoming messages, allowing efficient and scalable message processing within a distributed environment.
# The ChaskiStreamer can enter an asynchronous context, enabling the user to stream messages using the `async with` statement.
# This allows for handling messages dynamically as they arrive, enhancing the responsiveness and flexibility of the system.
#