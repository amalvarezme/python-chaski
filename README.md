# Chaski Confluent

Chaski Confluent is an advanced distributed node communication framework that utilizes TCP/IP
for inter-node connections, ensuring efficient message handling, serialization, discovery, and
pairing of nodes based on common subscription topics, thus facilitating dynamic and resilient
data exchange in complex network topologies.

## Main Features of Chaski Confluent

The Chaski Confluent framework provides various powerful features that make it suitable for managing distributed systems. Here are some of the key features:

**1. TCP and UDP Communication:**
Chaski Confluent supports both TCP and UDP protocols, allowing for reliable and timely message delivery between nodes. The framework ensures efficient data transfer irrespective of the underlying network conditions.

**2. Node Discovery and Pairing:**
Automatic discovery of nodes based on shared subscription topics is a crucial feature. Chaski Confluent facilitates the pairing of nodes with common interests, making it easy to build dynamic and scalable network topologies.

**3. Ping and Latency Management:**
The framework includes built-in mechanisms for measuring latency between nodes through ping operations. This helps in maintaining healthy connections and ensures that communication within the network is optimal.

**4. Subscription Management:**
Nodes can subscribe to specific topics, and messages are routed efficiently based on these subscriptions. This allows for effective communication and data exchange only with relevant nodes.

**5. Keep-alive and Disconnection Handling:**
Chaski Confluent ensures that connections between nodes remain active by implementing keep-alive checks. If a connection is lost, the framework handles reconnection attempts gracefully to maintain network integrity.

**6. Remote Method Invocation:**
The Chaski Remote class enables remote method invocation and interaction across distributed nodes. Nodes can communicate transparently, invoking methods and accessing attributes on remote objects as if they were local.

## Chaski Node

The Chaski Node is an essential component of the Chaski Confluent system. It is responsible for initiating and managing
network communication between distributed nodes. This class handles functions such as connection establishment,
message passing, node discovery, and pairing based on shared subscriptions.

## Chaski Streamer

The Chaski Streamer extends the functionality of Chaski Node by introducing asynchronous message streaming capabilities.
It sets up an internal message queue to manage incoming messages, allowing efficient and scalable message processing within a distributed environment.
The ChaskiStreamer can enter an asynchronous context, enabling the user to stream messages using the `async with` statement.
This allows for handling messages dynamically as they arrive, enhancing the responsiveness and flexibility of the system.

## Chaski Remote

The Chaski Remote class enhances the Chaski Node functionality by enabling remote method invocation and interaction
across distributed nodes. It equips nodes with the ability to communicate transparently, invoking methods and accessing
attributes on remote objects as if they were local. This is achieved by utilizing the Proxy class, which wraps around
the remote objects and provides a clean interface for method calls and attribute access.

