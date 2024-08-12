> Developed by [Yeison Nolberto Cardona Álvarez, MSc.](https://github.com/yeisonCardona)  
[Andrés Marino Álvarez Meza, PhD.](https://github.com/amalvarezme)  
César Germán Castellanos Dominguez, PhD.  
> _Digital Signal Processing and Control Group_  | _Grupo de Control y Procesamiento Digital de Señales ([GCPDS](https://github.com/UN-GCPDS/))_  
> _Universidad Nacional de Colombia sede Manizales_  

----

# Chaski Confluent

[Chaski-Confluent](https://github.com/dunderlab/python-chaski) is an advanced distributed communication framework designed to
streamline data exchange between nodes over TCP/IP networks. It features robust
node discovery, efficient message handling, dynamic pairing based on subscription
topics, and extends functionality with remote interactions, ensuring resilience and
flexibility in complex network topologies.

![GitHub top language](https://img.shields.io/github/languages/top/dunderlab/python-chaski)
![PyPI - License](https://img.shields.io/pypi/l/chaski)
![PyPI](https://img.shields.io/pypi/v/chaski)
![PyPI - Status](https://img.shields.io/pypi/status/chaski)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/chaski)
![GitHub last commit](https://img.shields.io/github/last-commit/dunderlab/python-chaski)
![CodeFactor Grade](https://img.shields.io/codefactor/grade/github/dunderlab/python-chaski)
[![Documentation Status](https://readthedocs.org/projects/chaski-confluent/badge/?version=latest)](https://chaski-confluent.readthedocs.io/en/latest/?badge=latest)

The project aims to provide a reliable and scalable solution for distributed systems,
addressing the challenges of latency management, subscription routing, remote method
invocation, and connection stability. With **Chaski-Confluent**, developers can easily
build and maintain efficient communication protocols in their distributed applications.

**Chaski-Confluent** is a comprehensive solution to the challenges of distributed systems.
Built with robustness and scalability in mind, it leverages advanced networking
techniques to facilitate data exchange between nodes. Its architecture allows for
dynamic scaling, maintaining communication efficiency without compromising performance
as the network grows. This makes **Chaski-Confluent** an ideal choice for resilient and
scalable distributed applications that need to adapt to changing conditions and workloads.

One of the standout features of **Chaski-Confluent** is its support for both TCP and UDP
protocols. This dual-protocol capability ensures that developers can choose the most
appropriate method for their specific use cases. Additionally, the sophisticated node
discovery mechanism and intelligent subscription-based message routing enable the
creation of dynamic network topologies where nodes can communicate effortlessly.
These features, along with effective latency management and remote method invocation,
position **Chaski-Confluent** as a powerful tool for developing modern distributed systems.


## Main Features of Chaski Confluent

The **Chaski-Confluent** framework provides various powerful features that make it suitable for managing distributed systems. Here are some of the key features:

**TCP and UDP Communication:**
Chaski Confluent supports both TCP and UDP protocols, allowing for reliable and timely message delivery between nodes. The framework ensures efficient data transfer irrespective of the underlying network conditions.

**Node Discovery and Pairing:**
Automatic discovery of nodes based on shared subscription topics is a crucial feature. Chaski Confluent facilitates the pairing of nodes with common interests, making it easy to build dynamic and scalable network topologies.

**Ping and Latency Management:**
The framework includes built-in mechanisms for measuring latency between nodes through ping operations. This helps in maintaining healthy connections and ensures that communication within the network is optimal.

**Subscription Management:**
Nodes can subscribe to specific topics, and messages are routed efficiently based on these subscriptions. This allows for effective communication and data exchange only with relevant nodes.

**Keep-alive and Disconnection Handling:**
Chaski Confluent ensures that connections between nodes remain active by implementing keep-alive checks. If a connection is lost, the framework handles reconnection attempts gracefully to maintain network integrity.

**Remote Method Invocation:**
The Chaski Remote class enables remote method invocation and interaction across distributed nodes. Nodes can communicate transparently, invoking methods and accessing attributes on remote objects as if they were local.

**Security:**
Implement robust security measures to protect data and ensure safe communication between the nodes. Features like encryption and authentication are essential to safeguarding the integrity of the network. For example, you can set up a Certificate Authority (CA) within your network to manage SSL certificates and ensure encrypted communication.

**Flexible Configuration:**
The framework offers a flexible configuration system, allowing users to customize various parameters such as timeouts, retry intervals, and buffer sizes. This adaptability helps in optimizing the performance according to specific requirements.

**Logging and Monitoring:**
Comprehensive logging and monitoring capabilities are integrated into the framework, providing real-time insights into the network activity and performance metrics. This aids in troubleshooting and maintaining the health of the system.


## Chaski-Confluent components

### Chaski Node

The Chaski_ Node is an essential component of the Chaski-Confluent system. It is responsible for initiating and managing
network communication between distributed nodes. This class handles functions such as connection establishment,
message passing, node discovery, and pairing based on shared subscriptions.

### Chaski Streamer

The Chaski-Streamer extends the functionality of Chaski-Node by introducing asynchronous message streaming capabilities.
It sets up an internal message queue to manage incoming messages, allowing efficient and scalable message processing within a distributed environment.
The ChaskiStreamer can enter an asynchronous context, enabling the user to stream messages using the `async with` statement.
This allows for handling messages dynamically as they arrive, enhancing the responsiveness and flexibility of the system.

### Chaski Remote

The Chaski-Remote class enhances the Chaski-Node functionality by enabling remote method invocation and interaction
across distributed nodes. It equips nodes with the ability to communicate transparently, invoking methods and accessing
attributes on remote objects as if they were local. This is achieved by utilizing the Proxy class, which wraps around
the remote objects and provides a clean interface for method calls and attribute access.


## Asynchronous Communication Architecture

The core functionalities of Chaski-Confluent revolve around efficient and scalable
communication mechanisms integral to modern distributed systems. Central to its
architecture is the use of the Python `asyncio` library, which facilitates asynchronous
programming to manage concurrent connections without the overhead of traditional
threading models. This allows for high-performance message handling and real-time
node interactions, optimizing the framework for low-latency and responsive communication.

In implementing Chaski-Confluent, leveraging asyncio ensures that tasks such as
node discovery, subscription management, and remote method invocation are carried
out efficiently. Asynchronous programming enables the framework to handle multiple
network operations simultaneously, maintaining high throughput and scalability even
under heavy network loads. The integration of `asyncio` thus provides a robust
foundation for building dynamic and resilient distributed systems, ensuring seamless
and efficient data exchange across nodes.

## Certification Authority

Certification Authority (CA) is crucial for securing communications within the Chaski-Confluent framework.
By acting as a trust anchor, CA issues and manages digital certificates, ensuring that nodes in the network
can verify each other's identities. This mechanism helps to maintain the integrity and confidentiality of
the data being exchanged.

The CA in Chaski-Confluent can generate, sign, and distribute SSL certificates, providing a robust
security layer. This ensures that all communication between nodes is encrypted and authenticated,
significantly reducing the risk of data breaches or unauthorized access.

