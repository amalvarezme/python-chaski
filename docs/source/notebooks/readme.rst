   Developed by `Yeison Nolberto Cardona Álvarez,
   MSc. <https://github.com/yeisonCardona>`__ `Andrés Marino Álvarez
   Meza, PhD. <https://github.com/amalvarezme>`__ César Germán
   Castellanos Dominguez, PhD. *Digital Signal Processing and Control
   Group* \| *Grupo de Control y Procesamiento Digital de Señales
   (*\ `GCPDS <https://github.com/UN-GCPDS/>`__\ *)* *Universidad
   Nacional de Colombia sede Manizales*

--------------

.. container:: alert alert-block alert-warning

   Warning: This module is still under development and its features are
   subject to change in the future.

Chaski Confluent
================

Chaski Confluent is an advanced distributed node communication framework
that utilizes TCP/IP for inter-node connections, ensuring efficient
message handling, serialization, discovery, and pairing of nodes based
on common subscription topics, thus facilitating dynamic and resilient
data exchange in complex network topologies.

|GitHub top language| |PyPI - License| |PyPI| |PyPI - Status| |PyPI -
Python Version| |GitHub last commit| |CodeFactor Grade| |Documentation
Status|

.. |GitHub top language| image:: https://img.shields.io/github/languages/top/dunderlab/python-chaski
.. |PyPI - License| image:: https://img.shields.io/pypi/l/chaski
.. |PyPI| image:: https://img.shields.io/pypi/v/chaski
.. |PyPI - Status| image:: https://img.shields.io/pypi/status/chaski
.. |PyPI - Python Version| image:: https://img.shields.io/pypi/pyversions/chaski
.. |GitHub last commit| image:: https://img.shields.io/github/last-commit/dunderlab/python-chaski
.. |CodeFactor Grade| image:: https://img.shields.io/codefactor/grade/github/dunderlab/python-chaski
.. |Documentation Status| image:: https://readthedocs.org/projects/chaski-confluent/badge/?version=latest
   :target: https://chaski-confluent.readthedocs.io/en/latest/?badge=latest

Chaski Confluent is engineered to address the communication challenges
in distributed systems, offering a robust and efficient framework. It
leverages the flexibility of TCP/IP protocols to ensure reliable data
exchange across varied network conditions. By utilizing advanced node
discovery and pairing mechanisms, Chaski Confluent enables seamless
integration and collaboration among nodes.

Through features such as ping and latency management, the framework
keeps communications optimal, which is crucial for maintaining the
health and performance of the network. Subscription management ensures
that messages are only sent to relevant nodes, enhancing efficiency and
reducing unnecessary data transfer. Moreover, Chaski Confluent’s
resilience through keep-alive checks and graceful reconnections
guarantees continuous and stable network operations.

Work Plan
---------

**Capture Exceptions**: Implement robust exception handling across the
framework to ensure that all potential errors are caught and managed
gracefully.

**Add Usage Examples**: Provide detailed examples demonstrating the use
of Chaski Confluent in various scenarios. This will help users
understand how to integrate and utilize the framework effectively.

**Add Containers**: Integrate containerization support (e.g., Docker) to
facilitate the deployment and management of Chaski Confluent nodes in
different environments.

**Add ‘Attach Function’ Feature to ChaskiRemote**: Enhance the
ChaskiRemote class by adding a feature that allows functions to be
dynamically attached and executed remotely. This will increase the
flexibility and usability of remote method invocation.

Main Features of Chaski Confluent
---------------------------------

The Chaski Confluent framework provides various powerful features that
make it suitable for managing distributed systems. Here are some of the
key features:

**TCP and UDP Communication:** Chaski Confluent supports both TCP and
UDP protocols, allowing for reliable and timely message delivery between
nodes. The framework ensures efficient data transfer irrespective of the
underlying network conditions.

**Node Discovery and Pairing:** Automatic discovery of nodes based on
shared subscription topics is a crucial feature. Chaski Confluent
facilitates the pairing of nodes with common interests, making it easy
to build dynamic and scalable network topologies.

**Ping and Latency Management:** The framework includes built-in
mechanisms for measuring latency between nodes through ping operations.
This helps in maintaining healthy connections and ensures that
communication within the network is optimal.

**Subscription Management:** Nodes can subscribe to specific topics, and
messages are routed efficiently based on these subscriptions. This
allows for effective communication and data exchange only with relevant
nodes.

**Keep-alive and Disconnection Handling:** Chaski Confluent ensures that
connections between nodes remain active by implementing keep-alive
checks. If a connection is lost, the framework handles reconnection
attempts gracefully to maintain network integrity.

**Remote Method Invocation:** The Chaski Remote class enables remote
method invocation and interaction across distributed nodes. Nodes can
communicate transparently, invoking methods and accessing attributes on
remote objects as if they were local.

Chaski Node
-----------

The Chaski Node is an essential component of the Chaski Confluent
system. It is responsible for initiating and managing network
communication between distributed nodes. This class handles functions
such as connection establishment, message passing, node discovery, and
pairing based on shared subscriptions.

Chaski Streamer
---------------

The Chaski Streamer extends the functionality of Chaski Node by
introducing asynchronous message streaming capabilities. It sets up an
internal message queue to manage incoming messages, allowing efficient
and scalable message processing within a distributed environment. The
ChaskiStreamer can enter an asynchronous context, enabling the user to
stream messages using the ``async with`` statement. This allows for
handling messages dynamically as they arrive, enhancing the
responsiveness and flexibility of the system.

Chaski Remote
-------------

The Chaski Remote class enhances the Chaski Node functionality by
enabling remote method invocation and interaction across distributed
nodes. It equips nodes with the ability to communicate transparently,
invoking methods and accessing attributes on remote objects as if they
were local. This is achieved by utilizing the Proxy class, which wraps
around the remote objects and provides a clean interface for method
calls and attribute access.
