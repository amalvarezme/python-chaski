# ---
# jupyter:
#   jupytext:
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

# %% [markdown]
# # Chaski Confluent: Scripts
#
# Once Chaski Confluent is installed, a suite of utility scripts become available for managing various services and functionalities.

# %% [markdown]
# ## ```chaski_certificate_authority```
#
# The `chaski_certificate_authority` is a script to start the Chaski Certificate Authority (CA) server.
# The CA server is responsible for managing SSL/TLS certificates, which are essential for secure communication in Chaski Confluent.
# It creates and stores certificates in the `.chaski_confluent/ca` directory in the user's home directory.
#
# To start the CA server, simply run:
# ```bash
# chaski_certificate_authority
# ```
# This will initialize the CA service and print the CA address.

# %% [markdown]
# ## ```chaski_remote_proxy```
#
# The `chaski_remote_proxy` script is used to start the Chaski Remote Proxy server.
# This server acts as a mediator to manage remote method invocation across distributed nodes.
# By using `ChaskiRemote`, objects can interact transparently with other objects on different network nodes.
# Start the server by running:
# ```bash
# chaski_remote_proxy -p <port> -n <name> <modules>
# ```
# where:
# - `-p <port>` specifies the port number (default: 65432)
# - `-n <name>` specifies the name of the server (default: ChaskiRemote)
# - `<modules>` is a comma-separated list of the modules available for remote invocation
#

# %% [markdown]
# ## ```chaski_terminate_connections```
#
# The `chaski_terminate_connections` script is used to close active connections on a specified range of ports.
# It's useful for cleaning up lingering connections that may interfere with network services.
# Run the script with the following command:
# """bash
# chaski_terminate_connections <start_port>-<end_port>
# """
# where:
# - `<start_port>` is the beginning of the port range
# - `<end_port>` is the end of the port range
