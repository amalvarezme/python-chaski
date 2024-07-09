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

# %% nbsphinx="hidden"
import sys
sys.path.append('../../..')

# %% [markdown]
# # ChaskiRemote: Proxy for Distributed Network Interactions
#
# ChaskiRemote is designed to facilitate communication across distributed networks. It acts as an intermediary, managing interactions and ensuring robust connectivity.
#
# **Key Features:**  
#
# - **Scalability**: Easily manage multiple network nodes.  
# - **Reliability**: Ensure consistent and reliable data transmission.  
# - **Flexibility**: Adapt to various network topologies and protocols.  

# %% [markdown]
# ## Server
#
# The `ChaskiRemote` server facilitates network communication by acting as a proxy for distributed nodes.
# It ensures robust connectivity, reliable data transmission, and handles the management of various
# network protocols and topologies seamlessly.

# %%
from chaski.remote import ChaskiRemote
import numpy

server = ChaskiRemote(port='65432')
server.register('np', numpy)

server.address

# %% [markdown]
# ## Client
#
# The `ChaskiRemote` client leverages the server's capabilities to facilitate remote interactions.
# By connecting to the proxy server, clients can seamlessly execute distributed commands
# and access shared resources across the network without dealing with the complexities
# of network communication protocols.
#

# %%
client = ChaskiRemote()
await client.connect("ChaskiRemote@127.0.0.1:65432")

# %% [markdown]
# ### Connect to Remote NumPy
#
# Once connected to the `ChaskiRemote` server, you can access and use the registered libraries,
# such as NumPy, as if they were local.
#

# %%
np = await client.proxy('np')
np

# %% [markdown]
# ### Use remote Numpy
#
# The following code generates a 4x4 matrix with normally distributed random numbers using
# the remote NumPy library connected through `ChaskiRemote`.

# %%
await np.random.normal(0, 1, (4, 4))

# %%
