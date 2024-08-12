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

# %% nbsphinx="hidden" editable=true slideshow={"slide_type": ""}
import sys
import asyncio

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

server = ChaskiRemote(port='65432')
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
await client.connect(server.address)

# %% [markdown]
# ### Connect to Remote NumPy
#
# Once connected to the `ChaskiRemote` server, you can access and use the registered libraries,
# such as NumPy, as if they were local.
#

# %%
np = client.proxy('numpy')
np

# %% [markdown]
# ### Use remote Numpy
#
# The following code generates a 4x4 matrix with normally distributed random numbers using
# the remote NumPy library connected through `ChaskiRemote`.

# %%
np.random.normal(0, 1, (4, 4))

# %% [markdown]
# ## Restrict Module Access
#
# To limit the module access in `ChaskiRemote`, you can specify the modules that the server can provide to clients.
# This ensures that only the specified modules are accessible, enhancing security and control over the network interactions.

# %%
server = ChaskiRemote(port='65433', available=['numpy'])
server.address

# %% [markdown]
# Here we initialize the `ChaskiRemote` client and set up a connection to the server.
# This connection allows the client to interact with remote resources and execute
# distributed commands seamlessly.

# %%
client = ChaskiRemote()
await client.connect(server.address)

# %% [markdown]
# After connecting, we use the `client.proxy` method to access the `numpy` library
# remotely. This feature enables the use of remote libraries as if they were local,
# making distributed computing more accessible.

# %%
client.proxy('numpy')

# %% [markdown]
# Similarly, other libraries like `scipy` cannot be accessed using the `client.proxy`
# method because it is not in the list of available modules. This demonstrates the
# security and control provided by `ChaskiRemote` in limiting module access for
# network-based operations.

# %%
client.proxy('scipy')

# %% [markdown]
# ## Attributes and Methods
#
# Here we initialize a `ChaskiRemote` server specifying `os` as an available module.
# Then we set up a client to connect to this server, allowing remote access to the `os` module.

# %%
server = ChaskiRemote(port='65434', available=['os'])
client = ChaskiRemote()

await asyncio.sleep(0.3)
await client.connect(server.address)
await asyncio.sleep(0.3)

remote_os = client.proxy('os')
remote_os

# %% [markdown]
# `os.name` should be a string

# %%
remote_os.name

# %% [markdown]
# The type of `remote_os.name` is `ObjectProxying`

# %%
type(remote_os.name)

# %% [markdown]
# It is possible to access the correct value by calling `remote_os.name._`

# %%
remote_os.name._

# %% [markdown]
# or using Context Managers, you can ensure that the remote attribute is correctly obtained and cleaned up,
# thus preventing potential resource leaks or unintended states in your distributed network interactions.

# %%
with remote_os.name as name:
    name  # is a string
