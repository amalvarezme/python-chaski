"""
========================================================
ChaskiRemote: Proxy for Distributed Network Interactions
========================================================

This module provides functionality for remote method invocation, enabling
transparent interaction with objects across distributed network nodes.
Key classes include Proxy and ChaskiRemote, building upon the foundation
provided by the ChaskiNode class. These classes facilitate the creation
and management of proxies that allow remote method invocations, making
distributed computations seamless.

Classes
=======

    - *Proxy*: A class that wraps an object and allows remote method invocation
    and attribute access as if the object were local.
    - *ChaskiRemote*: An extension of ChaskiNode that enables the creation of proxies
    for remote interaction and method invocation.
"""

import asyncio
import logging
from typing import Any

from chaski.node import ChaskiNode

logger_remote = logging.getLogger("ChaskiRemote")


########################################################################
class Proxy:
    """
    Proxy class for remote method invocation.

    The `Proxy` class provides a transparent way of interacting with objects
    across remote nodes. This class wraps an object and allows remote method
    invocation and attribute access as if the object were local. It is primarily
    used within the `ChaskiRemote` framework.

    Notes
    -----
    The `Proxy` class uses dynamic attribute access and method invocation to interact
    with the proxied object. If the attribute accessed is callable, it wraps the attribute
    in a callable class to allow method invocation. Otherwise, it creates a new `Proxy` instance
    for attribute access. The `Proxy` instance itself can be called asynchronously to perform
    remote method invocation.

    See Also
    --------
    chaski.node.ChaskiNode : Main class representing a node in the network.
    chaski.remote.ChaskiRemote : Subclass of `ChaskiNode` for remote interaction and proxies.
    """

    # ----------------------------------------------------------------------
    def __init__(self, name: str, obj: Any = None, node: 'ChaskiNode' = None):
        """
        Initialize a Proxy instance.

        This constructor initializes a Proxy object that wraps around another object (`obj`)
        and provides a way to interact with it via a remote `ChaskiNode`. The Proxy is identified
        by a unique name.

        Parameters
        ----------
        name : str
            The name of the proxy. This identifier is used to reference the proxied object.
        obj : Any, optional
            The object to be proxied. If not provided, the proxy will handle attribute
            access dynamically.
        node : ChaskiNode, optional
            The remote node associated with the proxy. This allows remote method invocation
            on the proxied object via the node.
        """
        self.name = name
        self.obj = obj
        self.node = node

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Provide a string representation of the Proxy object.

        This method returns a string representation of the Proxy instance, which
        includes the name of the proxied object. This can be useful for debugging
        and logging purposes to identify the proxied object easily.

        Returns
        -------
        str
            A string in the format "Proxy(<name>)" where <name> is the name of the proxied object.
        """
        return f"Proxy({self.name})"

    # ----------------------------------------------------------------------
    def __getattr__(self, attr: str) -> Any:
        """
        Automatically retrieve or wrap the attribute from the proxied object.

        This method intercepts access to attributes of the `Proxy` instance.
        If the attribute is callable, it wraps the attribute in a callable class
        wrapper. Otherwise, it creates a new `Proxy` instance for the attribute.

        Parameters
        ----------
        attr : str
            The name of the attribute to retrieve from the proxied object.

        Returns
        -------
        Any
            The attribute value if it is not callable, otherwise a callable class
            wrapping the attribute.
        """
        obj = getattr(self.obj, attr, None)

        if callable(obj):

            class wrapper:
                def __call__(cls, *args, **kwargs):
                    return obj(*args, **kwargs)

                def __repr__(cls):
                    return str(obj)

            return wrapper()

        else:
            return Proxy(f"{self.name}.{attr}", obj=obj, node=self.node)

    # ----------------------------------------------------------------------
    def _obj(self, obj_chain: list[str]) -> Any:
        """
        Traverse a chain of object attributes.

        This method navigates through a chain of attributes starting from the
        initial object (`self.obj`) and follows each attribute in the provided
        `obj_chain`. It returns the final object obtained by this traversal.

        Parameters
        ----------
        obj_chain : list of str
            A list of attribute names to traverse. Each string in the list
            represents an attribute name to follow in sequence.

        Returns
        -------
        Any
            The final object obtained by traversing the attribute chain.
        """
        obj = self.obj
        for obj_ in obj_chain:
            obj = getattr(obj, obj_)
        return obj

    # ----------------------------------------------------------------------
    async def __call__(self, *args: Any, **kwargs: dict[str, Any]) -> Any:
        """
        Perform an asynchronous remote method invocation.

        This special method allows the Proxy instance to be callable.
        When called, it sends a request to the associated remote service
        to invoke a method with the provided arguments and keyword arguments.

        Parameters
        ----------
        *args : Any
            Positional arguments to pass to the remote method.
        **kwargs : dict of {str: Any}
            Keyword arguments to pass to the remote method.

        Returns
        -------
        Any
            The result of the remote method call.
        """
        data = {
            'name': self.name.split('.')[0],
            'obj': self.name.split('.')[1:],
            'args': args,
            'kwargs': kwargs,
        }

        response = await self.node._generic_request_udp(
            '_test_generic_response_udp', data
        )
        return response


########################################################################
class ChaskiRemote(ChaskiNode):
    """
    Represents a remote Chaski node.

    The `ChaskiRemote` class extends the `ChaskiNode` class to enable
    the creation of proxies that facilitate remote method invocations.
    It maintains a dictionary of proxy objects associated with the services to be accessed remotely.
    """

    # ----------------------------------------------------------------------
    def __init__(self, *args: tuple, **kwargs: dict):
        """
        Initialize a ChaskiRemote instance with the provided arguments.

        This constructor initializes a ChaskiRemote node, inheriting from the ChaskiNode
        base class. It also sets up a dictionary to hold proxy objects associated with
        services to be remotely accessed.

        Parameters
        ----------
        *args : tuple
            Positional arguments to be passed to the parent ChaskiNode class.
        **kwargs : dict
            Keyword arguments to be passed to the parent ChaskiNode class.
        """
        super().__init__(*args, **kwargs)
        self.proxies = {}

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        """
        Represent the ChaskiRemote node as a string.

        This method returns a string representation of the ChaskiRemote node,
        indicating its address. If the node is paired, the address is prefixed
        with an asterisk (*).

        Returns
        -------
        str
            The representation of the ChaskiRemote node, optionally prefixed
            with an asterisk if paired.
        """
        h = '*' if self.paired else ''
        return h + self.address

    # ----------------------------------------------------------------------
    @property
    def address(self) -> str:
        """
        Construct and retrieve the address string for the ChaskiRemote node.

        This property method returns a formatted string representing the address
        of the ChaskiRemote node, showing its IP and port.

        Returns
        -------
        str
            A formatted string in the form "ChaskiRemote@<IP>:<Port>" indicating the node's address.
        """
        return f"ChaskiRemote@{self.ip}:{self.port}"

    # ----------------------------------------------------------------------
    def register(self, name: str, service: Any) -> None:
        """
        Register a service with a proxy.

        This method registers a service with the node by associating it with a proxy.
        The proxy can then be used to remotely invoke methods on the registered service.

        Parameters
        ----------
        name : str
            The name to associate with the service.
        service : Any
            The service object to register. This object can have methods that will be
            accessible remotely via the proxy.
        """
        self.proxies[name] = Proxy(name, obj=service, node=self)

    # ----------------------------------------------------------------------
    async def proxy(self, name: str) -> Proxy:
        """
        Retrieve a proxy object for the specified service name.

        This asynchronous method obtains a proxy associated with a given service name.
        The proxy can be used to remotely invoke methods on the registered service.

        Parameters
        ----------
        name : str
            The name of the service to retrieve a proxy for.

        Returns
        -------
        Proxy
            The proxy object associated with the specified service name.
        """
        return Proxy(name, node=self)

    # ----------------------------------------------------------------------
    async def _test_generic_response_udp(self, **echo_data: dict[str, Any]) -> Any:
        """
        Test generic response using UDP.

        This is an asynchronous method used to test generic UDP responses. It processes
        the incoming `echo_data` and uses this data to simulate a remote procedure call
        via UDP. The method logs the call details and invokes the corresponding service
        method on the proxied object.

        Parameters
        ----------
        echo_data : dict
            A dictionary containing the following keys:
            - 'name': str
                The name of the proxy service.
            - 'obj': list
                A chain of object attributes to traverse for the method call.
            - 'args': tuple
                Positional arguments to pass to the method.
            - 'kwargs': dict
                Keyword arguments to pass to the method.

        Returns
        -------
        Any
            The result of the remote method call based on the proxied service.
        """
        await asyncio.sleep(0)

        name = echo_data['name']
        obj = echo_data['obj']
        args = echo_data['args']
        kwargs = echo_data['kwargs']

        logger_remote.debug(
            f"Calling {name}.{'.'.join(obj)} with args:{args} kwargs:{kwargs}"
        )

        return self.proxies[name]._obj(obj)(*args, **kwargs)
