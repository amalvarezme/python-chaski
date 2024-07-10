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

from dataclasses import dataclass
from typing import Any, Optional
import asyncio
import logging
import importlib
from datetime import datetime

from chaski.node import ChaskiNode

# Initialize logger for ChaskiRemote operations
logger_remote = logging.getLogger("ChaskiRemote")


########################################################################
@dataclass
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

    name: str
    obj: Any = None
    node: 'ChaskiNode' = None
    edge: 'Edge' = None

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

        # If the proxied attribute is callable, wrap it in a class that handles the call and provides a representation of the object.
        if callable(obj):
            # This wrapper class is created to handle the invocation of callable attributes.
            # When the attribute is called, it invokes the actual method on the proxied object.
            # The __repr__ method is overridden to return the string representation of the proxied object.
            class wrapper:
                def __call__(cls, *args, **kwargs):
                    return obj(*args, **kwargs)

                def __repr__(cls):
                    return str(obj)

            return wrapper()

        else:
            # Return a new Proxy instance for non-callable attributes, chaining the attribute name for nested object access.
            return Proxy(f"{self.name}.{attr}", obj=obj, node=self.node, edge=self.edge)

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
        # Traverse each attribute in the obj_chain starting from self.obj
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
            'timestamp': datetime.now(),
        }

        # Send a request to the remote node using UDP for performing a generic test response.
        response = await self.node._generic_request_udp(
            callback='_call_obj_by_proxy',
            kwargs=data,
            edge=self.edge,
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
    def __init__(
        self,
        available: Optional[str] = None,
        *args: tuple[Any, ...],
        **kwargs: dict[str, Any],
    ):
        """
        Initialize a ChaskiRemote instance.

        This constructor initializes a ChaskiRemote node, inheriting from the ChaskiNode
        base class. It also sets up a dictionary to hold proxy objects associated with
        services to be remotely accessed.

        Parameters
        ----------
        available : str, optional
            A string indicating available services for the remote node.
        *args : tuple of Any
            Positional arguments to be passed to the parent ChaskiNode class.
        **kwargs : dict of {str: Any}
            Keyword arguments to be passed to the parent ChaskiNode class.
        """
        super().__init__(*args, **kwargs)
        self.proxies = {}
        self.available = available

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
    async def proxy(self, module: str) -> Proxy:
        """
        Retrieve a proxy object for the specified service name.

        This asynchronous method obtains a proxy associated with a given service name.
        The proxy can be used to remotely invoke methods on the registered service.

        Parameters
        ----------
        module : str
            The name of the service/module to retrieve a proxy for.

        Returns
        -------
        Proxy
            The proxy object associated with the specified service name.
        """

        edge = await self._verify_availability(module=module)
        if edge:
            return Proxy(module, node=self, edge=edge)
        else:
            logger_remote.warning(f"Module {module} not found in the conected edges")

    # ----------------------------------------------------------------------
    async def _call_obj_by_proxy(self, **kwargs: dict[str, Any]) -> Any:
        """
        Asynchronously call a method on a proxied object with provided arguments.

        This method performs an asynchronous remote method invocation using the proxy.
        It logs the call and retrieves the result from the proxied object.

        Parameters
        ----------
        kwargs : dict
            A dictionary containing the following keys:
                - 'name': str
                    The name of the proxy service.
                - 'obj': list of str
                    A chain of object attributes to traverse for the method call.
                - 'args': tuple
                    Positional arguments to pass to the method.
                - 'kwargs': dict
                    Keyword arguments to pass to the method.
                - 'timestamp': datetime
                    The timestamp representing when the request was initiated.

        Returns
        -------
        Any
            The result of the remote method call based on the proxied service.

        Notes
        -----
        This method uses async calls and expects the proxied methods to be asynchronous.
        The call is logged with the service name, method, and arguments.
        """
        await asyncio.sleep(0)

        name = kwargs['name']
        obj = kwargs['obj']
        args = kwargs['args']
        timestamp = kwargs['timestamp']
        kwargs_ = kwargs['kwargs']

        logger_remote.warning(
            f"{self.name}-{timestamp}: Calling {name}.{'.'.join(obj)} with args:{args} kwargs:{kwargs_}"
        )

        if name in self.proxies:
            # Invoke the resolved method on the proxied object with specified arguments and keyword arguments.
            return self.proxies[name]._obj(obj)(*args, **kwargs_)
        else:
            return None

    # ----------------------------------------------------------------------
    async def _verify_availability(self, module: str) -> Any:
        """
        Verify the availability of a specified module across connected nodes.

        This asynchronous method checks if a specified module is available on any of the
        connected edges (nodes) in the network. It sends a generic UDP request to each edge
        to verify if the module is available for remote interaction.

        Parameters
        ----------
        module : str
            The name of the module to check for availability.

        Returns
        -------
        Any
            The edge where the module is available if found, otherwise False.

        Notes
        -----
        This method iterates through all connected edges and sends a UDP request to verify
        the module's availability. It returns the first edge that confirms the module's
        presence or False if no such edge is found.
        """
        data = {
            'module': module,
        }
        # Iterate through each connected edge to check if the specified module is available on any of them.
        for edge in self.edges:
            # Sends a request to verify if the specified module is available on the remote node.
            available = await self._generic_request_udp('_verify_module', data, edge)
            if available:
                return edge
        return False

    # ----------------------------------------------------------------------
    async def _verify_module(self, **kwargs: dict[str, Any]) -> Any:
        """
        Verify the availability of a specified module and register it if available.

        This method checks whether a given module is available for import on the remote node.
        If the module can be successfully imported, it registers the module as a service with
        the node. It logs the registration process and returns True if successful, or False
        otherwise.

        Parameters
        ----------
        kwargs : dict
            A dictionary containing the following key:
                - 'module': str
                    The name of the module to verify and potentially register.

        Returns
        -------
        bool
            True if the module is successfully imported and registered, False otherwise.

        Notes
        -----
        This method uses the `importlib` to dynamically load modules and `asyncio` to manage
        asynchronous operations.
        """
        await asyncio.sleep(0)
        module = kwargs['module']

        # Check if the module is listed as available on this node
        if (self.available) and (not module in self.available):
            return False

        try:
            # Dynamically import the specified module
            imported_module = importlib.import_module(module)

            # Register the dynamically imported module as a service with the node
            self.register(module, imported_module)

            # Log the registration of the module on the remote node
            logger_remote.warning(f"{self.name}: Registered {module}")
            return True
        except:
            return False
