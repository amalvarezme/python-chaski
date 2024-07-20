"""
=======================================================================
ChaskiRemote: Transparent Python Framework for Remote Method Invocation
=======================================================================

`ChaskiRemote` is a transparent proxy python objects framework for remote method invocation, enabling
transparent interaction with objects across distributed network nodes.
Key classes include `Proxy` and `ChaskiRemote`, building upon the foundation
provided by the `ChaskiNode` class. These classes facilitate the creation
and management of proxies that allow remote method invocations, making
distributed computations seamless.

Classes
=======
    - *ObjectProxying*: Provides the ability to create proxy objects for remote method invocation transparently.
    - *Proxy*: Wraps an object allowing remote method invocation and attribute access as though the object were local.
    - *ChaskiRemote*: Extends `ChaskiNode` to create and manage proxies, enabling remote interactions and method invocations.
"""

import asyncio
import logging
import importlib
import nest_asyncio
from datetime import datetime
from typing import Any, Optional

from chaski.node import ChaskiNode

# Initialize logger for ChaskiRemote operations
logger_remote = logging.getLogger("ChaskiRemote")

# Apply nested asyncio event loop to allow recursive event loop usage
nest_asyncio.apply()


########################################################################
class ObjectProxying(object):
    """
    This class provides proxying capabilities for enabling remote method invocation
    transparently. It acts as an intermediary to forward method calls and attribute
    access to the real object being proxied.

    Notes
    -----
    - The `_special_names` list contains names of special methods to be intercepted.
    - Custom `__getattr__`, `__delattr__`, and `__setattr__` methods ensure delegation
      of attribute access and modification to the proxied object.
    """

    # Define slots to restrict attribute creation and save memory, also allow use with weak references
    __slots__ = ["_obj", "__weakref__"]

    # Special method names that will be intercepted by the ObjectProxying class
    _special_names = [
        '__abs__',
        '__add__',
        '__and__',
        '__call__',
        '__cmp__',
        '__coerce__',
        '__contains__',
        '__delitem__',
        '__delslice__',
        '__div__',
        '__divmod__',
        '__eq__',
        '__float__',
        '__floordiv__',
        '__ge__',
        '__getitem__',
        '__getslice__',
        '__gt__',
        '__hex__',
        '__iadd__',
        '__iand__',  #'__hash__',
        '__idiv__',
        '__idivmod__',
        '__ifloordiv__',
        '__ilshift__',
        '__imod__',
        '__imul__',
        '__int__',
        '__invert__',
        '__ior__',
        '__ipow__',
        '__irshift__',
        '__isub__',
        '__iter__',
        '__itruediv__',
        '__ixor__',
        '__le__',
        '__len__',
        '__long__',
        '__lshift__',
        '__lt__',
        '__mod__',
        '__mul__',
        '__ne__',
        '__neg__',
        '__oct__',
        '__or__',
        '__pos__',
        '__pow__',
        '__radd__',
        '__rand__',
        '__rdiv__',
        '__rdivmod__',
        '__reduce__',
        '__reduce_ex__',
        '__repr__',
        '__reversed__',
        '__rfloorfiv__',
        '__rlshift__',
        '__rmod__',
        '__rmul__',
        '__ror__',
        '__rpow__',
        '__rrshift__',
        '__rshift__',
        '__rsub__',
        '__rtruediv__',
        '__rxor__',
        '__setitem__',
        '__setslice__',
        '__sub__',
        '__truediv__',
        '__xor__',
        'next',
    ]

    # ----------------------------------------------------------------------
    def __init__(self, obj: Any, instance: Any, parent: Any, name: str):
        """
        Initialize an `ObjectProxying` instance.

        This constructor sets up the necessary attributes for the ObjectProxying
        instance, allowing it to delegate method calls and attribute access to
        the proxied object.

        Parameters
        ----------
        obj : Any
            The object that is being proxied.
        instance : Any
            The instance of the class that contains the Proxy as a descriptor.
        parent : Any
            The parent proxy that manages this instance.
        name : str
            The name of the proxy.

        Notes
        -----
        The attributes are set using `object.__setattr__` to avoid triggering
        custom `__setattr__` implementations of the proxied object.
        """
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_instance", instance)
        object.__setattr__(self, "_parent", parent)
        object.__setattr__(self, "_name", name)

    # ----------------------------------------------------------------------
    @classmethod
    def _create_class_proxy(cls, theclass: type) -> type:
        """
        Create a proxy class for the given class type.

        This method generates a proxy class that wraps the methods of the
        specified class type (`theclass`). This allows for method calls on
        the class to be intercepted and processed through the proxy mechanism.

        Parameters
        ----------
        cls : type
            The proxy class that is being constructed.
        theclass : type
            The original class type for which the proxy is to be created.

        Returns
        -------
        type
            A new proxy class that wraps the specified class type.

        Notes
        -----
        This method defines a `make_method` inner function that handles the
        invocation of methods, ensuring that the chain of proxied method calls
        is properly managed. If an exception occurs during method invocation,
        the `processor_method` of the parent proxy is called instead.
        """

        def make_method(name):
            def method(self, *args, **kw):
                # Set _chain attribute of the _instance to restart the chain from the initial element
                setattr(
                    object.__getattribute__(self, "_instance"),
                    '_chain',
                    [object.__getattribute__(self, "_instance")._chain[0]],
                )
                # Attempt to call the requested method on the proxied object
                try:
                    return getattr(object.__getattribute__(self, "_obj"), name)(
                        args, kw
                    )
                except:
                    # If an exception occurs, use the parent's processor method
                    return getattr(
                        object.__getattribute__(self, "_parent"), 'processor_method'
                    )(args, kw)

            return method

        # Populate the namespace with method names that should be proxied.
        # If theclass contains a method in _special_names, we add it to the namespace
        # with its corresponding method from make_method.
        namespace = {}
        for name in cls._special_names:
            if hasattr(theclass, name):
                namespace[name] = make_method(name)

        # Return a new proxy class that wraps the specified class type, enabling method calls to be intercepted and processed through the proxy mechanism.
        return type(f"{cls.__name__}({theclass.__name__})", (cls,), namespace)

    # ----------------------------------------------------------------------
    def __new__(cls, obj: Any, *args: Any, **kwargs: Any) -> Any:
        """
        Create a new instance of the class, potentially a class proxy.

        This method attempts to use a class proxy cache to find or create
        a proxy class for the given object. If a proxy class already exists
        in the cache for the object's class, it is used; otherwise, a new
        class proxy is created and cached.

        Parameters
        ----------
        cls : type
            The class being instantiated.
        obj : Any
            The object that needs to be proxied.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Any
            A new instance of the class proxy for the given object.
        """
        # Attempting to retrieve the existing class proxy cache from the class's dictionary.
        try:
            cache = cls.__dict__["_class_proxy_cache"]
        # If the cache does not exist, initialize it as an empty dictionary.
        except KeyError:
            cls._class_proxy_cache = cache = {}

        # Attempt to retrieve the proxy class from the cache; if it doesn't exist, create and cache it.
        try:
            theclass = cache[obj.__class__]
        except KeyError:
            cache[obj.__class__] = theclass = cls._create_class_proxy(obj.__class__)

        # Create a new instance of the class proxy for the given object
        return object.__new__(theclass)

    # ----------------------------------------------------------------------
    # This code block defines custom attribute and method handling for a proxied object.
    # It overrides attribute access, deletion, assignment, and several special methods
    # to delegate these operations to the actual proxied object.

    def __getattr__(self, attr):
        return getattr(object.__getattribute__(self, "_instance"), attr)

    def __delattr__(self, attr):
        delattr(object.__getattribute__(self, "_obj"), attr)

    def __setattr__(self, attr, value):
        setattr(object.__getattribute__(self, "_obj"), attr, value)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, "_obj"))

    def __str__(self):
        return str(object.__getattribute__(self, "_obj"))

    def __repr__(self):
        return repr(object.__getattribute__(self, "_obj"))

    def __hash__(self):
        return hash(object.__getattribute__(self, "_obj"))


########################################################################
class Proxy:
    """
    A class that represents a proxy for remote method invocation.

    The `Proxy` class facilitates interaction with a remote object as if
    it were local. It supports dynamic attribute access and method
    invocation, enabling seamless distributed computations.
    """

    # ----------------------------------------------------------------------
    def __init__(
        self,
        name: str,
        obj: Optional[Any] = None,
        node: Optional[Any] = None,
        edge: Optional[Any] = None,
        root: bool = False,
        chain: Optional[list[str]] = None,
    ):
        """
        Initialize the `Proxy` instance.

        Parameters
        ----------
        name : str
            The name associated with the proxy.
        obj : Any, optional
            The object to be proxied, by default None.
        node : Any, optional
            The node to which the proxied object belongs, by default None.
        edge : Any, optional
            The edge associated with the proxied object, by default None.
        root : bool, optional
            Flag indicating whether this is the root proxy, by default False.
        chain : list of str, optional
            The chain of attribute names accessed on the proxied object,
            by default None.
        """
        self._name = name
        self._obj = obj
        self._node = node
        self._edge = edge
        self._root = root

        # Initialize the attribute chain for the proxied object.
        # If no chain is provided, use the proxy's name as the initial chain.
        if chain is None:
            self._chain = [name]
        else:
            self._chain = chain

    # ----------------------------------------------------------------------
    def _object(self, obj_chain: list[str]) -> Any:
        """
        Retrieve the object specified by the chain of attribute names.

        This method traverses the chain of attribute names provided as `obj_chain`
        on the proxied object and returns the final object obtained after the traversal.

        Parameters
        ----------
        obj_chain : list of str
            A list of attribute names to be accessed sequentially on the proxied object.

        Returns
        -------
        Any
            The final object obtained after traversing the attribute chain on
            the proxied object.

        Notes
        -----
        This method is primarily used internally by the proxy mechanism to
        dynamically access attributes of the proxied object.
        """
        obj = self._obj
        for obj_ in obj_chain:
            obj = getattr(obj, obj_)
        return obj

    # ----------------------------------------------------------------------
    def __get__(self, instance: Any, owner: Any) -> ObjectProxying:
        """
        Retrieve the proxied attribute for the instance.

        This method is called when an attribute is accessed on an instance
        of a class that contains a Proxy as a descriptor. It returns an
        ObjectProxying instance that acts as an intermediary, allowing
        dynamic retrieval of the proxied object's attribute.

        Parameters
        ----------
        instance : Any
            The instance of the class from which the Proxy is being accessed.
        owner : Any
            The owner class of the instance.

        Returns
        -------
        ObjectProxying
            An ObjectProxying instance that will delegate attribute access
            to the underlying proxied object.
        """
        return ObjectProxying(self._proxy_get, instance, self, self._name)

    # ----------------------------------------------------------------------
    def __getattr__(self, attr: str) -> Any:
        """
        Retrieve the attribute of the proxy object.

        This method appends the requested attribute to the chain of attributes
        being accessed on the proxied object. If the attribute starts with an
        underscore, it will be ignored (commonly used for internal variables).

        Parameters
        ----------
        attr : str
            The name of the attribute to retrieve.

        Returns
        -------
        Any
            The proxy object itself, allowing for chained attribute access.
        """
        if attr.startswith('_'):
            return

        # Append the requested attribute to the chain for dynamic attribute access.
        self._chain.append(attr)

        # Dynamically adds attributes to the Proxy class.
        # When an attribute is accessed, it creates a new Proxy instance for that attribute,
        # setting the appropriate object, node, edge, and chain.
        setattr(
            self.__class__,
            attr,
            Proxy(
                attr,
                obj=self._obj,
                node=self._node,
                edge=self._edge,
                chain=self._chain,
            ),
        )

        # If the requested attribute starts with an underscore, returning None.
        # Otherwise, returning the Proxy itself, facilitating chained attribute access.
        return getattr(self, attr)

    # ----------------------------------------------------------------------
    @property
    def _proxy_get(self) -> Any:
        """
        Retrieve the result of the proxied method call.

        This property method processes the proxied method call using the
        `processor_method` and returns the resulting value.

        Returns
        -------
        Any
            The result of the method invocation on the proxied object.
            The type of the return value depends on the proxied method.
        """
        return self.processor_method()

    # ----------------------------------------------------------------------
    def processor_method(
        self, args: Optional[tuple] = None, kwargs: Optional[dict] = None
    ) -> Any:
        """
        Process the method invocation on the proxied object.

        This method constructs a data dictionary containing details of the method
        invocation, such as the name of the proxy service, the chain of object attributes,
        the positional and keyword arguments, and the timestamp of the request. It then
        performs the asynchronous request to invoke the method on the remote object.

        Parameters
        ----------
        args : tuple, optional
            Positional arguments to be passed to the remote method. Default is None.
        kwargs : dict, optional
            Keyword arguments to be passed to the remote method. Default is None.

        Returns
        -------
        Any
            The result of the method invocation on the remote object. The return value will
            be deserialized from the response obtained from the remote call.

        Raises
        ------
        Exception
            If an exception is encountered during the execution of the remote method, it
            will be raised.

        Notes
        -----
        This method uses asyncio's event loop to synchronously wait for the completion
        of the asynchronous remote request. The execution is blocked until the remote
        method call completes and the result is returned or an exception is raised.
        """
        data = {
            'name': self._chain[0],
            'obj': self._chain[1:],
            'args': args,
            'kwargs': kwargs,
            'timestamp': datetime.now(),
        }

        # This block synchronously executes an asynchronous request to perform a remote method call on the proxied object.
        status, response = asyncio.get_event_loop().run_until_complete(
            self._node._generic_request_udp(
                callback='_call_obj_by_proxy',
                kwargs=data,
                edge=self._edge,
            )
        )

        match status:
            case 'serialized':
                return self._node.deserializer(response)
            case 'exception':
                raise response
            case 'repr':
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
    def proxy(self, module: str) -> Proxy:
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
        edge = asyncio.get_event_loop().run_until_complete(
            self._verify_availability(module=module)
        )

        if edge:
            return Proxy(module, node=self, edge=edge, root=True)
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
            if args or kwargs_:
                # Invoke the resolved method on the proxied object with specified arguments and keyword arguments.
                try:
                    attr = self.proxies[name]._object(obj)(*args, **kwargs_)
                except Exception as e:
                    return 'exception', e

            else:
                # Return the proxied object obtained after traversing the attribute chain.
                try:
                    attr = self.proxies[name]._object(obj)
                except Exception as e:
                    return 'exception', e

            if callable(attr):
                return 'repr', repr(attr)
            else:
                return 'serialized', self.serializer(attr)
        else:
            return 'exception', 'No proxy available for the requested service'

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
        except Exception as e:
            print(e)
            return False
