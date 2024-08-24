"""
=======================================================================
ChaskiRemote: Transparent Python Framework for Remote Method Invocation
=======================================================================

`ChaskiRemote` is a transparent proxy python objects framework for remote method invocation, enabling
transparent interaction with objects across distributed network nodes.
Key classes include `ChaskitProxy` and `ChaskiRemote`, building upon the foundation
provided by the `ChaskiNode` class. These classes facilitate the creation
and management of proxies that allow remote method invocations, making
distributed computations seamless.

Classes
=======
    - *ChaskiObjectProxying*: Provides the ability to create proxy objects for remote method invocation transparently.
    - *ChaskitProxy*: Wraps an object allowing remote method invocation and attribute access as though the object were local.
    - *ChaskiRemote*: Extends `ChaskiNode` to create and manage proxies, enabling remote interactions and method invocations.
"""

import asyncio
import logging
import importlib
import nest_asyncio
from copy import copy
from datetime import datetime
from typing import Any, Optional

from chaski.node import ChaskiNode
from chaski.utils.debug import styled_logger

# Initialize logger for ChaskiRemote operations
logger_remote = styled_logger(logging.getLogger("ChaskiRemote"))

# Apply nested asyncio event loop to allow recursive event loop usage
nest_asyncio.apply()


########################################################################
class ChaskiObjectProxying(object):
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

    # Special method names that will be intercepted by the ChaskiObjectProxying class
    _special_names = [
        '__call__',
        '__reduce__',
        '__reduce_ex__',
        '__repr__',
        'next',
    ]

    # ----------------------------------------------------------------------
    def __init__(self, name: str, obj: Any, instance: Any):
        """
        Initialize an `ChaskiObjectProxying` instance.

        This constructor sets up the necessary attributes for the ChaskiObjectProxying
        instance, allowing it to delegate method calls and attribute access to
        the proxied object.

        Parameters
        ----------
        name : str
            The name of the proxy.
        obj : Any
            The object that is being proxied.
        instance : Any
            The instance of the class that contains the ChaskitProxy as a descriptor.

        Notes
        -----
        The attributes are set using `object.__setattr__` to avoid triggering
        custom `__setattr__` implementations of the proxied object.
        """
        # Using object.__setattr__ to directly set attributes, avoiding any custom __setattr__ in the proxied object
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_instance", instance)
        object.__setattr__(self, "_argskwargs", True)

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
            def method(self, *args, **kwargs):
                # Set _chain attribute of the _instance to restart the chain from the initial element

                if object.__getattribute__(self, '_argskwargs'):

                    try:
                        obj = getattr(
                            object.__getattribute__(self, "_instance"),
                            'processor_method',
                        )(
                            object.__getattribute__(self, "_instance"),
                            args,
                            kwargs,
                        )

                    except Exception as e:
                        if "object is not callable" in str(e):
                            try:
                                obj = getattr(
                                    object.__getattribute__(
                                        self, "_instance"
                                    ),
                                    'processor_method',
                                )()
                            except Exception as e:
                                obj = getattr(
                                    object.__getattribute__(
                                        self, "_instance"
                                    ),
                                    'processor_method',
                                )

                else:
                    obj = getattr(
                        object.__getattribute__(self, "_instance"),
                        'processor_method',
                    )(
                        object.__getattribute__(self, "_instance"),
                        False,
                        False,
                    )

                setattr(
                    object.__getattribute__(self, "_instance"),
                    '_chain',
                    [
                        getattr(
                            object.__getattribute__(self, "_instance"),
                            '_chain',
                        )[0]
                    ],
                )

                return obj

            return method

        # Populate the namespace with method names that should be proxied.
        # If theclass contains a method in _special_names, we add it to the namespace
        # with its corresponding method from make_method.
        namespace = {}
        for name in cls._special_names:
            if hasattr(theclass, name):
                namespace[name] = make_method(name)

        # Return a new proxy class that wraps the specified class type, enabling method
        # calls to be intercepted and processed through the proxy mechanism.
        return type(
            f"{cls.__name__}({theclass.__name__})", (cls,), namespace
        )

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
            # Check if the proxy class for the object's class type exists in the cache;
            # if not, create the proxy class and cache it for future use.
            cache[obj.__class__] = theclass = cls._create_class_proxy(
                obj.__class__
            )

        # Create a new instance of the class proxy for the given object
        return object.__new__(theclass)
        # ins = object.__new__(theclass)
        # theclass.__init__(ins, obj, *args, **kwargs)
        # return ins

    # ----------------------------------------------------------------------
    # This code block defines custom attribute and method handling for a proxied object.
    # It overrides attribute access, deletion, assignment, and several special methods
    # to delegate these operations to the actual proxied object.

    def __getattr__(self, attr):
        object.__setattr__(self, "_argskwargs", False)
        return getattr(object.__getattribute__(self, "_instance"), attr)

    def __delattr__(self, attr):
        delattr(object.__getattribute__(self, "_obj"), attr)

    def __setattr__(self, attr, value):
        setattr(object.__getattribute__(self, "_obj"), attr, value)

    def __nonzero__(self):
        return bool(object.__getattribute__(self, "_obj"))

    def __str__(self):
        return str(object.__getattribute__(self, "_obj"))

    # def __repr__(self):
    #     return repr(object.__getattribute__(self, "_obj"))

    def __hash__(self):
        return hash(object.__getattribute__(self, "_obj"))

    # Operaciones Aritméticas

    def __add__(self, other):
        return object.__getattribute__(self, "_obj") + other

    def __sub__(self, other):
        return object.__getattribute__(self, "_obj") - other

    def __mul__(self, other):
        return object.__getattribute__(self, "_obj") * other

    def __truediv__(self, other):
        return object.__getattribute__(self, "_obj") / other

    def __floordiv__(self, other):
        return object.__getattribute__(self, "_obj") // other

    def __mod__(self, other):
        return object.__getattribute__(self, "_obj") % other

    def __pow__(self, other):
        return object.__getattribute__(self, "_obj") ** other

    def __neg__(self):
        return -object.__getattribute__(self, "_obj")

    def __abs__(self):
        return abs(object.__getattribute__(self, "_obj"))

    def __divmod__(self, other):
        return divmod(object.__getattribute__(self, "_obj"), other)

    def __pos__(self):
        return +object.__getattribute__(self, "_obj")

    # Inverse Arithmetic Operations

    def __radd__(self, other):
        return other + object.__getattribute__(self, "_obj")

    def __rsub__(self, other):
        return other - object.__getattribute__(self, "_obj")

    def __rmul__(self, other):
        return other * object.__getattribute__(self, "_obj")

    def __rtruediv__(self, other):
        return other / object.__getattribute__(self, "_obj")

    def __rfloordiv__(self, other):
        return other // object.__getattribute__(self, "_obj")

    def __rmod__(self, other):
        return other % object.__getattribute__(self, "_obj")

    def __rpow__(self, other):
        return other ** object.__getattribute__(self, "_obj")

    def __rdivmod__(self, other):
        return divmod(other, object.__getattribute__(self, "_obj"))

    # In-Place Arithmetic Operations

    def __iadd__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj += other
        return self

    def __isub__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj -= other
        return self

    def __imul__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj *= other
        return self

    def __itruediv__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj /= other
        return self

    def __ifloordiv__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj //= other
        return self

    def __imod__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj %= other
        return self

    def __ipow__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj **= other
        return self

    # Comparison Operations

    def __eq__(self, other):
        return object.__getattribute__(self, "_obj") == other

    def __ne__(self, other):
        return object.__getattribute__(self, "_obj") != other

    def __lt__(self, other):
        return object.__getattribute__(self, "_obj") < other

    def __le__(self, other):
        return object.__getattribute__(self, "_obj") <= other

    def __gt__(self, other):
        return object.__getattribute__(self, "_obj") > other

    def __ge__(self, other):
        return object.__getattribute__(self, "_obj") >= other

    # Bitwise Operations

    def __and__(self, other):
        return object.__getattribute__(self, "_obj") & other

    def __or__(self, other):
        return object.__getattribute__(self, "_obj") | other

    def __xor__(self, other):
        return object.__getattribute__(self, "_obj") ^ other

    def __lshift__(self, other):
        return object.__getattribute__(self, "_obj") << other

    def __rshift__(self, other):
        return object.__getattribute__(self, "_obj") >> other

    def __invert__(self):
        return ~object.__getattribute__(self, "_obj")

    # Conversion Operations

    def __int__(self):
        return int(object.__getattribute__(self, "_obj"))

    def __bool__(self):
        return bool(object.__getattribute__(self, "_obj"))

    def __float__(self):
        return float(object.__getattribute__(self, "_obj"))

    # Container Operations

    def __getitem__(self, key):
        return object.__getattribute__(self, "_obj")[key]

    def __setitem__(self, key, value):
        object.__getattribute__(self, "_obj")[key] = value

    def __delitem__(self, key):
        del object.__getattribute__(self, "_obj")[key]

    def __contains__(self, item):
        return item in object.__getattribute__(self, "_obj")

    def __len__(self):
        return len(object.__getattribute__(self, "_obj"))

    # Bitwise Operations

    def __iand__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj &= other
        return self

    def __ilshift__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj <<= other
        return self

    def __ior__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj |= other
        return self

    def __irshift__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj >>= other
        return self

    def __ixor__(self, other):
        obj = object.__getattribute__(self, "_obj")
        obj ^= other
        return self

    def __rand__(self, other):
        return other & object.__getattribute__(self, "_obj")

    def __rfloordiv__(self, other):
        return other // object.__getattribute__(self, "_obj")

    def __rlshift__(self, other):
        return other << object.__getattribute__(self, "_obj")

    def __ror__(self, other):
        return other | object.__getattribute__(self, "_obj")

    def __rrshift__(self, other):
        return other >> object.__getattribute__(self, "_obj")

    def __rxor__(self, other):
        return other ^ object.__getattribute__(self, "_obj")

    # Iteration Operations

    def __reversed__(self):
        return reversed(object.__getattribute__(self, "_obj"))

    def __iter__(self):
        return iter(object.__getattribute__(self, "_obj"))

    # ----------------------------------------------------------------------
    @property
    def _(self) -> Any:
        """
        Retrieve the type of the proxied object instance.

        This property method returns the type of the actual proxied object,
        wrapped in a new instance of the same type. It effectively provides
        access to a new instance of the type of the proxied object.

        Returns
        -------
        Any
            A new instance of the type of the proxied object.
        """
        return type(object.__getattribute__(self, "_obj"))(
            object.__getattribute__(self, "_obj")
        )

    # ----------------------------------------------------------------------
    def __enter__(self) -> Any:
        """
        Enter the context manager.

        This method is called when entering a context managed by `ChaskiObjectProxying`.
        It returns the proxied object, allowing it to be used within the context.

        Returns
        -------
        Any
            The proxied object instance.
        """
        return self._

    # ----------------------------------------------------------------------
    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        """
        Exit the context manager.

        This method is called when exiting a context managed by `ChaskiObjectProxying`.
        It performs any necessary cleanup, but in this implementation, it does nothing.
        """
        pass


########################################################################
class ChaskiProxy:
    """
    A class that represents a proxy for remote method invocation.

    The `ChaskitProxy` class facilitates interaction with a remote object as if
    it were local. It supports dynamic attribute access and method
    invocation, enabling seamless distributed computations.
    """

    # ----------------------------------------------------------------------
    def __init__(
        self,
        name: str,
        node: Optional[Any] = None,
        obj: Optional[Any] = None,
        edge: Optional[Any] = None,
        chain: Optional[list[str]] = None,
        root: Optional['ChaskiProxy'] = None,
    ):
        """
        Initialize a `ChaskiProxy` instance.

        Parameters
        ----------
        name : str
            The name of the proxy.
        node : Any, optional
            The node associated with the proxied object. Default is None.
        obj : Any, optional
            The object that is being proxied. Default is None.
        edge : Any, optional
            The edge associated with the proxied object. Default is None.
        chain : list[str], optional
            The chain of attribute names being accessed on the proxied object. Default is None.
        root : ChaskiProxy, optional
            The root proxy reference, if this is a nested proxy. Default is None.

        Notes
        -----
        The `chain` parameter is used to keep track of the sequence of attribute
        names accessed on the proxied object. If no `chain` is provided, the
        proxy's name will be used as the initial chain.
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
    def __repr__(self) -> str:
        """
        Provide a string representation of the ChaskiProxy instance.

        This method returns a string that describes the ChaskiProxy instance, including
        its name, whether it is a remote or local proxy, and the address of the associated node.

        Returns
        -------
        str
            A string representation of the ChaskiProxy instance.

        Notes
        -----
        This string representation is useful for debugging and logging purposes.
        """
        return f"ChaskiProxy({self._name}:{'remote' if self._obj else 'local'}, {self._node.address})"

    # ----------------------------------------------------------------------
    def _reset(self) -> None:
        """
        Reset the attribute chain to its initial state.

        This method reinitializes the `self._chain` attribute, which tracks
        the sequence of attribute accesses on the proxied object. It sets
        the chain back to its initial state, containing only the name of
        the root attribute. This is typically used after a method invocation
        to ensure that subsequent attribute accesses start from the root.
        """
        self._chain = [self._chain[0]]

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
    def __get__(self, instance: Any, owner: Any) -> ChaskiObjectProxying:
        """
        Retrieve the proxied attribute for the instance.

        This method is called when an attribute is accessed on an instance
        of a class that contains a ChaskitProxy as a descriptor. It returns an
        ChaskiObjectProxying instance that acts as an intermediary, allowing
        dynamic retrieval of the proxied object's attribute.

        Parameters
        ----------
        instance : Any
            The instance of the class from which the ChaskitProxy is being accessed.
        owner : Any
            The owner class of the instance.

        Returns
        -------
        ChaskiObjectProxying
            An ChaskiObjectProxying instance that will delegate attribute access
            to the underlying proxied object.
        """
        return ChaskiObjectProxying(
            name=self._name,
            obj=self._proxy_get,
            instance=instance,
        )

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
        # If the requested attribute starts with an underscore, returning None.
        if attr.startswith('_'):
            return None

        # If the proxied object is not yet fully resolved, append the attribute to the chain.
        if not getattr(self, '_obj'):
            setattr(self, '_chain', getattr(self, '_chain') + [attr])

            # If this is the root proxy, create a new ChaskiProxy for the current attribute
            if getattr(self, '_root') is None:
                proxy = ChaskiProxy(
                    name=self._name,
                    node=self._node,
                    edge=self._edge,
                    root=self,
                )
                self._reset()
                return getattr(proxy, attr)

        # Dynamically adds attributes to the ChaskitProxy class.
        # When an attribute is accessed, it creates a new ChaskitProxy instance for that attribute,
        # setting the appropriate object, node, edge, and chain.
        setattr(
            self.__class__,
            attr,
            ChaskiProxy(
                name=attr,
                node=self._node,
                edge=self._edge,
                chain=self._chain,
                root=self,
            ),
        )

        # If the requested attribute starts with an underscore, returning None.
        # Otherwise, returning the ChaskitProxy itself, facilitating chained attribute access.
        return getattr(self, attr)

    # ----------------------------------------------------------------------
    def _cleanup_dynamic_attribute(self, attr) -> None:
        """
        Clean up the attributes of the proxy class.

        This method attempts to remove the specified attribute from the
        proxy class, effectively cleaning up dynamically added attributes.

        Parameters
        ----------
        attr : str
            The name of the attribute to be cleaned up from the proxy class.

        Notes
        -----
        This method uses `delattr` to remove the attribute. If the attribute
        does not exist, the method simply passes without raising an error.
        """
        try:
            delattr(self.__class__, attr)
        except AttributeError:
            pass

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
        self,
        instance=None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
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
            'name': copy(self._chain[0]),
            'obj': copy(self._chain[1:]),
            'args': copy(args),
            'kwargs': copy(kwargs),
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

        # Clean up dynamic attributes added to ChaskiProxy during method calls
        for obj in self._chain[1:]:
            self._cleanup_dynamic_attribute(obj)

        # Process the status and response from the proxied method call.
        # Depending on the status, return the deserialized object, raise an exception,
        # or return a textual representation of the object.
        match status:
            case 'serialized':
                return self._node.deserializer(response)
            case 'exception':
                raise Exception(response)
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
        self.proxy_lock = asyncio.Lock()

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
    def register_module(self, module: str, service: Any) -> None:
        """
        Register a service with a proxy.

        This method registers a service with the node by associating it with a proxy.
        The proxy can then be used to remotely invoke methods on the registered service.

        Parameters
        ----------
        module : str
            The name to associate with the service.
        service : Any
            The service object to register. This object can have methods that will be
            accessible remotely via the proxy.
        """
        self.proxies[module] = ChaskiProxy(
            name=module,
            node=self,
            obj=service,
        )

    # ----------------------------------------------------------------------
    def proxy(self, module: str, edge=None) -> ChaskiProxy:
        """
        Retrieve a proxy object for the specified service name.

        This asynchronous method obtains a proxy associated with a given service name.
        The proxy can be used to remotely invoke methods on the registered service.

        Parameters
        ----------
        module : str
            The name of the service/module to retrieve a proxy for.
        edge : Optional[str]
            The specific edge to connect with for the module's availability. Default is None.

        Returns
        -------
        ChaskitProxy
            The proxy object associated with the specified service name.
        """
        edge = asyncio.get_event_loop().run_until_complete(
            self._verify_availability(module=module, edge=edge)
        )

        if edge:
            return ChaskiProxy(name=module, node=self, edge=edge, root=None)
        else:
            logger_remote.warning(
                f"Module {module} not found in the conected edges"
            )

    #     # ----------------------------------------------------------------------
    #     def geeeet(self, obj, obj_chain):
    #         """"""
    #
    #         for obj_ in obj_chain:
    #             obj = getattr(obj, obj_)
    #         return obj

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

        if args or kwargs_:
            logger_remote.warning(
                f"{self.name}-{timestamp}: Calling {name}.{'.'.join(obj)} with args:{args} kwargs:{kwargs_}"
            )

        if name in self.proxies:

            if (args is None) and (kwargs_ is None):
                try:
                    attr = self.proxies[name]._object(obj)()
                except Exception as e:
                    if "object is not callable" in str(e):
                        attr = self.proxies[name]._object(obj)
                    else:
                        return 'exception', e

            elif (args is False) and (kwargs_ is False):
                try:
                    attr = self.proxies[name]._object(obj)
                except Exception as e:
                    return 'exception', e

            else:
                try:
                    attr = self.proxies[name]._object(obj)(*args, **kwargs_)
                except Exception as e:
                    return 'exception', e

            self.proxies[name]._reset()

            if callable(attr):
                return 'repr', repr(attr)
            else:
                try:
                    return 'serialized', self.serializer(attr)
                except:
                    return 'serialized', repr(attr)
        else:
            return (
                'exception',
                'No proxy available for the requested service',
            )

    # ----------------------------------------------------------------------
    async def _verify_availability(self, module: str, edge=None) -> Any:
        """
        Verify the availability of a specified module across connected nodes.

        This asynchronous method checks if a specified module is available on any of the
        connected edges (nodes) in the network. It sends a generic UDP request to each edge
        to verify if the module is available for remote interaction.

        Parameters
        ----------
        module : str
            The name of the module to check for availability.
        edge : Optional[str]
            The specific edge to check for the module's availability. If not provided, checks all edges.

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
        for edge_ in self.edges:

            if edge:
                if edge != edge_:
                    return

            # Sends a request to verify if the specified module is available on the remote node.
            available = await self._generic_request_udp(
                '_verify_module', data, edge_
            )
            if available:
                return edge_
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
            self.register_module(module, imported_module)

            # Log the registration of the module on the remote node
            logger_remote.warning(f"{self.name}: Registered {module}")
            return True
        except Exception as e:
            logger_remote.error(e)
            return False
