"""The `ThreadedConnection` adapter module implements blocking semantics on top
of Pika's core AMQP driver. In the basic use-case, it's a drop-in replacement
API-wise for BlockingConnection.

`ThreadedConnection`'s AMQP client connection driver runs in a separate thread,
enabling heartbeats to be processed independent of the application.

Furthermore, `ThreadedConnection` provides a mechanism for sharing a single AMQP
connection by multiple threads via connection cloning.

The user facing classes in the module consist of the
:py:class:`~pika.adapters.threaded_connection.ThreadedConnection`,
and the :class:`~pika.adapters.blocking_connection_base.BlockingChannel`
classes.

"""
# Disable "access to protected member warnings: this wrapper implementation is
# a friend of those instances
# pylint: disable=W0212


import logging
##import Queue
import threading

from pika.adapters import blocking_connection_base
import pika.connection
import pika.exceptions

from pika.adapters import bg_connection_service

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters import select_connection


LOGGER = logging.getLogger(__name__)


def verify_overrides(cls):
    """Class Decorator to verify that methods tagged by
    the `overrides_instance_method` decorator actually override corresonding
    methods in one of the base classes

    :raises TypeError: if an unbound method doesn't override another one.
    """

    for name in dir(cls):
        method = getattr(cls, name)

        if not hasattr(method, 'check_method_override'):
            continue

        if not callable(method):
            raise TypeError('{} is not callable', method)

        if not hasattr(method, '__self__'):
            raise TypeError('{} does not have `__self__` member'.format(method))

        if method.__self__ is not None:
            raise TypeError(
                '{} is not an unbound method: `__self__` {} is not None'
                .format(method, method.__self__))

        for base_cls in method.im_class.__bases__:
            base_method = getattr(base_cls, method.__name__, None)

            if base_method is None:
                continue

            if not callable(base_method):
                raise TypeError('{} attempts to override non-callable {}'
                                .format(method, base_method))

            if not hasattr(base_method, '__self__'):
                raise TypeError(
                    '{}\'s base {} does not have `__self__` member'.format(
                        method, base_method))

            if base_method.__self__ is not None:
                raise TypeError('{}\'s base {} is not an unbound method: '
                                '`__self__` {} is not None'.format(
                                    method, base_method, base_method.__self__))

            break
        else:
            raise TypeError('Nothing to override for {}'.format(method))


    return cls


def overrides_instance_method(func):
    """Method decorator that marks the method for verifying that it overrides
    an instance method. The verification is performed by class decorator
    `verify_overrides`

    :raises TypeError: if func is not an unbound instance method

    """
    func.check_method_override = True

    return func


@verify_overrides
class ThreadedConnection(blocking_connection_base.BlockingConnectionBase):
    """The `ThreadedConnection` adapter module implements blocking semantics on
    top of Pika's core AMQP driver. API-wise, it's a drop-in replacement for
    BlockingConnection. `ThreadedConnection`'s AMQP client connection driver
    runs in a separate thread, enabling heartbeats to be processed independent
    of the application. Furthermore, `ThreadedConnection` provides a mechanism
    for sharing a single AMQP connection by multiple threads via connection
    cloning.

    `ThreadedConnection`'s engine consits of an asynchronous proxy connection
    based on `pika.connection.Connection` that executes in the context of user's
    thread and a true AMQP connection based on `SelectConnection` that executes
    on a separate thread. `ThreadedConnection` shuttles channel-level AMQP
    messages between the proxy connection's channels and the true connection
    using event-driven semantics.

    Communication with the background connection is facilitated by thread-safe
    queues and signaling socket.
    """

    def __init__(self, parameters=None):
        """Create a new instance of the `ThreadedConnection` object.

        :param pika.connection.Parameters parameters: Connection parameters;
            None for default parameters.

        :raises AMQPConnectionError:

        """
        super(ThreadedConnection, self).__init__()

        self._impl = self._establish_amqp_connection(parameters)
        self._impl._ioloop.activate_poller()

        self._process_io_for_connection_setup()

    def _establish_amqp_connection(self, parameters):
        """Establish AMQP connection with broker and set up proxy connection

        :param pika.connection.Parameters parameters: Connection parameters;
            None for default parameters.

        :returns: initialized proxy connection reflecting state of connection
            establishment
        :rtype: _ProxyConnection

        """
        pass

    @overrides_instance_method
    def _manage_io(self, *waiters):
        """ [pure virtual method override] Flush output and process input and
        asyncronous timers while waiting for any of the given  callbacks to
        return true. The wait is unconditionally aborted upon connection-close.
        Otherwise, processing continues until the output is flushed AND at least
        one of the callbacks returns true. If there are no callbacks, then
        processing ends when all output is flushed.

        Conditions for terminating the processing loop:
          connection closed
                OR
          flushed outbound buffer and no waiters
                OR
          flushed outbound buffer and any waiter is ready


        :param waiters: sequence of zero or more callables taking no args and
                        returning true when it's time to stop processing.
                        Their results are OR'ed together.

        """
        pass

    @overrides_instance_method
    def _cleanup(self):
        """[override base] Clean up members that might inhibit garbage
        collection

        """
        self._impl._ioloop.deactivate_poller()

        super(ThreadedConnection, self)._cleanup()


class _ThreadSafeChannelNumberPool(object):
    """Thread-safe channel number allocator"""

    def __init__(self, max_channels):

        self._max_channels = max_channels

        self._lock = threading.Lock()

        # Allocated channel numbers
        self._allocated_channels = set()

    def allocate(self):
        """Reserve a channel number, making it unavailable until it is returned
        back to channel number pool via `_ThreadSafeChannelNumberPool.free`

        :returns: channel number
        :rtype: int

        :raises pika.exceptions.NoFreeChannels: if no more channels are
            available

        """

        channel_number = None

        with self._lock:
            if len(self._allocated_channels) >= self._max_channels:
                raise pika.exceptions.NoFreeChannels()

            for channel_number in xrange(1, len(self._allocated_channels) + 1):
                if channel_number not in self._allocated_channels:
                    break
            else:
                channel_number = len(self._allocated_channels) + 1

            self._allocated_channels.add(channel_number)

        return channel_number

    def free(self, channel_number):
        """Release a channel number, making it available to for allocation

        :param int channel_number: channel number to release; must be in
            reserved set
        :raises KeyError: if `channel_number` is not in reserved set

        """
        with self._lock:
            self._allocated_channels.remove(channel_number)


@verify_overrides
class _ProxyConnection(pika.connection.Connection):
    """`_ProxyConnection` serves as a proxy for the background AMQP connection.
    It provides the asyncronous ("_impl") connection services expected by
    `BlockingConnectionBase`, such as timers, creation of channels, and
    registration of callbacks for connection-level events

    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        """
        Available Parameters classes are the `ConnectionParameters` class and
        `URLParameters` class.

        :param pika.connection.Parameters parameters: Connection parameters
           None for default parameters.
        :param method on_open_callback: Called when the connection is opened
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)

        """
        self._ioloop = select_connection.IOLoop()

        super(_ProxyConnection, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback)

    @overrides_instance_method
    def connect(self):
        """[replace base] Replace the connection machinery. `_ProxyConnection`
        connects to AMQP broker indirectly via `BackgroundConnectionService`.

        This method is invoked by the base `Connection` class to initiate
        connection-establishment with AMQP broker

        """
        pass

    @overrides_instance_method
    def _send_connection_close(self, reply_code, reply_text):
        """[replace base] Send a Connection.Close method frame.

        :param int reply_code: The reason for the close
        :param str reply_text: The text reason for the close

        """
        pass

    @overrides_instance_method
    def add_on_connection_blocked_callback(self, callback_method):
        """[supplement base] Add a callback to be notified when RabbitMQ has
        sent a `Connection.Blocked` frame indicating that RabbitMQ is low on
        resources. Publishers can use this to voluntarily suspend publishing,
        instead of relying on back pressure throttling. The callback will be
        passed the `Connection.Blocked` method frame.

        :param method callback_method: Callback to call on `Connection.Blocked`,
            having the signature callback_method(pika.frame.Method), where the
            method frame's `method` member is of type
            `pika.spec.Connection.Blocked`

        """
        pass

    @overrides_instance_method
    def add_on_connection_unblocked_callback(self, callback_method):
        """[supplement base] Add a callback to be notified when RabbitMQ has
        sent a `Connection.Unblocked` frame letting publishers know it's ok to
        start publishing again. The callback will be passed the
        `Connection.Unblocked` method frame.

        :param method callback_method: Callback to call on
            `Connection.Unblocked`, having the signature
            callback_method(pika.frame.Method), where the method frame's
            `method` member is of type `pika.spec.Connection.Unblocked`

        """

    @overrides_instance_method
    def channel(self, on_open_callback, channel_number=None):
        """[supplement base] Create a new channel with the next available
        channel number or pass in a channel number to use. Must be non-zero if
        you would like to specify but it is recommended that you let Pika manage
        the channel numbers.

        :param method on_open_callback: The callback when the channel is opened
        :param int channel_number: The channel number to use, defaults to the
                                   next available.
        :rtype: pika.channel.Channel

        """
        pass

    @overrides_instance_method
    def add_timeout(self, deadline, callback_method):
        """[override pure virtual] Create a single-shot timer to fire after
        deadline seconds. Do not confuse with Tornado's timeout where you pass
        in the time you want to have your callback called. Only pass in the
        seconds until it's to be called.

        :param float deadline: The number of seconds to wait to call callback
        :param callable callback_method: The callback method with the signature
            callback_method()

        :returns: opaque timer

        """
        return self._ioloop.add_timeout(deadline, callback_method)

    @overrides_instance_method
    def remove_timeout(self, timer):
        """[override pure virtual] Remove a timer that hasn't been dispatched
        yet

        :param timer: The opaque timer to remove

        """
        self._ioloop.remove_timeout(timer)

    @overrides_instance_method
    def _adapter_connect(self):
        """[override pure virtual] Subclasses should override to set up the
        outbound socket connection.

        """
        pass

    @overrides_instance_method
    def _adapter_disconnect(self):
        """[override pure virtual] Subclasses should override this to cause the
        underlying transport (socket) to close.

        """
        pass

    @overrides_instance_method
    def _flush_outbound(self):
        """[override pure virtual] Adapters should override to flush the
        contents of outbound_buffer out along the socket.

        """
        pass