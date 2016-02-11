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

# Disable pylint messages concerning "Too few public methods"
# pylint: disable=R0903


import logging
##import Queue
##import threading

from pika.adapters import blocking_connection_base
import pika.connection

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters import select_connection


LOGGER = logging.getLogger(__name__)


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

    def _cleanup(self):
        """[override base] Clean up members that might inhibit garbage
        collection

        """
        self._impl._ioloop.deactivate_poller()

        super(ThreadedConnection, self)._cleanup()


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

    def connect(self):
        """Replace the connection machinery. `_ProxyConnection` connects to
        AMQP broker indirectly via `_BackgroundConnection`.

        This method is invoked by the base `Connection` class to initiate
        connection-establishment with AMQP broker

        """
        pass

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

    def remove_timeout(self, timer):
        """[override pure virtual] Remove a timer that hasn't been dispatched
        yet

        :param timer: The opaque timer to remove

        """
        self._ioloop.remove_timeout(timer)

    def _adapter_connect(self):
        """[override pure virtual] Subclasses should override to set up the
        outbound socket connection.

        """
        pass

    def _adapter_disconnect(self):
        """[override pure virtual] Subclasses should override this to cause the
        underlying transport (socket) to close.

        """
        pass

    def _flush_outbound(self):
        """[override pure virtual] Adapters should override to flush the
        contents of outbound_buffer out along the socket.

        """
        pass


class _BackgroundConnection(object):
    """`_BackgroundConnection` runs the true AMQP connection instance in a
    background thread. It communicates with `ThreadedConnection` via thread-safe
    queues and an inbound event socket for efficient event-driven I/O.

    """
    pass


class _ServiceProxy(object):
    """Interface to `_BackgroundConnection` for use by `ThreadedConnection`"""

    def __init__(self, queue, notify):
        """
        :param Queue.Queue queue: Thread-safe queue for depositing events
            destined for `_BackgroundConnection`
        :param callable notify: Function for notifying `_BackgroundConnection`
          that an event is available; it has the signature`notify()`

        """
        self._evt_queue = queue
        self._notify = notify

    def send(self, event):
        """Send an event to `_BackgroundConnection`

        :param event: event object destined for client

        """
        self._evt_queue.put(event)
        self._notify()


class _ClientProxy(object):
    """Interface to `ThreadedConnection` for use by `_BackgroundConnection`"""

    def __init__(self, queue):
        """
        :param Queue.Queue queue: Thread-safe queue for depositing events
            destined for the client

        """
        self._evt_queue = queue


    def send(self, event):
        """Send an event to client

        :param event: event object destined for client

        """
        self._evt_queue.put(event)


class _ClientRegEvent(object):
    """Client registration event for registering a client with
    `_BackgroundConnection`.
    """

    def __init__(self, client):
        """
        :param _ClientProxy:

        """
        self.client = client


class _ClientUnregEvent(object):
    """Client de-registration event for unregistering a client with
    `_BackgroundConnection`.
    """

    def __init__(self, client):
        """
        :param _ClientProxy:
        """
        self.client = client


class _ChannelRegEvent(object):
    """Channel registration event for informing `_BackgroundConnection` of the
    association between channel number and client
    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param _ClientProxy:

        """
        self.channel_number = channel_number
        self.client = client


class _ChannelUnregEvent(object):
    """Channel de-registration event for informing `_BackgroundConnection` to
    remove the association between the given channel number and client
    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param _ClientProxy:

        """
        self.channel_number = channel_number
        self.client = client


class _BlockedConnectionRequestEvent(object):
    """Request from a client for receiving "blocked/unblocked" connection
    frames from `_BackgroundConnection`
    """

    def __init__(self, client):
        """
        :param _ClientProxy:

        """
        self.client = client


class _DataToBrokerEvent(object):
    """Container for serialized frames destined for AMQP broker. This type of
    event is dispatched by `ThreadedConnection` to `_BackgroundConnection`.
    """

    def __init__(self, client, buffers):
        """
        :param _ClientProxy:
        :param buffers: sequence of serialized frames destined for AMQP broker

        """
        self.client = client
        self.buffers = buffers


class _FramesToClientEvent(object):
    """Container for frames destined for client. This type of event is
    dispatched by `_BackgroundConnection` to `ThreadedConnection`
    """

    def __init__(self, frames):
        """
        :param frames: sequence of `pika.spec` frames destined for client

        """
        self.frames = frames
