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

# Suppress pylint messages concerning "Too few public methods"
# pylint: disable=R0903


import copy
import logging
import Queue
import threading
import time

from pika.adapters import blocking_connection_base
import pika.channel
import pika.connection
import pika.exceptions

from pika.adapters import bg_connection_service


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

    `ThreadedConnection`'s engine consits of an asynchronous connection proxy
    based on `pika.connection.Connection` that executes in the context of user's
    thread and a true AMQP connection based on `SelectConnection` that executes
    on a separate thread. `ThreadedConnection` shuttles channel-level AMQP
    messages between the connection proxy's channels and the true connection
    using event-driven semantics.

    Communication with the background connection is facilitated by thread-safe
    queues and signaling socket.
    """
    # Suppress warnings concerning "Access to a protected member"
    # pylint: disable=W0212

    def __init__(self, parameters=None, **kwargs):
        """Create a new instance of the `ThreadedConnection` object.

        :param pika.connection.Parameters parameters: Connection parameters;
            None for default parameters.
        :param bg_connection_service.ServiceProxy service_proxy: private arg via
            kwargs only. `ThreadedConnection.clone` passes this arg to
            initialize the cloned connection
        :param _ThreadsafeChannelNumberAllocator channel_number_allocator:
            private arg via kwargs only.  `ThreadedConnection.clone` passes this
            arg to initialize the cloned connection

        :raises AMQPConnectionError:

        """
        super(ThreadedConnection, self).__init__()

        self._gw_event_rx_queue = Queue.Queue()
        self._client_proxy = bg_connection_service.ClientProxy(
            self._gw_event_rx_queue)

        self._service_proxy = kwargs.pop('service_proxy')
        self._channel_number_allocator = kwargs.pop('channel_number_allocator')

        if kwargs:
            raise TypeError('ThreadedConnection received unexpected kwargs {!r}'
                            .format(kwargs))

        if self._service_proxy is None:
            assert self._channel_number_allocator is None, (
                repr(self._channel_number_allocator))

            # Start Connection Gateway
            self._service_proxy = (
                bg_connection_service.BackgroundConnectionService(
                    parameters=copy.deepcopy(parameters)).start())

        # Initiate client connection setup
        self._impl = _ConnectionProxy(
            parameters=parameters,
            on_open_callback=self._opened_result.set_value_once,
            on_open_error_callback=self._open_error_result.set_value_once,
            on_close_callback=self._closed_result.set_value_once,
            client_proxy=self._client_proxy,
            service_proxy=self._service_proxy)

        self._process_io_for_connection_setup()

        if self._channel_number_allocator is None:
            self._channel_number_allocator = _ThreadsafeChannelNumberAllocator(
                self._impl.params.channel_max or pika.channel.MAX_CHANNELS)

    @overrides_instance_method
    def _manage_io(self, *waiters):
        """[implement pure virtual method] Flush output and process input and
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
        # Process I/O until our completion condition is satisified
        is_done = None
        while True:
            if self._impl._outbound_frames:
                # Outbound frames were added, need to update is_done
                is_done = None

            if is_done is None:
                frames_sent_result = blocking_connection_base._CallbackResult()

                # Dispatch pending frames to Connection Gateway
                if self._impl._outbound_frames:
                    # Dispatch pending outbound frames to Connection Gateway
                    event = bg_connection_service.FramesToBrokerEvent(
                        client=self._client_proxy,
                        frames=self._impl._outbound_frames,
                        on_result_rx=frames_sent_result.signal_once)

                    self._impl._outbound_frames = []

                    self._service_proxy.dispatch(event)
                else:
                    frames_sent_result.signal_once()

                is_done = (
                    lambda:
                    self.is_closed or
                    (frames_sent_result.ready and
                     (not waiters or any(ready() for ready in  waiters))))

            if is_done():
                break

            # Wait for event from Connection Gateway
            try:
                event = self._gw_event_rx_queue.get(
                    timeout=self._impl.ioloop.get_remaining_interval())
            except Queue.Empty:
                pass
            else:
                self._on_event_from_gateway(event)

                # Process all other events that are available right now
                for _ in xrange(self._gw_event_rx_queue.qsize()):
                    self._on_event_from_gateway(
                        self._gw_event_rx_queue.get(block=False))

            self._impl.ioloop.process_timeouts()

    def _on_event_from_gateway(self, event):
        """Process an event received from Connection Gateway

        :param event: event from Connection Gateway
        """
        if isinstance(event, bg_connection_service.RpcEventFamily):
            event.on_result_rx(event.result)
        elif isinstance(event, bg_connection_service.FramesToClientEvent):
            for frame in event.frames:
                # TODO Register Connection for blocked/unblocked
                # TODO and decouple internal blocked/unblocked reg from user reg
                self._impl._process_frame(frame)
        else:
            raise TypeError('Unexpected event type {!r}'.format(event))


@verify_overrides
class _ConnectionProxy(pika.connection.Connection):
    """`_ConnectionProxy` serves as a proxy for the background AMQP connection.
    It provides the asyncronous ("_impl") connection services expected by
    `BlockingConnectionBase`, such as timers, creation of channels, and
    registration of callbacks for connection-level events

    """

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback,
                 client_proxy,
                 service_proxy):
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
        :param bg_connection_service.ClientProxy client_proxy: Connection
            Gateway's interface to client
        :param bg_connection_service.ServiceProxy service_proxy: Client's
            interface to Connection Gateway

        """
        # Initialize ahead of super, because Connection constructor uses adapter
        # services
        self.ioloop = _IOLoop()
        self._client_proxy = client_proxy
        self._service_proxy = service_proxy

        # Frames to be dispatched to Connection Gateway during next
        # ThreadedConnection._manage_io cycle
        self._outbound_frames = []

        super(_ConnectionProxy, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback)

        # Detect if Connection code attempts to add to outbound_buffer directly
        # without going through _send_frame, which we override
        assert not self.outbound_buffer
        self.outbound_buffer = None

##    @overrides_instance_method
##    def connect(self):
##        """[supplement base] Replace the connection machinery.
##        `_ConnectionProxy` connects to AMQP broker indirectly via
##        `BackgroundConnectionService`.
##
##        This method is invoked by the base `Connection` class to initiate
##        connection-establishment with AMQP broker
##
##        """
##        pass
##
##    @overrides_instance_method
##    def _send_connection_close(self, reply_code, reply_text):
##        """[replace base] Send a Connection.Close method frame.
##
##        :param int reply_code: The reason for the close
##        :param str reply_text: The text reason for the close
##
##        """
##        pass
##
##    @overrides_instance_method
##    def add_on_connection_blocked_callback(self, callback_method):
##        """[supplement base] Add a callback to be notified when RabbitMQ has
##        sent a `Connection.Blocked` frame indicating that RabbitMQ is low on
##        resources. Publishers can use this to voluntarily suspend publishing,
##        instead of relying on back pressure throttling. The callback will be
##        passed the `Connection.Blocked` method frame.
##
##        :param method callback_method: Callback to call on `Connection.Blocked`,
##            having the signature callback_method(pika.frame.Method), where the
##            method frame's `method` member is of type
##            `pika.spec.Connection.Blocked`
##
##        """
##        pass
##
##    @overrides_instance_method
##    def add_on_connection_unblocked_callback(self, callback_method):
##        """[supplement base] Add a callback to be notified when RabbitMQ has
##        sent a `Connection.Unblocked` frame letting publishers know it's ok to
##        start publishing again. The callback will be passed the
##        `Connection.Unblocked` method frame.
##
##        :param method callback_method: Callback to call on
##            `Connection.Unblocked`, having the signature
##            callback_method(pika.frame.Method), where the method frame's
##            `method` member is of type `pika.spec.Connection.Unblocked`
##
##        """
##
##    @overrides_instance_method
##    def channel(self, on_open_callback, channel_number=None):
##        """[supplement base] Create a new channel with the next available
##        channel number or pass in a channel number to use. Must be non-zero if
##        you would like to specify but it is recommended that you let Pika manage
##        the channel numbers.
##
##        :param method on_open_callback: The callback when the channel is opened
##        :param int channel_number: The channel number to use, defaults to the
##                                   next available.
##        :rtype: pika.channel.Channel
##
##        """
##        pass

    @overrides_instance_method
    def add_timeout(self, deadline, callback_method):
        """[implement pure virtual] Create a single-shot timer to fire after
        deadline seconds. Do not confuse with Tornado's timeout where you pass
        in the time you want to have your callback called. Only pass in the
        seconds until it's to be called.

        :param float deadline: The number of seconds to wait to call callback
        :param callable callback_method: The callback method with the signature
            callback_method()

        :returns: opaque timer

        """
        return self.ioloop.add_timeout(deadline, callback_method)

    @overrides_instance_method
    def remove_timeout(self, timer):
        """[implement pure virtual] Remove a timer that hasn't been dispatched
        yet

        :param timer: The opaque timer to remove

        """
        self.ioloop.remove_timeout(timer)

    @overrides_instance_method
    def _adapter_connect(self):
        """[implement pure virtual] Subclasses should override to set up the
        outbound socket connection.

        """
        # Register this client with the Connection Gateway
        self._service_proxy.dispatch(
            bg_connection_service.ClientRegEvent(self._client_proxy))

        return None

    @overrides_instance_method
    def _adapter_disconnect(self):
        """[implement pure virtual] Subclasses should override this to cause the
        underlying transport (socket) to close.

        """
        # Unegister this client from the Connection Gateway
        self._service_proxy.dispatch(
            bg_connection_service.ClientUnregEvent(self._client_proxy))

    @overrides_instance_method
    def _flush_outbound(self):
        """[implement pure virtual] Adapters should override to flush the
        contents of outbound_buffer out along the socket.

        """
        return

    @overrides_instance_method
    def _send_frame(self, frame_value):
        """[replace base] Append the frame to the "outbound frames" list to be
        dispatched to the Connection Gateway at the next
        `ThreadedConnection._manage_io` cycle.

        :param frame_value: The frame to write
        :type frame_value:  pika.frame.Frame|pika.frame.ProtocolHeader
        :raises: exceptions.ConnectionClosed

        """
        self._outbound_frames.append(frame_value)

    @overrides_instance_method
    def _send_message(self, channel_number, method_frame, content):
        """[supplement base] Make a deep copy of the messages properties to
        guard against race condition between user code and Connection Gateway.

        :param int channel_number: The channel number for the frame
        :param pika.object.Method method_frame: The method frame to send
        :param tuple content: A content frame, which is tuple of properties and
                              body.

        """
        return super(_ConnectionProxy, self)._send_message(
            channel_number,
            method_frame,
            (copy.deepcopy(content[0]), content[1]))


class _ThreadsafeChannelNumberAllocator(object):
    """Thread-safe channel number allocator"""

    def __init__(self, max_channels):

        self._max_channels = max_channels

        self._lock = threading.Lock()

        # Allocated channel numbers
        self._allocated_channels = set()

    def allocate(self):
        """Reserve a channel number, making it unavailable until it is returned
        back to channel number pool via `_ThreadsafeChannelNumberPool.free`

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

class _Timer(object):
    """Represents a timer created via `_TimerManager`"""

    def __init__(self, timer_mgr, deadline, callback):
        """
        :param _TimerManager timer_mgr:
        :param float deadline: timer expiration as epoch timestamp
        """

        self._timer_mgr = timer_mgr

        self._deadline = deadline

        self._callback = callback

        LOGGER.debug('%r created timer %r with deadline %s and callback %r',
                     self._timer_mgr, self, self._deadline, self._callback)

    def cancel(self):
        """Deactivate the timer; it's an error to cancel a triggered or
        deactivated timer """

        # Suppress warnings concerning "Access to a protected member"
        # pylint: disable=W0212

        if self._timer_mgr is not None:
            self._timer_mgr._cancel_timer(self)
            self._timer_mgr = None
        else:
            LOGGER.error('Attempted to cancel deactivated timer %r', self)


class _TimerManager(object):
    """Manage timers for use in ioloop"""

    # Suppress warnings concerning "Access to a protected member"
    # pylint: disable=W0212

    def __init__(self):
        self._timers = set()
        self._next_timeout = None

    def add_timeout(self, period, callback):
        """Schedule a one-shot timer.

        NOTE: you may cancel the timer before dispatch of the callback. Timer
            Manager cancels the timer upon dispatch of the callback.

        :param float period: The number of seconds from now until expiration
        :param method callback: The callback method, having the signature
            `callback()`

        :rtype: _Timer

        """
        deadline = time.time() + period

        timer = _Timer(timer_mgr=self, deadline=deadline, callback=callback)

        self._timers.add(timer)

        if self._next_timeout is None or deadline < self._next_timeout:
            self._next_timeout = deadline

        LOGGER.debug('add_timeout: added timer %r; period=%s at %s',
                     timer, period, deadline)

        return timer

    def _cancel_timer(self, timer):
        """Cancel the timer

        :param _Timer timer: The timer to cancel

        """
        self._timers.remove(timer)

        if timer._deadline == self._next_timeout:
            self._next_timeout = None

        LOGGER.debug('_cancel_timer: removed %r', timer)

    def get_remaining_interval(self):
        """Get the interval to the next timer expiration

        :returns: number of seconds until next timer expiration; None if there
            are no timers
        :rtype: float

        """
        if self._next_timeout is not None:
            # Compensate in case time was adjusted
            interval = max((self._next_timeout - time.time(), 0))

        elif self._timers:
            self._next_timeout = min(t._deadline for t in self._timers)
            interval = max((self._next_timeout - time.time(), 0))

        else:
            interval = None

        return interval

    def process_timeouts(self):
        """Process pending timeouts, invoking callbacks for those whose time has
        come

        """
        now = time.time()

        to_run = sorted((timer for timer in self._timers
                         if timer._deadline <= now),
                        key=lambda item: item._deadline)

        for timer in to_run:
            if timer._timer_mgr is None:
                # Previous invocation(s) deleted the timer.
                continue

            timer.cancel()

            timer._callback()



class _IOLoop(object):
    """Bare-bones ioloop for `ThreadedConnection`"""

    def __init__(self):
        self._timer_mgr = _TimerManager()

    def add_timeout(self, deadline, callback_method):
        """Schedule a one-shot timer.

        NOTE: you may cancel the timer before dispatch of the callback. _IOLoop
            cancels the timer upon dispatch of the callback.

        :param float period: The number of seconds from now until expiration
        :param method callback: The callback method, having the signature
            `callback()`

        :returns: opaque timer handle

        """
        return self._timer_mgr.add_timeout(deadline, callback)

    def remove_timeout(self, timer):
        """Cancel a timer; may be called only on a timer that hasn't been
        called/cancelled yet.

        :param timer: The timer handle created by `add_timeout`.

        """
        timer.cancel()

    def process_timeouts(self):
        """Process pending timeouts, invoking callbacks for those whose time has
        come

        """
        self._timer_mgr.process_timeouts()

    def get_remaining_interval(self):
        """Get the interval to the next timer expiration

        :returns: number of seconds until next timer expiration; None if there
            are no timers
        :rtype: float

        """
        return self._timer_mgr.get_remaining_interval()
