"""`BackgroundConnectionService` runs the true AMQP connection instance in
a background thread. It communicates with `ThreadedConnection` via
thread-safe queues and an inbound event socket for efficient event-driven
I/O.

"""

# Disable "access to protected member" warnings: this wrapper implementation is
# a friend of those instances
# pylint: disable=W0212

# Suppress pylint messages concerning "Too few public methods"
# pylint: disable=R0903

# Suppress pylint message concerning "Too many instance attributes"
# pylint: disable=R0902

# Suppress pylint messages concerning "Invalid method name"
# pylint: disable=C0103


import collections
import copy
import errno
import logging
import os
import Queue
import sys
import threading

from pika.adapters.subclass_utils import verify_overrides
from pika.adapters.subclass_utils import overrides_instance_method
from pika.compat import xrange  # pylint: disable=W0622
import pika.exceptions
import pika.frame
import pika.spec

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters import select_connection


LOGGER = logging.getLogger(__name__)


class BackgroundConnectionService(threading.Thread):
    """`BackgroundConnectionService` runs the true AMQP connection instance in
    a background thread. It communicates with `ThreadedConnection` via
    thread-safe queues and an inbound event socket for efficient event-driven
    I/O.

    """

    def __init__(self, parameters):
        """
        :param pika.connection.Parameters: parameters for establishing
            connection; None for default Pika connection parameters.

        """
        super(BackgroundConnectionService, self).__init__()

        self._conn_parameters = parameters

        # We're shutting down, don't process any more client events
        self._shutting_down = False

        # Will hold SelectConnection instance
        self._conn = None

        # Input event queue
        self._input_queue = Queue.Queue()

        self._attention_lock = threading.Lock()
        self._attention_pending = False
        self._r_attention, self._w_attention = (
            select_connection._PollerBase._get_interrupt_pair())

        # Exception object representing reason for establishment failure
        self._conn_end_exc = None

        # Connetion.Blocked frame when connection is in blocked state; None
        # when connection is not blocked. `pika.frame.Method` having `method`
        # member of type `pika.spec.Connection.Blocked`
        self._conn_blocked_frame = None

        # Exception that caused service to terminate
        self._service_exit_exc = None

        # Registered ClientProxy instances
        self._clients = set()

        # Connection.Blocked subscribers: registered ClientProxy instances that
        # subscribed to receive Connection.Blocked frames
        self._conn_blocked_subscribers = set()

        # Mapping of registered channel numbers to their ClientProxy instances
        # for dispatching of frames
        self._channel_to_client_map = dict()

        # Mapping of ClientProxy instances to the lists of their registered
        # channel numbers (for cleanup)
        self._client_to_channels_map = collections.defaultdict(set)

        # Enable process to exit even if our thread is still running
        self.setDaemon(True)


    def start(self):
        """Start the connection service in a background thread

        :returns: ServiceProxy instance for interacting with the service
        :rtype: ServiceProxy
        """
        super(BackgroundConnectionService, self).start()

        return ServiceProxy(self._enqueue_event)

    def run(self):
        """Entry point for background thread"""

        try:
            self._run_service()
        except:
            LOGGER.exception('_run_service exited with exception')
            self._service_exit_exc = sys.exc_info()[1]
            raise

    def _run_service(self):
        """Execute the service; called from background thread

        """
        self._conn = _GatewayConnection(
            parameters=self._conn_parameters,
            on_open_callback=self._on_connection_established,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed)

        self._conn.ioloop.add_handler(self._r_attention.fileno(),
                                      self._on_attention,
                                      select_connection.READ)

        self._conn.add_on_connection_blocked_callback(
            self._on_connection_blocked)
        self._conn.add_on_connection_unblocked_callback(
            self._on_connection_unblocked)

        # Run ioloop; this won't return until we decide to stop the loop
        self._conn.ioloop.start()

    def _enqueue_event(self, event):
        """Implementation of `ServiceProxy.dispatch`"""
        self._input_queue.put(event)

        # Wake up attention handler, while avoiding unnecessary I/O
        if not self._attention_pending:
            with self._attention_lock:
                if not self._attention_pending:
                    self._attention_pending = True

                    try:
                        # Send byte to interrupt the poll loop
                        os.write(self._w_attention.fileno(), b'X')
                    except OSError as err:
                        if err.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                            raise

    def _on_attention(self, interrupt_fd, events):  # pylint: disable=W0613
        """Called by connection's ioloop when read is pending on our "attention"
        socket. Process incoming events.

        :param int interrupt_fd: The file descriptor to read from
        :param int events: (unused) The events generated for this fd
        """

        # Purge data from attention socket so we don't get stuck with endless
        # callbacks
        try:
            os.read(interrupt_fd, 512)
        except OSError as err:
            if err.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise

        self._attention_pending = False

        # Process the number of events presently in the input queue to avoid
        # starving the connection's I/O. Since we're the only consumer, qsize is
        # reliable for our purposes.
        for _ in xrange(self._input_queue.qsize()):
            event = self._input_queue.get(block=False)

            if self._shutting_down:
                continue

            if isinstance(event, ClientOpEventFamily):
                self._on_client_op_event(event)
            elif isinstance(event, ChannelOpEventFamily):
                self._on_channel_op_event(event)
            elif isinstance(event, BlockedConnectionSubscribeEvent):
                self._on_blocked_connection_sub_event(event)
            elif isinstance(event, FramesToBrokerEvent):
                self._on_frames_to_broker_event(event)

            else:
                raise TypeError('Unexpected event type {!r}'.format(event))

    def _on_client_op_event(self, event):
        """Handle client reg/unreg events

        :param ClientOpEventFamily event:

        """
        client = event.client

        if isinstance(event, ClientRegEvent):
            assert client not in self._clients, (
                '{!r} already registered'.format(client))

            self._clients.add(client)

            LOGGER.info('Registered client %r', client)

        elif isinstance(event, ClientUnregEvent):
            assert client in self._clients, (
                '{!r} is not registered'.format(client))

            self._clients.remove(client)
            self._conn_blocked_subscribers.discard(client)

            for channel_number in self._client_to_channels_map[client]:
                del self._channel_to_client_map[channel_number]

            del self._client_to_channels_map[client]

            LOGGER.info('Unregistered client %r', client)

            if not self._clients:
                # No more clients, it's time to close the connection

                LOGGER.info('No more clients, closing/shutting-down')

                assert not self._conn_blocked_subscribers, (
                    self._conn_blocked_subscribers)

                assert not self._client_to_channels_map, (
                    self._client_to_channels_map)

                assert not self._channel_to_client_map, (
                    self._channel_to_client_map)

                self._shutting_down = True

                if self._conn.is_open:
                    self._conn.close()
                elif self._conn.is_closed:
                    self._conn.ioloop.stop()

        else:
            raise TypeError('Unexpected event type {!r}'.format(event))

    def _on_channel_op_event(self, event):
        """Handle channel reg/unreg events

        :param ChannelOpEventFamily event:

        """
        client = event.client
        channel_number = event.channel_number

        assert client in self._clients, '{!r} unknown'.format(client)

        if isinstance(event, ChannelRegEvent):
            assert channel_number not in self._channel_to_client_map, (
                client, channel_number,
                self._channel_to_client_map[channel_number])
            self._channel_to_client_map[channel_number] = client
            self._client_to_channels_map[client].add(channel_number)

            LOGGER.info('Registered channel %s with %r', channel_number, client)

        elif isinstance(event, ChannelUnregEvent):
            assert channel_number in self._channel_to_client_map, (
                client, channel_number)

            del self._channel_to_client_map[channel_number]
            self._client_to_channels_map[client].remove(channel_number)

            LOGGER.info('Unregistered channel %s from %r', channel_number,
                        client)

        else:
            raise TypeError('Unexpected event type {!r}'.format(event))

    def _on_blocked_connection_sub_event(self, event):
        """Handle BlockedConnectionSubscribeEvent

        :param BlockedConnectionSubscribeEvent event:

        """
        client = event.client

        assert client in self._clients, client
        assert client not in self._conn_blocked_subscribers, client

        self._conn_blocked_subscribers.add(client)

        # Notify client if connection is already in blocked state
        if not self._conn.is_closed and self._conn_blocked_frame is not None:
            self._send_blocked_state_to_client(client, self._conn_blocked_frame)

        LOGGER.info('Subscribed %r to Connection.Blocked/Unblocked', client)

    def _on_connection_established(self, connection):  # pylint: disable=W0613
        """Called by `SelectConnection `when connection-establishment succeeds

        :param SelectConnection connection:

        """
        # Notify clients
        # TODO Resume protocol setup with clients that requested it
        pass

    def _on_connection_open_error(self, connection, error):  # pylint: disable=W0613
        """Called by `SelectConnection` if the connection can't be established

        :param SelectConnection connection:
        :param error: str or exception

        """
        if not isinstance(error, Exception):
            error = pika.exceptions.AMQPConnectionError(repr(error))

        self._conn_end_exc = error

        # Notify clients
        # TODO Notify clients that are in protocol state about open error

    def _on_connection_closed(self, connection, reason_code, reason_text):  # pylint: disable=W0613
        """Called when closing of connection completes, including when
        connection-establishment fails (alongside `_on_connection_open_error`)

        :param SelectConnection connection:
        :param int reason_code:
        :param str reason_text:

        """
        # _on_connection_open_error has precedence, since _on_connection_closed
        # gets called on any disconnect, including after open error callback.
        if self._conn_end_exc is None:
            self._conn_end_exc = pika.exceptions.ConnectionClosed(reason_code,
                                                                  reason_text)
            # Notify clients
            # TODO Notify clients that are in protocol state about conn closed

        if self._shutting_down:
            self._conn.ioloop.stop()

    def _on_connection_blocked(self, method_frame):
        """Handle Connection.Blocked notification from RabbitMQ broker

        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked`

        """
        self._conn_blocked_frame = method_frame

        # Notify clients
        for client in self._conn_blocked_subscribers:
            self._send_blocked_state_to_client(client, method_frame)

    def _on_connection_unblocked(self, method_frame):
        """Handle Connection.Unblocked notification from RabbitMQ broker

        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Unblocked`

        """
        self._conn_blocked_frame = None

        # Notify clients
        for client in self._conn_blocked_subscribers:
            self._send_blocked_state_to_client(client, method_frame)

    def _on_frames_to_broker_event(self, event):
        """Handle `FramesToBrokerEvent`

        :param FramesToBrokerEvent event:

        """
        client = event.client

        for frame in event.frames:
            LOGGER.debug('_on_frames_to_broker_event: %r from client %r',
                         frame, client)

            if client.conn_state != ClientProxy.CONN_STATE_HANDSHAKE_DONE:
                if isinstance(frame, pika.frame.ProtocolHeader):
                    client.conn_state = ClientProxy.CONN_STATE_HANDSHAKE
                    # TODO Send spec.Connection.Start if connected
                    # TODO Send spec.Connection.Close if connection failed
                    continue
                elif isinstance(frame, pika.frame.Method):
                    if isinstance(frame.method, pika.spec.Connection.StartOk):
                        # TODO Send spec.Connection.Tune
                        continue
                    elif isinstance(frame.method, pika.spec.Connection.TuneOk):
                        # Nothing to do here; expect spec.Connection.Open next
                        continue
                    elif isinstance(frame.method, pika.spec.Connection.Open):
                        # TODO Send spec.Connection.OpenOk
                        client.conn_state = (
                            ClientProxy.CONN_STATE_HANDSHAKE_DONE)
                        continue

                raise TypeError('Unexpected frame during connection setup from '
                                'client {!r}: {!r}'.format(client, frame))

            # Forward all other frames to broker
            self._conn._append_outbound_frame(frame)


        self._conn._flush_outbound()

        if self._conn.params.backpressure_detection:
            self._conn._detect_backpressure()

    @staticmethod
    def _send_blocked_state_to_client(client, method_frame):
        """Send `FramesToClientEvent` with the given method frame to client

        :param ClientProxy client:
        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked` or
            `pika.spec.Connection.Unblocked`

        """
        client.send(FramesToClientEvent([method_frame]))


@verify_overrides
class _GatewayConnection(select_connection.SelectConnection):
    """Connection that serves as the gateway to the broker"""

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback):
        """
        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)

        """
        super(_GatewayConnection, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            stop_ioloop_on_close=False)


class ServiceProxy(object):
    """Interface to `BackgroundConnectionService` for use by
    `ThreadedConnection`
    """

    def __init__(self, dispatch):
        """
        :param callable dispatch: Function for sending an event to
            `BackgroundConnectionService`; it has the signature`dispatch(event)`

        """
        self.dispatch = dispatch


class ClientProxy(object):
    """Interface to `ThreadedConnection` for use by
    `BackgroundConnectionService`

    """

    CONN_STATE_PENDING = 0  # expecting pika.frame.ProtocolHeader
    CONN_STATE_HANDSHAKE = 1  # Performing connection handshake
    CONN_STATE_HANDSHAKE_DONE = 2

    def __init__(self, queue):
        """
        :param Queue.Queue queue: Thread-safe queue for depositing events
            destined for the client

        """
        self._evt_queue = queue

        # Connection state for use only by `BackgroundConnectionProxy` to track
        # connection-establishment: CONN_STATE_*
        self.conn_state = self.CONN_STATE_PENDING

    def send(self, event):
        """Send an event to client

        :param event: event object destined for client

        """
        self._evt_queue.put(event)


class RpcEventFamily(object):
    """Base class for service-destined event that solicits a reply"""

    def __init__(self, on_result_rx):
        """
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.
        """
        self.on_result_rx = on_result_rx

        # Result to be set by responder
        self.result = None


class AsyncEventFamily(object):
    """Base class for client or service-destined event that has no reply"""
    pass


class ConnectionStateEvent(object):
    """Base class for client-destined events about connection state"""
    pass


class ClientOpEventFamily(object):
    """Designates subclass as client operation event """
    pass


class ClientRegEvent(ClientOpEventFamily, AsyncEventFamily):
    """Client registration event for registering a client with
    `BackgroundConnectionService`.
    """

    def __init__(self, client):
        """
        :param ClientProxy:

        """
        self.client = client


class ClientUnregEvent(ClientOpEventFamily, AsyncEventFamily):
    """Client de-registration event for unregistering a client with
    `BackgroundConnectionService`.
    """

    def __init__(self, client):
        """
        :param ClientProxy:

        """
        self.client = client


class ChannelOpEventFamily(object):
    """Designates subclass as channel operation event """
    pass


class ChannelRegEvent(ChannelOpEventFamily, AsyncEventFamily):
    """Channel registration event for informing `BackgroundConnectionService`
    of the association between channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.client = client
        self.channel_number = channel_number


class ChannelUnregEvent(ChannelOpEventFamily, AsyncEventFamily):
    """Channel de-registration event for informing
    `BackgroundConnectionService` to remove the association between the given
    channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.client = client
        self.channel_number = channel_number


class BlockedConnectionSubscribeEvent(AsyncEventFamily):
    """Request from a client for receiving "blocked/unblocked" connection
    frames from `BackgroundConnectionService`
    """

    def __init__(self, client):
        """
        :param ClientProxy:

        """
        self.client = client


class FramesToBrokerEvent(RpcEventFamily):
    """Container for serialized frames destined for AMQP broker. This type of
    event is dispatched by `ThreadedConnection` to
    `BackgroundConnectionService`.

    """

    def __init__(self, client, frames, on_result_rx):
        """
        :param ClientProxy:
        :param frames: sequence of frames destined for AMQP broker
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.

        """
        super(FramesToBrokerEvent, self).__init__(on_result_rx)
        self.client = client
        self.frames = frames


class FramesToClientEvent(AsyncEventFamily):
    """Container for frames destined for client. This type of event is
    dispatched by `BackgroundConnectionService` to `ThreadedConnection`
    """

    def __init__(self, frames):
        """
        :param frames: sequence of `pika.spec` frames destined for client

        """
        self.frames = frames
