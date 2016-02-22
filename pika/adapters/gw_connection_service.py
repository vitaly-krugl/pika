"""`GatewayConnectionService` runs the true AMQP connection instance in a
background thread. It communicates with `ThreadedConnection` via thread-safe
queues and an inbound event socket for efficient event-driven I/O.

"""

# Suppress pylint messages concerning "Invalid method name"
# pylint: disable=C0103

# Suppress pylint message concerning "Too many instance attributes"
# pylint: disable=R0902

# Suppress pylint messages concerning "Too few public methods"
# pylint: disable=R0903

# Suppress pylint message concerning "Too many arguments"
# pylint: disable=R0913

# Disable "access to protected member" warnings: this wrapper implementation is
# a friend of those instances
# pylint: disable=W0212


import collections
import copy
import errno
import logging
import os
import Queue
import threading
import traceback

from pika.adapters.blocking_connection_base import (
    _UNKNOWN_CLOSE_REASON_CODE,
    _UNEXPECTED_FAILURE_REASON_CODE)
from pika.adapters.subclass_utils import verify_overrides
from pika.adapters.subclass_utils import overrides_instance_method
from pika.compat import xrange  # pylint: disable=W0622
import pika.exceptions
import pika.frame
import pika.spec

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters import select_connection


LOGGER = logging.getLogger(__name__)


class GatewayStoppedError(Exception):
    """Raised by ServiceProxy health check method if the service thread stopped.
    This exception has two args:

    - The first arg (at index=0) is an instance of `AMQPConnectionError` (or its
      subclass) representing the reason for connection-establishment failure.
      None if failure or closing of the connection occurred after successful
      establishment of the connection.
    - The second arg (at index=1) is a two-tuple (code-int, text-str)
      representing the reason for the connection's closing or failure (including
      during connection-establishment).
    """
    pass


class GatewayConnectionService(threading.Thread):
    """`GatewayConnectionService` runs the true AMQP connection instance in a
    background thread. It communicates with `ThreadedConnection` via thread-safe
    queues and an inbound event socket for efficient event-driven I/O.

    """

    def __init__(self, parameters):
        """
        :param pika.connection.Parameters: parameters for establishing
            connection; None for default Pika connection parameters.

        """
        super(GatewayConnectionService, self).__init__()

        self._conn_parameters = parameters

        self._service_proxy = ServiceProxy(service_thread=self,
                                           dispatch=self._enqueue_event_to_gw)

        # We're shutting down, don't process any more events from clients
        self._shutting_down = False

        # Will hold SelectConnection instance
        self._conn = None

        # Input event queue
        self._input_queue = Queue.Queue()

        self._attention_lock = threading.Lock()
        self._attention_pending = False
        self._r_attention, self._w_attention = (
            select_connection._PollerBase._get_interrupt_pair())

        # True if connection establishment completed successfully and will not
        # be changed when connection fails
        self._conn_open_completed = False

        # Exception object representing reason for failure of connection
        # establishment
        self._conn_open_exc = None

        # Two-tuple reason for closing of the connection: (code-int, text-str)
        self._conn_close_reason_pair = None

        # Connetion.Blocked frame when connection is in blocked state; None
        # when connection is not blocked. `pika.frame.Method` having `method`
        # member of type `pika.spec.Connection.Blocked`
        self._conn_blocked_frame = None

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
        super(GatewayConnectionService, self).start()

        return self._service_proxy

    def run(self):
        """Entry point for background thread

        """
        try:
            self._run_service()
        except:
            LOGGER.exception('_run_service failed')
            reason_pair = (_UNEXPECTED_FAILURE_REASON_CODE,
                           ''.join(traceback.format_exc()))

            if self._conn_open_completed:
                open_exc = None
            else:
                open_exc = pika.exceptions.AMQPConnectionError(*reason_pair)

            self._service_proxy._set_service_exit_exception(
                GatewayStoppedError(open_exc, reason_pair))

            raise
        else:
            self._service_proxy._set_service_exit_exception(
                GatewayStoppedError(self._conn_open_exc,
                                    self._conn_close_reason_pair))

            LOGGER.info('_run_service exited: %r',
                        self._service_proxy._service_exc)

    def _run_service(self):
        """Execute the service; called from background thread

        """
        self._conn = _GatewayConnection(
            parameters=self._conn_parameters,
            on_open_callback=self._on_connection_established,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
            channel_to_client_map=self._channel_to_client_map)

        self._conn.ioloop.add_handler(self._r_attention.fileno(),
                                      self._on_attention,
                                      select_connection.READ)

        self._conn.add_on_connection_blocked_callback(
            self._on_connection_blocked)
        self._conn.add_on_connection_unblocked_callback(
            self._on_connection_unblocked)

        if not self._conn.is_closed:
            # Run ioloop; it won't return until we stop the loop upon failure or
            # closing of the connection
            self._conn.ioloop.start()
        else:
            LOGGER.warning('Connection is closed, skipping ioloop')

        # Cleanup
        LOGGER.info('Cleaning up')

        if self._conn_close_reason_pair is None:
            # Fix up connection close reason; presently, Connection doesn't
            # provide this when socket connection fails.
            self._conn_close_reason_pair = (self._conn_open_exc.args[0],
                                            self._conn_open_exc.args[1])

        # Notify registered clients about closing and unregister them.
        for client in list(self._clients):
            self._send_connection_closed_event_to_client(
                client,
                open_exc=self._conn_open_exc,
                close_reason_pair=self._conn_close_reason_pair)

            self._unregister_client(client)

    def _enqueue_event_to_gw(self, event):
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
                LOGGER.debug('Shutting down, so dropping %r from client', event)
                continue

            if isinstance(event, ChannelOpEventFamily):
                self._on_channel_op_event(event)
            elif isinstance(event, BlockedConnectionSubscribeEvent):
                self._on_blocked_connection_sub_event(event)
            elif isinstance(event, FramesToBrokerEvent):
                self._on_frames_to_broker_event(event)

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
        LOGGER.info('%r: connection established', self)

        self._conn_open_completed = True

        # Resume protocol setup with clients that requested it
        for client in self._clients:
            if client.conn_state == ClientProxy.CONN_STATE_HANDSHAKE:
                self._send_connection_start_to_client(client)

    def _on_connection_open_error(self, connection, error):  # pylint: disable=W0613
        """Called by `SelectConnection` if the connection can't be established.
        Request shutdown of the service.

        :param SelectConnection connection:
        :param error: str or exception

        """
        # NOTE Presently, Connection might dispatch this callback before its
        # constructor returns, so self._conn might be uninitialized at this time

        # NOTE Presently, when socket connection fails, Connection calls this
        # callback, but not _on_connection_closed. Upon connection failures
        # after the socket is connected, both callbacks are called

        LOGGER.error('Gateway AMQP connection setup failed: %r', error)

        # Save the error
        if not isinstance(error, pika.exceptions.AMQPConnectionError):
            error = pika.exceptions.AMQPConnectionError(
                _UNKNOWN_CLOSE_REASON_CODE,
                repr(error))

        self._conn_open_exc = error

        # Request shutdown of the service
        self._request_shutdown(reason_code=None, reason_text=None)


    def _on_connection_closed(self, connection, reason_code, reason_text):  # pylint: disable=W0613
        """Called when closing of connection completes, including in some cases
        when connection-establishment fails (alongside
        `_on_connection_open_error`).

        - Notify and unregiseter all registered clients, which triggers request
          to stop the ioloop.

        :param SelectConnection connection:
        :param int reason_code:
        :param str reason_text:

        """
        if reason_code == 200:
            LOGGER.info('Gateway AMQP connection closed: (%s) %s', reason_code,
                        reason_text)
        else:
            LOGGER.error('Gateway AMQP connection closed: (%s) %s', reason_code,
                         reason_text)

        self._conn_close_reason_pair = (reason_code, reason_text)

        # Request shutdown of the service
        self._request_shutdown(reason_code=None, reason_text=None)

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

        num_frames_appended = 0

        for frame in event.frames:
            LOGGER.debug('_on_frames_to_broker_event: %r from client %r',
                         frame, client)

            if client.conn_state == ClientProxy.CONN_STATE_CLIENT_CLOSED:
                raise ValueError(
                    'Unexpected frame from {!r} in CLOSED state: {!r}'.format(
                        client, frame))

            if client.conn_state < ClientProxy.CONN_STATE_HANDSHAKE_DONE:
                if isinstance(frame, pika.frame.ProtocolHeader):
                    client.conn_state = ClientProxy.CONN_STATE_HANDSHAKE
                    self._register_client(client)

                    # Send spec.Connection.Start if connected; otherwise, it
                    # will be sent once connection with broker is established
                    if self._conn.is_open:
                        self._send_connection_start_to_client(client)
                    continue
                elif isinstance(frame, pika.frame.Method):
                    if isinstance(frame.method, pika.spec.Connection.StartOk):
                        self._send_connection_tune_to_client(client)
                        continue
                    elif isinstance(frame.method, pika.spec.Connection.TuneOk):
                        # Nothing to do here; expect spec.Connection.Open next
                        continue
                    elif isinstance(frame.method, pika.spec.Connection.Open):
                        client.conn_state = (
                            ClientProxy.CONN_STATE_HANDSHAKE_DONE)
                        self._send_connection_open_ok_to_client(client)
                        continue

                raise TypeError('Unexpected frame during connection setup from '
                                'client {!r}: {!r}'.format(client, frame))

            if (isinstance(frame, pika.frame.Method) and
                    isinstance(frame.method, pika.spec.Connection.Close)):
                # This client wants to close
                LOGGER.info('%r is closing via %r', client, frame)
                client.conn_state = ClientProxy.CONN_STATE_CLIENT_CLOSED

                # If it's a forced-close request or the last client, initiate
                # service shutdown
                if client.force_close or len(self._clients) == 1:
                    self._request_shutdown(reason_code=frame.method.reply_code,
                                           reason_text=frame.method.reply_text)

                self._send_connection_close_ok_to_client(client)

                self._unregister_client(client)
                continue

            # Forward all other frames to broker

            self._conn._append_outbound_frame(frame)
            num_frames_appended += 1


        event.frames = None  # facilitate timely garbage collection

        if num_frames_appended:
            self._conn.appended_frames_from_event(event)
        else:
            # Let client know that all frames have been disposed
            client.dispatch(event)

    def _register_client(self, client):
        """Register new client

        :param ClientProxy client:

        """
        assert client not in self._clients, (
            '{!r} already registered'.format(client))

        self._clients.add(client)

        LOGGER.info('Registered client %r', client)

    def _unregister_client(self, client):
        """Unregister a previously-registered client.

        :param ClientProxy client:

        :raises AssertionError:

        """
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

    def _request_shutdown(self, reason_code, reason_text):
        """Request service to shut down

        :param reason_code: Integer reason code to pass to `Connection.close`
          or None if connection is already closed
        :param reason_text: Reason string to pass to `Connection.close`
          or None if connection is already closed

        """
        if not self._shutting_down:
            # Block processing of events from clients
            self._shutting_down = True

            LOGGER.info('%r: Initiating service shut-down', self)
        else:
            LOGGER.info('%r: Continuing service shut-down', self)


        # NOTE: self._conn might not be set up when called during connection
        # establishment failure; presently, Connection constructor might invoke
        # a callback before the constructor returns.
        if self._conn is not None:
            # Close the AMQP connection
            if self._conn.is_closed:
                LOGGER.info('Requesting ioloop-stop')
                self._conn.ioloop.stop()
            else:
                LOGGER.info('Requesting connection-close')
                assert reason_code is not None
                assert reason_text is not None
                self._conn.close(reply_code=reason_code, reply_text=reason_text)

    @staticmethod
    def _send_blocked_state_to_client(client, method_frame):
        """Send `FramesToClientEvent` with the given method frame to client

        :param ClientProxy client:
        :param pika.frame.Method method_frame: method frame having `method`
            member of type `pika.spec.Connection.Blocked` or
            `pika.spec.Connection.Unblocked`

        """
        client.dispatch(FramesToClientEvent([method_frame]))

    @staticmethod
    def _send_connection_closed_event_to_client(client, open_exc,
                                                close_reason_pair):
        """Dispatch `ConnectionClosedEvent` to client

        :param open_exc: pika.exception.AMQPConnectionError object if
            failure occurred during connection-establishment, None if failure or
            closing of connection occurred after successful connection
            establishment.
        :param tuple close_reason_pair: a two-tuple of (code-int, text-str)
            representing the reason for connection's failure (including
            establishment failure) or closing.

        """
        event = ConnectionClosedEvent(open_exc=open_exc,
                                      close_reason_pair=close_reason_pair)
        LOGGER.debug('Sending %r to %r', event, client)
        client.dispatch(event)

    @staticmethod
    def _send_connection_close_ok_to_client(client):
        """Send Connection.CloseOk to client

        :param ClientProxy client:

        """
        frame = pika.frame.Method(0, pika.spec.Connection.CloseOk())
        LOGGER.debug('Sending %r to %r', frame, client)
        client.dispatch(FramesToClientEvent([frame]))

    def _send_connection_open_ok_to_client(self, client):
        """Send spec.Connection.OpenOk to client

        :param ClientProxy client:
        """
        frame = self._conn._gw_connection_open_ok_frame
        LOGGER.debug('Sending %r to %r', frame, client)
        client.dispatch(FramesToClientEvent([copy.deepcopy(frame)]))

    def _send_connection_start_to_client(self, client):
        """Send spec.Connection.Start to client

        :param ClientProxy client:
        """
        frame = self._conn._gw_connection_start_frame
        LOGGER.debug('Sending %r to %r', frame, client)
        client.dispatch(FramesToClientEvent([copy.deepcopy(frame)]))

    def _send_connection_tune_to_client(self, client):
        """Send spec.Connection.Tune to client

        :param ClientProxy client:
        """
        frame = self._conn._gw_connection_tune_frame
        LOGGER.debug('Sending %r to %r', frame, client)
        client.dispatch(FramesToClientEvent([copy.deepcopy(frame)]))


@verify_overrides
class _GatewayConnection(select_connection.SelectConnection):
    """Connection that serves as the gateway to the broker"""

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback,
                 channel_to_client_map):
        """
        :param pika.connection.Parameters parameters: Connection parameters
        :param method on_open_callback: Method to call on connection open
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param dict channel_to_client_map: mapping of channel numbers to their
            ClientProxy instances

        """
        # TODO Clean up references on termination to help with gc

        # Base class may start streaming from the scope of its constructor, so
        # we need to init self ahead of super

        self._gw_channel_to_client_map = channel_to_client_map

        self._gw_tx_frames_streamed = 0
        self._gw_outbound_event_markers = collections.deque()

        # Captured connection setup frames from broker for use suring connection
        # setup with client's connection proxy
        self._gw_connection_start_frame = None
        self._gw_connection_tune_frame = None
        self._gw_connection_open_ok_frame = None

        # Our `_deliver_frame_to_channel` override saves incoming channel frames
        # here for dispatch by our `_on_data_available` override. They are
        # stored in incoming order grouped by ClientProxy
        self._gw_incoming_client_frames = collections.defaultdict(list)

        super(_GatewayConnection, self).__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            stop_ioloop_on_close=False)

    def appended_frames_from_event(self, event):
        """ Called by `GatewayConnectionService`. We will schedule buffered data
        for writing to socket and cache the given event to be sent back to
        client when the currently-buffered outbound frames have been streamed to
        the socket.

        :param FramesToBrokerEvent event: event to be sent to client after
            currently bufferred frames have been streamed
        """
        # Note to send event to client when the streamed frame counter reaches
        # the buffered frame counter
        self._gw_outbound_event_markers.append((self.tx_frames_buffered, event))

        self._flush_outbound()

        if self.params.backpressure_detection:
            self._detect_backpressure()

    @overrides_instance_method
    def _on_connection_open_ok(self, method_frame):
        """[supplement base] Capture `Connection.OpenOk` method frame from
        broker for use during connection set-up with client's Connection Proxy

        """
        self._gw_connection_open_ok_frame = copy.deepcopy(method_frame)
        return super(_GatewayConnection, self)._on_connection_open_ok(
            method_frame)

    @overrides_instance_method
    def _on_connection_start(self, method_frame):
        """[supplement base] Capture `Connection.Start` method frame from broker
        for use during connection set-up with client's Connection Proxy

        """
        self._gw_connection_start_frame = copy.deepcopy(method_frame)
        return super(_GatewayConnection, self)._on_connection_start(
            method_frame)

    @overrides_instance_method
    def _on_connection_tune(self, method_frame):
        """[supplement base] Capture `Connection.Tune` method frame from broker
        for use during connection set-up with client's Connection Proxy

        """
        self._gw_connection_tune_frame = copy.deepcopy(method_frame)
        return super(_GatewayConnection, self)._on_connection_tune(method_frame)


    @overrides_instance_method
    def _deliver_frame_to_channel(self, frame):
        """[replace base] Cache channel frames in incoming order, grouped by
        ClientProxy. This is called in the scope of `_on_data_available`. Our
        `_on_data_available` method will dispatch them to clients

        """
        try:
            client = self._gw_channel_to_client_map[frame.channel_number]
        except KeyError:
            LOGGER.error('No client for incoming frame %r', frame)
            return

        self._gw_incoming_client_frames[client].append(frame)
        LOGGER.debug('Caching %r for %r', frame, client)

    @overrides_instance_method
    def _on_data_available(self, data_in):
        """[supplement base] Dispatch client-bound frames to corresponding
        clients

        """

        try:
            return super(_GatewayConnection, self)._on_data_available(data_in)
        finally:
            while self._gw_incoming_client_frames:
                client, frames = self._gw_incoming_client_frames.popitem()
                LOGGER.debug('Dispatching %i frames to %r', len(frames), client)
                client.dispatch(FramesToClientEvent(frames))


    @overrides_instance_method
    def _handle_write(self):
        """[supplement base] Track streamed frames and dispatch event markers
        to clients when their streamed frame thresholds are reached
        """
        num_outbound_frames_before = len(self.outbound_buffer)

        try:
            return super(_GatewayConnection, self)._handle_write()
        finally:
            # Update streamed frame counter and dispatch ready markers
            self._gw_tx_frames_streamed += (num_outbound_frames_before -
                                            len(self.outbound_buffer))
            while self._gw_outbound_event_markers:
                if (self._gw_tx_frames_streamed >=
                        self._gw_outbound_event_markers[0][0]):
                    # Signal that client's frames have been streamed
                    threshold, event = self._gw_outbound_event_markers.popleft()
                    event.client.dispatch(event)
                    LOGGER.debug('_handle_write: ACKed %r to %r; thresh=%i '
                                 'frames=%i', event, event.client, threshold,
                                 self._gw_tx_frames_streamed)
                else:
                    break


class ServiceProxy(object):
    """Interface to `GatewayConnectionService` for use by `ThreadedConnection`
    """

    def __init__(self, service_thread, dispatch):
        """
        :param callable dispatch: Function for sending an event to
            `GatewayConnectionService`; it has the signature`dispatch(event)`

        """
        self.dispatch = dispatch

        # For use only by check_health
        self._service_thread = service_thread
        # For use only by check_health and GatewayConnectionService
        self._service_exc = None # exception that caused service to stop

    def _set_service_exit_exception(self, exc):
        """Called by Gateway Connection Service to set the exception
        corresponding to reason for service's shutdown

        :param GatewayStoppedError exc:

        """
        self._service_exc = exc

    def check_health(self):
        """Check whether Gateway Connection Service stopped

        :raises GatewayStoppedError: if connection gateway service stopped. This
            exception contains information about the cause. See
            `GatewayStoppedError` for more info.
        """
        if self._service_thread.isAlive():
            return

        if self._service_exc is not None:
            assert isinstance(self._service_exc, GatewayStoppedError), repr(
                self._service_exc)
            raise self._service_exc  # pylint: disable=E0702
        else:
            raise GatewayStoppedError(
                None,
                pika.exceptions.AMQPConnectionError(
                    _UNEXPECTED_FAILURE_REASON_CODE,
                    '{!r} died'.format(self._service_thread)))

class ClientProxy(object):
    """Interface to `ThreadedConnection` for use by `GatewayConnectionService`

    """

    # Connection states; numerical order is significant for the logic
    CONN_STATE_PENDING = 0        # expecting frame.ProtocolHeader from client
    CONN_STATE_HANDSHAKE = 1      # performing connection handshake with client
    CONN_STATE_HANDSHAKE_DONE = 2 # connection established with client
    CONN_STATE_CLIENT_CLOSED = 3  # Received Connection.Close from client

    def __init__(self, queue):
        """
        :param Queue.Queue queue: Thread-safe queue for depositing events
            destined for the client

        """
        self._evt_queue = queue

        # Set to True by client before sending Connection.Close to force closing
        # of the shared AMQP connection
        self.force_close = False

        # Connection state for use only by `BackgroundConnectionProxy` to track
        # connection state: ClientProxy.CONN_STATE_*
        self.conn_state = self.CONN_STATE_PENDING

    def dispatch(self, event):
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


class ChannelOpEventFamily(object):
    """Designates subclass as channel operation event """
    pass


class ChannelRegEvent(ChannelOpEventFamily, AsyncEventFamily):
    """Channel registration event for informing `GatewayConnectionService` of
    the association between channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.client = client
        self.channel_number = channel_number


class ChannelUnregEvent(ChannelOpEventFamily, AsyncEventFamily):
    """Channel de-registration event for informing `GatewayConnectionService` to
    remove the association between the given channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.client = client
        self.channel_number = channel_number


class BlockedConnectionSubscribeEvent(AsyncEventFamily):
    """Request from a client for receiving "blocked/unblocked" connection frames
    from `GatewayConnectionService`
    """

    def __init__(self, client):
        """
        :param ClientProxy:

        """
        self.client = client


class FramesToBrokerEvent(RpcEventFamily):
    """Container for serialized frames destined for AMQP broker. This type of
    event is dispatched by `ThreadedConnection` to `GatewayConnectionService`.

    """

    def __init__(self, client, frames, on_result_rx):
        """
        :param ClientProxy:
        :param list frames: list of frames destined for AMQP broker
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.

        """
        super(FramesToBrokerEvent, self).__init__(on_result_rx)
        self.client = client

        # Connection Gateway resets this to None after reaping frames.
        self.frames = frames


class FramesToClientEvent(AsyncEventFamily):
    """Container for frames destined for client. This type of event is
    dispatched by `GatewayConnectionService` to `ThreadedConnection`
    """

    def __init__(self, frames):
        """
        :param frames: sequence of `pika.spec` frames destined for client

        """
        self.frames = frames


class ConnectionClosedEvent(AsyncEventFamily):
    """Notify client about closing or failure of the connection"""

    def __init__(self, open_exc, close_reason_pair):
        """

        :param open_exc: pika.exception.AMQPConnectionError object if
            failure occurred during connection-establishment, None if failure or
            closing of connection occurred after successful connection
            establishment.
        :param tuple close_reason_pair: a two-tuple of (code-int, text-str)
            representing the reason for connection's failure (including
            establishment failure) or closing.

        """
        self.open_exc = open_exc
        self.close_reason_pair = close_reason_pair
