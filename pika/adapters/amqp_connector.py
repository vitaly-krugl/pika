"""Implements the workflow of performing multiple TCP/[SSL]/AMQP connection
attempts with timeouts and retries until one succeeds or all attempts fail.

"""

import functools
import logging
import socket

import pika.compat
import pika.tcp_socket_opts


_LOG = logging.getLogger(__name__)


class AMQPConnectorException(Exception):
    """Base exception for this module"""
    pass


class AMQPConnectorTimeout(AMQPConnectorException):
    """Overall TCP/[SSL]/AMQP connection attempt timed out."""
    pass


class AMQPConnectorAborted(AMQPConnectorException):
    """Asynchronous request was aborted"""
    pass


class AMQPConnectorWrongState(AMQPConnectorException):
    """Operation requested in wrong state, such as aborting after completion was
    reported.
    """
    pass


class AMQPConnector(object):
    """Performs a single TCP/[SSL]/AMQP connection workflow.

    """

    STATE_INIT = 0  # start() hasn't been called yet
    STATE_TCP = 1  # TCP/IP connection establishment
    STATE_TRANSPORT = 2  # [SSL] and transport linkup
    STATE_AMQP = 3  # AMQP connection handshake
    STATE_TIMEOUT = 4  # overall TCP/[SSL]/AMQP timeout
    STATE_ABORTING = 5  # abort() called - aborting workflow
    STATE_DONE = 6  # result reported to client

    def __init__(self, conn_factory, async_services):
        """

        :param callable conn_factory: A function that takes no args and
            returns a brand new `pika.connection.Connection`-based object
            each time it is called.
        :param pika.adapters.async_interface.AbstractAsyncServices async_services:

        """
        self._conn_factory = conn_factory
        self._async_services = async_services
        self._addr_record = None  # type: tuple
        self._conn_params = None  # type: pika.connection.Parameters
        self._on_done = None  # will be provided via start()
        # TCP connection timeout
        self._tcp_timeout_ref = None  # type: AbstractTimerReference
        # Overall TCP/[SSL]/AMQP timeout
        self._timeout_ref = None  # type: AbstractTimerReference
        # Current task
        self._task_ref = None  # type: AbstractAsyncReference
        self._sock = None  # type: socket.socket
        self._amqp_conn = None  # type: pika.connection.Connection

        self._state = self.STATE_INIT

    def start(self, addr_record, conn_params, timeout, on_done):
        """Asynchronously perform a single TCP/[SSL]/AMQP connection attempt.

        :param tuple addr_record: a single resolved address record compatible
            with `socket.getaddrinfo()` format.
        :param pika.connection.Parameters conn_params:
        :param int | float timeout: Timeout in seconds for the entire
            TCP/[SSL]/AMQP connection sequence.
        :param callable on_done: Function to call upon completion of the
            workflow: `on_done(pika.connection.Connection | BaseException)`.

        """
        if self._state != self.STATE_INIT:
            raise AMQPConnectorWrongState(
                'Already in progress or finished; state={}'.format(self._state))

        self._addr_record = addr_record
        self._conn_params = conn_params
        self._on_done = on_done

        # Create socket and initiate TCP/IP connection
        self._state = self.STATE_TCP
        self._sock = socket.socket(*self._addr_record[:3])
        self._sock.setsockopt(pika.compat.SOL_TCP, socket.TCP_NODELAY, 1)
        pika.tcp_socket_opts.set_sock_opts(self._conn_params.tcp_options,
                                           self._sock)
        self._sock.setblocking(False)

        addr = self._addr_record[4]
        _LOG.info('Connecting to AMQP broker at %r', addr)
        self._task_ref = self._async_services.connect_socket(
            self._sock,
            addr,
            on_done=self._on_tcp_connection_done)

        # Start socket connection timeout timer
        self._tcp_timeout_ref = self._async_services.call_later(
            self._conn_params.socket_timeout,
            self._on_tcp_connection_timeout)

        # Start overall TCP/[SSL]/AMQP connection timeout timer
        self._timeout_ref = self._async_services.call_later(
            max(timeout, self._conn_params.socket_timeout),
            self._on_overall_timeout)

    def abort(self):
        """Abort the workflow asynchronously. The completion callback will be
        called with an instance of AMQPConnectorAborted.

        NOTE: we can't cancel/close synchronously because aborting pika
        Connection and its transport requires an asynchronous operation.

        :raises AMQPConnectorWrongState: If called after completion has been
            reported or the workflow not started yet.
        """
        if self._state == self.STATE_INIT:
            raise AMQPConnectorWrongState('Cannot abort before starting.')
        elif self._state == self.STATE_DONE:
            raise AMQPConnectorWrongState(
                'Cannot abort after completion was reported')

        self._state = self.STATE_ABORTING
        self._deactivate()

        _LOG.info('AMQPConnector: beginning client-initiated asynchronous '
                  'abort; %r/%s', self._conn_params.host, self._addr_record)

        if self._amqp_conn is None:
            _LOG.debug('AMQPConnector.abort(): no connection, so just '
                       'scheduling completion report via I/O loop.')
            self._async_services.add_callback_threadsafe(
                functools.partial(
                    self._report_completion_and_cleanup,
                    AMQPConnectorAborted()))
        else:
            if not self._amqp_conn.is_closing:
                # Initiate close of AMQP connection and wait for asynchronous
                # callback from the Connection instance before reporting
                # completion to client
                _LOG.debug('AMQPConnector.abort(): closing Connection.')
                self._amqp_conn.add_on_close_callback(self._on_amqp_closed)
                self._amqp_conn.close(320,
                                      'Client-initiated abort of AMQPConnector')
            else:
                # It's already closing, must be due to our timeout processing,
                # so we'll just piggy back on the callback it registered
                _LOG.debug('AMQPConnector.abort(): closing of Connection was '
                           'already initiated.')
                assert self._state == self.STATE_TIMEOUT, \
                    ('Connection is closing, but not in TIMEOUT state; state={}'
                     .format(self._state))

    def _deactivate(self):
        """Cancel asynchronous tasks.

        """
        # NOTE: self._amqp_conn requires special handling as it doesn't support
        # synchronous closing. We special-case it elsewhere in the code where
        # needed.
        assert self._amqp_conn is None, \
            '_deactivate called with self._amqp_conn not None'

        if self._tcp_timeout_ref is not None:
            self._tcp_timeout_ref.cancel()
            self._tcp_timeout_ref = None

        if self._timeout_ref is not None:
            self._timeout_ref.cancel()
            self._timeout_ref = None

        if self._task_ref is not None:
            self._task_ref.cancel()
            self._task_ref = None

    def _close(self):
        """Cancel asynchronous tasks and cleanup to assist garbage collection.

        Transition to STATE_DONE.

        """
        self._deactivate()

        if self._sock is not None:
            self._sock.close()
            self._sock = None

        self._conn_factory = None
        self._async_services = None
        self._addr_record = None
        self._on_done = None

        self._state = self.STATE_DONE

    def _report_completion_and_cleanup(self, result):
        """Clean up and invoke client's `on_done` callback.

        :param pika.connection.Connection | BaseException result: value to pass
            to user's `on_done` callback.
        """
        _LOG.info('AMQPConnector - reporting completion result: %r', result)

        on_done = self._on_done
        self._close()

        on_done(result)

    def _on_tcp_connection_timeout(self):
        """Handle TCP connection timeout

        """
        error = socket.timeout(
            'TCP connection attempt timed out: {!r}/{}'.format(
                self._conn_params.host, self._addr_record))
        self._report_completion_and_cleanup(error)

    def _on_overall_timeout(self):
        """Handle overall TCP/[SSL]/AMQP connection attempt timeout by reporting
        `Timeout` error to the client.

        """
        self._timeout_ref = None

        prev_state = self._state
        self._state = self.STATE_TIMEOUT

        if prev_state == self.STATE_TCP:
            msg = 'Timed out while connecting socket to {!r}/{}'.format(
                self._conn_params.host, self._addr_record)
        elif prev_state == self.STATE_TRANSPORT:
            msg = ('Timed out while setting up transport to {!r}/{}; ssl={}'
                   .format(self._conn_params.host, self._addr_record,
                           bool(self._conn_params.ssl_options)))
        else:
            assert prev_state == self.STATE_AMQP
            msg = ('Timed out while setting up AMQP to {!r}/{}; ssl={}'
                   .format(self._conn_params.host, self._addr_record,
                           bool(self._conn_params.ssl_options)))
            _LOG.error(msg)
            # Initiate close of AMQP connection and wait for asynchronous
            # callback from the Connection instance before reporting completion
            # to client
            assert not self._amqp_conn.is_open, \
                'Unexpected open state of {!r}'.format(self._amqp_conn)
            self._amqp_conn.add_on_close_callback(self._on_amqp_closed)
            self._amqp_conn.close(320, msg)
            return

        self._report_completion_and_cleanup(AMQPConnectorTimeout(msg))

    def _on_tcp_connection_done(self, exc):
        """Handle completion of asynchronous stream socket connection attempt.

        On failure, attempt to connect to the next address, if any, from DNS
        lookup.

        :param None|BaseException exc: None on success; exception object on
            failure

        """
        self._task_ref = None
        self._tcp_timeout_ref.cancel()

        if exc is not None:
            _LOG.error('TCP Connection attempt failed: %r; dest=%r',
                       exc, self._addr_record)
            self._report_completion_and_cleanup(exc)
            return

        # We succeeded in making a TCP/IP connection to the server
        _LOG.debug('TCP connection to broker established: %r.', self._sock)

        # Now set up the transport
        self._state = self.STATE_TRANSPORT

        ssl_context = server_hostname = None
        if self._conn_params.ssl_options is not None:
            ssl_context = self._conn_params.ssl_options.context
            server_hostname = self._conn_params.ssl_options.server_hostname
            if server_hostname is None:
                server_hostname = self._conn_params.host

        self._task_ref = self._async_services.create_streaming_connection(
            protocol_factory=self._conn_factory,
            sock=self._sock,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            on_done=self._on_transport_establishment_done)

        self._sock = None  # create_streaming_connection() takes ownership

    def _on_transport_establishment_done(self, result):
        """Handle asynchronous completion of
        `AbstractAsyncServices.create_streaming_connection()`

        :param sequence|BaseException result: On success, a two-tuple
            (transport, protocol); on failure, exception instance.

        """
        self._task_ref = None

        if isinstance(result, BaseException):
            _LOG.error('Attempt to create the streaming transport failed: %r; '
                       '%r/%s; ssl=%s',
                       result, self._conn_params.host, self._addr_record,
                       bool(self._conn_params.ssl_options))
            self._report_completion_and_cleanup(result)
            return

        # We succeeded in setting up the streaming transport!
        # result is a two-tuple (transport, protocol)
        _LOG.info('Streaming transport linked up: %r.', result)
        _transport, self._amqp_conn = result

        # AMQP handshake is in progress - initiated during transport link-up
        self._state = self.STATE_AMQP
        # We explicitly remove default handler because it raises an exception.
        self._amqp_conn.add_on_open_error_callback(self._on_amqp_handshake_done,
                                                   remove_default=True)
        self._amqp_conn.add_on_open_callback(self._on_amqp_handshake_done)

    def _on_amqp_handshake_done(self, connection, error=None):
        """Handle completion of AMQP connection handsahke attempt.

        NOTE: we handle two types of callbacks - success with just connection
        arg as well as the open-error callback with connection and error

        :param pika.connection.Connection connection:
        :param BaseException | None error: None on success, otherwise
            failure
        """

        if self._state != self.STATE_AMQP:
            # We timed out or aborted and initiated closing of the connection,
            # but this callback snuck in
            _LOG.debug('Ignoring AMQP hanshake completion notification due to '
                       'wrong state=%s; error=%r; conn=%r',
                       self._state, error, connection)
            return

        self._amqp_conn = None

        if error is None:
            _LOG.debug(
                'AMQPConnector: AMQP connection established for %r/%s: %r',
                self._conn_params.host, self._addr_record, connection)
            result = connection
        else:
            _LOG.debug(
                'AMQPConnector: AMQP connection handshake failed for %r/%s: %r',
                self._conn_params.host, self._addr_record, error)
            result = error

        self._report_completion_and_cleanup(result)

    def _on_amqp_closed(self, _connection, reply_code, reply_text):
        """Handle closing of connection. We request this callback when forcing
        the Conneciton instance closed durin timeout and abort handling.

        :param pika.connection.Connection connection:
        :param int reply_code:
        :param text reply_text:
        """
        self._amqp_conn = None

        _LOG.debug(
            'AMQPConnector: AMQP connection close completed; state=%s; '
            'reply_code=%s; reply_text=%r; %r/%s',
            self._state, reply_code, reply_text, self._conn_params.host,
            self._addr_record)

        if self._state == self.STATE_ABORTING:
            # Client-initiated abort takes precedence over timeout
            self._report_completion_and_cleanup(AMQPConnectorAborted())
        elif self._state == self.STATE_TIMEOUT:
            self._report_completion_and_cleanup(AMQPConnectorTimeout())
        else:
            _LOG.critical('AMQPConnector._on_amqp_closed() called in '
                          'unexpected state=%s', self._state)


class AMQPConnectionWorkflow(object):
    """Implements the workflow of performing multiple TCP/[SSL]/AMQP connection
    attempts with timeouts and retries until one succeeds or all attempts fail.

    The workflow:
        while not success and retries remain:
            1. For each given config (pika.connection.Parameters object):
                A. Perform DNS resolution of the config's host.
                B. Attempt to establish TCP/[SSL]/AMQP for each resolved address
                   until one succeeds, in which case we're done.
            2. If all configs failed but retries remain, resume from beginning
               after the configured retry interval.

    """

    def __init__(self,
                 connection_configs,
                 connection_factory,
                 async_services,
                 connection_timeout=20,
                 retries=0,
                 retry_pause=2):
        """
        :param sequence connection_configs: A sequence of
            `pika.connection.Parameters`-based objects. Will attempt to connect
            using each config in the given order.
        :param callable connection_factory: A function that takes no args and
            returns a brand new `pika.connection.Connection`-based object
            each time it is called.
        :param pika.adapters.async_interface.AbstractAsyncServices async_services:
        :param int | float connection_timeout: Overall timeout for a given
            TCP/[SSL]/AMQP connection attempt.
        :param int retries: Non-negative maximum number of retries after the
            initial attempt to connect using the entire config sequence fails.
            Defaults to 0.
        :param int | float retry_pause: Non-negative number of seconds to wait
            before retrying the config sequence. Meaningful only if retries is
            greater than 0. Defaults to 2 seconds.

        TODO: Would it be useful to implement exponential back-off?

        """
        pass

    def start(self, on_done):
        """Asynchronously perform the workflow until success or all retries
        are exhausted.

        :param callable on_done: Function to call upon completion of the
            workflow: `on_done(pika.connection.Connection | BaseException)`.

        """
        pass

    def abort(self):
        """Abort the workflow asynchronously. The completion callback will be
        called with an instance of AMQPConnectorAborted.

        NOTE: we can't cancel/close synchronously because aborting pika
        Connection and its transport requires an asynchronous operation.

        :raises AMQPConnectorWrongState: If called after completion has been
            reported.
        """
        pass
