"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import abc
import logging

import pika.compat
import pika.exceptions
import pika.tcp_socket_opts

from pika.adapters import connection_workflow
from pika import connection


LOGGER = logging.getLogger(__name__)

def perr(*args):
    import sys
    print(*args, file=sys.stderr)

class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback,
                 async_services,
                 internal_connection_workflow):
        """Create a new instance of the Connection object.

        :param None|pika.connection.Parameters parameters: Connection parameters
        :param None|method on_open_callback: Method to call on connection open
        :param None|method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|BaseException)
        :param None|method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param async_interface.AbstractAsyncServices async_services:
            asynchronous services
        :param bool internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not %r' % parameters)

        self._async = async_services

        self._connection_workflow = None  # type: connection_workflow.AMQPConnectionWorkflow
        self._transport = None  # type: pika.adapters.async_interface.AbstractStreamTransport

        self._got_eof = False  # transport indicated EOF (connection reset)

        super(BaseConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            internal_connection_workflow=internal_connection_workflow)

    def _init_connection_state(self):
        """Initialize or reset all of our internal state variables for a given
        connection. If we disconnect and reconnect, all of our state needs to
        be wiped.

        """
        super(BaseConnection, self)._init_connection_state()

        self._connection_workflow = None
        self._transport = None
        self._got_eof = False

    def __repr__(self):

        # def get_socket_repr(sock):
        #     """Return socket info suitable for use in repr"""
        #     if sock is None:
        #         return None
        #
        #     sockname = None
        #     peername = None
        #     try:
        #         sockname = sock.getsockname()
        #     except pika.compat.SOCKET_ERROR:
        #         # closed?
        #         pass
        #     else:
        #         try:
        #             peername = sock.getpeername()
        #         except pika.compat.SOCKET_ERROR:
        #             # not connected?
        #             pass
        #
        #     return '%s->%s' % (sockname, peername)
        # TODO need helpful __repr__ in transports
        return ('<%s %s transport=%s params=%s>' %
                (self.__class__.__name__,
                 self._STATE_NAMES[self.connection_state],
                 self._transport, self.params))

    @classmethod
    @abc.abstractmethod
    def create_connection(cls,
                          connection_configs,
                          on_done,
                          custom_ioloop=None,
                          workflow=None):
        """Asynchronously create a connection to an AMQP broker using the given
        configurations. Will attempt to connect using each config in the given
        order, including all compatible resolved IP addresses of the hostname
        supplied in each config, until one is established or all attempts fail.

        See also `_start_connection_workflow()`.

        :param sequence connection_configs: A sequence of one or more
            `pika.connection.Parameters`-based objects.
        :param callable on_done: as defined in
            `pika.adapters.connection_workflow.AbstractAMQPConnectionWorkflow.start()`.
        :param object | None custom_ioloop: Provide a custom I/O loop that is
            native to the specific adapter implementation; if None, the adapter
            will use a default loop instance, which is typically a singleton.
        :param connection_workflow.AbstractAMQPConnectionWorkflow | None workflow:
            Pass an instance of an implementation of the
            `connection_workflow.AbstractAMQPConnectionWorkflow` interface;
            defaults to a `connection_workflow.AMQPConnectionWorkflow` instance
            with default values for optional args.

        :return: Connection workflow instance in use. The user should limit
            their interaction with this object only to it's `abort()` method.
        :rtype: connection_workflow.AbstractAMQPConnectionWorkflow

        """
        raise NotImplementedError

    @staticmethod
    def _start_connection_workflow(connection_configs,
                                   connection_factory,
                                   async_services,
                                   workflow,
                                   on_done):
        """Helper function for custom implementations of `create_connection()`.

        :param sequence connection_configs: A sequence of one or more
            `pika.connection.Parameters`-based objects.
        :param callable connection_factory: A function that takes no args and
            returns a brand new `pika.connection.Connection`-based adapter
            instance each time it is called.
        :param pika.adapters.async_interface.AbstractAsyncServices async_services:
        :param connection_workflow.AbstractAMQPConnectionWorkflow | None workflow:
            Pass an instance of an implementation of the
            `connection_workflow.AbstractAMQPConnectionWorkflow` interface;
            defaults to a `connection_workflow.AMQPConnectionWorkflow` instance
            with default values for optional args.
        :param callable on_done: as defined in
            `pika.adapters.connection_workflow.AbstractAMQPConnectionWorkflow.start()`.

        :return: Connection workflow instance in use. The user should limit
            their interaction with this object only to it's `abort()` method.
        :rtype: connection_workflow.AbstractAMQPConnectionWorkflow

        """
        if workflow is None:
            workflow = connection_workflow.AMQPConnectionWorkflow()
            LOGGER.debug('Created default connection workflow %r', workflow)

        if isinstance(workflow, connection_workflow.AMQPConnectionWorkflow):
            workflow._set_async_services(async_services)

        def create_connector():
            """`AMQPConnector` factory."""
            return connection_workflow.AMQPConnector(connection_factory,
                                                     async_services)

        workflow.start(
            connection_configs=connection_configs,
            connector_factory=create_connector,
            native_loop=async_services.get_native_ioloop(),
            on_done=on_done)

        return workflow


    @property
    def ioloop(self):
        """
        :return: the native I/O loop instance underlying async services selected
            by user or the default selected by the specialized connection
            adapter (e.g., Twisted reactor, `asyncio.SelectorEventLoop`,
            `select_connection.IOLoop`, etc.)
        """
        return self._async.get_native_ioloop()

    def add_timeout(self, deadline, callback):
        """Add the callback to the IOLoop timer to fire after deadline
        seconds.

        :param float deadline: The number of seconds to wait to call callback
        :param method callback: The callback method
        :return: Handle that can be passed to `remove_timeout()` to cancel the
            callback.

        """
        return self._async.call_later(deadline, callback)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        """
        timeout_id.cancel()

    def add_callback_threadsafe(self, callback):
        """Requests a call to the given function as soon as possible in the
        context of this connection's IOLoop thread.

        NOTE: This is the only thread-safe method offered by the connection. All
         other manipulations of the connection must be performed from the
         connection's thread.

        For example, a thread may request a call to the
        `channel.basic_ack` method of a connection that is running in a
        different thread via

        ```
        connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=...))
        ```

        :param method callback: The callback method; must be callable.

        """
        if not callable(callback):
            raise TypeError(
                'callback must be a callable, but got %r' % (callback,))

        self._async.add_callback_threadsafe(callback)

    def _adapter_connect_stack(self):
        """Initiate full-stack connection establishment asynchronously for
        internally-initiated connection bring-up.

        Upon failed completion, we will invoke
        `Connection._on_stack_connection_workflow_failed()`. NOTE: On success,
        the stack will be up already, so there is no corresponding callback.

        """
        self._connection_workflow = connection_workflow.AMQPConnectionWorkflow(
            connection_timeout=self.params.socket_timeout * 1.5,
            retries=self.params.connection_attempts - 1,
            retry_pause=self.params.retry_delay,
            _until_first_amqp_attempt=True)

        self._connection_workflow._set_async_services(self._async)

        def create_connector():
            """`AMQPConnector` factory"""
            return connection_workflow.AMQPConnector(lambda: self, self._async)

        self._connection_workflow.start(
            [self.params],
            connector_factory=create_connector,
            native_loop=self._async.get_native_ioloop(),
            on_done=self._on_connection_workflow_done)

    def _adapter_abort_connection_workflow(self):
        """Asynchronously abort connection workflow. Upon
        completion, call `Connection._on_stack_connection_workflow_failed()`
        with None as the argument.

        Assumption: may be called only while connection is opening.

        """
        assert self._opening, (
            '_adapter_abort_connection_workflow() may be called only when '
            'connection is opening.')

        if self._transport is None:
            # NOTE: this is possible only when user calls Connection.close() to
            # interrupt internally-initiated connection establishment.
            # self._connection_workflow.abort() would not call
            # Connection.close() before pairing of connection with transport.
            #
            # This will result in call to _on_connection_workflow_done() upon
            # completion
            self._connection_workflow.abort()
        else:
            # NOTE: we can't use self._connection_workflow.abort() in this case,
            # because it would result in infinite recursion as we're called
            # from Connection.close() and _connection_workflow.abort() calls
            # Connection.close() to abort a connection that's already been
            # paired with a transport. During internally-initiated connection
            # establishment, AMQPConnectionWorkflow will discover that user
            # aborted the connection when it receives
            # pika.exceptions.ConnectionOpenAborted.

            # This completes asynchronously, culminating in call to our method
            # `connection_lost()`
            self._transport.abort()

    def _on_connection_workflow_done(self, conn_or_exc):
        """`AMQPConnectionWorkflow` completion callback.

        :param BaseConnection | Exception conn_or_exc: Our own connection
            instance on success; exception on failure. See
            `AbstractAMQPConnectionWorkflow.start()` for details.

        """
        perr('_on_connection_workflow_done({!r})'.format(conn_or_exc))

        LOGGER.debug('Full-stack connection workflow completed: %r',
                     conn_or_exc)

        self._connection_workflow = None

        # Notify protocol of failure
        if isinstance(conn_or_exc, Exception):
            self._transport = None
            if isinstance(conn_or_exc,
                          connection_workflow.AMQPConnectionWorkflowAborted):
                LOGGER.info('Full-stack connection workflow aborted: %r',
                            conn_or_exc)
                # So that _on_stack_connection_workflow_failed() will know it's
                # not a failure
                conn_or_exc = None
            else:
                LOGGER.error('Full-stack connection workflow failed: %r',
                             conn_or_exc)
                if (isinstance(conn_or_exc,
                               connection_workflow
                               .AMQPConnectionWorkflowError) and
                        isinstance(conn_or_exc.exceptions[-1],
                                   connection_workflow
                                   .AMQPConnectorSocketConnectError)):
                    conn_or_exc = pika.exceptions.AMQPConnectionError(
                        conn_or_exc)


            self._on_stack_connection_workflow_failed(conn_or_exc)
        else:
            # NOTE: On success, the stack will be up already, so there is no
            #       corresponding callback.
            assert conn_or_exc is self, \
                'Expected self conn={!r} from workflow, but got {!r}.'.format(
                    self, conn_or_exc)

    def _adapter_disconnect(self):
        """Asynchronously bring down the streaming transport layer and invoke
        `Connection._on_stack_terminated()` asynchronously when complete.

        """
        # This completes asynchronously, culminating in call to our method
        # `connection_lost()`
        self._transport.abort()

    def _adapter_emit_data(self, data):
        """Take ownership of data and send it to AMQP server as soon as
        possible.

        :param bytes data:

        """
        self._transport.write(data)

    def _adapter_get_write_buffer_size(self):
        """
        Subclasses must override this

        :return: Current size of output data buffered by the transport
        :rtype: int

        """
        return self._transport.get_write_buffer_size()

    def connection_made(self, transport):
        """Introduces transport to protocol after transport is connected.

        `async_interface.AbstractStreamProtocol` interface method.

        :param async_interface.AbstractStreamTransport transport:
        :raises Exception: Exception-based exception on error

        """
        self._transport = transport

        # Lset connection know that stream is available
        self._on_stream_connected()

    def connection_lost(self, error):
        """Called upon loss or closing of TCP connection.

        `async_interface.AbstractStreamProtocol` interface method.

        NOTE: `connection_made()` and `connection_lost()` are each called just
        once and in that order. All other callbacks are called between them.

        :param BaseException | None error: An exception (check for
            `BaseException`) indicates connection failure. None indicates that
            connection was closed on this side, such as when it's aborted or
            when `AbstractStreamProtocol.eof_received()` returns a falsy result.
        :raises Exception: Exception-based exception on error

        """
        self._transport = None

        if error is None:
            # Either result of `eof_received()` or abort
            if self._got_eof:
                error = pika.exceptions.StreamLostError(
                    'Transport indicated EOF')
        else:
            error = pika.exceptions.StreamLostError(
                'Stream connection lost: {!r}'.format(error))

        LOGGER.log(logging.DEBUG if error is None else logging.ERROR,
                   'connection_lost: %r',
                   error)

        self._on_stack_terminated(error)

    def eof_received(self):  # pylint: disable=R0201
        """Called after the remote peer shuts its write end of the connection.

        `async_interface.AbstractStreamProtocol` interface method.

        :return: A falsy value (including None) will cause the transport to
            close itself, resulting in an eventual `connection_lost()` call
            from the transport. If a truthy value is returned, it will be the
            protocol's responsibility to close/abort the transport.
        :rtype: falsy | truthy
        :raises Exception: Exception-based exception on error

        """
        LOGGER.error('Transport indicated EOF.')

        self._got_eof = True

        # This is how a reset connection will typically present itself
        # when we have nothing to send to the server over plaintext stream.
        #
        # Have transport tear down the connection and invoke our
        # `connection_lost` method
        return False

    def data_received(self, data):
        """Called to deliver incoming data from the server to the protocol.

        `async_interface.AbstractStreamProtocol` interface method.

        :param data: Non-empty data bytes.
        :raises Exception: Exception-based exception on error

        """
        self._on_data_available(data)
