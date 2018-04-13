"""Base class extended by connection adapters. This extends the
connection.Connection class to encapsulate connection behavior but still
isolate socket and low level communication.

"""
import errno
import logging
import os

import pika.compat
import pika.exceptions
import pika.tcp_socket_opts

from pika.adapters import connection_workflow # import (AMQPConnectionWorkflow,
                                              # AMQPConnector)
from pika import connection


LOGGER = logging.getLogger(__name__)


class BaseConnection(connection.Connection):
    """BaseConnection class that should be extended by connection adapters"""

    def __init__(self,
                 parameters,
                 on_open_callback,
                 on_open_error_callback,
                 on_close_callback,
                 async_services):
        """Create a new instance of the Connection object.

        :param None|pika.connection.Parameters parameters: Connection parameters
        :param None|method on_open_callback: Method to call on connection open
        :param None|method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|BaseException)
        :param None|method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param async_interface.AbstractAsyncServices async_services:
            asynchronous services
        :raises: RuntimeError
        :raises: ValueError

        """
        if parameters and not isinstance(parameters, connection.Parameters):
            raise ValueError(
                'Expected instance of Parameters, not %r' % parameters)

        self._async = async_services

        self._connection_workflow = None  # type: connection_workflow.AMQPConnectionWorkflow
        self._transport = None  # type: async_interface.AbstractStreamTransport

        self._got_eof = False  # transport indicated EOF (connection reset)

        super(BaseConnection,
              self).__init__(parameters, on_open_callback,
                             on_open_error_callback, on_close_callback)

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
        self-initiated connection brin-up.

        Upon failed completion, we will invoke
        `Connection._on_stack_connection_workflow_failed()`. NOTE: On success,
        the stack will be up already, so there is no corresponding callback.

        """
        self._connection_workflow = connection_workflow.AMQPConnectionWorkflow(
            [self.params],
            connection_timeout=self.params.socket_timeout * 1.5,
            retries=self.params.connection_attempts - 1,
            retry_pause=self.params.retry_delay,
            _until_first_amqp_attempt=True)

        def create_connector():
            return connection_workflow.AMQPConnector(lambda: self, self._async)

        self._connection_workflow.start(
            connector_factory=create_connector,
            native_loop=self._async.get_native_ioloop(),
            on_done=self._on_connection_workflow_done)

    def _adapter_abort_connection_workflow(self):
        """Asynchronously abort connection workflow. Upon completion, call
        `Connection._on_stack_connection_workflow_failed()` with None as the
        argument.

        """
        self._connection_workflow.abort()

    def _on_connection_workflow_done(self, conn_or_exc):
        """`AMQPConnectionWorkflow` completion callback.

        :param BaseConnection | Exception conn_or_exc: Our own connection
            instance on success; exception on failure. See
            `AbstractAMQPConnectionWorkflow.start()` for details.

        """
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

            self._on_stack_connection_workflow_failed(conn_or_exc)
        else:
            # NOTE: On success, the stack will be up already, so there is no
            #       corresponding callback.
            assert conn_or_exc is self, \
                'Expected self conn={!r} from workflow, but got {!r}.'.format(
                    self, conn_or_exc)

    def _adapter_disconnect(self):
        """Disconnect

        """
        # TODO: deal with connection workflow and tranport abort instead of drop.
        # TODO: get rid of _transport_mgr
        if self._transport_mgr is not None:
            self._transport_mgr.close()
            self._transport_mgr = None

        if self._transport is not None:
            self._transport.drop()
            self._transport = None

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

        if error is not None and self._error is None:
            self._error = error

        self._on_stack_terminated()

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
