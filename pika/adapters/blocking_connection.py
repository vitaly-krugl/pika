"""The blocking connection adapter module implements blocking semantics on top
of Pika's core AMQP driver. While most of the asynchronous expectations are
removed when using the blocking connection adapter, it attempts to remain true
to the asynchronous RPC nature of the AMQP protocol, supporting server sent
RPC commands.

The user facing classes in the module consist of the
:py:class:`~pika.adapters.blocking_connection.BlockingConnection`
and the :class:`~pika.adapters.blocking_connection.BlockingChannel`
classes.

"""
# Disable "access to protected member warnings: this wrapper implementation is
# a friend of those instances
# pylint: disable=W0212

from collections import namedtuple, deque
import contextlib
import functools
import logging
import time

import pika.channel
from pika import compat
from pika import exceptions
import pika.spec

from pika.adapters import blocking_connection_base

# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters.select_connection import SelectConnection


LOGGER = logging.getLogger(__name__)


class BlockingConnection(blocking_connection_base.BlockingConnectionBase):
    """The BlockingConnection creates a layer on top of Pika's asynchronous core
    providing methods that will block until their expected response has
    returned. Due to the asynchronous nature of the `Basic.Deliver` and
    `Basic.Return` calls from RabbitMQ to your application, you can still
    implement continuation-passing style asynchronous methods if you'd like to
    receive messages from RabbitMQ using
    :meth:`basic_consume <BlockingChannel.basic_consume>` or if you want to be
    notified of a delivery failure when using
    :meth:`basic_publish <BlockingChannel.basic_publish>` .

    For more information about communicating with the blocking_connection
    adapter, be sure to check out the
    :class:`BlockingChannel <BlockingChannel>` class which implements the
    :class:`Channel <pika.channel.Channel>` based communication for the
    blocking_connection adapter.

    """

    def __init__(self, parameters=None, _impl_class=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :param _impl_class: for tests/debugging only; implementation class;
            None=default

        :raises AMQPConnectionError:

        """
        super(BlockingConnection, self).__init__()

        impl_class = _impl_class or SelectConnection
        self._impl = impl_class(
            parameters=parameters,
            on_open_callback=self._opened_result.set_value_once,
            on_open_error_callback=self._open_error_result.set_value_once,
            on_close_callback=self._closed_result.set_value_once,
            stop_ioloop_on_close=False)

        self._impl.ioloop.activate_poller()

        self._process_io_for_connection_setup()

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
        is_done = (lambda:
                   self.is_closed or
                   (not self._impl.outbound_buffer and
                    (not waiters or any(ready() for ready in  waiters))))

        # Process I/O until our completion condition is satisified
        while not is_done():
            self._impl.ioloop.poll()
            self._impl.ioloop.process_timeouts()

    def _cleanup(self):
        """[override base] Clean up members that might inhibit garbage collection"""
        self._impl.ioloop.deactivate_poller()

        super(BlockingConnection, self)._cleanup()
