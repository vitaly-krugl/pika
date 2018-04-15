"""Use pika with the Tornado IOLoop

"""
import logging

from tornado import ioloop

import pika.adapters.async_interface
from pika.adapters import base_connection, selector_ioloop_adapter


LOGGER = logging.getLogger(__name__)


class TornadoConnection(base_connection.BaseConnection):
    """The TornadoConnection runs on the Tornado IOLoop.
    """

    def __init__(self,
                 parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 custom_ioloop=None,
                 internal_connection_workflow=True):
        """Create a new instance of the TornadoConnection class, connecting
        to RabbitMQ automatically

        :param pika.connection.Parameters parameters: Connection parameters
        :param on_open_callback: The method to call when the connection is open
        :type on_open_callback: method
        :param method on_open_error_callback: Called if the connection can't
            be established: on_open_error_callback(connection, str|exception)
        :param method on_close_callback: Called when the connection is closed:
            on_close_callback(connection, reason_code, reason_text)
        :param None | ioloop.IOLoop |
            pika.adapters.async_interface.AbstractAsyncServices custom_ioloop:
                Override using the global IOLoop in Tornado
        :param bool internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.

        """
        if isinstance(custom_ioloop,
                      pika.adapters.async_interface.AbstractAsyncServices):
            async_services = custom_ioloop
        else:
            async_services = (
                selector_ioloop_adapter.SelectorAsyncServicesAdapter(
                    custom_ioloop or ioloop.IOLoop.instance()))
        super(TornadoConnection, self).__init__(
            parameters,
            on_open_callback,
            on_open_error_callback,
            on_close_callback,
            async_services,
            internal_connection_workflow=internal_connection_workflow)

    @classmethod
    def create_connection(cls,
                          connection_configs,
                          on_done,
                          custom_ioloop=None,
                          workflow=None):
        """Override `BaseConnection.create_connection()` pure virtual method.

        See `BaseConnection.create_connection()` documentation.

        """
        async_services = selector_ioloop_adapter.SelectorAsyncServicesAdapter(
            custom_ioloop or ioloop.IOLoop.instance())

        def connection_factory(params):
            """Connection factory."""
            if params is None:
                raise ValueError('Expected pika.connection.Parameters '
                                 'instance, but got None in params arg.')
            return cls(
                parameters=params,
                custom_ioloop=async_services,
                internal_connection_workflow=False)

        return cls._start_connection_workflow(
            connection_configs=connection_configs,
            connection_factory=connection_factory,
            async_services=async_services,
            workflow=workflow,
            on_done=on_done)
