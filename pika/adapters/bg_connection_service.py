"""`BackgroundConnectionService` runs the true AMQP connection instance in
a background thread. It communicates with `ThreadedConnection` via
thread-safe queues and an inbound event socket for efficient event-driven
I/O.

"""

# Disable pylint messages concerning "Too few public methods"
# pylint: disable=R0903


class BackgroundConnectionService(object):
    """`BackgroundConnectionService` runs the true AMQP connection instance in
    a background thread. It communicates with `ThreadedConnection` via
    thread-safe queues and an inbound event socket for efficient event-driven
    I/O.

    """
    pass


class ServiceProxy(object):
    """Interface to `BackgroundConnectionService` for use by
    `ThreadedConnection`
    """

    def __init__(self, queue, notify):
        """
        :param Queue.Queue queue: Thread-safe queue for depositing events
            destined for `BackgroundConnectionService`
        :param callable notify: Function for notifying
            `BackgroundConnectionService` that an event is available; it has
            the signature`notify()`

        """
        self._evt_queue = queue
        self._notify = notify

    def send(self, event):
        """Send an event to `BackgroundConnectionService`

        :param event: event object destined for client

        """
        self._evt_queue.put(event)
        self._notify()


class ClientProxy(object):
    """Interface to `ThreadedConnection` for use by
    `BackgroundConnectionService`

    """

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


class RpcEvent(object):
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


class AsyncEvent(object):
    """Base class for client or service-destined event that has no reply"""
    pass


class ClientRegEvent(RpcEvent):
    """Client registration event for registering a client with
    `BackgroundConnectionService`.
    """

    def __init__(self, client, on_result_rx):
        """
        :param ClientProxy:
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.

        """
        super(ClientRegEvent, self).__init__(on_result_rx)
        self.client = client


class ClientUnregEvent(RpcEvent):
    """Client de-registration event for unregistering a client with
    `BackgroundConnectionService`.
    """

    def __init__(self, client, on_result_rx):
        """
        :param ClientProxy:
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.

        """
        super(ClientUnregEvent, self).__init__(on_result_rx)
        self.client = client


class ChannelRegEvent(AsyncEvent):
    """Channel registration event for informing `BackgroundConnectionService`
    of the association between channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.channel_number = channel_number
        self.client = client


class ChannelUnregEvent(AsyncEvent):
    """Channel de-registration event for informing
    `BackgroundConnectionService` to remove the association between the given
    channel number and client

    """

    def __init__(self, client, channel_number):
        """
        :param int channel_number:
        :param ClientProxy:

        """
        self.channel_number = channel_number
        self.client = client


class BlockedConnectionSubscribeEvent(AsyncEvent):
    """Request from a client for receiving "blocked/unblocked" connection
    frames from `BackgroundConnectionService`
    """

    def __init__(self, client):
        """
        :param ClientProxy:

        """
        self.client = client


class DataToBrokerEvent(RpcEvent):
    """Container for serialized frames destined for AMQP broker. This type of
    event is dispatched by `ThreadedConnection` to
    `BackgroundConnectionService`.

    """

    def __init__(self, client, buffers, on_result_rx):
        """
        :param ClientProxy:
        :param buffers: sequence of serialized frames destined for AMQP broker
        :param callable on_result_rx: callable for handling the result in user
            context, having the signature `on_result_rx(result)`, where `result`
            is the value provided by responder.

        """
        super(DataToBrokerEvent, self).__init__(on_result_rx)
        self.client = client
        self.buffers = buffers


class FramesToClientEvent(AsyncEvent):
    """Container for frames destined for client. This type of event is
    dispatched by `BackgroundConnectionService` to `ThreadedConnection`
    """

    def __init__(self, frames):
        """
        :param frames: sequence of `pika.spec` frames destined for client

        """
        self.frames = frames
