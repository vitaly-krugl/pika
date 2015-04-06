"""The SynchronousConnection is a direct replacement for BlockingConnection.
It's implemented as an adapter around SelectConnection.
"""

from collections import namedtuple
import logging
import time

import pika.channel
from pika import exceptions
import pika.spec
# NOTE: import SelectConnection after others to avoid circular depenency
from pika.adapters.select_connection import SelectConnection

LOGGER = logging.getLogger(__name__)


class _ChannelWrapper(pika.channel.Channel):
    """ We ask SelectConnection to create channels of this class that we can
    use to override method calls for debugging

    TODO Remove this class
    """
    pass


class _CallbackResult(object):
    """ CallbackResult is a non-thread-safe implementation for receiving
    callback results; INTERNAL USE ONLY!
    """
    def __init__(self, value_class=None):
        """
        :param callable value_class: only needed if the CallbackResult
                                     instance will be used with
                                     `set_value_once` and `append_element`.
                                     *args and **kwargs of the value setter
                                     methods will be passed to this class.
                                        
        """
        self._value_class = value_class
        self.reset()

    def reset(self):
        self._ready = False
        self._values = None

    def __bool__(self):
        """ Called by python runtime to implement truth value testing and the
        built-in operation bool(); NOTE: python 3.x
        """
        return self.is_ready()

    # python 2.x version of __bool__
    __nonzero__ = __bool__

    def __enter__(self):
        """ Entry into context manager that automatically resets the object
        on exit; this usage pattern helps garbage-collection by eliminating
        potential circular references.
        """
        return self

    def __exit__(self, *args, **kwargs):
        self.reset()

    def is_ready(self):
        return self._ready
    
    @property
    def ready(self):
        return self._ready

    def signal_once(self, *_args, **_kwargs):
        """ Set as ready

        :raises AssertionError: if result was already signalled
        """
        assert not self._ready, '_CallbackResult was already set'
        self._ready = True

    def set_value_once(self, *args, **kwargs):
        """ Set as ready with value; the value may be retrived via the `value`
        property getter

        :raises AssertionError: if result was already set
        """
        self.signal_once()
        try:
            self._values = (self._value_class(*args, **kwargs),)
        except Exception as e:
            LOGGER.error(
                "set_value_once failed: value_class=%r; args=%r; kwargs=%r",
                self._value_class, args, kwargs)
            raise

    def append_element(self, *args, **kwargs):
        """
        """
        assert not self._ready or isinstance(self._values, list), (
            '_CallbackResult state is incompatible with append_element: '
            'ready=%r; values=%r' % (self._ready, self._values))

        try:
            value = self._value_class(*args, **kwargs)
        except Exception as e:
            LOGGER.error(
                "append_element failed: value_class=%r; args=%r; kwargs=%r",
                self._value_class, args, kwargs)
            raise

        if self._values is None:
            self._values = [value]
        else:
            self._values.append(value)

        self._ready = True


    @property
    def value(self):
        """
        :returns: a reference to the value that was set via `set_value_once`
        :raises AssertionError: if result was not set or value is incompatible
                                with `set_value_once`
        """
        assert self._ready, '_CallbackResult was not set'
        assert isinstance(self._values, tuple) and len(self._values) == 1, (
            '_CallbackResult value is incompatible with set_value_once: %r'
            % (self._values,))

        return self._values[0]


    @property
    def elements(self):
        """
        :returns: a reference to the list containing one or more elements that
            were added via `append_element`
        :raises AssertionError: if result was not set or value is incompatible
                                with `append_element`
        """
        assert self._ready, '_CallbackResult was not set'
        assert isinstance(self._values, list) and len(self._values) > 0, (
            '_CallbackResult value is incompatible with append_element: %r'
            % (self._values,))

        return self._values


class SynchronousConnection(object):
    """
    TODO flesh out docstring

    """
    # Connection-opened callback args
    _OnOpenedArgs = namedtuple('SynchronousConnection__OnOpenedArgs',
                               'connection')

    # Connection-establishment error callback args
    _OnOpenErrorArgs = namedtuple('SynchronousConnection__OnOpenErrorArgs',
                                  'connection error_text')

    # Connection-closing callback args
    _OnClosedArgs = namedtuple('SynchronousConnection__OnClosedArgs',
                               'connection reason_code reason_text')

    # Channel-opened callback args
    _OnChannelOpenedArgs = namedtuple(
        'SynchronousConnection__OnChannelOpenedArgs',
        'channel')

    def __init__(self, parameters=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :raises: RuntimeError

        """
        # Receives on_open_callback args from Connection
        self._opened_result = _CallbackResult(self._OnOpenedArgs)

        # Receives on_open_error_callback args from Connection
        self._open_error_result= _CallbackResult(self._OnOpenErrorArgs)

        # Receives on_close_callback args from Connection
        self._closed_result = _CallbackResult(self._OnClosedArgs)

        # Set to True when when user calls close() on the connection
        # NOTE: this is a workaround to detect socket error because
        # on_close_callback passes reason_code=0 when called due to socket error
        self._user_initiated_close = False

        # TODO verify suitability of stop_ioloop_on_close value 
        self._impl = SelectConnection(
            parameters=parameters,
            on_open_callback=self._opened_result.set_value_once,
            on_open_error_callback=self._open_error_result.set_value_once,
            on_close_callback=self._closed_result.set_value_once,
            stop_ioloop_on_close=False)

        self._process_io_for_connection_setup()

    def _clean_up(self):
        """ Perform clean-up that is necessary for re-connecting

        """
        self._opened_result.reset()
        self._closed_result.reset()
        self._open_error_result.reset()
        self._user_initiated_close = False

    def _process_io_for_connection_setup(self):
        """ Perform follow-up processing for connection setup request: flush
        connection output and process input while waiting for connection-open
        or connection-error.

        :raises AMQPConnectionError: on connection open error
        """
        self._flush_output(self._opened_result.is_ready,
                           self._open_error_result.is_ready)

        if self._open_error_result.ready:
            raise exceptions.AMQPConnectionError(
                self._open_error_result.value.error_text)

        assert self._opened_result.ready
        assert self._opened_result.value.connection is self._impl

    def _flush_output(self, *waiters):
        """ Flush output and process input while waiting for any of the given
        callbacks to return true. The wait is aborted upon connection-close.
        Otherwise, processing continues until the output is flushed AND at least
        one of the callbacks returns true. If there are no callbacks, then
        processing ends when all output is flushed.

        :param waiters: sequence of zero or more callables taking no args and
                        returning true when it's time to stop processing.
                        Their results are OR'ed together.
        """
        if self._impl.is_closed:
            raise exceptions.ConnectionClosed()

        # Conditions for terminating the processing loop:
        #   connection closed
        #         OR
        #   empty outbound buffer and no waiters
        #         OR
        #   empty outbound buffer and any waiter is ready
        check_completion = (lambda:
            self._closed_result.ready or
            (not self._impl.outbound_buffer and
             (not waiters or any(ready() for ready in  waiters))))

        self._impl._process_io_and_events(check_completion)

        if self._closed_result.ready:
            result = self._closed_result.value
            if result.reason_code not in [0, 200]:
                LOGGER.critical('Connection close detected; result=%r', result)
                raise exceptions.ConnectionClosed(*result)
            elif not self._user_initiated_close:
                # NOTE: unfortunately, upon socket error, on_close_callback
                # presently passes reason_code=0, so we don't detect that as an
                # error
                LOGGER.critical('Connection close detected')
                raise exceptions.ConnectionClosed()
            else:
                LOGGER.info('Connection closed; result=%r', result)

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        LOGGER.info('Closing connection (%s): %s', reply_code, reply_text)
        self._user_initiated_close = True
        self._impl.close(reply_code, reply_text)

        self._flush_output(self._closed_result.is_ready)

        assert self._closed_result.ready

    def add_backpressure_callback(self, callback_method):
        """Call method "callback" when pika believes backpressure is being
        applied.

        :param method callback_method: The method to call

        """
        self._impl.add_backpressure_callback(callback_method)

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :returns: timeout id

        """
        return self._impl.add_timeout(deadline, callback_method)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :param str timeout_id: The id of the timeout to remove

        """
        self._impl.remove_timeout(timeout_id)

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :rtype: pika.synchronous_connection.SynchronousChannel
        """
        with _CallbackResult(self._OnChannelOpenedArgs) as openedArgs:
            channel = self._impl.channel(
                on_open_callback=openedArgs.set_value_once,
                channel_number=channel_number,
                _channel_class=_ChannelWrapper)

            channel = SynchronousChannel(channel, self)
            channel._flush_output(openedArgs.is_ready)

        return channel

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        assert not self._impl.is_open, (
            'Connection was not closed; connection_state=%r'
            % (self._impl.connection_state,))

        self._clean_up()

        self._impl.connect()

        self._process_io_for_connection_setup()

    def set_backpressure_multiplier(self, value=10):
        """Alter the backpressure multiplier value. We set this to 10 by default.
        This value is used to raise warnings and trigger the backpressure
        callback.

        :param int value: The multiplier value to set

        """
        self._impl.set_backpressure_multiplier(value)

    def sleep(self, duration):
        """A safer way to sleep than calling time.sleep() directly which will
        keep the adapter from ignoring frames sent from RabbitMQ. The
        connection will "sleep" or block the number of seconds specified in
        duration in small intervals.

        :param float duration: The time to sleep in seconds

        """
        assert duration >= 0, duration

        deadline = time.time() + duration
        self._flush_output(lambda: time.time() >= deadline)

    #
    # Connections state properties
    #

    @property
    def is_closed(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_closing

    @property
    def is_open(self):
        """
        Returns a boolean reporting the current connection state.
        """
        return self._impl.is_open

    #
    # Properties that reflect server capabilities for the current connection
    #

    @property
    def basic_nack(self):
        """Specifies if the server supports basic.nack on the active connection.

        :rtype: bool

        """
        return self._impl.basic_nack

    @property
    def consumer_cancel_notify(self):
        """Specifies if the server supports consumer cancel notification on the
        active connection.

        :rtype: bool

        """
        return self._impl.consumer_cancel_notify

    @property
    def exchange_exchange_bindings(self):
        """Specifies if the active connection supports exchange to exchange
        bindings.

        :rtype: bool

        """
        return self._impl.exchange_exchange_bindings

    @property
    def publisher_confirms(self):
        """Specifies if the active connection can use publisher confirmations.

        :rtype: bool

        """
        return self._impl.publisher_confirms


class SynchronousChannel(object):
    """The SynchronousChannel implements blocking semantics for most things that
    one would use callback-passing-style for with the
    :py:class:`~pika.channel.Channel` class. In addition,
    the `SynchronousChannel` class implements a :term:`generator` that allows
    you to :doc:`consume messages </examples/blocking_consumer_generator>`
    without using callbacks.

    Example of creating a SynchronousChannel::

        import pika

        # Create our connection object
        connection = pika.BlockingConnection()

        # The returned object will be a synchronous channel
        channel = connection.channel()

    :param channel_impl: Channel implementation object as returned from
                         SelectConnection.channel()
    :param SynchronousConnection connection: The connection object

    TODO fill in missing channel methods see BlockingChannel methods in
    http://pika.readthedocs.org/en/latest/modules/adapters/blocking.html

    """

    # `SynchronousChannel.consume_messages()` yields incoming messages as
    # instances of this class
    Delivery = namedtuple(
        'SynchronousChannel_Delivery',
        [
            'consumer_tag', # str
            'delivery_tag', # str
            'redelivered',  # bool
            'exchange',     # str
            'routing_key',  # str
            'properties',   # pika.BasicProperties
            'body'          # str or equivalent
        ])

    # `SynchronousChannel.consume_messages()` yields server-initiated consumer
    # cancellation notices as instances of this class
    ConsumerCancellation = namedtuple(
        'SynchronousChannel_ConsumerCancellation',
        [
            'consumer_tag'      # str
        ])

    # Basic.Return args from broker
    _OnMessageReturnedArgs = namedtuple(
        'SynchronousChannel__OnMessageReturnedArgs',
        [
            'channel',       # implementation Channel instance
            'method',        # spec.Basic.Return
            'properties',    # pika.spec.BasicProperties
            'body'           # returned message body (None or str/equivalent)
        ])


    # Basic.Deliver args from broker
    _OnMessageDeliveredArgs = namedtuple(
        'SynchronousChannel__OnMessageDeliveredArgs',
        [
            'channel',       # implementation Channel instance
            'method',        # Basic.Deliver or Basic.Cancel
            'properties',    # pika.spec.BasicProperties; ignore if Basic.Cancel
            'body'           # returned message body; ignore if Basic.Cancel
        ])


    # For use by any _CallbackResult that expects method_frame as the only
    # arg
    _MethodFrameCallbackResultArgs = namedtuple(
        'SynchronousChannel__MethodFrameCallbackResultArgs',
        'method_frame')

    # Broker's basic-ack/basic-nack when delivery confirmation is enabled;
    # may concern a single or multiple messages
    _OnMessageConfirmationReportArgs = namedtuple(
        'SynchronousChannel__OnMessageConfirmationReportArgs',
        'method_frame')

    # Basic.GetEmpty response args
    _OnGetEmptyResponseArgs = namedtuple(
        'SynchronousChannel__OnGetEmptyResponseArgs',
        'method_frame')

    # Parameters for broker-inititated Channel.Close request: reply_code
    # holds the broker's non-zero error code and reply_text holds the
    # corresponding error message text.
    _OnChannelClosedByBrokerArgs = namedtuple(
        'SynchronousChannel__OnChannelClosedByBrokerArgs',
        'method_frame')


    def __init__(self, channel_impl, connection):
        """Create a new instance of the Channel

        :param channel_impl: Channel implementation object as returned from
                             SelectConnection.channel()
        :param SynchronousConnection connection: The connection object

        """
        self._impl = channel_impl
        self._connection = connection

        # Whether `basic_publish` has been called at least once
        self._basic_publish_used = False

        # Whether RabbitMQ delivery confirmation has been enabled
        self._delivery_confirmation = False

        # Receives message delivery confirmation report (Basic.ack or
        # Basic.nack) from broker when delivery confirmations are enabled
        self._message_confirmation_result = _CallbackResult(
            self._OnMessageConfirmationReportArgs)

        # Receives Basic.Return results when published messages are returned by
        # broker
        self._message_return_results = _CallbackResult(
            self._OnMessageReturnedArgs)

        self._impl.add_on_return_callback(self._on_message_returned)

        # Receives multiple consumer message delivery notifications and is also
        # overloaded to receive Basic.Cancel notifications from rabbitmq
        self._message_delivery_results = _CallbackResult(
            self._OnMessageDeliveredArgs)

        self._impl.add_on_cancel_callback(
            lambda method_frame:
                self._message_delivery_results.append_element(
                    channel=self._impl,
                    method=method_frame.method,
                    properties=None,
                    body=None))

        # Receives Basic.ConsumeOk reply from server
        self._basic_consume_ok_result = _CallbackResult()
        self._impl.add_callback(
            self._basic_consume_ok_result.signal_once,
            replies=[pika.spec.Basic.ConsumeOk],
            one_shot=False)

        # Receives the broker-inititated Channel.Close parameters
        self._channel_closed_by_broker_result = _CallbackResult(
            self._OnChannelClosedByBrokerArgs)

        self._impl.add_callback(
            self._channel_closed_by_broker_result.set_value_once,
            replies=[pika.spec.Channel.Close],
            one_shot=True)

        # Receives args from Basic.GetEmpty response
        #  http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get
        self._nothing_to_get_result = _CallbackResult(
            self._OnGetEmptyResponseArgs)

        self._impl.add_callback(
            self._nothing_to_get_result.set_value_once,
            replies=[pika.spec.Basic.GetEmpty],
            one_shot=False)

        LOGGER.info("Created channel=%s", self._impl.channel_number)

    @property
    def connection(self):
        return self._connection

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        return self._impl.is_closed

    @property
    def is_closing(self):
        """Returns True if the channel is closing.

        :rtype: bool

        """
        return self._impl.is_closing

    @property
    def is_open(self):
        """Returns True if the channel is open.

        :rtype: bool

        """
        return self._impl.is_open

    _ALWAYS_READY_WAITERS = ((lambda: True), )

    def _flush_output(self, *waiters):
        """ Flush output and process input while waiting for any of the given
        callbacks to return true. The wait is aborted upon channel-close or
        connection-close.
        Otherwise, processing continues until the output is flushed AND at least
        one of the callbacks returns true. If there are no callbacks, then
        processing ends when all output is flushed.

        :param waiters: sequence of zero or more callables taking no args and
                        returning true when it's time to stop processing.
                        Their results are OR'ed together.
        """
        if self._impl.is_closed:
            raise exceptions.ChannelClosed()

        if not waiters:
            waiters = self._ALWAYS_READY_WAITERS

        self._connection._flush_output(
            self._channel_closed_by_broker_result.is_ready,
            *waiters)

        if self._channel_closed_by_broker_result:
            # Channel was force-closed by broker
            raise exceptions.ChannelClosed(
                self._channel_closed_by_broker_result.value)

    def _on_message_returned(self, args):
        """ Called as the result of Basic.Returns from broker. If
        delivery-confirmation is enabled, appends the info to
        self._message_return_results
        
        :param args: a 4-tuple of the following elements:
                     pika.Channel channel: our self._impl channel
                     pika.spec.Basic.Return method:
                     pika.spec.BasicProperties properties: message properties
                     str body: returned message body (may be None)
        """
        args = self._OnMessageReturnedArgs(*args)
        
        assert args.channel is self._impl, (
            args.channel.channel_number, self._impl.channel_number)

        assert isinstance(args.method, pika.spec.Basic.Return), args.method
        assert isinstance(args.properties, pika.spec.BasicProperties), (
            args.properties)

        LOGGER.warn(
            "Published message was returned: _delivery_confirmation=%s; "
            "channel=%s; method=%r; properties=%r; body_size=%d; "
            "body_prefix=%r", self._delivery_confirmation,
            args.channel.channel_number, args.method, args.properties,
            len(args.body) if args.body is not None else None,
            args.body[:30] if args.body is not None else None)

        if self._delivery_confirmation:
            self._message_return_results.append_element(*args)

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)
        try:
            with _CallbackResult() as close_ok_result:
                self._impl.add_callback(callback=close_ok_result.signal_once,
                                        replies=[pika.spec.Channel.CloseOk],
                                        one_shot=True)
        
                self._impl.close(reply_code=reply_code, reply_text=reply_text)
                self._flush_output(close_ok_result.is_ready)
        finally:
            # Clean up members that might inhibit garbage collection
            self._message_confirmation_result.reset()
            self._message_return_results.reset()

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        """
        self._impl.basic_ack(delivery_tag=delivery_tag, multiple=multiple)
        self._flush_output()

    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_nack(delivery_tag=delivery_tag, multiple=multiple,
                              requeue=requeue)
        self._flush_output()

    def create_consumer(self, queue='', no_ack=False, exclusive=False,
                        arguments=None):
        """Sends the AMQP command Basic.Consume to the broker.

        For more information on basic_consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param queue: The queue to consume from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a response
        :param bool exclusive: Don't allow other consumers on the queue
        :param dict arguments: Custom key/value pair arguments for the consume
        :returns: consumer tag
        :rtype: str

        """
        with self._basic_consume_ok_result as ok_result:
            ok_result.reset()
            consumer_tag = self._impl.basic_consume(
                consumer_callback=self._message_delivery_results.append_element,
                queue=queue,
                no_ack=no_ack,
                exclusive=exclusive,
                arguments=arguments)
    
            self._flush_output(ok_result.is_ready)

        return consumer_tag

    def cancel_consumer(self, consumer_tag):
        """ Cancel consumer with the given consumer_tag

        :param str consumer_tag: consumer tag
        """
        if consumer_tag not in self._impl._consumers:
            LOGGER.error("Attempting to cancel an unknown consumer=%s; "
                         "already cancelled?", consumer_tag)
            return

        with _CallbackResult() as cancel_ok_result:
            self._impl.basic_cancel(
                callback=cancel_ok_result.signal_once,
                consumer_tag=consumer_tag,
                nowait=False)
            self._flush_output(cancel_ok_result.is_ready)

    def consume_messages(self, inactivity_timeout=None):
        """ Creates a generator iterator that yields events from all active
        consumers. The iterator terminates when there are no more consumers.
        The following events may be yielded:

            SynchronousChannel.Delivery - contains a message.
            SynchronousChannel.ConsumerCancellation - broker-initiated consumer
                cancellation.
            None - upon expiration of inactivity timeout, if one was specified.

        :param float inactivity_timeout: if a number is given (in seconds), will
            cause the generator to yield None after the given period of
            inactivity; this permits for pseudo-regular maintenance activities
            to be carried out by the user while waiting for messages to arrive.
            NOTE that the underlying implementation doesn't permit a high level
            of timing granularity.
        """
        with _CallbackResult() as timeoutResult:
            if inactivity_timeout is None:
                waiters = (self._message_delivery_results.is_ready,)
            else:
                waiters = (self._message_delivery_results.is_ready,
                           timeoutResult.is_ready)

            while self._impl._consumers:
                if inactivity_timeout is not None:
                    # Start inactivity timer
                    timeout_id = self._connection.add_timeout(
                        inactivity_timeout,
                        timeoutResult.signal_once)
                try:
                    # Wait for message delivery or inactivity timeout, whichever
                    # occurs first
                    self._flush_output(*waiters)
                finally:
                    if inactivity_timeout is not None:
                        # Reset timer
                        timeoutResult.reset()
                        if not timeoutResult:
                            self._connection.remove_timeout(timeout_id)

                if self._message_delivery_results:
                    # Got message(s) and/or consumer cancellation(s)
                    # NOTE: new deliveries may occur as the side-effect of
                    # user's activity on the channel or connection
                    events = self._message_delivery_results.elements
                    self._message_delivery_results.reset()

                    for evt in events:
                        if evt.method.__class__ is pika.spec.Basic.Deliver:
                            yield self.Delivery(
                                consumer_tag=evt.method.consumer_tag,
                                delivery_tag=evt.method.delivery_tag,
                                redelivered=evt.method.redelivered,
                                exchange=evt.method.exchange,
                                routing_key=evt.method.routing_key,
                                properties=evt.properties,
                                body=evt.body)
                        elif evt.method.__class__ is pika.spec.Basic.Cancel:
                            yield self.ConsumerCancellation(
                                consumer_tag=evt.method.consumer_tag)
                        else:
                            raise RuntimeError("Consumed unexpected event=%r"
                                               % (evt,))
                    else:
                        del events
                else:
                    # Inactivity timeout
                    yield None

    def basic_get(self, queue=None, no_ack=False):
        """Get a single message from the AMQP broker. Returns a set with the 
        method frame, header frame and body.

        :param queue: The queue to get a message from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a reply
        :rtype: (None, None, None)|(spec.Basic.Get,
                                    spec.Basic.Properties,
                                    str or unicode)

        """
        raise NotImplementedError

        # TODO define actual on_basic_get_ok
        self._impl.basic_get(callback=None if nowait else on_basic_get_ok,
                             queue=queue,
                             no_ack=no_ack)
        # TODO pump messages and wait for get-ok or Basic.GetEmpty
        # TODO return value form get_ok_callback or three-tuple with None's
        #  from Basic.GetEmpty

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        Returns a boolean value indicating the success of the operation. For 
        more information on basic_publish and what the parameters do, see:

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the return.

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body
        :type body: str or unicode
        :param pika.spec.Properties properties: Basic.properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :returns: None if delivery confirmation is not enabled; otherwise
                  returns False if the message could not be deliveved (
                  Basic.nack or msg return) and True if the message was
                  delivered (Basic.ack and no msg return)
        """
        self._basic_publish_used = True

        self._message_return_results.reset()

        with self._message_return_results, self._message_confirmation_result:
            self._impl.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=body,
                                     properties=properties,
                                     mandatory=mandatory,
                                     immediate=immediate)
            if self._delivery_confirmation:
                self._flush_output(self._message_confirmation_result.is_ready)
                conf_method = (self._message_confirmation_result.value
                               .method_frame
                               .method)
                if isinstance(conf_method, pika.spec.Basic.Ack):
                    if self._message_return_results.is_ready():
                        # Message was returned by broker
                        result = False
                    else:
                        # Broker accepted responsibility for message
                        result = True
                elif isinstance(conf_method, pika.spec.Basic.Nack):
                    # Broker was unable to process the message due to internal
                    # error
                    LOGGER.warn(
                        "Message was Nack'ed by broker: nack=%r; channel=%s; "
                        "exchange=%s; routing_key=%s; mandatory=%r; "
                        "immediate=%r", conf_method, self._impl.channel_number,
                        exchange, routing_key, mandatory, immediate)
                    result = False
                else:
                    raise ValueError('Unexpected method type: %r', conf_method)
            else:
                self._flush_output()
                result = None  # Non-confirmation-mode result

            
            return result

    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        """Specify quality of service. This method requests a specific quality
        of service. The QoS can be specified for the current channel or for all
        channels on the connection. The client can request that messages be sent
        in advance so that when the client finishes processing a message, the
        following message is already held locally, rather than needing to be
        sent down the channel. Prefetching gives a performance improvement.

        :param int prefetch_size:  This field specifies the prefetch window
                                   size. The server will send a message in
                                   advance if it is equal to or smaller in size
                                   than the available prefetch size (and also
                                   falls into other prefetch limits). May be set
                                   to zero, meaning "no specific limit",
                                   although other prefetch limits may still
                                   apply. The prefetch-size is ignored if the
                                   no-ack option is set.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored if the no-ack
                                   option is set.
        :param bool all_channels: Should the QoS apply to all channels

        """
        with _CallbackResult() as qos_ok_result:
            self._impl.basic_qos(callback=qos_ok_result.signal_once,
                                 prefetch_size=prefetch_size,
                                 prefetch_count=prefetch_count,
                                 all_channels=all_channels)
            self._flush_output(qos_ok_result.is_ready)

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        with _CallbackResult() as recover_ok_result:
            self._impl.basic_recover(callback=recover_ok_result.signal_once,
                                     requeue=requeue)
            self._flush_output(recover_ok_result.is_ready)

    def basic_reject(self, delivery_tag=None, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
        self._flush_output()

    def confirm_delivery(self, nowait=False):
        """Turn on RabbitMQ-proprietary Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :param bool nowait: Do not send a reply frame (Confirm.SelectOk)

        """
        if self._delivery_confirmation:
            LOGGER.error('confirm_delivery: confirmation was already enabled '
                         'on channel=%s', self._impl.channel_number)
            return

        if self._basic_publish_used:
            # Force synchronization with the broker to flush any pending
            # basic-returns on messages that might have been published
            # prior to this call
            if nowait:
                LOGGER.warn('confirm_delivery: overriding nowait on channel=%s '
                            'to force synchronization on previously-published '
                            'channel', self._impl.channel_number)
            nowait = False

        with _CallbackResult() as select_ok_result:
            if not nowait:
                self._impl.add_callback(callback=select_ok_result.signal_once,
                                        replies=[pika.spec.Confirm.SelectOk],
                                        one_shot=True)
    
            self._impl.confirm_delivery(
                callback=self._message_confirmation_result.set_value_once,
                nowait=nowait)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(select_ok_result.is_ready)

        self._delivery_confirmation = True

    def force_data_events(self, enable):
        """Turn on and off forcing the blocking adapter to stop and look to see
        if there are any frames from RabbitMQ in the read buffer. By default
        the BlockingChannel will check for a read after every RPC command which
        can cause performance to degrade in scenarios where you do not care if
        RabbitMQ is trying to send RPC commands to your client connection.

        NOTE: This is a NO-OP here, since we're fixing the performance issue;
        provided for API compatibility with BlockingChannel

        Examples of RPC commands of this sort are:

        - Heartbeats
        - Connection.Close
        - Channel.Close
        - Basic.Return
        - Basic.Ack and Basic.Nack when using delivery confirmations

        Turning off forced data events can be a bad thing and prevents your
        client from properly communicating with RabbitMQ. Forced data events
        were added in 0.9.6 to enforce proper channel behavior when
        communicating with RabbitMQ.

        Note that the BlockingConnection also has the constant
        WRITE_TO_READ_RATIO which forces the connection to stop and try and
        read after writing the number of frames specified in the constant.
        This is a way to force the client to received these types of frames
        in a very publish/write IO heavy workload.

        :param bool enable: Set to False to disable

        """
        # This is a NO-OP here, since we're fixing the performance issue
        LOGGER.warn("%s.force_data_events() is a NO-OP",
                    self.__class__.__name__)
        pass

    def exchange_bind(self, destination=None, source=None, routing_key='',
                      nowait=False, arguments=None):
        """Bind an exchange to another exchange.

        :param destination: The destination exchange to bind
        :type destination: str or unicode
        :param source: The source exchange to bind to
        :type source: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as bind_ok_result:
            self._impl.exchange_bind(
                callback=None if nowait else bind_ok_result.signal_once,
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(bind_ok_result.is_ready)

    def exchange_declare(self, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False,
                         arguments=None, **kwargs):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param exchange: The exchange name consists of a non-empty sequence of
                          these characters: letters, digits, hyphen, underscore,
                          period, or colon.
        :type exchange: str or unicode
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param bool nowait: Do not expect an Exchange.DeclareOk response
        :param dict arguments: Custom key/value pair arguments for the exchange
        :param str type: via kwargs: the deprecated exchange type parameter

        """
        assert len(kwargs) <= 1, kwargs
        
        with _CallbackResult() as declare_ok_result:
            self._impl.exchange_declare(
                callback=None if nowait else declare_ok_result.signal_once,
                exchange=exchange,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                nowait=nowait,
                arguments=arguments,
                type=kwargs["type"] if kwargs else None)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(declare_ok_result.is_ready)

    def exchange_delete(self, exchange=None, if_unused=False, nowait=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused
        :param bool nowait: Do not wait for an Exchange.DeleteOk

        """
        with _CallbackResult() as delete_ok_result:
            self._impl.exchange_delete(
                callback=None if nowait else delete_ok_result.signal_once,
                exchange=exchange,
                if_unused=if_unused,
                nowait=nowait)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(delete_ok_result.is_ready)

    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        nowait=False, arguments=None):
        """Unbind an exchange from another exchange.

        :param destination: The destination exchange to unbind
        :type destination: str or unicode
        :param source: The source exchange to unbind from
        :type source: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for an Exchange.UnbindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as unbind_ok_result:
            self._impl.exchange_unbind(
                callback=None if nowait else unbind_ok_result.signal_once,
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(unbind_ok_result.is_ready)

    def queue_bind(self, queue, exchange, routing_key=None, nowait=False,
                   arguments=None):
        """Bind the queue to the specified exchange

        :param queue: The queue to bind to the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param bool nowait: Do not wait for a Queue.BindOk
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as bind_ok_result:
            self._impl.queue_bind(callback=None if nowait else bind_ok_result.signal_once,
                                  queue=queue,
                                  exchange=exchange,
                                  routing_key=routing_key,
                                  nowait=nowait,
                                  arguments=arguments)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(bind_ok_result.is_ready)

    def queue_declare(self, queue='', passive=False, durable=False,
                      exclusive=False, auto_delete=False, nowait=False,
                      arguments=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Leave the queue name empty for a auto-named queue in RabbitMQ

        :param queue: The queue name
        :type queue: str or unicode
        :param bool passive: Only check to see if the queue exists
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param bool nowait: Do not wait for a Queue.DeclareOk
        :param dict arguments: Custom key/value arguments for the queue

        """
        with _CallbackResult() as declare_ok_result:
            self._impl.queue_declare(
                callback=None if nowait else declare_ok_result.signal_once,
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                nowait=nowait,
                arguments=arguments)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(declare_ok_result.is_ready)

    def queue_delete(self, queue='', if_unused=False, if_empty=False,
                     nowait=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty
        :param bool nowait: Do not wait for a Queue.DeleteOk

        """
        with _CallbackResult() as delete_ok_result:
            self._impl.queue_delete(callback=None if nowait else delete_ok_result.signal_once,
                                    queue=queue,
                                    if_unused=if_unused,
                                    if_empty=if_empty,
                                    nowait=nowait)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(delete_ok_result.is_ready)

    def queue_purge(self, queue='', nowait=False):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode
        :param bool nowait: Do not expect a Queue.PurgeOk response

        """
        with _CallbackResult() as purge_ok_result:
            self._impl.queue_purge(callback=None if nowait else purge_ok_result.signal_once,
                                   queue=queue,
                                   nowait=nowait)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(purge_ok_result.is_ready)

    def queue_unbind(self, queue='', exchange=None, routing_key=None,
                     arguments=None):
        """Unbind a queue from an exchange.

        :param queue: The queue to unbind from the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind from
        :type exchange: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        """
        with _CallbackResult() as unbind_ok_result:
            self._impl.queue_unbind(callback=None if nowait else unbind_ok_result.signal_once,
                                    queue=queue,
                                    exchange=exchange,
                                    routing_key=routing_key,
                                    arguments=arguments)
            if nowait:
                self._flush_output()
            else:
                self._flush_output(unbind_ok_result.is_ready)

    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        """
        with _CallbackResult() as select_ok_result:
            self._impl.tx_select(select_ok_result.signal_once)
            self._flush_output(select_ok_result.is_ready)

    def tx_commit(self):
        """Commit a transaction."""
        with _CallbackResult() as commit_ok_result:
            self._impl.tx_commit(commit_ok_result.signal_once)
            self._flush_output(commit_ok_result.is_ready)

    def tx_rollback(self):
        """Rollback a transaction."""
        with _CallbackResult() as rollback_ok_result:
            self._impl.tx_rollback(rollback_ok_result.signal_once)
            self._flush_output(rollback_ok_result.is_ready)
