"""The SynchronousConnection is a direct replacement for BlockingConnection.
It's implemented as an adapter around SelectConnection.
"""

from collections import namedtuple
import logging
import time

from pika.adapters.select_connection import SelectConnection
import pika.channel
from pika.callback import CallbackResult
from pika import exceptions
import pika.spec

LOGGER = logging.getLogger(__name__)


class _ChannelWrapper(pika.channel.Channel):
    """ We ask SelectConnection to create channels of this class that we can
    use to override method calls for debugging

    TODO Remove this class
    """
    pass


class SynchronousConnection(object):
    """
    TODO flesh out docstring

    """
    # Connection-opened callback args
    _OnOpenedArgs = namedtuple('_OnOpenedArgs', 'connection')

    # Connection-establishment error callback args
    _OnOpenErrorArgs = namedtuple('_OnOpenErrorArgs', 'connection error_text')

    # Connection-closing callback args
    _OnClosedArgs = namedtuple('_OnClosedArgs', 
                               'connection reason_code reason_text')

    # Channel-opened callback args
    _OnChannelOpenedArgs = namedtuple('_OnChannelOpenedArgs', 'channel')

    def __init__(self, parameters=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :raises: RuntimeError

        """
        # Receives on_open_callback args from Connection
        self._on_open_args = CallbackResult(self._OnOpenedArgs)

        # Receives on_open_error_callback args from Connection
        self._on_open_error_args= CallbackResult(self._OnOpenErrorArgs)

        # Receives on_close_callback args from Connection
        self._on_closed_args = CallbackResult(self._OnClosedArgs)

        # Set to True when when user calls close() on the connection
        # NOTE: this is a workaround to detect socket error because
        # on_close_callback passes reason_code=0 when called due to socket error
        self._user_initiated_close = False

        # TODO verify suitability of stop_ioloop_on_close value 
        self._impl = SelectConnection(
            parameters=parameters,
            on_open_callback=self._on_open_args.set_value_once,
            on_open_error_callback=self._on_open_error_args.set_value_once,
            on_close_callback=self._on_closed_args.set_value_once,
            stop_ioloop_on_close=False)

        self._process_io_for_connection_setup()

    def _clean_up(self):
        """ Perform clean-up that is necessary for re-connecting

        """
        self._on_open_args.reset()
        self._on_closed_args.reset()
        self._on_open_error_args.reset()
        self._user_initiated_close = False

    def _process_io_for_connection_setup(self):
        """ Perform follow-up processing for connection setup request: flush
        connection output and process input while waiting for connection-open
        or conneciton-error.

        :raises AMQPConnectionError: on connection open error
        """
        self._flush_output(self._on_open_args.is_ready,
                           self._on_open_error_args.is_ready)

        if self._on_open_error_args.ready:
            raise exceptions.AMQPConnectionError(
                self._on_open_error_args.value.error_text)

        assert self._on_open_args.ready
        assert self._on_open_args.value.connection is self._impl

    def _flush_output(self, *waiters):
        """ Flush output and process input while waiting for any of the given
        callbacks to return true. The wait is aborted upon connection-close.
        Otherwise, processing continues until the output if flushed AND at least
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
            self._on_closed_args.ready or
            (not self._impl.outbound_buffer and
             (not waiters or any(ready() for ready in  waiters))))

        self._impl._process_io_and_events(check_completion)

        if self._on_closed_args.ready:
            result = self._on_closed_args.value
            LOGGER.critical('Connection close detected; result=%r', result)
            if result.reason_code not in [0, 200]:
                raise exceptions.ConnectionClosed(*result)
            elif not self._user_initiated_close:
                # NOTE: unfortunately, upon socket error, on_close_callback
                # presently passes reason_code=0, so we don't detect that as an
                # error
                raise exceptions.ConnectionClosed()

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

        self._flush_output(self._on_closed_args.is_ready)

        assert self._on_closed_args.ready

    def add_backpressure_callback(self, callback_method):
        """Call method "callback" when pika believes backpressure is being
        applied.

        :param method callback_method: The method to call

        """
        self._impl.add_backpressure_callback(callback_method)

    def add_on_close_callback(self, callback_method_unused):
        """This is not supported in SynchronousConnection. When a connection is
        closed in SynchronousConnection, a pika.exceptions.ConnectionClosed
        exception will be raised instead.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('SynchronousConnection will raise '
                                  'ConnectionClosed exception')

    def add_on_open_callback(self, callback_method_unused):
        """This method is not supported in SynchronousConnection.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('Connection callbacks not supported in '
                                  'SynchronousConnection')

    def add_on_open_error_callback(self, callback_method_unused,
                                   remove_default=False):
        """This method is not supported in SynchronousConnection.

        A pika.exceptions.AMQPConnectionError will be raised instead.

        :param method callback_method_unused: Unused
        :raises: NotImplementedError

        """
        raise NotImplementedError('Connection callbacks not supported in '
                                  'SynchronousConnection')

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :returns: timeout id

        """
        self._impl.add_timeout(deadline, callback_method)

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
        with CallbackResult(self._OnChannelOpenedArgs) as openedArgs:
            channel = self._impl.channel(
                on_open_callback=openedArgs.set_value_once,
                channel_number=None,
                _channel_class=_ChannelWrapper)

            self._flush_output(openedArgs.is_ready)

        return SynchronousChannel(channel, self)

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

        # TODO register for channel callbacks (on-error, on-cancel, etc.)
        # TODO register add_on_return_callback: here, if
        #  _delivery_confirmation is False, we log.warn and drop returned
        #  messages.
        # TODO Register on_channel_close via Channel.add_on_close_callback()
        # TODO Register callback for Basic.GetEmpty via Channel.add_callback()
        #  callback should expect only one parameter, frame.
        #  http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get

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

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)

        # TODO define actual on_channel_close_ok
        self._impl.add_callback(callback=on_channel_close_ok,
                                replies=[pika.spec.Channel.CloseOk],
                                one_shot=True)

        self._impl.close(reply_code=reply_code, reply_text=reply_text)
        # TODO pump messages and wait for on_channel_close_ok

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
        self._imp.basic_ack(delivery_tag=delivery_tag, multiple=multiple)
        # TODO flush output

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
        # TODO flush output

    def basic_consume(self, consumer_callback, queue='', no_ack=False,
                      exclusive=False, consumer_tag=None, arguments=None):
        """Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. Returns
        the consumer tag.

        For more information on basic_consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param method consumer_callback: The method to callback when consuming
        :param queue: The queue to consume from
        :type queue: str or unicode
        :param bool no_ack: Tell the broker to not expect a response
        :param bool exclusive: Don't allow other consumers on the queue
        :param consumer_tag: Specify your own consumer tag
        :type consumer_tag: str or unicode
        :param dict arguments: Custom key/value pair arguments for the consume
        :rtype: str

        """
        consumer_tag = self._impl.basic_consume(
            consumer_callback=consumer_callback,
            queue=queue,
            no_ack=no_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments)
        # TODO Flush output
    

    def basic_cancel(self, consumer_tag='', nowait=False):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param str consumer_tag: Identifier for the consumer
        :param bool nowait: Do not expect a Basic.CancelOk response

        """
        # TODO define actual on_consumer_cancel_ok
        self._impl.basic_cancel(
            callback=None if nowait else on_consumer_cancel_ok,
            consumer_tag=consumer_tag,
            nowait=nowait)
        # TODO pump messages; also wait for cancel-ok if nowait is false

    def start_consuming(self):
        """Starts consuming from registered callbacks."""
        # TODO
        pass

    def stop_consuming(self, consumer_tag=None):
        """Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.

        """
        # TODO
        pass

    def consume(self, queue, no_ack=False, exclusive=False):
        """Blocking consumption of a queue instead of via a callback. This
        method is a generator that returns messages a tuple of method,
        properties, and body.

        NOTE: This is a SynchronousChannel-specific extension of the
        pika.Channel API

        Example:

            for method, properties, body in channel.consume('queue'):
                print body
                channel.basic_ack(method.delivery_tag)

        You should call SynchronousChannel.cancel() when you escape out of the
        generator loop. Also note this turns on forced data events to make
        sure that any acked messages actually get acked.

        :param queue: The queue name to consume
        :type queue: str or unicode
        :param no_ack: Tell the broker to not expect a response
        :type no_ack: bool
        :param exclusive: Don't allow other consumers on the queue
        :type exclusive: bool
        :rtype: tuple(spec.Basic.Deliver, spec.BasicProperties, str or unicode)

        """
        # TODO borrow from BlockingChannel
        pass

    def cancel(self):
        """Cancel the consumption of a queue, rejecting all pending messages.
        This should only work with the generator-based
        SynchronousChannel.consume method. If you're looking to cancel a
        consumer issued with SynchronousChannel.basic_consume then you should
        call SynchronousChannel.basic_cancel.

        NOTE: This is a SynchronousChannel-specific extension of the
        pika.Channel API

        :return int: The number of messages requeued by Basic.Nack

        """
        # TODO borrow from BlockingChannel
        pass

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
        self._basic_publish_used = True

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
        self._impl.basic_publish(exchange=exchange,
                                 routing_key=routing_key,
                                 body=body,
                                 properties=properties,
                                 mandatory=mandatory,
                                 immediate=immediate)
        # TODO pump messages; if _delivery_confirmation mode, also wait for
        # confirmation that will come with callback registred via
        # confirm_delivery: if ack and no msg-return yet
        # (add_on_return_callback), then return success; if nack or msg-return,
        # then return failure

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
        # TODO define actual on_qos_ok
        self._impl.basic_qos(callback=on_qos_ok,
                             prefetch_size=prefetch_size,
                             prefetch_count=prefetch_count,
                             all_channels=all_channels)
        # TODO pump messages and wait for on_qos_ok

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        # TODO define actual on_recover_ok
        self._impl.basic_recover(callback=on_recover_ok, requeue=requeue)
        # TODO pump messages and wait for on_recover_ok

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
        # TODO flush output

    def confirm_delivery(self, nowait=False):
        """Turn on RabbitMQ-proprietary Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :param bool nowait: Do not send a reply frame (Confirm.SelectOk)

        """
        if self._delivery_confirmation:
            LOGGER.warn('confirm_delivery: confirmation was already enabled on '
                        'channel=%s', self._impl.channel_number)

        # Set it ahead of the call so that subsequent `basic_publish` calls
        # will use logic appropriate for this mode
        self._delivery_confirmation = True

        if self._basic_publish_used:
            # Force synchronization with the broker to flush any pending
            # basic-returns on messages that might have been published
            # prior to this call
            if nowait:
                LOGGER.warn('confirm_delivery: overriding nowait on channel=%s '
                            'to force synchronization on previously-published '
                            'channel', self._impl.channel_number)
            nowait = False

        # TODO define actual on_confirm_select_ok
        if not nowait:
            self._impl.add_callback(callback=on_confirm_select_ok,
                                    replies=[pika.spec.Confirm.SelectOk],
                                    one_shot=True)

        # TODO define actual on_msg_delivery_confirmation
        self._impl.confirm_delivery(callback=on_msg_delivery_confirmation,
                                    nowait=nowait)

        # TODO pump messages; also, if nowait=False, wait for
        # on_confirm_select_ok

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
                    self.__clas__.__name__)
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
        # TODO define on_exchange_bind_ok
        self._impl.exchange_bind(
            callback=None if nowait else on_exchange_bind_ok,
            destination=destination,
            source=source,
            routing_key=routing_key,
            nowait=nowait,
            arguments=arguments)
        # TODO pump messages; also wait for exchange bind-ok if nowait is false

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
        
        # TODO define on_exchange_declare_ok
        self._impl.exchange_declare(
            callback=None if nowait else on_exchange_declare_ok,
            exchange=exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            nowait=nowait,
            arguments=arguments,
            type=kwargs["type"] if kwargs else None)
        # TODO pump messages; also wait for exchange declare-ok if nowait is
        # false

    def exchange_delete(self, exchange=None, if_unused=False, nowait=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused
        :param bool nowait: Do not wait for an Exchange.DeleteOk

        """
        # TODO define on_exchange_delete_ok
        self._impl.exchange_delete(
            callback=None if nowait else on_exchange_delete_ok,
            exchange=exchange,
            if_unused=if_unused,
            nowait=nowait)
        # TODO pump messages; also wait for exchange delete-ok if nowait is
        # false

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
        # TODO define on_exchange_unbind_ok
        self._impl.exchange_unbind(
            callback=None if nowait else on_exchange_unbind_ok,
            destination=destination,
            source=source,
            routing_key=routing_key,
            nowait=nowait,
            arguments=arguments)
        # TODO pump messages; also wait for exchange unbind-ok if nowait is
        # false

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
        # TODO define actual on_queue_bind_ok
        self._impl.queue_bind(callback=None if nowait else on_queue_bind_ok,
                              queue=queue,
                              exchange=exchange,
                              routing_key=routing_key,
                              nowait=nowait,
                              arguments=arguments)
        # TODO pump messages; also wait for queue bind-ok if nowait is false

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
        # TODO define actual on_queue_declare_ok
        self._impl.queue_declare(
            callback=None if nowait else on_queue_declare_ok,
            queue=queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            nowait=nowait,
            arguments=arguments)
        # TODO pump messages; also wait for queue declare-ok if nowait is false

    def queue_delete(self, queue='', if_unused=False, if_empty=False,
                     nowait=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty
        :param bool nowait: Do not wait for a Queue.DeleteOk

        """
        # TODO define on_queue_delete_ok
        self._impl.queue_delete(callback=None if nowait else on_queue_delete_ok,
                                queue=queue,
                                if_unused=if_unused,
                                if_empty=if_empty,
                                nowait=nowait)
        # TODO pump messages; also wait for queue delete-ok if nowait is false

    def queue_purge(self, queue='', nowait=False):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode
        :param bool nowait: Do not expect a Queue.PurgeOk response

        """
        # TODO define on_queue_purge_ok
        self._impl.queue_purge(callback=None if nowait else on_queue_purge_ok,
                               queue=queue,
                               nowait=nowait)
        # TODO pump messages; also wait for queue purge-ok if nowait is false

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
        # TODO define on_queue_unbind_ok
        self._impl.queue_unbind(callback=None if nowait else on_queue_unbind_ok,
                                queue=queue,
                                exchange=exchange,
                                routing_key=routing_key,
                                arguments=arguments)
        # TODO pump messages; also wait for queue unbind-ok if nowait is false

    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        """
        self._impl.tx_select(on_tx_select_ok)
        # TODO flush output and wait for tx select-ok

    def tx_commit(self):
        """Commit a transaction."""
        self._impl.tx_commit(on_tx_commit_ok)
        # TODO flush output and wait for tx commit-ok

    def tx_rollback(self):
        """Rollback a transaction."""
        self._impl.tx_rollback(on_tx_rollback_ok)
        # TODO flush output and wait for tx rollback-ok
