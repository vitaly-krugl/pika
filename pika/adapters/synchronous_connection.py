"""The SynchronousConnection is a direct replacement for the BlockingConnection.
It's a subclass of and reuses 100% of SelectConnection.
"""

import logging
import time

from pika.adapters import select_connection
import pika.channel
import pika.spec

LOGGER = logging.getLogger(__name__)


class SynchronousConnection(select_connection.SelectConnection):
    """
    """

    def __init__(self, parameters=None):
        """Create a new instance of the Connection object.

        :param pika.connection.Parameters parameters: Connection parameters
        :raises: RuntimeError

        """
        # TODO define passed callbacks
        super(SynchronousConnection, self).__init__(parameters,
                                                    on_open_callback,
                                                    on_open_error_callback,
                                                    on_close_callback,
                                                    stop_ioloop_on_close=False)
        # TODO pump messages

    def add_timeout(self, deadline, callback_method):
        """Add the callback_method to the IOLoop timer to fire after deadline
        seconds. Returns a handle to the timeout. Do not confuse with
        Tornado's timeout where you pass in the time you want to have your
        callback called. Only pass in the seconds until it's to be called.

        :param int deadline: The number of seconds to wait to call callback
        :param method callback_method: The callback method
        :rtype: str

        """
        super(SynchronousConnection, self).add_timeout(deadline,
                                                       callback_method)

    def remove_timeout(self, timeout_id):
        """Remove the timeout from the IOLoop by the ID returned from
        add_timeout.

        :param str timeout_id: The id of the timeout to remove

        """
        super(SynchronousConnection, self).remove_timeout(timeout_id)

    def channel(self, channel_number=None):
        """Create a new channel with the next available channel number or pass
        in a channel number to use. Must be non-zero if you would like to
        specify but it is recommended that you let Pika manage the channel
        numbers.

        :rtype: pika.synchronous_connection.SynchronousChannel
        """
        # TODO define actual on_open_callback
        channel = super(SynchronousConnection, self).channel(
            on_open_callback,
            channel_number=None,
            _channel_class=SynchronousChannel)
        # TODO pump messages

        return channel

    def connect(self):
        """Invoke if trying to reconnect to a RabbitMQ server. Constructing the
        Connection object should connect on its own.

        """
        super(SynchronousConnection, self).connect()
        # TODO pump messages

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        """Disconnect from RabbitMQ. If there are any open channels, it will
        attempt to close them prior to fully disconnecting. Channels which
        have active consumers will attempt to send a Basic.Cancel to RabbitMQ
        to cleanly stop the delivery of messages prior to closing the channel.

        :param int reply_code: The code number for the close
        :param str reply_text: The text reason for the close

        """
        LOGGER.info("Closing connection (%s): %s", reply_code, reply_text)
        super(SynchronousConnection, self).close(reply_code, reply_text)
        # TODO pump messages

    def sleep(self, duration):
        """A safer way to sleep than calling time.sleep() directly which will
        keep the adapter from ignoring frames sent from RabbitMQ. The
        connection will "sleep" or block the number of seconds specified in
        duration in small intervals.

        :param float duration: The time to sleep in seconds

        """
        deadline = time.time() + duration
        while duration > 0:
            # TODO define _process_events
            super(SynchronousConnection, self)._process_events(duration)
            duration = time.time() - deadline

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


class SynchronousChannel(channel.Channel):

    def __init__(self, connection, channel_number, on_open_callback):
        """Create a new instance of the Channel

        :param SynchronousConnection connection: The connection
        :param int channel_number: The channel number for this instance
        :param method on_open_callback: The method to call on channel open

        """
        super(SynchronousChannel, self).__init__(connection,
                                                 channel_number,
                                                 on_open_callback)
        self.__delivery_confirmation = False

        # TODO register for channel callbacks (on-error, etc.)
        # TODO register add_on_return_callback: here, if
        #  __delivery_confirmation is False, we log.warn and drop returned
        #  messages.
        # TODO Register on_channel_close via Channel.add_on_close_callback()
        # TODO Register callback for Basic.GetEmpty via Channel.add_callback()
        #  callback should expect only one parameter, frame.
        #  http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get

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
        # TODO define actual cancel_ok_callback
        cancel_ok_callback = None if nowait else cancel_ok_callback
        super(SynchronousChannel, self).basic_cancel(cancel_ok_callback,
                                                     consumer_tag=consumer_tag,
                                                     nowait=nowait)
        # TODO pump messages; also wait for cancel-ok if nowait is false

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
        # TODO define actual get_ok_callback
        get_ok_callback = None if nowait else get_ok_callback
        super(SynchronousChannel, self).basic_get(get_ok_callback,
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

        :returns: None if delivery confirmation is disabled; otherwise returns
                  False if the message could not be deliveved (Basic.nack or
                  msg return) and True if the message was delivered (Basic.ack
                  and no msg return)
        """
        super(SynchronousChannel, self).basic_publish(exchange=exchange,
                                                      routing_key=routing_key,
                                                      body=body,
                                                      properties=properties,
                                                      mandatory=mandatory,
                                                      immediate=immediate)
        # TODO pump messages; if __delivery_confirmation mode, also wait for
        # confirmation that will come with callback registred via
        # confirm_delivery: if ack and no return yet (add_on_return_callback),
        #  then return success; if nack or return, then return failure

    def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.info('Channel.close(%s, %s)', reply_code, reply_text)

        # TODO define actual on_close_ok
        self.add_callback(on_close_ok,
                          [pika.spec.Channel.CloseOk],
                          one_shot=True)

        super(SynchronousChannel, self).close(reply_code=reply_code,
                                              reply_text=reply_text)
        # TODO pump messages and wait for on_close_ok

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
        # TODO define actual qos_ok_callback
        super(SynchronousChannel, self).basic_qos(qos_ok_callback,
                                                  prefetch_size=prefetch_size,
                                                  prefetch_count=prefetch_count,
                                                  all_channels=all_channels)
        # TODO pump messages and wait for qos_ok_callback

    def basic_recover(self, requeue=False):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.

        """
        # TODO define actual recover_ok_callback
        super(SynchronousChannel, self).basic_recover(recover_ok_callback,
                                                      requeue=requeue)
        # TODO pump messages and wait for recover_ok_callback

    def confirm_delivery(self, nowait=False):
        """Turn on RabbitMQ-proprietary Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms

        :param bool nowait: Do not send a reply frame (Confirm.SelectOk)

        """
        # TODO define actual on_confirm_select_ok
        if not nowait:
            self.add_callback(on_confirm_select_ok,
                              [pika.spec.Confirm.SelectOk],
                              one_shot=True)

        # TODO define actual on_msg_delivery_confirmation
        super(SynchronousChannel, self).confirm_delivery(
            on_msg_delivery_confirmation,
            nowait=nowait)

        # TODO pump messages; also, if nowait=False, wait for
        # on_confirm_select_ok

        self.__delivery_confirmation = True

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
        # TODO define actual bind_ok_callback
        bind_ok_callback = None if nowait else bind_ok_callback
        super(SynchronousChannel, self).queue_bind(callback=bind_ok_callback,
                                                   queue=queue,
                                                   exchange=exchange,
                                                   routing_key=routing_key,
                                                   nowait=nowait,
                                                   arguments=arguments)
        # TODO pump messages; also wait for bind-ok if nowait is false

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
        # TODO define actual declare_ok_callback
        declare_ok_callback = None if nowait else declare_ok_callback
        super(SynchronousChannel, self).queue_declare(declare_ok_callback,
                                                      queue=queue,
                                                      passive=passive,
                                                      durable=durable,
                                                      exclusive=exclusive,
                                                      auto_delete=auto_delete,
                                                      nowait=nowait,
                                                      arguments=arguments)

        # TODO pump messages; also wait for declare-ok if nowait is false
