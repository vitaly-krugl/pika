"""Pika specific exceptions"""


class AMQPError(Exception):

    def __repr__(self):
        return 'An unspecified AMQP error has occurred'


class AMQPConnectionError(AMQPError):

    def __repr__(self):
        if len(self.args) == 2:
            return '{}: ({}) {}'.format(self.__class__.__name__,
                                        self.args[0],
                                        self.args[1])
        else:
            return '{}: {}'.format(self.__class__.__name__, self.args)


class ConnectionOpenAborted(AMQPConnectionError):
    """Client closed connection while opening."""
    pass


class StreamLostError(AMQPConnectionError):
    """Stream (TCP) connection lost."""
    pass


class IncompatibleProtocolError(AMQPConnectionError):

    def __repr__(self):
        return ('The protocol returned by the server is not supported: %s' %
                (self.args,))


class AuthenticationError(AMQPConnectionError):

    def __repr__(self):
        return ('Server and client could not negotiate use of the %s '
                'authentication mechanism' % self.args[0])


class ProbableAuthenticationError(AMQPConnectionError):

    def __repr__(self):
        return ('Client was disconnected at a connection stage indicating a '
                'probable authentication error: %s' % (self.args,))


class ProbableAccessDeniedError(AMQPConnectionError):

    def __repr__(self):
        return ('Client was disconnected at a connection stage indicating a '
                'probable denial of access to the specified virtual host: %s' %
                (self.args,))


class NoFreeChannels(AMQPConnectionError):

    def __repr__(self):
        return 'The connection has run out of free channels'


class ConnectionWrongStateError(AMQPConnectionError):
    def __repr__(self):
        if self.args:
            return super(ConnectionWrongStateError, self).__repr__()
        else:
            return (
                'The connection is in wrong state for the requested operation.')


class ConnectionClosed(AMQPConnectionError):

    # TODO: grep for proper usage (args passed)

    def __init__(self, reply_code, reply_text):
        """

        :param int reply_code: reply-code that was used in user's or broker's
            `Connection.Close` method. NEW in v1.0.0
        :param str reply_text: reply-text that was used in user's or broker's
            `Connection.Close` method. Human-readable string corresponding to
            `reply_code`. NEW in v1.0.0
        """
        super(ConnectionClosed, self).__init__(int(reply_code), str(reply_text))

    def __repr__(self):
        return '{}: ({}) {!r}'.format(self.__class__.__name__,
                                      self.reply_code,
                                      self.reply_text)

    @property
    def reply_code(self):
        """ NEW in v1.0.0
        :rtype: int

        """
        return self.args[0]

    @property
    def reply_text(self):
        """ NEW in v1.0.0
        :rtype: str

        """
        return self.args[1]

class ConnectionClosedByBroker(ConnectionClosed):
    """Connection.Close from broker."""
    pass


class ConnectionClosedByClient(ConnectionClosed):
    """Connection was closed at request of Pika client."""
    pass


class ConnectionBlockedTimeout(AMQPConnectionError):
    """RabbitMQ-specific: timed out waiting for connection.unblocked."""
    pass


class AMQPHeartbeatTimeout(AMQPConnectionError):
    """Connection was dropped as result of heartbeat timeout."""
    pass


class AMQPChannelError(AMQPError):

    def __repr__(self):
        return 'An unspecified AMQP channel error has occurred: {!r}'.format(
            self.args)


class ChannelClosed(AMQPChannelError):
    """The channel is in a state other than OPEN (e.g., OPENING, CLOSING, or
    CLOSED). You may introspect the channel's state properties (`is_closed`,
    `is_closing`).

    """
    def __init__(self, reply_code, reply_text):
        """

        :param int reply_code: reply-code that was used in user's or broker's
            `Channel.Close` method. One of the AMQP-defined Channel Errors or
            negative error values from `Channel.ClientChannelErrors`;
            NEW in v1.0.0
        :param str reply_text: reply-text that was used in user's or broker's
            `Channel.Close` method. Human-readable string corresponding to
            `reply_code`;
            NEW in v1.0.0
        """
        super(ChannelClosed, self).__init__(int(reply_code), str(reply_text))

    def __repr__(self):
        return '{}: ({}) {!r}'.format(self.__class__.__name__,
                                      self.reply_code,
                                      self.reply_text)

    @property
    def reply_code(self):
        """ NEW in v1.0.0
        :rtype: int

        """
        return self.args[0]

    @property
    def reply_text(self):
        """ NEW in v1.0.0
        :rtype: str

        """
        return self.args[1]


class ChannelAlreadyClosing(AMQPChannelError):
    """Raised when `Channel.close` is called while channel is already closing"""
    pass


class DuplicateConsumerTag(AMQPChannelError):

    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(AMQPChannelError):

    def __repr__(self):
        return 'Server cancelled consumer'


class UnroutableError(AMQPChannelError):
    """Exception containing one or more unroutable messages returned by broker
    via Basic.Return.

    Used by BlockingChannel.

    In publisher-acknowledgements mode, this is raised upon receipt of Basic.Ack
    from broker; in the event of Basic.Nack from broker, `NackError` is raised
    instead
    """

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of `blocking_connection.ReturnedMessage`
           objects
        """
        super(UnroutableError, self).__init__(
            "%s unroutable message(s) returned" % (len(messages)))

        self.messages = messages

    def __repr__(self):
        return '%s: %i unroutable messages returned by broker' % (
            self.__class__.__name__, len(self.messages))


class NackError(AMQPChannelError):
    """This exception is raised when a message published in
    publisher-acknowledgements mode is Nack'ed by the broker.

    Used by BlockingChannel.
    """

    def __init__(self, messages):
        """
        :param messages: sequence of returned unroutable messages
        :type messages: sequence of `blocking_connection.ReturnedMessage`
            objects
        """
        super(NackError, self).__init__(
            "%s message(s) NACKed" % (len(messages)))

        self.messages = messages

    def __repr__(self):
        return '%s: %i unroutable messages returned by broker' % (
            self.__class__.__name__, len(self.messages))


class InvalidChannelNumber(AMQPError):

    def __repr__(self):
        return 'An invalid channel number has been specified: %s' % self.args[0]


class ProtocolSyntaxError(AMQPError):

    def __repr__(self):
        return 'An unspecified protocol syntax error occurred'


class UnexpectedFrameError(ProtocolSyntaxError):

    def __repr__(self):
        return 'Received a frame out of sequence: %r' % self.args[0]


class ProtocolVersionMismatch(ProtocolSyntaxError):

    def __repr__(self):
        return 'Protocol versions did not match: %r vs %r' % (self.args[0],
                                                              self.args[1])


class BodyTooLongError(ProtocolSyntaxError):

    def __repr__(self):
        return ('Received too many bytes for a message delivery: '
                'Received %i, expected %i' % (self.args[0], self.args[1]))


class InvalidFrameError(ProtocolSyntaxError):

    def __repr__(self):
        return 'Invalid frame received: %r' % self.args[0]


class InvalidFieldTypeException(ProtocolSyntaxError):

    def __repr__(self):
        return 'Unsupported field kind %s' % self.args[0]


class UnsupportedAMQPFieldException(ProtocolSyntaxError):

    def __repr__(self):
        return 'Unsupported field kind %s' % type(self.args[1])


class UnspportedAMQPFieldException(UnsupportedAMQPFieldException):
    """Deprecated version of UnsupportedAMQPFieldException"""


class MethodNotImplemented(AMQPError):
    pass


class ChannelError(Exception):

    def __repr__(self):
        return 'An unspecified error occurred with the Channel'


class InvalidMinimumFrameSize(ProtocolSyntaxError):
    """ DEPRECATED; pika.connection.Parameters.frame_max property setter now
    raises the standard `ValueError` exception when the value is out of bounds.
    """

    def __repr__(self):
        return 'AMQP Minimum Frame Size is 4096 Bytes'


class InvalidMaximumFrameSize(ProtocolSyntaxError):
    """ DEPRECATED; pika.connection.Parameters.frame_max property setter now
    raises the standard `ValueError` exception when the value is out of bounds.
    """

    def __repr__(self):
        return 'AMQP Maximum Frame Size is 131072 Bytes'


class ReentrancyError(Exception):
    """The requested operation would result in unsupported recursion or
    reentrancy.

    Used by BlockingConnection/BlockingChannel

    """


class ShortStringTooLong(AMQPError):

    def __repr__(self):
        return ('AMQP Short String can contain up to 255 bytes: '
                '%.300s' % self.args[0])


class DuplicateGetOkCallback(ChannelError):

    def __repr__(self):
        return ('basic_get can only be called again after the callback for the'
                'previous basic_get is executed')
