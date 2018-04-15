# -*- coding: utf8 -*-
"""
Tests for pika.adapters.blocking_connection.BlockingConnection

"""
import unittest

import mock
from mock import patch

import pika
from pika.adapters import blocking_connection, async_interface
import pika.channel
import pika.exceptions


# Disable protected-access
# pylint: disable=W0212

# Disable missing-docstring
# pylint: disable=C0111

# Disable invalid-name
# pylint: disable=C0103

# Disable no-self-use
# pylint: disable=R0201


class BlockingConnectionMockTemplate(blocking_connection.BlockingConnection):
    pass


class SelectConnectionTemplate(blocking_connection.SelectConnection):
    is_closed = None
    is_closing = None
    is_open = None
    _channels = None
    ioloop = None
    _transport = None
    _adapter_get_write_buffer_size = None


class BlockingConnectionTests(unittest.TestCase):
    """TODO: test properties"""

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_constructor(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        select_connection_class_mock.assert_called_once_with(
            parameters='params',
            on_open_callback=mock.ANY,
            on_open_error_callback=mock.ANY,
            on_close_callback=mock.ANY)

        self.assertEqual(connection._impl.ioloop.activate_poller.call_count, 1)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup(self,
                                             select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._process_io_for_connection_setup()

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_process_io_for_connection_setup_fails_with_open_error(
            self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        exc_value = pika.exceptions.AMQPConnectionError('failed')
        connection._open_error_result.set_value_once(
            select_connection_class_mock.return_value, exc_value)

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
                connection._process_io_for_connection_setup()

            self.assertEqual(cm.exception, exc_value)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate,
        is_closed=False)
    def test_flush_output(self, select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        get_buffer_size_mock = mock.Mock(
            name='_adapter_get_write_buffer_size',
            side_effect=[100, 50, 0],
            spec=async_interface.AbstractStreamTransport.get_write_buffer_size)

        transport_mock = mock.NonCallableMock(
            spec_set=async_interface.AbstractStreamTransport)

        connection._impl._transport = transport_mock
        connection._impl._adapter_get_write_buffer_size = get_buffer_size_mock

        connection._opened_result.set_value_once(
            select_connection_class_mock.return_value)

        connection._flush_output(lambda: False, lambda: True)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate,
        is_closed=False)
    def test_flush_output_user_initiated_close(self,
                                               select_connection_class_mock):
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = True
        original_exc = pika.exceptions.ConnectionClosedByClient(200, 'success')
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value, original_exc)

        connection._flush_output(lambda: False, lambda: True)

        self.assertEqual(connection._impl.ioloop.activate_poller.call_count, 1)
        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate,
        is_closed=False)
    def test_flush_output_server_initiated_error_close(
            self, select_connection_class_mock):

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        original_exc = pika.exceptions.ConnectionClosedByBroker(404,
                                                                'not found')
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value, original_exc)

        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (404, 'not found'))

        self.assertEqual(connection._impl.ioloop.activate_poller.call_count, 1)
        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate,
        is_closed=False)
    def test_flush_output_server_initiated_no_error_close(
            self,
            select_connection_class_mock):

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        connection._user_initiated_close = False
        original_exc = pika.exceptions.ConnectionClosedByBroker(200, 'ok')
        connection._closed_result.set_value_once(
            select_connection_class_mock.return_value,
            original_exc)

        with self.assertRaises(pika.exceptions.ConnectionClosed) as cm:
            connection._flush_output(lambda: False, lambda: True)

        self.assertSequenceEqual(cm.exception.args, (200, 'ok'))

        self.assertEqual(connection._impl.ioloop.activate_poller.call_count, 1)
        self.assertEqual(connection._impl.ioloop.close.call_count, 1)

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_close(self, select_connection_class_mock):
        select_connection_class_mock.return_value.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        impl_channel_mock = mock.Mock()
        connection._impl._channels = {1: impl_channel_mock}

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

        impl_channel_mock._get_cookie.return_value.close.assert_called_once_with(
            200, 'text')
        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_close_with_channel_closed_exception(self,
                                                 select_connection_class_mock):
        select_connection_class_mock.return_value.is_closed = False

        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        channel1_mock = mock.Mock(
            is_open=True,
            close=mock.Mock(
                side_effect=pika.exceptions.ChannelClosed(-1, 'Just because'),
                spec_set=pika.channel.Channel.close),
            spec_set=blocking_connection.BlockingChannel)

        channel2_mock = mock.Mock(
            is_open=True, spec_set=blocking_connection.BlockingChannel)

        connection._impl._channels = {
            1:
            mock.Mock(
                _get_cookie=mock.Mock(
                    return_value=channel1_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                spec_set=pika.channel.Channel),
            2:
            mock.Mock(
                _get_cookie=mock.Mock(
                    return_value=channel2_mock,
                    spec_set=pika.channel.Channel._get_cookie),
                spec_set=pika.channel.Channel)
        }

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection._closed_result.signal_once()
            connection.close(200, 'text')

            channel1_mock.close.assert_called_once_with(200, 'text')
            channel2_mock.close.assert_called_once_with(200, 'text')
        select_connection_class_mock.return_value.close.assert_called_once_with(
            200, 'text')

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    @patch.object(
        blocking_connection,
        'BlockingChannel',
        spec_set=blocking_connection.BlockingChannel)
    def test_channel(
            self,
            blocking_channel_class_mock,  # pylint: disable=W0613
            select_connection_class_mock):  # pylint: disable=W0613
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection.channel()

    @patch.object(
        blocking_connection,
        'SelectConnection',
        spec_set=SelectConnectionTemplate)
    def test_sleep(self, select_connection_class_mock):  # pylint: disable=W0613
        with mock.patch.object(blocking_connection.BlockingConnection,
                               '_process_io_for_connection_setup'):
            connection = blocking_connection.BlockingConnection('params')

        with mock.patch.object(
                blocking_connection.BlockingConnection,
                '_flush_output',
                spec_set=blocking_connection.BlockingConnection._flush_output):
            connection.sleep(0.00001)

    def test_connection_blocked_evt(self):
        blocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Blocked('reason'))
        evt = blocking_connection._ConnectionBlockedEvt(
            blocked_buffer.append,
            frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(blocked_buffer), 1)
        self.assertIs(blocked_buffer[0], frame)

    def test_connection_unblocked_evt(self):
        unblocked_buffer = []
        frame = pika.frame.Method(0, pika.spec.Connection.Unblocked())
        evt = blocking_connection._ConnectionUnblockedEvt(
            unblocked_buffer.append,
            frame)
        repr(evt)
        evt.dispatch()
        self.assertEqual(len(unblocked_buffer), 1)
        self.assertIs(unblocked_buffer[0], frame)
