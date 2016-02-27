# -*- encoding: utf-8 -*-
"""
Tests for pika.channel.ContentFrameDispatcher

"""

# Disable pylint warnings concerning access to protected member
# pylint: disable=W0212

# Disable pylint messages concerning invalid method name
# pylint: disable=C0103

# Disable pylint messages concerning missing docstrings
# pylint: disable=C0111


import marshal

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from pika import channel
from pika import exceptions
from pika import frame
from pika import spec


class ContentFrameDispatcherTests(unittest.TestCase):

    def setUp(self):
        self.obj = channel.ContentFrameDispatcher()

    def test_init_method_frame(self):
        self.assertEqual(self.obj._method_frame, None)

    def test_init_header_frame(self):
        self.assertEqual(self.obj._header_frame, None)

    def test_init_seen_so_far(self):
        self.assertEqual(self.obj._seen_so_far, 0)

    def test_init_body_fragments(self):
        self.assertEqual(self.obj._body_fragments, list())

    def test_process_with_basic_deliver_method_only(self):
        value = frame.Method(1, spec.Basic.Deliver(), 24)
        self.assertIsNone(self.obj.process(value))
        self.assertIs(self.obj._method_frame, value)

    def test_process_with_content_header_partial(self):
        self.obj.process(frame.Method(1, spec.Basic.Deliver(), 24))
        value = frame.Header(1, 100, spec.BasicProperties, 156)
        self.assertIsNone(self.obj.process(value))
        self.assertIs(self.obj._header_frame, value)
        self.assertEqual(self.obj._header_frame._total_raw_in_size, 180)

    def test_process_with_zero_fragment_full_message(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)

        header_frame = frame.Header(1, 0, spec.BasicProperties, 156)
        response = self.obj.process(header_frame)
        self.assertEqual(response, (method_frame, header_frame, b''))
        self.assertEqual(header_frame._total_raw_in_size, 180)
        self._check_reset_members()

    def test_process_with_one_fragment_partial(self):
        value = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(value)

        value = frame.Header(1, 100, spec.BasicProperties, 156)
        self.obj.process(value)

        value = frame.Body(1, b'abc123', 10)
        self.assertIsNone(self.obj.process(value))
        self.assertEqual(self.obj._body_fragments, [value.fragment])
        self.assertEqual(self.obj._seen_so_far, 6)

        self.assertEqual(self.obj._header_frame._total_raw_in_size, 190)

    def test_process_with_one_fragment_full_message(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)

        header_frame = frame.Header(1, 6, spec.BasicProperties, 156)
        self.obj.process(header_frame)

        body_frame = frame.Body(1, b'abc123', 10)
        response = self.obj.process(body_frame)
        self.assertEqual(response, (method_frame, header_frame, b'abc123'))
        self.assertEqual(header_frame._total_raw_in_size, 190)
        self._check_reset_members()

    def test_process_with_two_fragments_full_message(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)

        header_frame = frame.Header(1, 11, spec.BasicProperties, 156)
        self.obj.process(header_frame)

        body1 = frame.Body(1, b'abc123', 10)
        self.assertIsNone(self.obj.process(body1))
        self.assertEqual(self.obj._body_fragments, [body1.fragment])
        self.assertEqual(self.obj._seen_so_far, 6)

        body2 = frame.Body(1, b'01234', 9)
        response = self.obj.process(body2)
        self.assertEqual(response, (method_frame, header_frame, b'abc12301234'))
        self.assertEqual(header_frame._total_raw_in_size, 199)
        self._check_reset_members()

    def test_process_with_body_frame_too_big(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties, 156)
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abcd1234', 12)
        self.assertRaises(exceptions.BodyTooLongError, self.obj.process,
                          body_frame)

    def test_process_with_unexpected_frame_type(self):
        value = frame.Method(1, spec.Basic.Qos())
        self.assertRaises(exceptions.UnexpectedFrameError, self.obj.process,
                          value)

    def _check_reset_members(self):
        """Check member values in reset state"""
        self.assertEqual(self.obj._method_frame, None)
        self.assertEqual(self.obj._header_frame, None)
        self.assertEqual(self.obj._seen_so_far, 0)
        self.assertEqual(self.obj._body_fragments, list())

    def test_reset(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties, 156)
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123', 10)
        self.obj.process(body_frame)
        self.obj._reset()
        self._check_reset_members()

    def test_ascii_bytes_body_instance(self):
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 11, spec.BasicProperties, 156)
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'foo-bar-baz', 15)
        method_frame, header_frame, body_value = self.obj.process(body_frame)
        self.assertIsInstance(body_value, bytes)

    def test_ascii_body_value(self):
        expectation = b'foo-bar-baz'
        self.obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 11, spec.BasicProperties, 156)
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'foo-bar-baz', 15)
        method_frame, header_frame, body_value = self.obj.process(body_frame)
        self.assertEqual(body_value, expectation)
        self.assertIsInstance(body_value, bytes)

    def test_binary_non_unicode_value(self):
        expectation = ('a', 0.8)
        self.obj = channel.ContentFrameDispatcher()
        method_frame = frame.Method(1, spec.Basic.Deliver(), 24)
        self.obj.process(method_frame)
        marshalled_body = marshal.dumps(expectation)
        header_frame = frame.Header(1, len(marshalled_body),
                                    spec.BasicProperties, 156)
        self.obj.process(header_frame)
        body_frame = frame.Body(1, marshalled_body, len(marshalled_body) + 4)
        method_frame, header_frame, body_value = self.obj.process(body_frame)
        self.assertEqual(marshal.loads(body_value), expectation)
