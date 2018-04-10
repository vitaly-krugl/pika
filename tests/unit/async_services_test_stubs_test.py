"""
Test for async_services_test_stups.py

"""

try:
    import asyncio
except ImportError:
    asyncio = None

import threading
import unittest

import tornado.ioloop
import twisted.internet.reactor

from pika.adapters import select_connection

from ..async_services_test_stubs import AsyncServicesTestStubs


# Suppress invalid-name, since our test names are descriptive and quite long
# pylint: disable=C0103

# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111


# Tornado does some magic that substitutes the class dynamically
_TORNADO_IO_LOOP = tornado.ioloop.IOLoop()
_TORNADO_IOLOOP_CLASS = _TORNADO_IO_LOOP.__class__
_TORNADO_IO_LOOP.close()
del _TORNADO_IO_LOOP

_SUPPORTED_LOOP_CLASSES = set([
    select_connection.IOLoop,
    _TORNADO_IOLOOP_CLASS,
    # NOTE: twisted automagically makes its reactor module look like an IO Loop
    # instance
    twisted.internet.reactor.__class__,  # pylint: disable=E1101
])

if asyncio is not None:
    _SUPPORTED_LOOP_CLASSES.add(asyncio.get_event_loop().__class__)


class TestStartCalledFromOtherThreadAndWithVaryingNativeLoops(
        unittest.TestCase,
        AsyncServicesTestStubs):

    _native_loop_classes = None

    @classmethod
    def setUpClass(cls):
        cls._native_loop_classes = set()

    @classmethod
    def tearDownClass(cls):
        # Now check against what was made available to us by
        # AsyncServicesTestStubs
        if cls._native_loop_classes != _SUPPORTED_LOOP_CLASSES:
            raise AssertionError(
                'Expected these {} native I/O loop classes from '
                'AsyncServicesTestStubs: {!r}, but got these {}: {!r}'.format(
                    len(_SUPPORTED_LOOP_CLASSES),
                    _SUPPORTED_LOOP_CLASSES,
                    len(cls._native_loop_classes),
                    cls._native_loop_classes))

    def setUp(self):
        self._runner_thread_id = threading.current_thread().ident

    def start(self):
        async_svcs = self.create_async()
        native_loop = async_svcs.get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)

        self._native_loop_classes.add(native_loop.__class__)

        # Check that we're called from a different thread than the one that
        # set up this test.
        self.assertNotEqual(threading.current_thread().ident,
                            self._runner_thread_id)

        # And make sure the loop actually works using this rudimentary test
        async_svcs.add_callback_threadsafe(async_svcs.stop)
        async_svcs.run()
