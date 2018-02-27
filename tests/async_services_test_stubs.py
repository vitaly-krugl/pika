"""
Test stubs for running tests against all supported adaptations of
async_interface.AbstractAsyncServices and variations such as without SSL and
with SSL.

Usage example:

```
import unittest

from ..async_services_test_stubs import AsyncServicesTestStubs


class TestGetNativeIOLoop(unittest.TestCase,
                          AsyncServicesTestStubs):

    def start(self):
        native_loop = self.create_async().get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)
```

"""

import sys
import unittest

from .threaded_test_wrapper import run_in_thread_with_timeout

# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103


class AsyncServicesTestStubs(object):
    """Provides a stub test method for each combination of parameters we wish to
    test

    """
    # Overridden by framework-specific test methods
    _async_factory = None
    _native_loop = None
    _use_ssl = None

    def start(self):
        """ Subclasses must override to run the test. This method is called
        from a thread.

        """
        raise NotImplementedError

    def create_async(self):
        """Create the configured AbstractAsyncServices adaptation and schedule
        it to be closed automatically when the test terminates.

        :param unittest.TestCase self:
        :rtype: pika.adapters.async_interface.AbstractAsyncServices

        """
        async_svc = self._async_factory()
        self.addCleanup(async_svc.close)  # pylint: disable=E1101
        return async_svc

    def _run_start(self, async_factory, native_loop, use_ssl=False):
        """Called by framework-specific test stubs to initialize test paramters
        and execute the `self.start()` method.

        :param async_interface.AbstractAsyncServices _() async_factory: function
            to call to create an instance of `AbstractAsyncServices` adaptation.
        :param native_loop: native loop implementation instance
        :param bool use_ssl: Whether to test with SSL instead of Plaintext
            transport. Defaults to Plaintext.
        """
        self._async_factory = async_factory
        self._native_loop = native_loop
        self._use_ssl = use_ssl

        self.start()

    # Suppress missing-docstring to allow test method names to be printed by our
    # test runner
    # pylint: disable=C0111

    @run_in_thread_with_timeout
    def test_with_select_connection_async_services(self):
        # Test entry point for `select_connection.IOLoop`-based async services
        # implementation.

        from pika.adapters.select_connection import IOLoop
        from pika.adapters.selector_ioloop_adapter import (
            SelectorAsyncServicesAdapter)
        native_loop = IOLoop()
        self._run_start(
            async_factory=lambda: SelectorAsyncServicesAdapter(native_loop),
            native_loop=native_loop)

    @run_in_thread_with_timeout
    def test_with_tornado_async_services(self):
        # Test entry point for `tornado.ioloop.IOLoop`-based async services
        # implementation.

        from tornado.ioloop import IOLoop
        from pika.adapters.selector_ioloop_adapter import (
            SelectorAsyncServicesAdapter)

        native_loop = IOLoop()
        self._run_start(
            async_factory=lambda: SelectorAsyncServicesAdapter(native_loop),
            native_loop=native_loop)

    @run_in_thread_with_timeout
    def test_with_twisted_async_services(self):
        # Test entry point for `twisted.reactor`-based async services
        # implementation.

        from twisted.internet import reactor
        from pika.adapters.twisted_connection import (
            _TwistedAsyncServicesAdapter)

        # NOTE: reactor magically sets reactor.__class__ to the class of the
        # I/O loop that it automagically selected for this platform
        native_loop = reactor.__class__()  # pylint: disable=E1101
        self._run_start(
            async_factory=lambda: _TwistedAsyncServicesAdapter(native_loop),
            native_loop=native_loop)

    @unittest.skipIf(sys.version_info < (3, 4),
                     "Asyncio is available only with Python 3.4+")
    @run_in_thread_with_timeout
    def test_with_asyncio_async_services(self):
        # Test entry point for `asyncio` event loop-based async services
        # implementation.

        import asyncio
        from pika.adapters.asyncio_connection import (
            _AsyncioAsyncServicesAdapter)

        native_loop = asyncio.new_event_loop()
        self._run_start(
            async_factory=lambda: _AsyncioAsyncServicesAdapter(native_loop),
            native_loop=native_loop)
