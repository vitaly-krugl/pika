"""
Tests of async_interface.AbstractAsyncServices adaptations

"""

import collections
import errno
import logging
import platform
import socket
import time
import unittest

import pika.compat

from ..async_services_test_stubs import AsyncServicesTestStubs


# Suppress missing-docstring to allow test method names to be printed by our the
# test runner
# pylint: disable=C0111

# invalid-name
# pylint: disable=C0103

# protected-access
# pylint: disable=W0212


ON_WINDOWS = platform.system() == 'Windows'


class AsyncServicesTestBase(unittest.TestCase):

    @property
    def logger(self):
        """Return the logger for tests to use

        """
        return logging.getLogger(self.__class__.__module__ + '.' +
                                 self.__class__.__name__)

    def create_nonblocking_tcp_socket(self):
        """Create a TCP stream socket and schedule cleanup to close it

        """
        sock = socket.socket()
        sock.setblocking(False)
        self.addCleanup(sock.close)
        return sock

    def create_nonblocking_socketpair(self):
        """Creates a non-blocking socket pair and schedules cleanup to close
        them

        :returns: two-tuple of connected non-blocking sockets

        """
        pair = pika.compat._nonblocking_socketpair()  # pylint: disable=W0212
        self.addCleanup(pair[0].close)
        self.addCleanup(pair[1].close)
        return pair

    def create_blocking_socketpair(self):
        """Creates a blocking socket pair and schedules cleanup to close
        them

        :returns: two-tuple of connected non-blocking sockets

        """
        pair = self.create_nonblocking_socketpair()
        pair[0].setblocking(True)  # pylint: disable=E1101
        pair[1].setblocking(True)
        return pair

    @staticmethod
    def safe_connect_nonblocking_socket(sock, addr_pair):
        """Initiate socket connection, suppressing EINPROGRESS/EWOULDBLOCK
        :param socket.socket sock
        :param addr_pair: two tuple of address string and port integer
        """
        try:
            sock.connect(addr_pair)
        except pika.compat.SOCKET_ERROR as error:
            # EINPROGRESS for posix and EWOULDBLOCK for windows
            if error.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK,):
                raise

    def get_dead_socket_address(self):
        """

        :return: socket address pair (ip-addr, port) that will refuse connection

        """
        s1, s2 = pika.compat._nonblocking_socketpair()  # pylint: disable=W0212
        s2.close()
        self.addCleanup(s1.close)
        return s1.getsockname()  # pylint: disable=E1101


class TestGetNativeIOLoop(AsyncServicesTestBase,
                          AsyncServicesTestStubs):

    def start(self):
        native_loop = self.create_async().get_native_ioloop()
        self.assertIsNotNone(self._native_loop)
        self.assertIs(native_loop, self._native_loop)


class TestRunWithStopFromThreadsafeCallback(AsyncServicesTestBase,
                                            AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()

        bucket = []

        def callback():
            loop.stop()
            bucket.append('I was called')

        loop.add_callback_threadsafe(callback)
        loop.run()

        self.assertEqual(bucket, ['I was called'])


class TestCallLaterDoesNotCallAheadOfTime(AsyncServicesTestBase,
                                          AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        def callback():
            loop.stop()
            bucket.append('I was here')

        start_time = time.time()
        loop.call_later(0.1, callback)
        loop.run()
        self.assertGreaterEqual(round(time.time() - start_time, 3), 0.1)
        self.assertEqual(bucket, ['I was here'])


class TestCallLaterCancelReturnsNone(AsyncServicesTestBase,
                                     AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        self.assertIsNone(loop.call_later(0, lambda: None).cancel())


class TestCallLaterCancelTwiceFromOwnCallback(AsyncServicesTestBase,
                                              AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        def callback():
            timer.cancel()
            timer.cancel()
            loop.stop()
            bucket.append('I was here')

        timer = loop.call_later(0.1, callback)
        loop.run()
        self.assertEqual(bucket, ['I was here'])


class TestCallLaterCallInOrder(AsyncServicesTestBase,
                               AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        loop.call_later(0.3, lambda: bucket.append(3) or loop.stop())
        loop.call_later(0, lambda: bucket.append(1))
        loop.call_later(0.15, lambda: bucket.append(2))
        loop.run()
        self.assertEqual(bucket, [1, 2, 3])


class TestCallLaterCancelledDoesNotCallBack(AsyncServicesTestBase,
                                            AsyncServicesTestStubs):

    def start(self):
        loop = self.create_async()
        bucket = []

        timer1 = loop.call_later(0, lambda: bucket.append(1))
        timer1.cancel()
        loop.call_later(0.15, lambda: bucket.append(2) or loop.stop())
        loop.run()
        self.assertEqual(bucket, [2])


class SocketWatcherTestBase(AsyncServicesTestBase):

    WatcherActivity = collections.namedtuple(
        "async_services_test_WatcherActivity",
        ['readable', 'writable'])


    def _check_socket_watchers_fired(self, sock, expected):  # pylint: disable=R0914
        """Registers reader and writer for the given socket, runs the event loop
        until either one fires and asserts against expectation.

        :param AsyncServicesTestBase | AsyncServicesTestStubs self:
        :param socket.socket sock:
        :param WatcherActivity expected: What's expected by caller
        """
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()  # pylint: disable=E1101

        stops_requested = []
        def stop_loop():
            if not stops_requested:
                svcs.stop()
            stops_requested.append(1)

        reader_bucket = [False]
        def on_readable():
            self.logger.debug('on_readable() called.')
            reader_bucket.append(True)
            stop_loop()

        writer_bucket = [False]
        def on_writable():
            self.logger.debug('on_writable() called.')
            writer_bucket.append(True)
            stop_loop()

        timeout_bucket = []
        def on_timeout():
            timeout_bucket.append(True)
            stop_loop()

        timeout_timer = svcs.call_later(5, on_timeout)
        svcs.set_reader(sock.fileno(), on_readable)
        svcs.set_writer(sock.fileno(), on_writable)

        try:
            svcs.run()
        finally:
            timeout_timer.cancel()
            svcs.remove_reader(sock.fileno())
            svcs.remove_writer(sock.fileno())

        if timeout_bucket:
            raise AssertionError('which_socket_watchers_fired() timed out.')

        readable = reader_bucket[-1]
        writable = writer_bucket[-1]

        if readable != expected.readable:
            raise AssertionError(
                'Expected readable={!r}, but got {!r} (writable={!r})'.format(
                    expected.readable,
                    readable,
                    writable))

        if writable != expected.writable:
            raise AssertionError(
                'Expected writable={!r}, but got {!r} (readable={!r})'.format(
                    expected.writable,
                    writable,
                    readable))


class TestSocketWatchersUponConnectionAndNoIncomingData(SocketWatcherTestBase,
                                                        AsyncServicesTestStubs):

    def start(self):
        s1, _s2 = self.create_blocking_socketpair()

        expected = self.WatcherActivity(readable=False, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersUponConnectionAndIncomingData(
        SocketWatcherTestBase,
        AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.send(b'abc')

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersWhenFailsToConnect(SocketWatcherTestBase,
                                           AsyncServicesTestStubs):
    def start(self):
        sock = self.create_nonblocking_tcp_socket()

        self.safe_connect_nonblocking_socket(sock,
                                             self.get_dead_socket_address())

        # NOTE: Unlike POSIX, Windows select doesn't indicate as
        # readable/writable a socket that failed to connect - it reflects the
        # failure only via exceptfds, which native ioloop's usually attribute to
        # the writable indication.
        expected = self.WatcherActivity(readable=False if ON_WINDOWS else True,
                                        writable=True)
        self._check_socket_watchers_fired(sock, expected)


class TestSocketWatchersAfterRemotePeerCloses(SocketWatcherTestBase,
                                              AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.close()

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterRemotePeerClosesWithIncomingData(
        SocketWatcherTestBase,
        AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.send(b'abc')
        s2.close()

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterRemotePeerShutsRead(SocketWatcherTestBase,
                                                 AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.shutdown(socket.SHUT_RD)

        expected = self.WatcherActivity(readable=False, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterRemotePeerShutsWrite(SocketWatcherTestBase,
                                                  AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.shutdown(socket.SHUT_WR)

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterRemotePeerShutsWriteWithIncomingData(
        SocketWatcherTestBase,
        AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.send(b'abc')
        s2.shutdown(socket.SHUT_WR)

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterRemotePeerShutsReadWrite(
        SocketWatcherTestBase,
        AsyncServicesTestStubs):

    def start(self):
        s1, s2 = self.create_blocking_socketpair()
        s2.shutdown(socket.SHUT_RDWR)

        expected = self.WatcherActivity(readable=True, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterLocalPeerShutsRead(SocketWatcherTestBase,
                                                AsyncServicesTestStubs):

    def start(self):
        s1, _s2 = self.create_blocking_socketpair()
        s1.shutdown(socket.SHUT_RD)  # pylint: disable=E1101

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RD.
        expected = self.WatcherActivity(readable=False if ON_WINDOWS else True,
                                        writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterLocalPeerShutsWrite(SocketWatcherTestBase,
                                                 AsyncServicesTestStubs):

    def start(self):
        s1, _s2 = self.create_blocking_socketpair()
        s1.shutdown(socket.SHUT_WR)  # pylint: disable=E1101

        expected = self.WatcherActivity(readable=False, writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestSocketWatchersAfterLocalPeerShutsReadWrite(SocketWatcherTestBase,
                                                     AsyncServicesTestStubs):

    def start(self):
        s1, _s2 = self.create_blocking_socketpair()
        s1.shutdown(socket.SHUT_RDWR)  # pylint: disable=E1101

        # NOTE: Unlike POSIX, Windows select doesn't indicate as readable socket
        #  that was shut down locally with SHUT_RDWR.
        expected = self.WatcherActivity(readable=False if ON_WINDOWS else True,
                                        writable=True)
        self._check_socket_watchers_fired(s1, expected)


class TestGetaddrinfoWWWGoogleDotComPort80(AsyncServicesTestBase,
                                           AsyncServicesTestStubs):

    def start(self):
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()

        result_bucket = []
        def on_done(result):
            result_bucket.append(result)
            svcs.stop()

        ref = svcs.getaddrinfo('www.google.com', 80,
                               socktype=socket.SOCK_STREAM,
                               on_done=on_done)

        svcs.run()

        self.assertEqual(len(result_bucket), 1)

        result = result_bucket[0]
        self.logger.debug('TestGetaddrinfoWWWGoogleDotComPort80: result=%r',
                          result)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result[0]), 5)

        for family, socktype, proto, canonname, sockaddr in result:
            self.assertIn(family, [socket.AF_INET, socket.AF_INET6])
            self.assertEqual(socktype, socket.SOCK_STREAM)
            if pika.compat.ON_WINDOWS:
                self.assertEqual(proto, socket.IPPROTO_IP)
            else:
                self.assertEqual(proto, socket.IPPROTO_TCP)
            self.assertEqual(canonname, '')  # AI_CANONNAME not requested
            ipaddr, port = sockaddr[:2]
            self.assertIsInstance(ipaddr, str)
            self.assertGreater(len(ipaddr), 0)
            socket.inet_pton(family, ipaddr)
            self.assertEqual(port, 80)

        self.assertEqual(ref.cancel(), False)


class TestGetaddrinfoNonExistentHost(AsyncServicesTestBase,
                                     AsyncServicesTestStubs):

    def start(self):
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()

        result_bucket = []
        def on_done(result):
            result_bucket.append(result)
            svcs.stop()

        ref = svcs.getaddrinfo('www.google.comSSS', 80,
                               socktype=socket.SOCK_STREAM,
                               proto=socket.IPPROTO_TCP, on_done=on_done)

        svcs.run()

        self.assertEqual(len(result_bucket), 1)

        result = result_bucket[0]
        self.assertIsInstance(result, socket.gaierror)

        self.assertEqual(ref.cancel(), False)


class TestGetaddrinfoCancelBeforeLoopRun(AsyncServicesTestBase,
                                         AsyncServicesTestStubs):

    def start(self):
        # NOTE: this test elicits an occasional asyncio
        # `RuntimeError: Event loop is closed` message on the terminal,
        # presumably when the `getaddrinfo()` executing in the thread pool
        # finally completes and attempts to set the value on the future, but
        # our cleanup logic will have closed the loop before then.

        # Provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()

        on_done_bucket = []
        def on_done(result):
            on_done_bucket.append(result)

        ref = svcs.getaddrinfo('www.google.com', 80,
                               socktype=socket.SOCK_STREAM,
                               on_done=on_done)

        self.assertEqual(ref.cancel(), True)

        svcs.add_callback_threadsafe(svcs.stop)
        svcs.run()

        self.assertFalse(on_done_bucket)


class TestGetaddrinfoCancelAfterLoopRun(AsyncServicesTestBase,
                                        AsyncServicesTestStubs):

    def start(self):
        # NOTE: this test elicits an occasional asyncio
        # `RuntimeError: Event loop is closed` message on the terminal,
        # presumably when the `getaddrinfo()` executing in the thread pool
        # finally completes and attempts to set the value on the future, but
        # our cleanup logic will have closed the loop before then.

        # Provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()

        on_done_bucket = []
        def on_done(result):
            self.logger.error(
                'Unexpected completion of cancelled getaddrinfo()')
            on_done_bucket.append(result)

        # NOTE: there is some probability that getaddrinfo() will have completed
        # and added its completion reporting callback quickly, so we add our
        # cancellation callback before requesting getaddrinfo() in order to
        # avoid the race condition wehreby it invokes our completion callback
        # before we had a chance to cancel it.
        cancel_result_bucket = []
        def cancel_and_stop_from_loop():
            self.logger.debug('Cancelling getaddrinfo() from loop callback.')
            cancel_result_bucket.append(getaddr_ref.cancel())
            svcs.stop()

        svcs.add_callback_threadsafe(cancel_and_stop_from_loop)

        getaddr_ref = svcs.getaddrinfo('www.google.com', 80,
                                       socktype=socket.SOCK_STREAM,
                                       on_done=on_done)

        svcs.run()

        self.assertEqual(cancel_result_bucket, [True])

        self.assertFalse(on_done_bucket)


class SocketConnectorTestBase(AsyncServicesTestBase):

    def set_up_sockets_for_connect(self, family):
        """
        :param AsyncServicesTestStubs | SocketConnectorTestBase self:

        :return: two-tuple (lsock, csock), where lscok is the listening sock and
            csock is the socket that's can be connected to the listening socket.
        :rtype: tuple
        """

        # Create listener
        lsock = socket.socket(family, socket.SOCK_STREAM)
        self.addCleanup(lsock.close)
        ipaddr = (pika.compat._LOCALHOST_V6 if family == socket.AF_INET6
                  else pika.compat._LOCALHOST)
        lsock.bind((ipaddr, 0))
        lsock.listen(1)
        # NOTE: don't even need to accept for this test, connection completes
        # from backlog

        # Create connection initiator
        csock = socket.socket(family, socket.SOCK_STREAM)
        self.addCleanup(csock.close)
        csock.setblocking(False)

        return lsock, csock


    def check_successful_connect(self, family):
        """
        :param AsyncServicesTestStubs | SocketConnectorTestBase self:
        """
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()  # pylint: disable=E1101

        lsock, csock = self.set_up_sockets_for_connect(family)

        # Initiate connection
        on_done_result_bucket = []
        def on_done(result):
            on_done_result_bucket.append(result)
            svcs.stop()

        connect_ref = svcs.connect_socket(csock, lsock.getsockname(), on_done)

        svcs.run()

        self.assertEqual(on_done_result_bucket, [None])
        self.assertEqual(csock.getpeername(), lsock.getsockname())
        self.assertEqual(connect_ref.cancel(), False)

    def check_failed_connect(self, family):
        """
        :param AsyncServicesTestStubs | SocketConnectorTestBase self:
        """
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()  # pylint: disable=E1101

        lsock, csock = self.set_up_sockets_for_connect(family)

        laddr = lsock.getsockname()

        # Close the listener to force failure
        lsock.close()

        # Initiate connection
        on_done_result_bucket = []
        def on_done(result):
            on_done_result_bucket.append(result)
            svcs.stop()

        connect_ref = svcs.connect_socket(csock, laddr, on_done)

        svcs.run()

        self.assertEqual(len(on_done_result_bucket), 1)
        self.assertIsInstance(on_done_result_bucket[0], Exception)
        with self.assertRaises(Exception):
            csock.getpeername()  # raises when not connected
        self.assertEqual(connect_ref.cancel(), False)

    def check_cancel_connect(self, family):
        """
        :param AsyncServicesTestStubs | SocketConnectorTestBase self:
        """
        # provided by AsyncServicesTestStubs mixin
        svcs = self.create_async()  # pylint: disable=E1101

        lsock, csock = self.set_up_sockets_for_connect(family)

        # Initiate connection
        on_done_result_bucket = []
        def on_done(result):
            on_done_result_bucket.append(result)
            self.fail('Got done callacks on cancelled connection request.')

        connect_ref = svcs.connect_socket(csock, lsock.getsockname(), on_done)

        self.assertEqual(connect_ref.cancel(), True)

        # Now let the loop run for an iteration
        svcs.add_callback_threadsafe(svcs.stop)

        svcs.run()

        self.assertFalse(on_done_result_bucket)
        with self.assertRaises(Exception):
            csock.getpeername()
        self.assertEqual(connect_ref.cancel(), False)


class TestConnectSocketIPv4Success(SocketConnectorTestBase,
                                   AsyncServicesTestStubs):

    def start(self):
        self.check_successful_connect(family=socket.AF_INET)


class TestConnectSocketIPv4Fail(SocketConnectorTestBase,
                                AsyncServicesTestStubs):

    def start(self):
        self.check_failed_connect(socket.AF_INET)


class TestConnectSocketToDisconnectedPeer(SocketConnectorTestBase,
                                          AsyncServicesTestStubs):
    def start(self):
        """Differs from `TestConnectSocketIPV4Fail` in that this test attempts
        to connect to the address of a socket whose peer had disconnected from
        it. `TestConnectSocketIPv4Fail` attempts to connect to a closed socket
        that was previously listening. We want to see what happens in this case
        because we're seeing strange behavior in TestConnectSocketIPv4Fail when
        testing with Twisted on Linux, such that the reactor calls the
        descriptors's `connectionLost()` method, but not its `write()` method.
        """
        svcs = self.create_async()

        csock = self.create_nonblocking_tcp_socket()

        badaddr = self.get_dead_socket_address()

        # Initiate connection
        on_done_result_bucket = []
        def on_done(result):
            on_done_result_bucket.append(result)
            svcs.stop()

        connect_ref = svcs.connect_socket(csock, badaddr, on_done)

        svcs.run()

        self.assertEqual(len(on_done_result_bucket), 1)
        self.assertIsInstance(on_done_result_bucket[0], Exception)
        with self.assertRaises(Exception):
            csock.getpeername()  # raises when not connected
        self.assertEqual(connect_ref.cancel(), False)


class TestConnectSocketIPv4Cancel(SocketConnectorTestBase,
                                  AsyncServicesTestStubs):

    def start(self):
        self.check_cancel_connect(socket.AF_INET)


class TestConnectSocketIPv6Success(SocketConnectorTestBase,
                                   AsyncServicesTestStubs):

    def start(self):
        self.check_successful_connect(family=socket.AF_INET6)


class TestConnectSocketIPv6Fail(SocketConnectorTestBase,
                                AsyncServicesTestStubs):

    def start(self):
        self.check_failed_connect(socket.AF_INET6)
