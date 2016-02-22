"""Helpers for threading"""

import abc
import collections
import errno
import logging
import os
import Queue
import select
import socket
import threading
import time

from pika import compat


LOGGER = logging.getLogger(__name__)


def create_attention_pair():
    """ Create a socket pair that is suitable for alerting threads.

    TODO select_connection could use this instead of its own _get_interrupt_pair
    """

    read_sock = write_sock = None

    try:
        read_sock, write_sock = socket.socketpair(socket.AF_UNIX,
                                                  socket.SOCK_STREAM)
    except AttributeError:
        pass

    if read_sock is None:
        # Create TCP/IP socket pair manually

        # Manually create and connect two sockets
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.bind(('localhost', 0))
        listen_sock.listen(1)

        write_sock = socket.socket()
        write_sock.connect(listen_sock.getsockname())

        read_sock = listen_sock.accept()[0]

        listen_sock.close()

        # Disable Nagle Algorithm to avoid delay (only applies to TCP/IP stream)
        write_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


    # Reduce memory footprint
    # NOTE actual buffers might be bigger than requested
    # TODO check impact of sockbuf sizes on performance
    read_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 100)  # pylint: disable=E1101
    read_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 10)  # pylint: disable=E1101

    write_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 10)
    write_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 100)

    # Make them non-blocking to avoid deadlock
    read_sock.setblocking(0)  # pylint: disable=E1101
    write_sock.setblocking(0)

    return read_sock, write_sock


class SimpleQueueIface(compat.AbstractBase):
    """Simple queue interface"""

    @abc.abstractmethod
    def close(self):
        """Release resources"""
        pass

    @abc.abstractmethod
    def qsize(self):
        """Return currrent queue size (number of items)

        :returns: currrent queue size (number of items)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, item):
        """Append an item to the queue"""
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, block=True, timeout=None):
        """ Get an item from the queue

        :param block: see `Queue.Queue.get`
        :param timeout: see `Queue.Queue.get`

        :returns: item from the beginning of the queue

        :raises Queue.Empty: if queue is empty following the requested wait.

        """
        raise NotImplementedError

# python version-specific implementation of SingleConsumerSimpleQueue.
#
# NOTE We use our own thread-safe queue implementation in py2, because
# `Queue.Queue.get` is very slow on py2 when passed a positive timeout value;
# `Queue.Queue` implements the wait via a polling loop with exponentially
# increasing sleep durations. py3 uses pthread_cond_timedwait, which should be
# much faster.

if not compat.PY2:
    # SingleConsumerSimpleQueue for Py3

    class SingleConsumerSimpleQueue(SimpleQueueIface):
        """Simple thread-safe queue, using the builtin `Queue.Queue`, suitable
        for single-reader/single-writer"""

        def __init__(self):
            self._queue = Queue.Queue()

        def close(self):
            """Release resources"""
            self._queue.queue.clear()

        def qsize(self):
            """Return currrent queue size (number of items)

            :returns: currrent queue size (number of items)

            """
            return self._queue.qsize()

        def put(self, item):
            """Append an item to the queue

            """
            self._queue.put(item)

        def get(self, block=True, timeout=None):
            """ Get an item from the queue

            :param block: see `Queue.Queue.get`
            :param timeout: see `Queue.Queue.get`

            :returns: item from the beginning of the queue

            :raises Queue.Empty: if queue is empty following the requested wait.

            """
            return self._queue.get(block=block, timeout=timeout)

else:
    # SingleConsumerSimpleQueue for PY2

    class SingleConsumerSimpleQueue(SimpleQueueIface):
        """ Simple thread-safe queue suitable for single-reader/single-writer"""

        def __init__(self):
            self._lock = threading.Lock()
            self._queue = collections.deque()
            self._r_attention, self._w_attention = create_attention_pair()
            self._attention_waiter = AttentionWaiter(
                self._r_attention.fileno())  #pylint: disable=E1101

        def close(self):
            """Release resources

            """
            self._attention_waiter.close()
            self._r_attention.close()
            self._r_attention = None

            self._w_attention.close()
            self._w_attention = None

            self._queue.clear()

        def qsize(self):
            """Return currrent queue size (number of items)

            :returns: currrent queue size (number of items)

            """
            with self._lock:
                return len(self._queue)

        def put(self, item):
            """Append an item to the queue

            """
            with self._lock:
                self._queue.append(item)

            try:
                os.write(self._w_attention.fileno(), b'X')
            except OSError as exc:
                if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    raise

        def get(self, block=True, timeout=None):
            """ Get an item from the queue

            :param block: see `Queue.Queue.get`
            :param timeout: see `Queue.Queue.get`

            :returns: item from the beginning of the queue

            :raises Queue.Empty: if queue is empty following the requested wait.

            """

            if not block:
                timeout = 0

            with self._lock:
                try:
                    return self._queue.popleft()
                except IndexError:
                    if timeout == 0:
                        raise Queue.Empty

            # Wait for it
            if timeout is None:
                wait_amount = timeout  # either 0 or None
            else:
                now = time.time()
                deadline = now + timeout

            while True:
                if timeout is not None:
                    wait_amount = max(0, deadline - now)
                    now = None

                if self._attention_waiter.wait(wait_amount):
                    with self._lock:
                        try:
                            return self._queue.popleft()
                        except IndexError:
                            pass

                if timeout is not None:
                    # Update current time cache and check for timeout
                    now = time.time()
                    if now >= deadline:
                        raise Queue.Empty


class AttentionWaiter(object):
    """Helper class to wait for the attention file descriptor to become readable
    using the poller it deems best for the task.
    """

    def __init__(self, attention_fd):
        """ Selects the attention poller it deems best for the task

        :param integer attention_fd: file descriptor to wait on becoming
            readable

        """
        self._attention_fd = attention_fd

        self._poller = None

        if hasattr(select, 'epoll'):
            LOGGER.debug('Using EPollAttentionPoller')
            self._poller = EPollAttentionPoller(attention_fd)

        elif hasattr(select, 'kqueue'):
            LOGGER.debug('Using KQueueAttentionPoller')
            self._poller = KQueueAttentionPoller(attention_fd)

        else:
            LOGGER.debug('Using SelectAttentionPoller')
            self._poller = SelectAttentionPoller(attention_fd)

    def close(self):
        """Clean up resources

        """
        self._poller.close()

    def wait(self, timeout=None):
        """Wait for the attention file descriptor to become readable

        :param timeout: 0 to return result immediately; None to wait forever for
            attention file descriptor to become readable; positive number
            specifies maximum number of seconds to wait.
        :type timeout: non-negative `float` or `None`

        :returns: True if attention file descriptor became readable; False if
            request did not become readable.
        """
        if timeout < 0:
            raise ValueError('timeout may be None, 0, or positive number, '
                             'but got {!r}'.format(timeout))

        ready = False

        if timeout > 0:
            now = time.time()
            deadline = now + timeout
        else:
            wait_amount = timeout  # either 0 or None

        while not ready:
            if timeout > 0:
                wait_amount = max(0, deadline - now)
                now = None

            # Might return prematurely with None if interrupted (e.g., signal)
            ready = self._poller.wait(timeout=wait_amount)

            if ready:
                # Purge sock data to prevent racing the CPU
                try:
                    os.read(self._attention_fd, 512)  # pylint: disable=E1101
                except OSError as exc:
                    if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        raise
            else:
                # Either not ready yet or interrupted by signal

                if ready is not None and timeout == 0:
                    break

                if timeout > 0:
                    # Update current time cache
                    now = time.time()

                    if ready is not None:
                        # Check for timeout
                        if now >= deadline:
                            break

        return ready


class AttentionPollerIface(compat.AbstractBase):
    """Interface for attention pollers. Derived classes are intended to be in
    support of the `AttentionWaiter`, which implements the higher-level logic.
    """

    @abc.abstractmethod
    def __init__(self, attention_fd):
        """

        :param integer attention_fd: file descriptor to wait on becoming
            readable

        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        """Clean up resources

        """
        raise NotImplementedError

    @abc.abstractmethod
    def wait(self, timeout=None):
        """Wait for attention file descriptor to become readable

        :param timeout: 0 to return result immediately; None to wait forever for
            attention file descriptor to become readable; positive number
            specifies maximum number of seconds to wait.
        :type timeout: non-negative `float` or `None`

        :returns: True if attention file descriptor became readable; False if
            it did not become readable; None if interrupted prematurely (e.g.,
            by signal)
        """
        raise NotImplementedError


class SelectAttentionPoller(AttentionPollerIface):
    """Attention Poller implementation using `select.select`"""

    def __init__(self, attention_fd):  # pylint: disable=W0231
        """

        :param integer attention_fd: file descriptor to wait on becoming
            readable

        """
        self._attention_fd = attention_fd

    def close(self):
        # Nothing to clean up for SelectAttentionPoller
        pass

    def wait(self, timeout=None):
        """Wait for attention file descriptor to become readable

        :param timeout: 0 to return result immediately; None to wait forever for
            attention file descriptor to become readable; positive number
            specifies maximum number of seconds to wait.
        :type timeout: non-negative `float` or `None`

        :returns: True if attention file descriptor became readable; False if
            it did not become readable; None if interrupted prematurely (e.g.,
            by signal)
        """
        try:
            rlist, _, _ = select.select([self._attention_fd], [], [],
                                        timeout)
        except compat.SELECT_EINTR_ERRORS as error:
            if compat.is_select_eintr(error):
                return None
            else:
                raise

        return bool(rlist)


class EPollAttentionPoller(AttentionPollerIface):
    """Attention Poller implementation using `select.epoll`"""

    def __init__(self, attention_fd):  # pylint: disable=W0231
        """

        :param integer attention_fd: file descriptor to wait on becoming
            readable

        """
        self._attention_fd = attention_fd
        self._poller = select.epoll()  # pylint: disable=E1101
        self._poller.register(self._attention_fd,
                              select.POLLIN)  # pylint: disable=E1101

    def close(self):
        self._poller.close()

    def wait(self, timeout=None):
        """Wait for attention file descriptor to become readable

        :param timeout: 0 to return result immediately; None to wait forever for
            attention file descriptor to become readable; positive number
            specifies maximum number of seconds to wait.
        :type timeout: non-negative `float` or `None`

        :returns: True if attention file descriptor became readable; False if
            it did not become readable; None if interrupted prematurely (e.g.,
            by signal)
        """
        try:
            events = self._poller.poll(timeout, 1)
        except compat.SELECT_EINTR_ERRORS as error:
            if compat.is_select_eintr(error):
                return None
            else:
                raise

        return bool(events)


class KQueueAttentionPoller(AttentionPollerIface):
    """Attention Poller implementation using `select.kqueue`"""

    def __init__(self, attention_fd):  # pylint: disable=W0231
        """

        :param integer attention_fd: file descriptor to wait on becoming
            readable

        """
        self._attention_fd = attention_fd
        self._kqueue = select.kqueue()  # pylint: disable=E1101
        self._kqueue.control(
            [select.kevent(attention_fd,
                           filter=select.KQ_FILTER_READ,
                           flags=select.KQ_EV_ADD)],
            0)

    def close(self):
        self._kqueue.close()

    def wait(self, timeout=None):
        """Wait for attention file descriptor to become readable

        :param timeout: 0 to return result immediately; None to wait forever for
            attention file descriptor to become readable; positive number
            specifies maximum number of seconds to wait.
        :type timeout: non-negative `float` or `None`

        :returns: True if attention file descriptor became readable; False if
            it did not become readable; None if interrupted prematurely (e.g.,
            by signal)
        """
        try:
            kevents = self._kqueue.control(None, 1, timeout)
        except compat.SELECT_EINTR_ERRORS as error:
            if compat.is_select_eintr(error):
                return None
            else:
                raise

        return bool(kevents)
