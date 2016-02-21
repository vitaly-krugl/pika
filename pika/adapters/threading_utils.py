"""Helpers for threading"""

import abc
import collections
import errno
import Queue
import os
import select
import socket
import threading
import time

from pika import compat

# TODO figure out how to share these the right way
from pika.adapters.select_connection import _SELECT_ERRORS, _is_resumable


def create_attention_pair():
    """ Create a socket pair that is suitable for alerting threads.
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
    # TODO check impact on performance
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



class SimpleQueueWithBuiltin(SimpleQueueIface):
    """Queue implementation using the builtin `Queue.Queue`"""

    def __init__(self):
        self._q = Queue.Queue()

    def close(self):
        """Release resources"""
        pass

    def qsize(self):
        """Return currrent queue size (number of items)

        :returns: currrent queue size (number of items)

        """
        return self._q.qsize()

    def put(self, item):
        """Append an item to the queue

        """
        self._q.put(item)

    def get(self, block=True, timeout=None):
        """ Get an item from the queue

        :param block: see `Queue.Queue.get`
        :param timeout: see `Queue.Queue.get`

        :returns: item from the beginning of the queue

        :raises Queue.Empty: if queue is empty following the requested wait.

        """
        return self._q.get(block=block, timeout=timeout)


class SimpleQueueWithSelectTimeout(SimpleQueueIface):
    """ Simple thread-safe queue suitable for single-reader/single-writer

    NOTE We use our own thread-safe queue implementation in py2, because
    `Queue.Queue.get` is very slow on py2 when passed a positive timeout value;
    `Queue.Queue` implements the wait via a polling loop with exponentially
    increasing sleep durations. py3 uses pthread_cond_timedwait, which should be
    much faster.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._q = collections.deque()
        self._r_attention, self._w_attention = create_attention_pair()

    def close(self):
        """Release resources

        """
        self._r_attention.close()
        self._r_attention = None

        self._w_attention.close()
        self._w_attention = None

    def qsize(self):
        """Return currrent queue size (number of items)

        :returns: currrent queue size (number of items)

        """
        with self._lock:
            return len(self._q)

    def put(self, item):
        """Append an item to the queue

        """
        with self._lock:
            self._q.append(item)

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
        if block and timeout is not None:
            deadline = time.time() + timeout

        while True:
            with self._lock:
                if self._q:
                    return self._q.popleft()

            if not block:
                raise Queue.Empty

            if timeout is not None:
                now = time.time()

                if now >= deadline:
                    raise Queue.Empty

                waitsec = deadline - now
            else:
                waitsec = None

            # TODO in order to support apps that use a lot of sockets, we need
            # to support kqueue and epoll. This is because select uses a fixed
            # size bitmask to represent sockets of interest (FD_SETSIZE). Even
            # though we're using only one socket, it's file descriptor number
            # could very well exceed the capacity of the app's FD_SETSIZE.
            try:
                rlist, _, _ = select.select([self._r_attention], [], [],
                                            waitsec)
            except _SELECT_ERRORS as error:
                if _is_resumable(error):
                    continue
                else:
                    raise

            if not rlist:
                raise Queue.Empty

            # Purge sock data to prevent racing the CPU
            try:
                # TODO would socket.recv_into be faster? (differnt errors)
                os.read(self._r_attention.fileno(), 512)  # pylint: disable=E1101
            except OSError as exc:
                if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    raise
