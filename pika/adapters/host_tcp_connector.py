"""
Implements the workflow of asynchronous DNS address resolution and TCP/IP
connection establishment
"""

import logging
import socket

import pika.compat
import pika.tcp_socket_opts


_LOG = logging.getLogger(__name__)


class HostTCPConnectorException(Exception):
    """Base HostTCPConnector-specific exception class"""
    pass


class NoMoreAddresses(HostTCPConnectorException):
    """No more addresses are available for connecting; consider creating a new
    `HostTCPConnector` instance later and trying again.
    """

    def __init__(self, connection_errors, *args):
        """

        :param list connection_errors: Possibly empty list of preceding
            connection errors (exceptions) since last successful connection.
        :param args: Args to pass to base class
        """
        super(NoMoreAddresses, self).__init__(*args)
        self.connection_errors = connection_errors


class HostTCPConnector(object):
    """Implements the workflow of asynchronous DNS address resolution and TCP/IP
    connection establishment.

    """

    _SOCK_TYPE = socket.SOCK_STREAM
    _IPPROTO = socket.IPPROTO_TCP

    def __init__(self, connection_params, async_services):
        """
        :param pika.connection.Parameters connection_params:
        :param pika.adapters.async_interface.AbstractAsyncServices async_services:
        """
        self._connection_params = connection_params
        self._async_services = async_services
        self._addrinfo_iter = None
        self._sock = None
        self._async_ref = None  # current cancelable asynchronous task
        self._timeout_ref = None  # current timeout timer

        # Exceptions from failed connection attempts since last successful
        # connection attempt to be passed to user when we run out of resolved
        # address records without success
        self._connection_errors = []

        self._on_done = None  # will be provided via try_next()

    def close(self):
        """Abruptly close the connector, ensuring no callback from it.

        """
        self._deactivate()

        if self._sock is not None:
            self._sock.close()
            self._sock = None

        self._connection_errors = None
        self._async_services = None
        self._connection_params = None
        self._on_done = None

    def try_next(self, on_done):
        """Attempt to connect asynchronously using the remaining address records
        from the DNS lookup, kick-starting the DNS lookup if it hasn't been
        performed yet.

        :param callable on_done: on_done(socket.socket|BaseException)`, will be
            invoked upon completion, where the arg value of socket.socket
            signals success, while an exception object signals failure of the
            workflow after exhausting all remaining address records from DNS
            lookup. Once all addresses are exhausted (`NoMoreAddresses` instance
            passed to the callback), instantiate a new `HostTCPConnector` object
            to try again.
        """
        assert self._async_ref is None, \
            'try_next() called while async operation is in progress'

        if not callable(on_done):
            raise TypeError(
                'Expected callable on_done, but got {!r}'.format(on_done))
        self._on_done = on_done

        if self._addrinfo_iter is None:
            # First check if we already have a resolved address
            addr_record = self._check_already_resolved(self._connection_params)
            if addr_record:
                _LOG.debug('Host is alredy resolved: %r', addr_record)
                self._addrinfo_iter = iter([addr_record])
            else:
                # Start address resolution
                _LOG.debug('Starting asynchronous address resolution of %r',
                           self._connection_params.host)
                self._async_ref = self._async_services.getaddrinfo(
                    host=self._connection_params.host,
                    port=self._connection_params.port,
                    socktype=self._SOCK_TYPE,
                    proto=self._IPPROTO,
                    on_done=self._on_getaddrinfo_done)
                return

        # Already resolved, kick off asynchronously in case need to invoke
        # callback on error
        self._async_ref = self._async_services.call_later(0,
                                                          self._try_next_async)

    def _deactivate(self):
        """Cancel all pending asynchronous tasks.

        """
        _LOG.debug('Deactivating asynchronous tasks.')
        if self._async_ref is not None:
            self._async_ref.cancel()
            self._async_ref = None

        if self._timeout_ref is not None:
            self._timeout_ref.cancel()
            self._timeout_ref = None

    @staticmethod
    def _inet_pton(af, ip):
        """Wrapper for `socket.inet_pton()` that handles platform-specific
        issues, such as lack of `inet_pton()` on Windows Python 2.7.x (up to
        2.7.14 at least) and non-standard behavior of the `inet_pton()` twisted
        monkey-patch in `twisted.python.compat`.

        :param int af: see `socket.inet_pton()`
        :param str ip: see `socket.inet_pton()`
        :return: return value of `socket.inet_pton()`
        :raises NotImplementedError: if `socket.inet_pton()` is not available.
        :raises socket.error | OSError: on error

        """
        try:
            socket.inet_pton
        except AttributeError as error:
            if not pika.compat.ON_WINDOWS:
                _LOG.error(
                    'Missing socket.inet_pton() on non-Windows platform.')
                raise AssertionError(
                    'Missing socket.inet_pton() on non-Windows platform.')

            _LOG.debug(
                'Unable to check resolved address: no socket.inet_pton().')
            raise NotImplementedError('{}'.format(error))

        try:
            return socket.inet_pton(af, ip)
        except ValueError as error:
            # NOTE: When socket.inet_pton() is not available or substandard,
            # twisted monkey-patches socket to add its own implementation of
            # inet_pton(). twisted's inet_pton() implementation on Python # v2.7
            # may raise ValueError instead of socket.error for AF_INET6;
            # observed on Windows Python v2.7.14 calling
            # `socket.inet_pton(socket.AF_INET6, 'localhost')`.
            if pika.compat.ON_WINDOWS:
                # Fix up exception class on Windows
                new_error = pika.compat.SOCKET_ERROR('{}'.format(error))
                _LOG.debug(
                    'Fixed up socket.inet_pton() on Windows from {!r} to {!r}'
                    .format(error, new_error),
                    exc_info=True)
                raise new_error

            raise

    @classmethod
    def _check_already_resolved(cls, connection_params):
        """Check if the host is an IP address.

        :param pika.connection.Parameters connection_params:
        :return: None if host is not an IP address; otherwise a single
            `getaddrinfo()`-compatible address record appropriate for the
            address family of the host IP address.
        """
        # Check for IPv4 address
        try:
            cls._inet_pton(socket.AF_INET, connection_params.host)
        except NotImplementedError:
            # socket.inet_pton is not supported on this platform/python version.
            pass
        except pika.compat.SOCKET_ERROR as error:
            _LOG.debug('%s is not IPv4 address: %r',
                       connection_params.host, error)
        else:
            return (socket.AF_INET, cls._SOCK_TYPE, cls._IPPROTO, '',
                    (connection_params.host, connection_params.port))

        # Check for IPv6 address
        try:
            cls._inet_pton(socket.AF_INET6, connection_params.host)
        except NotImplementedError:
            # socket.inet_pton is not supported on this platform/python version.
            pass
        except pika.compat.SOCKET_ERROR as error:
            _LOG.debug('%s is not IPv6 address: %r',
                       connection_params.host, error)
        else:
            return (socket.AF_INET6, cls._SOCK_TYPE, cls._IPPROTO, '',
                    (connection_params.host, connection_params.port, 0, 0))

        return None


    def _report_completion(self, result):
        """Clean up and invoke user's `on_done` callback.

        :param socket.socket | BaseException result: value to pass to user's
            `on_done` callback.
        """
        self._deactivate()

        if isinstance(result, BaseException):
            assert self._sock is None, '_sock is not None'

        self._sock = None
        self._connection_errors = []

        try:
            self._on_done(result)
        finally:
            self._on_done = None

    def _on_getaddrinfo_done(self, addrinfos_or_exc):
        """Handles completion callback from asynchronous `getaddrinfo()`.

        :param list | BaseException addrinfos_or_exc: resolved address records
            returned by `getaddrinfo()` or an exception object from failure.
        """
        self._async_ref = None

        if isinstance(addrinfos_or_exc, BaseException):
            exc = addrinfos_or_exc
            _LOG.error('getaddrinfo failed: %r', exc)
            self._report_completion(exc)
        else:
            addrinfos = addrinfos_or_exc
            _LOG.debug('getaddrinfo returned %s records', len(addrinfos))
            self._addrinfo_iter = iter(addrinfos)
            self._try_next_async()

    def _try_next_async(self):
        """Try connecting to next address in resolved address list

        :return:
        """
        self._async_ref = None

        try:
            addr_record = next(self._addrinfo_iter)
        except StopIteration:
            self._report_completion(
                NoMoreAddresses(self._connection_errors,
                                'Reached end of resolved addresses after '
                                '{} errors\n{}'.format(
                                    len(self._connection_errors),
                                    '\n'.join(repr(exc) for exc in
                                              self._connection_errors))))
            return

        _LOG.debug('Attempting to connect using address record %r',
                   addr_record)
        self._sock = socket.socket(addr_record[0],
                                   addr_record[1],
                                   addr_record[2])

        self._sock.setsockopt(pika.compat.SOL_TCP, socket.TCP_NODELAY, 1)

        pika.tcp_socket_opts.set_sock_opts(self._connection_params.tcp_options,
                                           self._sock)
        self._sock.setblocking(False)

        # Initiate asynchronous connection attempt
        addr = addr_record[4]
        _LOG.info('Connecting to server at %r', addr)
        self._async_ref = self._async_services.connect_socket(
            self._sock,
            addr,
            on_done=self._on_socket_connection_done)

        # Start connection timeout timer
        self._timeout_ref = self._async_services.call_later(
            self._connection_params.socket_timeout,
            self._on_connection_timeout)

    def _on_socket_connection_done(self, exc):
        """Handle completion of asynchronous stream socket connection attempt.

        On failure, attempt to connect to the next address, if any, from DNS
        lookup.

        :param None|BaseException exc: None on success; exception object on
            failure

        """
        self._async_ref = None
        self._timeout_ref.cancel()
        self._timeout_ref = None

        if exc is not None:
            _LOG.error('%r connection attempt failed: %r.', self._sock, exc)
            self._connection_errors.append(exc)
            self._sock.close()
            self._sock = None
            # Try to connect using next resolved address record
            self._try_next_async()
        else:
            # We succeeded in making a TCP/IP connection to the server
            _LOG.debug('%r connected.', self._sock)
            self._report_completion(self._sock)

    def _on_connection_timeout(self):
        """Handle connection attempt timeout; cancel the connection attempt,
        close the socket, and attempt to initiate connection using next resolved
        address record.
        """
        self._timeout_ref = None
        self._async_ref.cancel()
        self._async_ref = None

        _LOG.debug('Connection attempt timed out for %r', self._sock)
        self._connection_errors.append(
            socket.timeout('Timed out connecting {!r}'.format(self._sock)))

        self._sock.close()
        self._sock = None

        # Try to connect using next resolved address record
        self._try_next_async()
