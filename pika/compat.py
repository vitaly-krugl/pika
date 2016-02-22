"""
PY2/PY3 compatibility helpers
"""

import abc
import errno
import os
import select
import sys as _sys

PY2 = _sys.version_info < (3,)
PY3 = not PY2


# Map of exception classes that might be thrown by select.* poller methods to
# callables that take an exception object and return True if the exception
# indicates EINTR.
#
# The reason for this unconventional dict initialization is the fact that on some
# platforms select.error is an aliases for OSError. We don't want the lambda
# for select.error to win over one for OSError.
_SELECT_EINTR_CHECKERS = {}


if not PY2:
    # these were moved around for Python 3
    from urllib.parse import unquote as url_unquote, urlencode #pylint: disable=W0611,E0611,F0401

    # Python 3 does not have basestring anymore; we include
    # *only* the str here as this is used for textual data.
    basestring = (str,)  #pylint: disable=W0622

    # for assertions that the data is either encoded or non-encoded text
    str_or_bytes = (str, bytes)

    # xrange is gone, replace it with range
    xrange = range  #pylint: disable=W0622

    # the unicode type is str
    unicode_type = str


    def dictkeys(dct):
        """
        Returns a list of keys of dictionary

        dict.keys returns a view that works like .keys in Python 2
        *except* any modifications in the dictionary will be visible
        (and will cause errors if the view is being iterated over while
        it is modified).
        """

        return list(dct.keys())

    def dictvalues(dct):
        """
        Returns a list of values of a dictionary

        dict.values returns a view that works like .values in Python 2
        *except* any modifications in the dictionary will be visible
        (and will cause errors if the view is being iterated over while
        it is modified).
        """
        return list(dct.values())

    def byte(*args):
        """
        This is the same as Python 2 `chr(n)` for bytes in Python 3

        Returns a single byte `bytes` for the given int argument (we
        optimize it a bit here by passing the positional argument tuple
        directly to the bytes constructor.
        """
        return bytes(args)

    class long(int):  #pylint: disable=W0622
        """
        A marker class that signifies that the integer value should be
        serialized as `l` instead of `I`
        """

        def __repr__(self):
            return str(self) + 'L'

    def canonical_str(value):
        """
        Return the canonical str value for the string.
        In both Python 3 and Python 2 this is str.
        """

        return str(value)

    def is_integer(value):
        return isinstance(value, int)

    # Define a base class for deriving abstract base classes for compatibility
    # between python 2 and 3 (metaclass syntax changed in Python 3). Ideally,
    # would use `@six.add_metaclass` or `six.with_metaclass`, but pika
    # traditionally has resisted external dependencies in its core code.
    #
    # NOTE: Wrapping in exec, because module containing
    # `class AbstractBase(metaclass=abc.ABCMeta)` fails to load under python 2.
    exec('class AbstractBase(metaclass=abc.ABCMeta): pass')  # pylint: disable=W0122

    # InterruptedError is undefined in PY2
    _SELECT_EINTR_CHECKERS[InterruptedError] = lambda e: True

else:
    # PY2

    from urllib import unquote as url_unquote, urlencode

    basestring = basestring
    str_or_bytes = basestring
    xrange = xrange
    unicode_type = unicode
    dictkeys = dict.keys
    dictvalues = dict.values
    byte = chr
    long = long

    def canonical_str(value):
        """
        Returns the canonical string value of the given string.
        In Python 2 this is the value unchanged if it is an str, otherwise
        it is the unicode value encoded as UTF-8.
        """

        try:
            return str(value)
        except UnicodeEncodeError:
            return str(value.encode('utf-8'))

    def is_integer(value):
        return isinstance(value, (int, long))

    class AbstractBase(object):  # pylint: disable=R0903
        """PY2 Abstract base"""
        __metaclass__ = abc.ABCMeta


def as_bytes(value):
    if not isinstance(value, bytes):
        return value.encode('UTF-8')
    return value


HAVE_SIGNAL = os.name == 'posix'

EINTR_IS_EXPOSED = _sys.version_info[:2] <= (3, 4)

_SELECT_EINTR_CHECKERS[select.error] = lambda e: e.args[0] == errno.EINTR
_SELECT_EINTR_CHECKERS[IOError] = lambda e: e.errno == errno.EINTR
_SELECT_EINTR_CHECKERS[OSError] = lambda e: e.errno == errno.EINTR

# Exception classes thrown by polling methods of select.select,
# select.poll, select.epoll, and select.kqueue that might be carriers of EINTR.
# May be used in the `except` statement.
SELECT_EINTR_ERRORS = tuple(_SELECT_EINTR_CHECKERS.keys())


def is_select_eintr(exc):
    """Check if the exception caught from a select.* poller represents EINTR.

    :param exc: exception; must be one of classes in SELECT_EINTR_ERRORS

    :returns: True if the exception represents EINTR; False if not.

    """
    checker = _SELECT_EINTR_CHECKERS.get(exc.__class__, None)
    if checker is not None:
        return checker(exc)
    else:
        return False
