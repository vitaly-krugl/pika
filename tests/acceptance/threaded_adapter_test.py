""" Generic blocking API acceptance tests for ThreadedConnection
"""

# Suppress pylint warnings concerning wildcard imports and unused imports
# pylint: disable=W0401,W0614

# Suppress pylint messages concerning "invalid function name"
# pylint: disable=C0103

from blocking_adapter_test import *

from pika.adapters import threaded_connection

_BLOCKING_CONNECTION_CLASS = pika.BlockingConnection


# Suppress pylint error concerning "function-redefined"
# pylint: disable=E0102
def setUpModule():
    """Unittest fixture: Init module for testing"""
    pika.BlockingConnection = threaded_connection.ThreadedConnection

def tearDownModule():
    """Unittest fixture: Uninit module after testing"""
    pika.BlockingConnection = _BLOCKING_CONNECTION_CLASS


if __name__ == '__main__':
    unittest.main()
