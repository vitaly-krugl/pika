"""Implements the workflow of performing multiple TCP/[SSL]/AMQP connection
attempts with timeouts and retries until one succeeds or all attempts fail.

"""


class AMQPConnector(object):
    """Implements the workflow of performing multiple TCP/[SSL]/AMQP connection
    attempts with timeouts and retries until one succeeds or all attempts fail.

    The workflow:
        while not success and retries remain:
            1. For each given config (pika.connection.Parameters object):
                A. Perform DNS resolution of the config's host.
                B. Attempt to establish TCP/[SSL]/AMQP for each resolved address
                   until one succeeds, in which case we're done.
            2. If all configs failed but retries remain, resume from beginning
               after the configured retry interval.

    """

    def __init__(self,
                 connection_configs,
                 connection_factory,
                 async_services,
                 retries=0,
                 retry_pause=2):
        """
        :param sequence connection_configs: A sequence of
            `pika.connection.Parameters`-based objects. Will attempt to connect
            using each config in the given order.
        :param callable connection_factory: A function that takes no args and
            instantiates a brand new `pika.connection.Connection`-based object
            each time it is called.
        :param pika.adapters.async_interface.AbstractAsyncServices async_services:
        :param int retries: Non-negative maximum number of retries after the
            initial attempt to connect using the entire config sequence fails.
            Defaults to 0.
        :param int | float retry_pause: Non-negative number of seconds to wait
            before retrying the config sequence. Meaningful only if retries is
            greater than 0. Defaults to 2 seconds.

        TODO: Would it be useful to implement exponential back-off?

        """
        pass

    def close(self):
        """Abruptly close the connector, ensuring no callback from it.

        """
        pass

    def start(self, on_done):
        """Asynchronously perform the workflow until success or all retries
        are exhausted.

        :param callable on_done: Function to call upon completion of the
            workflow: `on_done(pika.connection.Connection | BaseException)`.
        """
        pass
