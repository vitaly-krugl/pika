"""Utility functions for subclassing"""


def verify_overrides(cls):
    """Class Decorator to verify that methods tagged by
    the `overrides_instance_method` decorator actually override corresonding
    methods in one of the base classes

    :raises TypeError: if an unbound method doesn't override another one.
    """

    for name in dir(cls):
        method = getattr(cls, name)

        if not hasattr(method, 'check_method_override'):
            continue

        if not callable(method):
            raise TypeError('{} is not callable', method)

        if not hasattr(method, '__self__'):
            raise TypeError('{} does not have `__self__` member'.format(method))

        if method.__self__ is not None:
            raise TypeError(
                '{} is not an unbound method: `__self__` {} is not None'
                .format(method, method.__self__))

        for base_cls in method.im_class.__bases__:
            base_method = getattr(base_cls, method.__name__, None)

            if base_method is None:
                continue

            if not callable(base_method):
                raise TypeError('{} attempts to override non-callable {}'
                                .format(method, base_method))

            if not hasattr(base_method, '__self__'):
                raise TypeError(
                    '{}\'s base {} does not have `__self__` member'.format(
                        method, base_method))

            if base_method.__self__ is not None:
                raise TypeError('{}\'s base {} is not an unbound method: '
                                '`__self__` {} is not None'.format(
                                    method, base_method, base_method.__self__))

            break
        else:
            raise TypeError('Nothing to override for {}'.format(method))


    return cls


def overrides_instance_method(func):
    """Method decorator that marks the method for verifying that it overrides
    an instance method. The verification is performed by class decorator
    `verify_overrides`

    :raises TypeError: if func is not an unbound instance method

    """
    func.check_method_override = True

    return func
