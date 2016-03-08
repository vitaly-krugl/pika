"""Utilities shared by adapters that implement the Blocking Connection interface
"""

import threading


class _DummyLock(object):
    """Dummy lock for use in place of `threading.Lock` context manager when
    threadsafety is not needed (such as for single-threaded clients)

    """
    def __enter__(self):
        pass

    def __exit__(self, exc_type, value, traceback):
        pass


class ThresholdTracker(object):
    """Tracks content units against high- and low-water marks. The tracker
    becomes in-excess when the amount of content exceeds the high-water mark,
    and returns to normal once content dips below the low-water mark.

    """

    def __init__(self, low_water, high_water, threadsafe):
        """
        :param number low_water: while in excess, exit excess mode when
            outstanding volume dips below this number of units
        :param number high_water: when outstanding volume surpasses this number,
            enter in-excess mode
        :param bool threadsafe: Pass True to enable thread locking around
            critical section (for multithreaded uses), False to forego threasafe
            locking (for synchronous uses)

        """
        self._low_water = low_water
        self._high_water = high_water

        self._lock = threading.Lock() if threadsafe else _DummyLock()
        self._volume = 0
        self._exceeded = False

    def add(self, amount):
        """Add to volume

        :param number amount: amount to add to volume; must be >= 0

        :return: True if this operation caused volume to enter in-excess mode;
            None if there was no change in mode

        """
        with self._lock:
            self._volume += amount

            if not self._exceeded and self._volume > self._high_water:
                self._exceeded = True
                return True
            else:
                return None

    def subtract(self, amount):
        """Subtract from consumed volume

        :param amount: amount to subract from volume; must be >= 0

        :return: True if this operation caused volume to exit in-excess mode;
            None if there was no change in mode

        :raises ValueError:

        """
        with self._lock:
            self._volume -= amount

            if self._volume < 0:
                raise ValueError("Content volume underflow %r", self._volume)

            if self._exceeded and self._volume < self._low_water:
                self._exceeded = False
                return True
            else:
                return None

    @property
    def in_excess(self):
        """
        :returns: True if presently in-excess state; False if not in-excess

        """
        return self._exceeded

    @property
    def volume(self):
        """
        :returns: current volume

        """
        return self._volume
