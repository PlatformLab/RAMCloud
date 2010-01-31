# Copyright (c) 2010 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""A nice way to loop until some iteration experiences no transient errors.

@see: L{test_retries}

"""

import time
import random

class ImmediateRetry(object):

    """A nice way to loop until some iteration experiences no transient errors.

    Example usage:

    >>> for retry in retries.ImmediateRetry():
    ...     try:
    ...        code that might experience a transient error
    ...     except ...:
    ...         retry.immediate()

    @type count: C{int}
    @ivar count: the number of iterations already completed, starting from 0

    @type need_retry: C{bool}
    @ivar need_retry: whether another iteration is scheduled

    @see: L{BackoffRetry}
    """

    def __init__(self):
        self.need_retry = True
        self.count = -1

    def __int__(self):
        """Return L{count}.

        @rtype: C{int}

        """
        return self.count

    def __iter__(self):
        """Return this object.

        @rtype: L{ImmediateRetry}

        """
        return self

    def next(self):
        """Return this object if there's another iteration scheduled.

        @rtype: L{ImmediateRetry}
        @raises StopIteration: if there's not another iteration scheduled

        """
        done = not self.need_retry
        self.need_retry = False
        self.count += 1
        if done:
            raise StopIteration
        else:
            return self

    def immediate(self):
        """Schedules another iteration."""

        self.need_retry = True

    later = immediate

class BackoffRetry(ImmediateRetry):

    """Like L{ImmediateRetry} but sleeps between iterations.


    @see: L{ExponentialBackoff}, L{FuzzyExponentialBackoff}, L{RandomBackoff}
    """

    def __init__(self, wait_time_iter, sleep_func=None):
        """
        @type  wait_time_iter: iter over C{float}
        @param wait_time_iter: amount of time to wait between iterations

            If the iterator raises C{StopIteration}, the last value returned
            will be used thereafter. If no value was ever returned from the
            iterator, the behavior is not to wait at all.

        @type  sleep_func: function
        @param sleep_func: function to call to sleep

            Defaults to C{time.sleep}, which is almost certainly what you want.
            This is here for testing purposes.

            C{sleep_func} takes a single float argument that is the desired
            duration of the sleep in seconds and returns nothing.

        """
        ImmediateRetry.__init__(self)
        self._wait_time_iter = wait_time_iter
        self._wait_time = 0.0
        self._wait_next = False
        if sleep_func:
            self._sleep_func = sleep_func
        else:
            self._sleep_func = time.sleep

    def next(self):
        """Optionally sleep, then return this object if there's another
        iteration scheduled."""

        ImmediateRetry.next(self)
        if self._wait_next:
            try:
                self._wait_time = self._wait_time_iter.next()
            except StopIteration:
                pass
            self._sleep_func(self._wait_time)
        self._wait_next = True
        return self

    def immediate(self):
        """Schedule another iteration without sleeping.

        If L{later} is also called, this class will sleep in L{next} before the
        next iteration.

        """

        if not self.need_retry:
            self._wait_next = False
        ImmediateRetry.immediate(self)

    def later(self):
        """Schedule a sleep and another iteration.

        Regardless of whether L{immediate} is called, this class will sleep in
        L{next} before the next iteration.

        """
        self._wait_next = True
        ImmediateRetry.immediate(self)


class ExponentialBackoff(BackoffRetry):

    """Exponential backoff retry strategy.

    Each sleep will be a scalar factor times longer than the previous.

    """

    def __init__(self, start=0.1, scale=2.0, limit=30.0):
        """
        @type  start: C{float}
        @param start: the time to sleep after the first iteration

        @type  scale: C{float}
        @param scale: the factor to scale the time by

        @type  limit: C{float}
        @param limit: the maximum time to sleep for between iterations

        """
        def wait_time_gen():
            time = start
            while time < limit:
                yield time
                time *= scale
            yield limit
        BackoffRetry.__init__(self, wait_time_gen())

class FuzzyExponentialBackoff(BackoffRetry):

    """Fuzzy exponential backoff retry strategy.

    Each sleep will be within some range times longer than the previous. The
    exact factor is chosen randomly from is uniform distribution.

    """

    def __init__(self, start=0.1, scale_lower=1.4, scale_upper=2.6, limit=30):
        """
        @type  start: C{float}
        @param start: the time to sleep after the first iteration

        @type  scale_lower: C{float}
        @param scale_lower: the minimum factor to scale the time by

        @type  scale_upper: C{float}
        @param scale_upper: the maximum factor to scale the time by

        @type  limit: C{float}
        @param limit: the maximum time to sleep for between iterations

        """
        def wait_time_gen():
            time = start
            while time < limit:
                yield time
                time *= random.uniform(scale_lower, scale_upper)
            yield limit
        BackoffRetry.__init__(self, wait_time_gen())

class RandomBackoff(BackoffRetry):

    """Random retry strategy.

    Each sleep time will be randomly chosen from a range. The times are
    independently chosen from a uniform distribution.

    """

    def __init__(self, lower=0.0, upper=3.0):
        """
        @type  lower: C{float}
        @param lower: the minimum time to sleep between iterations

        @type  upper: C{float}
        @param upper: the maximum time to sleep between iterations

        """
        def wait_time_gen():
            while True:
                yield random.uniform(lower, upper)
        BackoffRetry.__init__(self, wait_time_gen())
