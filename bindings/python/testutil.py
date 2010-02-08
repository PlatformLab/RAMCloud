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

"""Utilities for unit tests."""

import retries

class BreakException(Exception):
    """Break out of a unit test early."""
    pass

class Opaque(object):
    """A serializable object that equals only itself."""

    def __init__(self):
        self._id = id(self)
    def __hash__(self):
        return self._id
    def __cmp__(self, other):
        return cmp(self._id, other._id)

class Counter(object):

    """A strictly increasing counter.

    One way to use this class is with the C{with} statement. This way, you
    can't forget to call L{done}. See L{test_testutil.TestCounter.test_with}
    for an example.

    @ivar count: The number of times L{bump} has been called minus 1.
    @type count: C{int}
    """

    def __init__(self, tc, steps=None):
        """
        @param tc: The test case with which to make assertions.
        @type  tc: C{unittest.TestCase}

        @param steps: The number of times L{bump} should be called over the
                      lifetime of the counter. This is optional.
        @type  steps: C{int} or C{None}
        """

        self.tc = tc
        self.steps = steps
        self.count = -1

    def bump(self, expected=None):
        """Increment L{count}.

        If C{steps} was passed to the constructor and L{bump} has now been
        called more than C{steps} times, this method will fail the test case.

        @param expected: The value of L{count} expected after incrementing it.
                         This is optional. If an C{int} is passed in, this
                         method will test whether the new value of L{count}
                         equals C{expected}. If a container is passed in, this
                         method will test whether the new value of L{count} is
                         C{in expected}.
        @type  expected: C{int} or a container of C{int}s

        @return: The new value of L{count} as a convenience.
        @rtype:  C{int}
        """

        self.count += 1
        if self.steps is not None:
            self.tc.assert_(self.count + 1 <= self.steps,
                            "count=%d, steps=%d" % (self.count, self.steps))
        if expected is not None:
            try:
                self.tc.assert_(self.count in expected)
            except TypeError:
                self.tc.assertEquals(self.count, expected)
        return self.count

    def done(self):
        """Ensure L{bump} was called the required number of C{steps}, as given
        to L{__init__}."""

        if self.steps is not None:
            self.tc.assertEqual(self.count + 1, self.steps)

    # context manager interface:

    def __enter__(self):
        """No op.

        @return: this instance
        @rtype:  L{Counter}
        """

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Wrapper for L{done}.

        Prefers existing exceptions over those caused by L{done}.
        """

        try:
            self.done()
        except:
            if exc_type is None:
                raise
            else:
                # If there was already an exception, I'm betting it's more
                # interesting.
                print "Suppressed exception from Counter.__exit__()"

class MockRetry(retries.ImmediateRetry):
    """A mock implementation of a L{retries.ImmediateRetry}.

    This retry implementation only runs for a single iteration, which is usually
    enough.
    """

    def __init__(self, tc, expect_immediate=False, expect_later=False):
        """
        @param tc: The test case with which to make assertions.
        @type  tc: C{unittest.TestCase}

        @param expect_immediate: Whether to expect a call to L{immediate}.
        @type  expect_immediate: C{bool}

        @param expect_later: Whether to expect a call to L{later}.
        @type  expect_later: C{bool}
        """

        retries.ImmediateRetry.__init__(self)
        self.tc = tc
        self.expect_immediate = expect_immediate
        self.expect_later = expect_later

    def __call__(self):
        """Reinitialized this instance.

        This way L{MockRetry} behaves somewhat like a class.

        @return: this instance
        @rtype:  L{MockRetry}
        """

        retries.ImmediateRetry.__init__(self)
        return self

    def next(self):
        r = retries.ImmediateRetry.next(self)
        if self.count == 1:
            raise BreakException
        return r

    def immediate(self):
        self.tc.assert_(self.expect_immediate)
        self.expect_immediate = False
        retries.ImmediateRetry.immediate(self)

    def later(self):
        self.tc.assert_(self.expect_later)
        self.expect_later = False
        retries.ImmediateRetry.later(self)

    def done(self):
        """Ensures C{expect_immediate} and C{expect_later} have been
        satisfied."""

        self.tc.assertFalse(self.expect_immediate)
        self.tc.assertFalse(self.expect_later)

    def __enter__(self):
        """No op.

        @return: this instance
        @rtype:  L{MockRetry}
        """

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Wrapper for L{done}.

        Prefers existing exceptions over those caused by L{done}.
        """

        try:
            self.done()
        except:
            if exc_type is None:
                raise
            else:
                # If there was already an exception, I'm betting it's more
                # interesting.
                print "Suppressed exception from MockRetry.__exit__()"
