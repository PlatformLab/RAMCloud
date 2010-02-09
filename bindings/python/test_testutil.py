#!/usr/bin/env python

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

"""Unit tests for utilities for unit tests.

See L{testutil}.
"""

from __future__ import with_statement

import unittest
import cPickle as pickle
import sys
import StringIO

from testutil import BreakException, Opaque, Counter, MockRetry

class TestOpaque(unittest.TestCase):
    """Unit tests for L{Opaque}."""

    def test_equality(self):
        x = Opaque()
        y = Opaque()
        self.assertEqual(x, x)
        self.assertNotEqual(x, y)
        self.assert_(x == x)
        self.assert_(x != y)

    def test_pickle(self):
        x1 = Opaque()
        x2 = pickle.loads(pickle.dumps(x1))
        x3 = pickle.loads(pickle.dumps(x2, protocol=2))
        self.assertEqual(x1, x2)
        self.assertEqual(x1, x3)
        self.assertEqual(x2, x3)

class TestCounter(unittest.TestCase):
    """Unit tests for L{Counter}."""

    def test_normal(self):
        counter = Counter(self, 3)
        for i in range(3):
            counter.bump()
        counter.done()

    def test_no_steps(self):
        counter = Counter(self, 0)
        counter.done()

    def test_done_too_soon(self):
        counter = Counter(self, 1)
        self.assertRaises(self.failureException, counter.done)

    def test_done_too_late(self):
        counter = Counter(self, 2)
        counter.bump()
        counter.bump()
        self.assertRaises(self.failureException, counter.bump)

    def test_bump_int(self):
        counter = Counter(self, 3)
        counter.bump(0)
        self.assertRaises(self.failureException, counter.bump, 0)
        counter.bump(2)
        counter.done()

    def test_bump_list(self):
        counter = Counter(self, 3)
        counter.bump([0,7])
        self.assertRaises(self.failureException, counter.bump, [0,2])
        counter.bump([2,9])
        counter.done()

    def test_with(self):
        with Counter(self, 3) as counter:
            for i in range(3):
                counter.bump(i)

    def test_with_not_done(self):
        def f():
            with Counter(self, 3) as counter:
                pass
        self.assertRaises(self.failureException, f)

    def test_with_not_done_suppressed(self):
        class E(Exception):
            pass
        def f():
            with Counter(self, 3) as counter:
                raise E
        s = StringIO.StringIO()
        saved = sys.stdout
        try:
            sys.stdout = s
            self.assertRaises(E, f)
        finally:
            sys.stdout = saved
        self.assertEquals(s.getvalue(),
                          "Suppressed exception from Counter.__exit__()\n")

class TestMockRetry(unittest.TestCase):
    """Unit tests for L{testutil.MockRetry}."""

    def test_next(self):
        retries = MockRetry(self)
        self.assertEquals(retries.next(), retries)
        self.assertRaises(StopIteration, retries.next)
        retries.done()

    def test_later(self):
        retries = MockRetry(self, expect_later=True)
        retries.next()
        retries.later()
        self.assertRaises(BreakException, retries.next)
        retries.done()

    def test_later_bad(self):
        retries = MockRetry(self)
        retries.next()
        self.assertRaises(self.failureException, retries.later)

    def test_immediate(self):
        retries = MockRetry(self, expect_immediate=True)
        retries.next()
        retries.immediate()
        self.assertRaises(BreakException, retries.next)
        retries.done()

    def test_immediate_bad(self):
        retries = MockRetry(self)
        retries.next()
        self.assertRaises(self.failureException, retries.immediate)

    def test_with(self):
        with MockRetry(self) as retries:
            for i, retry in enumerate(retries):
                pass

    def test_with_not_done(self):
        def f():
            with MockRetry(self, expect_later=True) as retries:
                pass
        self.assertRaises(self.failureException, f)

    def test_with_not_done_suppressed(self):
        class E(Exception):
            pass
        def f():
            with MockRetry(self, expect_later=True) as retries:
                raise E
        s = StringIO.StringIO()
        saved = sys.stdout
        try:
            sys.stdout = s
            self.assertRaises(E, f)
        finally:
            sys.stdout = saved
        self.assertEquals(s.getvalue(),
                          "Suppressed exception from MockRetry.__exit__()\n")

if __name__ == '__main__':
    unittest.main()
