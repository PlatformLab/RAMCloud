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

"""Unit tests for C{retries.py}.

@see: L{retries}

"""

from __future__ import with_statement

import unittest
import random

from testutil import Counter
import retries

class TestImmediateRetry(unittest.TestCase):
    def test_fallthrough(self):
        with Counter(self, 1) as counter:
            for retry in retries.ImmediateRetry():
                i = counter.bump()
                self.assertEqual(retry.count, i)
                self.assertEqual(int(retry), i)
                self.assertEqual(retry.need_retry, False)
        self.assertEqual(retry.count, 1)
        self.assertEqual(int(retry), 1)
        self.assertEqual(retry.need_retry, False)

    def test_immediate(self):
        with Counter(self, 2) as counter:
            for retry in retries.ImmediateRetry():
                i = counter.bump()
                self.assertEqual(retry.need_retry, False)
                if i == 0:
                    retry.immediate()
                    self.assertEqual(retry.need_retry, True)
                self.assertEqual(retry.count, i)
                self.assertEqual(int(retry), i)
        self.assertEqual(retry.count, 2)
        self.assertEqual(int(retry), 2)
        self.assertEqual(retry.need_retry, False)

    def test_later(self):
        self.assertEqual(retries.ImmediateRetry.later,
                         retries.ImmediateRetry.immediate)

class TestBackoffRetry(unittest.TestCase):

    def sleep_func(self, wait_time_iter):
        def sf(time):
            try:
                expected = wait_time_iter.next()
            except StopIteration:
                self.fail('sleep func got StopIteration')
            self.assertEqual(time, expected)
        return sf

    def caffeine(self):
        return lambda t: self.fail('slept')

    def test_fallthrough(self):
        with Counter(self, 1) as counter:
            for retry in retries.BackoffRetry(iter([5]), self.caffeine()):
                i = counter.bump()
                self.assertEqual(retry.count, i)
                self.assertEqual(int(retry), i)
                self.assertEqual(retry.need_retry, False)
        self.assertEqual(retry.count, 1)
        self.assertEqual(int(retry), 1)
        self.assertEqual(retry.need_retry, False)

    def test_later(self):
        with Counter(self, 2) as counter:
            for retry in retries.BackoffRetry(iter([5]),
                                              self.sleep_func(iter([5]))):
                i = counter.bump()
                self.assertEqual(retry.need_retry, False)
                if i == 0:
                    retry.later()
                    self.assertEqual(retry.need_retry, True)
                self.assertEqual(retry.count, i)
                self.assertEqual(int(retry), i)
        self.assertEqual(retry.count, 2)
        self.assertEqual(int(retry), 2)
        self.assertEqual(retry.need_retry, False)

    def test_immediate(self):
        with Counter(self, 4) as counter:
            for retry in retries.BackoffRetry(iter([5]),
                                              self.sleep_func(iter([5]))):
                i = counter.bump()
                self.assertEqual(retry.need_retry, False)
                if i == 0:
                    retry.immediate()
                    self.assertEqual(retry.need_retry, True)
                elif i == 1:
                    retry.later()
                    self.assertEqual(retry.need_retry, True)
                elif i == 2:
                    retry.immediate()
                    self.assertEqual(retry.need_retry, True)
                elif i == 3:
                    pass
                else:
                    self.fail()
                self.assertEqual(retry.count, i)
                self.assertEqual(int(retry), i)
        self.assertEqual(retry.count, 4)
        self.assertEqual(int(retry), 4)
        self.assertEqual(retry.need_retry, False)

    def test_immediate_later(self):
        with Counter(self, 2) as counter:
            for retry in retries.BackoffRetry(iter([5]),
                                              self.sleep_func(iter([5]))):
                if counter.bump() == 0:
                    retry.immediate()
                    retry.later()

    def test_later_immediate(self):
        with Counter(self, 2) as counter:
            for retry in retries.BackoffRetry(iter([5]),
                                              self.sleep_func(iter([5]))):
                if counter.bump() == 0:
                    retry.later()
                    retry.immediate()

    def test_emptyiter(self):
        with Counter(self, 3) as counter:
            for retry in retries.BackoffRetry(iter([]),
                                              self.sleep_func(iter([0,0]))):
                if counter.bump() < 2:
                    retry.later()

    def test_stopiter(self):
        with Counter(self, 4) as counter:
            for retry in retries.BackoffRetry(iter([7,8]),
                                              self.sleep_func(iter([7,8,8,8]))):
                if counter.bump() < 3:
                    retry.later()

class TestExponentialBackoff(unittest.TestCase):
    def test_normal(self):
        wti = retries.ExponentialBackoff(0.3, 6.8, 90.1)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 0.3)
        self.assertAlmostEqual(wti.next(), 0.3 * 6.8)
        self.assertAlmostEqual(wti.next(), 0.3 * 6.8 ** 2)
        self.assertAlmostEqual(wti.next(), 90.1)
        self.assertRaises(StopIteration, wti.next)

    def test_low_limit(self):
        wti = retries.ExponentialBackoff(0.3, 6.8, 0.0)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 0.0)
        self.assertRaises(StopIteration, wti.next)

    def test_high_scale(self):
        wti = retries.ExponentialBackoff(2.0, 6.8, 3.0)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 2.0)
        self.assertAlmostEqual(wti.next(), 3.0)
        self.assertRaises(StopIteration, wti.next)

class TestFuzzyExponentialBackoff(unittest.TestCase):
    def setUp(self):
        random.seed(0)
        self.rand = [random.uniform(6.8, 8.5) for x in range(10)]
        random.seed(0)

    def test_normal(self):
        wti = retries.FuzzyExponentialBackoff(0.3, 6.8, 8.5,
                                              90.1)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 0.3)
        self.assertAlmostEqual(wti.next(), 0.3 * self.rand[0])
        self.assertAlmostEqual(wti.next(), 0.3 * self.rand[0] * self.rand[1])
        self.assertAlmostEqual(wti.next(), 90.1)
        self.assertRaises(StopIteration, wti.next)

    def test_low_limit(self):
        wti = retries.FuzzyExponentialBackoff(0.3, 6.8, 8.5,
                                              0.0)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 0.0)
        self.assertRaises(StopIteration, wti.next)

    def test_high_scale(self):
        wti = retries.FuzzyExponentialBackoff(2.0, 6.8, 8.5,
                                              3.0)._wait_time_iter
        self.assertAlmostEqual(wti.next(), 2.0)
        self.assertAlmostEqual(wti.next(), 3.0)
        self.assertRaises(StopIteration, wti.next)

class TestRandomBackoff(unittest.TestCase):
    def setUp(self):
        random.seed(0)
        self.rand = [random.uniform(6.8, 8.5) for x in range(10)]
        random.seed(0)

    def test_normal(self):
        wti = retries.RandomBackoff(6.8, 8.5)._wait_time_iter
        for r in self.rand:
            self.assertAlmostEqual(wti.next(), r)

if __name__ == '__main__':
    unittest.main()
