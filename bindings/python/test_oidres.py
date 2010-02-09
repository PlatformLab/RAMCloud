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

"""Unit tests for C{oidres.py}.

@see: L{oidres}

"""

from __future__ import with_statement

import unittest

from testutil import Opaque, Counter, BreakException, MockRetry

import ramcloud
import retries
import oidres

class TestOIDRes(unittest.TestCase):

    table = Opaque()
    oid = Opaque()

    class MockRAMCloud(object):
        def __init__(self, testcase):
            self.tc = testcase

        def read(self, table, oid):
            self.tc.assertEqual(table, self.tc.table)
            self.tc.assertEqual(oid, self.tc.oid)
            next_avail, version = self.real_read()
            return (oidres.pack(next_avail), version)

        def update(self, table, oid, data, want_version):
            self.tc.assertEqual(table, self.tc.table)
            self.tc.assertEqual(oid, self.tc.oid)
            self.real_update(oidres.unpack(data), want_version)

        def create(self, table, oid, data):
            self.tc.assertEqual(table, self.tc.table)
            self.tc.assertEqual(oid, self.tc.oid)
            self.real_create(oidres.unpack(data))

    def assertPackable(self, x):
        self.assertEqual(oidres.unpack(oidres.pack(x)), x)

    def test_pack(self):
        """Test packing and unpacking OIDs."""
        self.assertPackable(0)
        self.assertPackable(3823095)
        self.assertPackable(0xFFFFFFFFFFFFFFFF)
        self.assertRaises(Exception, lambda: oidres.unpack('blah'))

    def test_delta(self):
        """Test that L{oidres.OIDRes.delta} is set correctly."""
        res = oidres.OIDRes(rc=Opaque(), table=self.table, oid=self.oid,
                            delta=737)
        self.assertEqual(res.delta, 737)

    def test_next_object(self):
        """Test that L{oidres.OIDRes.next} works in the common case."""

        with Counter(self, 4) as counter:
            class xMockRAMCloud(self.MockRAMCloud):
                def real_read(self):
                    i = counter.bump([0, 2])
                    if i == 0:
                        return (70, 54321)
                    elif i == 2:
                        return (900, 123450)

                def real_update(self, next_avail, want_version):
                    i = counter.bump([1, 3])
                    if i == 1:
                        self.tc.assertEqual(next_avail, 70 + 737)
                        self.tc.assertEqual(want_version, 54321)
                    elif i == 3:
                        self.tc.assertEqual(next_avail, 900 + 737)
                        self.tc.assertEqual(want_version, 123450)

            rc = xMockRAMCloud(self)
            res = oidres.OIDRes(rc, self.table, self.oid, delta=737)
            for i in range(737):
                retry_strategy = MockRetry(self)
                self.assertEqual(res.next(retry_strategy), i + 70)
                retry_strategy.done()
                self.assertEqual(counter.count, 1)
            self.assertEqual(res.next(), 900)

    def test_next_no_object(self):
        """Test that L{oidres.OIDRes.next} works when there is no object."""

        with Counter(self, 2) as counter:
            class xMockRAMCloud(self.MockRAMCloud):
                def real_read(self):
                    counter.bump(0)
                    raise ramcloud.NoObjectError()

                def real_create(self, next_avail):
                    counter.bump(1)
                    self.tc.assertEqual(next_avail, 737)

            rc = xMockRAMCloud(self)
            res = oidres.OIDRes(rc, self.table, self.oid, delta=737)
            for i in range(737):
                retry_strategy = MockRetry(self)
                self.assertEqual(res.next(retry_strategy), i)
                retry_strategy.done()

    def test_mismatched_version(self):
        """Test that L{oidres.OIDRes.next} works under contention."""

        with Counter(self, 2) as counter:
            class xMockRAMCloud(self.MockRAMCloud):
                def real_read(self):
                    counter.bump(0)
                    return (70, 54321)

                def real_update(self, next_avail, want_version):
                    counter.bump(1)
                    self.tc.assertEqual(next_avail, 70 + 737)
                    self.tc.assertEqual(want_version, 54321)
                    raise ramcloud.VersionError(54321, 54322)

            rc = xMockRAMCloud(self)
            res = oidres.OIDRes(rc, self.table, self.oid, delta=737)
            retry_strategy = MockRetry(self, expect_later=True)
            self.assertRaises(BreakException, lambda: res.next(retry_strategy))
            retry_strategy.done()

    def test_reserve_lazily(self):
        """Test that L{oidres.OIDRes.reserve_lazily} works."""

        with Counter(self, 4) as counter:

            class xMockRAMCloud(self.MockRAMCloud):
                def real_read(self):
                    i = counter.bump([0, 2])
                    if i == 0:
                        return (70, 54321)
                    elif i == 2:
                        return (900, 123450)

                def real_update(self, next_avail, want_version):
                    i = counter.bump([1, 3])
                    if i == 1:
                        self.tc.assertEqual(next_avail, 70 + 737)
                        self.tc.assertEqual(want_version, 54321)
                    elif i == 3:
                        self.tc.assertEqual(next_avail, 900 + 737)
                        self.tc.assertEqual(want_version, 123450)

            rc = xMockRAMCloud(self)
            res = oidres.OIDRes(rc, self.table, self.oid, delta=737)
            l = []
            for i in range(740):
                l.append(res.reserve_lazily())
            self.assertEqual(counter.count, -1)
            self.assertEqual(int(l[30]), 70)
            self.assertEqual(counter.count, 1)
            self.assertEqual(int(l[739]), 71)
            self.assertEqual(int(l[0]), 72)
            self.assertEqual(counter.count, 1)
            for i in range(740):
                int(l[i])
            self.assertEqual(int(l[737]), 901)
            self.assertEqual(int(l[738]), 902)

if __name__ == '__main__':
    unittest.main()
