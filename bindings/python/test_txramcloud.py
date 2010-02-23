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

"""Unit tests for C{txramcloud.py}.

@see: L{txramcloud}

"""

from __future__ import with_statement

import unittest
import cPickle as pickle
import time

from testutil import Counter, Opaque, BreakException, MockRetry
import ramcloud
import txramcloud
from txramcloud import TxRAMCloud

class TestPackUnpack(unittest.TestCase):
    def assertPackable(self, txid, timeout, data):
        """Make sure what goes into L{txramcloud.pack} comes out of
        L{txramcloud.unpack}."""
        n = txramcloud.unpack(txramcloud.pack(txid, timeout, data))
        self.assertEqual(n[0], txid)
        self.assertEqual(n[1], timeout)
        self.assertEqual(n[2], data)

    def test_unmasked(self):
        """Test pack/unpack with no transaction."""
        self.assertPackable(0, 0, '')
        self.assertPackable(0, 0, 'foo')
        self.assertPackable(0, 0, 'foo\0bar\0')

    def test_masked(self):
        """Test pack/unpack with a transaction."""
        self.assertPackable(0xabcdef1234567890, 83520.238, '')
        self.assertPackable(0xabcdef1234567890, 83520.238, 'foo')
        self.assertPackable(0xabcdef1234567890, 83520.238, 'foo\0bar\0')

    def test_seed(self):
        """Test pack/unpack with a seed object."""
        self.assertPackable(0xabcdef1234567890, 83520.238, None)

    def test_invalid_short(self):
        """Test pack/unpack with an object that's too short."""
        self.assertRaises(TxRAMCloud.InconsistencyError,
                          txramcloud.unpack, 'foo')

    def test_invalid_garbage(self):
        """Test pack/unpack with an object that's garbage."""
        self.assertRaises(TxRAMCloud.InconsistencyError,
                          txramcloud.unpack, 'f' * 32)

    def test_outdated(self):
        """Test pack/unpack with a header version that's too new."""
        blob = txramcloud.HeaderFmt.pack(txramcloud.TXRAMCLOUD_HEADER,
                                         99, 0, 0, 0)
        self.assertRaises(TxRAMCloud.OutdatedClient, txramcloud.unpack, blob)

class Opaques(object):
    table = Opaque()
    data = Opaque()
    datas = pickle.dumps(data)
    oid = Opaque()
    reject_rules = Opaque()
    version = Opaque()
    version1 = Opaque()
    version2 = Opaque()
    blob = Opaque()
    txid = Opaque()
    timeout = Opaque()
    op = Opaque()
    mt = Opaque()

MUST_EXIST = ramcloud.RejectRules(object_doesnt_exist=True)
now = time.time()

def txrc_setup(tc, rc=None, retries=None):
    """Set up a L{txramcloud.TxRAMCloud} instance for testing.

    See the unit tests for examples.

    @param tc: The containing test case
    @type  tc: C{unittest.TestCase}

    @param rc: mock L{ramcloud.RAMCloud} instance
    @type  rc: C{object}

    @param retries: L{testutil.MockRetry} instance
    @type  retries: L{testutil.MockRetry}

    @return: context manager for use with C{with} statement
    @rtype: context manager
    """

    class ContextManager(object):
        def __enter__(cm):
            if rc is None:
                txramcloud.RAMCloud = object()
            else:
                txramcloud.RAMCloud = rc

            if retries is None:
                cm.retries = MockRetry(tc)
            else:
                cm.retries = retries
            txramcloud.RetryStrategy = cm.retries
            cm.retries.__enter__()

            return TxRAMCloud(0)

        def __exit__(cm, exc_type, exc_value, traceback):
            return cm.retries.__exit__(exc_type, exc_value, traceback)

    return ContextManager()

class TestTxRAMCloud(unittest.TestCase):
    """Tests L{txramcloud.TxRAMCloud}."""

    def setUp(self):
        self.save = {}
        self.save['RAMCloud'] = txramcloud.RAMCloud
        self.save['RetryStrategy'] = txramcloud.RetryStrategy

    def tearDown(self):
        txramcloud.RAMCloud = self.save['RAMCloud']
        txramcloud.RetryStrategy = self.save['RetryStrategy']

    def test_insert(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def insert(mockrc, txrc, table_id, blob):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    exp_blob = txramcloud.pack(0, 0, Opaques.datas)
                    self.assertEqual(blob, exp_blob)
                    return Opaques.oid
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                r = txrc.insert(Opaques.table, Opaques.datas)
                self.assertEqual(r, Opaques.oid)

    """Testing strategy for _write_tombstone:
    - test_write_tombstone_noobject tests the normal case of no object existing
    at txid and checks arguments to RAMCloud.write_rr.
    - test_write_tombstone_exists tests the case of an object already existing
    at txid.
    """

    def test_write_tombstone_noobject(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.txid)
                    self.assert_(type(txramcloud.unserialize(blob)) ==
                                 txramcloud.Tombstone)
                    rr = ramcloud.RejectRules(object_exists=True)
                    self.assertEqual(reject_rules, rr)
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = Opaques.table
                self.assertEqual(txrc._write_tombstone(Opaques.txid),
                                 Opaques.version)

    def test_write_tombstone_exists(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump()
                    raise ramcloud.ObjectExistsError()
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = Opaques.table
                self.assertEqual(txrc._write_tombstone(Opaques.txid), None)

    """Testing strategy for _clean:
    - test_clean tests the case of finding a MiniTransaction without errors and
    checks arguments to read_rr and _finish_mt.
    - test_clean_tombstone tests the case of finding a Tombstone without errors and
    checks arguments to _write_tombstone and _unmask_object.
    - test_clean_noobject tests the except clause for read_rr and checks
    arguments to _unmask_object.
    - test_clean_garbage tests the case of a bad unserialize.
    - test_clean_pickled_garbage tests the case of getting something unexpected
    out of unserialize.
    """

    def test_clean(self):
        tx_table_id = 2340
        my_mt = txramcloud.MiniTransaction()
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    self.assertEqual(table_id, tx_table_id)
                    self.assertEqual(key, Opaques.txid)
                    self.assertEqual(reject_rules, MUST_EXIST)
                    return (txramcloud.serialize(my_mt), Opaques.version)
            def mock_finish_mt(mt, txid, version):
                counter.bump(1)
                self.assertEquals(mt, my_mt)
                self.assertEquals(txid, Opaques.txid)
                self.assertEquals(version, Opaques.version)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = tx_table_id
                txrc._finish_mt = mock_finish_mt
                txrc._clean(Opaques.table, Opaques.oid, Opaques.txid,
                            Opaques.timeout)

    def test_clean_tombstone(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    st = txramcloud.serialize(txramcloud.Tombstone())
                    return (st, Opaques.version)
            def mock_unmask_object(table_id, key, txid):
                counter.bump(1)
                self.assertEquals(table_id, Opaques.table)
                self.assertEquals(key, Opaques.oid)
                self.assertEquals(txid, Opaques.txid)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object = mock_unmask_object
                txrc._clean(Opaques.table, Opaques.oid, Opaques.txid,
                            Opaques.timeout)

    def test_clean_noobject(self):
        with Counter(self, 3) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    raise ramcloud.NoObjectError
            def mock_write_tombstone(txid):
                counter.bump(1)
                self.assertEquals(txid, Opaques.txid)
                return Opaques.version
            def mock_unmask_object(table_id, key, txid):
                counter.bump(2)
                self.assertEquals(table_id, Opaques.table)
                self.assertEquals(key, Opaques.oid)
                self.assertEquals(txid, Opaques.txid)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._write_tombstone = mock_write_tombstone
                txrc._unmask_object = mock_unmask_object
                txrc._clean(Opaques.table, Opaques.oid, Opaques.txid,
                            time.time() - 10)

    def test_clean_garbage(self):
        class MockRAMCloud(object):
            def read_rr(mockrc, txrc, table_id, key, reject_rules):
                return ("garbage", Opaques.version)
        with txrc_setup(self, rc=MockRAMCloud()) as txrc:
            self.assertRaises(TxRAMCloud.InconsistencyError, txrc._clean,
                              Opaques.table, Opaques.oid, Opaques.txid,
                              Opaques.timeout)

    def test_clean_pickled_garbage(self):
        class MockRAMCloud(object):
            def read_rr(mockrc, txrc, table_id, key, reject_rules):
                return (txramcloud.serialize("garbage"), Opaques.version)
        with txrc_setup(self, rc=MockRAMCloud()) as txrc:
            self.assertRaises(TxRAMCloud.InconsistencyError, txrc._clean,
                              Opaques.table, Opaques.oid, Opaques.txid,
                              Opaques.timeout)

    """Testing strategy for _read_rr:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_read_no_mask tests the case where RAMCloud.read_rr returns an object
    that's not in a transaction (and the user reject rules don't specify
    object_exists). It tests the arguments to RAMCloud.read_rr.
    - test_read_mask tests the case where RAMCloud.read_rr returns an object
    that's in a transaction (with an expired timeout) and tests the arguments to
    _clean.
    """

    def test_read_no_mask(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules, MUST_EXIST)
                    return (txramcloud.pack(0, 0, Opaques.datas),
                            Opaques.version)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                r = txrc.read_rr(Opaques.table, Opaques.oid, MUST_EXIST)
                self.assertEqual(r, (Opaques.datas, Opaques.version))

    def test_read_expired_mask(self):
        my_timeout = time.time() - 10
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    blob = txramcloud.pack(1, my_timeout, Opaques.datas)
                    return (blob, Opaques.version)
            def mock_clean(table_id, key, txid, timeout):
                counter.bump(1)
                self.assertEquals(table_id, Opaques.table)
                self.assertEquals(key, Opaques.oid)
                self.assertEquals(txid, 1)
                self.assertEquals(timeout, my_timeout)
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                txrc._clean = mock_clean
                self.assertRaises(BreakException, txrc.read_rr,
                                  Opaques.table, Opaques.oid, MUST_EXIST)

    """Testing strategy for _delete_unsafe:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_delete_unsafe_clean tests the path where the object exists and we
    delete it conditionally without errors. It tests the arguments to read_rr
    and RAMCloud.delete_rr.
    - test_delete_unsafe_noobjecterror tests the case where the object has
    already been deleted.
    - test_delete_unsafe_versionerror tests the case where the conditional
    delete fails because the version has changed.
    """

    def test_delete_unsafe_clean(self):
        user_rr = ramcloud.RejectRules()
        del_rr  = ramcloud.RejectRules(version_gt_given=True,
                                       given_version=2839)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules, del_rr)
                    return 2839
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(reject_rules, user_rr)
                return (Opaques.data, 2839)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                r = txrc._delete_unsafe(Opaques.table, Opaques.oid, user_rr)
                self.assertEqual(r, 2839)

    def test_delete_unsafe_noobjecterror(self):
        rr = ramcloud.RejectRules()
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                pass
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                raise ramcloud.NoObjectError
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                txrc._delete_unsafe(Opaques.table, Opaques.oid, rr)

    def test_delete_unsafe_versionerror(self):
        user_rr = ramcloud.RejectRules()
        del_rr  = ramcloud.RejectRules(version_gt_given=True,
                                       given_version=2839)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
                    self.assertEqual(reject_rules, del_rr)
                    raise ramcloud.VersionError(2839, 2875)
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                return (Opaques.data, 2839)
            retries = MockRetry(self,expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(BreakException, txrc._delete_unsafe,
                                  Opaques.table, Opaques.oid, user_rr)

    """Testing strategy for delete_rr:
    - test_delete_rr_safe tests the case where the operation is safe to pass on
    to RAMCloud.delete_rr. It tests the arguments to RAMCloud.delete_rr.
    - test_delete_rr_unsafe tests the case where the operation is unsafe to pass
    on to RAMCloud.delete_rr directly. It tests the arguments to _delete_unsafe.
    """

    def test_delete_rr_safe(self):
        rr = [ramcloud.RejectRules.exactly(3492),
              ramcloud.RejectRules(object_exists=True)]
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules, rr[counter.count])
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                r = txrc.delete_rr(Opaques.table, Opaques.oid, rr[0])
                self.assertEqual(r, Opaques.version)
                r = txrc.delete_rr(Opaques.table, Opaques.oid, rr[1])
                self.assertEqual(r, Opaques.version)

    def test_delete_rr_unsafe(self):
        with Counter(self, 1) as counter:
            rr = ramcloud.RejectRules()
            def mock_delete_unsafe(table_id, key, reject_rules):
                counter.bump()
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(reject_rules, rr)
                return Opaques.version
            with txrc_setup(self) as txrc:
                txrc._delete_unsafe = mock_delete_unsafe
                r = txrc.delete_rr(Opaques.table, Opaques.oid, rr)
                self.assertEqual(r, Opaques.version)

    """Testing strategy for _update_unsafe:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_update_unsafe_clean tests the path where the object exists and we
    update it conditionally without errors. It tests the arguments to read_rr
    and RAMCloud.write_rr.
    - test_update_unsafe_read_no_object tests the path where the object does not
    exist during the read_rr.
    - test_update_unsafe_write_no_object tests the path where the object does
    not exist during the write_rr (it is deleted concurrently between the
    read_rr and the write_rr).
    - test_update_unsafe_write_version_error tests the path where the object
    is updated concurrently between the read_rr and the write_rr.
    """

    def test_update_unsafe_clean(self):
        user_rr = ramcloud.RejectRules(object_doesnt_exist=True)
        write_rr = ramcloud.RejectRules.exactly(2839)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(blob, Opaques.blob)
                    self.assertEqual(reject_rules, write_rr)
                    return Opaques.version
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(reject_rules, user_rr)
                return (Opaques.data, 2839)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                r = txrc._update_unsafe(Opaques.table, Opaques.oid,
                                        Opaques.blob, user_rr)
                self.assertEqual(r, Opaques.version)

    def test_update_unsafe_read_no_object(self):
        user_rr = ramcloud.RejectRules(object_doesnt_exist=True)
        with Counter(self, 1) as counter:
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump()
                raise ramcloud.NoObjectError()
            with txrc_setup(self) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(ramcloud.NoObjectError, txrc._update_unsafe,
                                  Opaques.table, Opaques.oid, Opaques.blob,
                                  user_rr)

    def test_update_unsafe_write_no_object(self):
        user_rr = ramcloud.RejectRules(object_doesnt_exist=True)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    raise ramcloud.NoObjectError()
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                return (Opaques.data, 2839)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(ramcloud.NoObjectError, txrc._update_unsafe,
                                  Opaques.table, Opaques.oid, Opaques.blob,
                                  user_rr)

    def test_update_unsafe_write_version_error(self):
        user_rr = ramcloud.RejectRules(object_doesnt_exist=True)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    raise ramcloud.VersionError(2839, 2892)
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                return (Opaques.data, 2839)
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(BreakException, txrc._update_unsafe,
                                  Opaques.table, Opaques.oid, Opaques.blob,
                                  user_rr)

    """Testing strategy for _write_unsafe:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_write_unsafe_clean tests the path where the object exists and we
    update it conditionally without errors. It tests the arguments to read_rr
    and RAMCloud.write_rr.
    - test_write_unsafe_no_object_object_exists tests the path where the object
    doesnt exist and we write it conditionally but then find it does exist. It
    tests the reject_rules argument to RAMCloud.write_rr.
    """

    def test_write_unsafe_clean(self):
        user_rr = ramcloud.RejectRules()
        write_rr = ramcloud.RejectRules(version_gt_given=True,
                                        given_version=2839)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(blob, Opaques.blob)
                    self.assertEqual(reject_rules, write_rr)
                    return Opaques.version
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(reject_rules, user_rr)
                return (Opaques.data, 2839)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                r = txrc._write_unsafe(Opaques.table, Opaques.oid,
                                       Opaques.blob, user_rr)
                self.assertEqual(r, Opaques.version)

    def test_write_unsafe_no_object_object_exists(self):
        user_rr = ramcloud.RejectRules()
        write_rr = ramcloud.RejectRules(object_exists=True)
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(reject_rules, write_rr)
                    raise ramcloud.ObjectExistsError
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                raise ramcloud.NoObjectError
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(BreakException, txrc._write_unsafe,
                                  Opaques.table, Opaques.oid, Opaques.blob,
                                  user_rr)

    """Testing strategy for write_rr:
    - test_write_rr_safe tests the case where the operation is safe to pass on
    to RAMCloud.write_rr. It tests the arguments to RAMCloud.write_rr.
    - test_write_rr_update_unsafe tests the case where the operation is unsafe
    to pass on to RAMCloud.write_rr directly and the object must already exist.
    It tests the arguments to _update_unsafe.
    - test_write_rr_update_unsafe tests the case where the operation is unsafe
    to pass on to RAMCloud.write_rr directly and the object may not already
    exist. It tests the arguments to _write_unsafe.
    """

    def test_write_rr_safe(self):
        rr = [ramcloud.RejectRules.exactly(3492),
              ramcloud.RejectRules(object_exists=True)]
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    exp_blob = txramcloud.pack(0, 0, Opaques.datas)
                    self.assertEqual(blob, exp_blob)
                    self.assertEqual(reject_rules, rr[counter.count])
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                r = txrc.write_rr(Opaques.table, Opaques.oid,
                                  Opaques.datas, rr[0])
                self.assertEqual(r, Opaques.version)
                r = txrc.write_rr(Opaques.table, Opaques.oid,
                                  Opaques.datas, rr[1])
                self.assertEqual(r, Opaques.version)

    def test_write_rr_update_unsafe(self):
        """The reject rules do not imply the write_rr is safe."""
        rr = ramcloud.RejectRules(object_doesnt_exist=True)
        with Counter(self, 1) as counter:
            def mock_update_unsafe(table_id, key, blob, reject_rules):
                counter.bump()
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(blob, txramcloud.pack(0, 0, Opaques.datas))
                self.assertEqual(reject_rules, rr)
                return Opaques.version
            with txrc_setup(self) as txrc:
                txrc._update_unsafe = mock_update_unsafe
                r = txrc.write_rr(Opaques.table, Opaques.oid, Opaques.datas, rr)
                self.assertEqual(r, Opaques.version)

    def test_write_rr_write_unsafe(self):
        """The reject rules do not imply the write_rr is safe."""
        rr = ramcloud.RejectRules()
        with Counter(self, 1) as counter:
            def mock_write_unsafe(table_id, key, blob, reject_rules):
                counter.bump()
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(blob, txramcloud.pack(0, 0, Opaques.datas))
                self.assertEqual(reject_rules, rr)
                return Opaques.version
            with txrc_setup(self) as txrc:
                txrc._write_unsafe = mock_write_unsafe
                r = txrc.write_rr(Opaques.table, Opaques.oid, Opaques.datas, rr)
                self.assertEqual(r, Opaques.version)

class TestCoordinator(unittest.TestCase):
    """Unit tests for coordinator part of L{txramcloud.TxRAMCloud}."""

    """Testing strategy for _unmask_object:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_unmask_object_noobject tests the case where the RAMCloud.read_rr
    returns no object. It tests the arguments to RAMCloud.read_rr.
    - test_unmask_object_no_txid tests the case where the RAMCloud.read_rr
    returns an object that's not in a transaction.
    - test_unmask_object_diff_txid tests the case where the RAMCloud.read_rr
    returns an object that's in a different transaction.
    - test_unmask_object_seed tests the case where the RAMCloud.read_rr returns
    a seed object and then conditionally deletes it. It tests the arguments to
    RAMCloud.delete_rr.
    - test_unmask_object_volatile_seed tests the case where the RAMCloud.read_rr
    returns a seed object and has a version error while conditionally deleting
    it.
    - test_unmask_object_full tests the case where the RAMCloud.read_rr returns
    a full object and then conditionally writes it. It tests the arguments to
    RAMCloud.write_rr.
    - test_unmask_object_volatile_full tests the case where the RAMCloud.read_rr
    returns a full object and has a version error while conditionally writing
    it.
    """

    def test_unmask_object_noobject(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules, MUST_EXIST)
                    raise ramcloud.NoObjectError
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object(Opaques.table, Opaques.oid, Opaques.txid)

    def test_unmask_object_no_txid(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    blob = txramcloud.pack(0, 0, Opaques.datas)
                    return (blob, Opaques.version)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object(Opaques.table, Opaques.oid, 30)

    def test_unmask_object_diff_txid(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    blob = txramcloud.pack(75, 0, Opaques.datas)
                    return (blob, Opaques.version)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object(Opaques.table, Opaques.oid, 30)

    def test_unmask_object_seed(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(30, 0, data=None), 9023)
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    rr = ramcloud.RejectRules(version_gt_given=True,
                                              given_version=9023)
                    self.assertEqual(reject_rules, rr)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object(Opaques.table, Opaques.oid, 30)

    def test_unmask_object_volatile_seed(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(30, 0, data=None), 9023)
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
                    raise ramcloud.VersionError(9023, 9025)
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                self.assertRaises(BreakException, txrc._unmask_object,
                                  Opaques.table, Opaques.oid, 30)

    def test_unmask_object_full(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(30, 0, Opaques.datas), 9023)
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    rr = ramcloud.RejectRules(version_gt_given=True,
                                              given_version=9023)
                    self.assertEqual(reject_rules, rr)
                    self.assertEqual(blob, txramcloud.pack(0, 0, Opaques.datas))
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._unmask_object(Opaques.table, Opaques.oid, 30)

    def test_unmask_object_volatile_full(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(30, 0, Opaques.datas), 9023)
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    raise ramcloud.VersionError(9023, 9025)
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                self.assertRaises(BreakException, txrc._unmask_object,
                                  Opaques.table, Opaques.oid, 30)

    def test_unmask_objects(self):
        txrc = TxRAMCloud(0)
        log = []
        txrc._unmask_object = lambda t, k, tx: log.append((t, k, tx))

        txrc._unmask_objects([], Opaques.txid)
        self.assertEqual(log[0:], [])

        txrc._unmask_objects([(Opaques.table, Opaques.oid)], Opaques.txid)
        self.assertEqual(log[0:], [(Opaques.table, Opaques.oid, Opaques.txid)])

        txrc._unmask_objects([(Opaques.table, Opaques.oid), (4, 7)],
                             Opaques.txid)
        self.assertEqual(log[1:], [(Opaques.table, Opaques.oid, Opaques.txid),
                                   (4, 7, Opaques.txid)])

    """Testing strategy for _mask_object:
    Because the loop iterations do not introduce new states, it suffices to
    check only one iteration of the loop.
    - test_mask_object_clean tests the case where read_rr returns an object and
    we conditionally write it with no problem. It tests the arguments to read_rr
    and RAMCloud.write_rr.
    - test_mask_object_version tests the case where read_rr raises a version
    error.
    - test_mask_object_no_object_raise tests the case where the user requests
    rejects on object_doesnt_exist and read_rr raises a no object error.
    - test_mask_object_no_object_make_seed tests the case where the user does
    not request rejects on object_doesnt_exist and read_rr raises a no object
    error. The conditional write to create the seed works without errors. It
    tests the arguments to RAMCloud.write_rr.
    - test_mask_object_unmasked_write_error tests the case where read_rr returns
    an object but writing it returns a version error.
    """

    def test_mask_object_clean(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules,
                                     ramcloud.RejectRules.exactly(9023))
                    exp_blob = txramcloud.pack(30, now + 10, Opaques.datas)
                    self.assertEqual(blob, exp_blob)
                    return Opaques.version
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                self.assertEqual(table_id, Opaques.table)
                self.assertEqual(key, Opaques.oid)
                self.assertEqual(reject_rules, MUST_EXIST)
                return (Opaques.datas, 9023)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                r = txrc._mask_object(Opaques.table, Opaques.oid, 30, now + 10,
                                      ramcloud.RejectRules())
                self.assertEquals(r, Opaques.version)

    def test_mask_object_object_version(self):
        txrc = TxRAMCloud(0)
        def mock_read_rr(table_id, key, reject_rules):
            self.assert_(reject_rules.object_doesnt_exist)
            self.assert_(reject_rules.object_exists)
            self.assert_(reject_rules.version_eq_given)
            self.assert_(reject_rules.version_gt_given)
            self.assertEquals(reject_rules.given_version, 999)
            raise ramcloud.VersionError(999, 1023)
        txrc.read_rr = mock_read_rr
        rr = ramcloud.RejectRules(object_doesnt_exist=False,
                                  object_exists=True, version_eq_given=True,
                                  version_gt_given=True, given_version=999)
        try:
            txrc._mask_object(Opaques.table, Opaques.oid, Opaques.txid,
                              Opaques.timeout, rr)
        except ramcloud.VersionError, e:
            self.assertEquals(e.table, Opaques.table)
            self.assertEquals(e.oid, Opaques.oid)
        else:
            self.fail()

    def test_mask_object_no_object_raise(self):
        def mock_read_rr(table_id, key, reject_rules):
            raise ramcloud.NoObjectError()
        with txrc_setup(self) as txrc:
            txrc.read_rr = mock_read_rr
            rr = ramcloud.RejectRules(object_doesnt_exist=True)
            try:
                txrc._mask_object(Opaques.table, Opaques.oid, Opaques.txid,
                                  Opaques.timeout, rr)
            except ramcloud.NoObjectError, e:
                self.assertEquals(e.table, Opaques.table)
                self.assertEquals(e.oid, Opaques.oid)
            else:
                self.fail()

    def test_mask_object_no_object_make_seed(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    rr = ramcloud.RejectRules(object_exists=True)
                    self.assertEqual(reject_rules, rr)
                    exp_blob = txramcloud.pack(30, now + 10, None)
                    self.assertEqual(blob, exp_blob)
                    return Opaques.version
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                raise ramcloud.NoObjectError()
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.read_rr = mock_read_rr
                r = txrc._mask_object(Opaques.table, Opaques.oid, 30, now + 10,
                                      ramcloud.RejectRules())
                self.assertEquals(r, Opaques.version)

    def test_mask_object_unmasked_write_error(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    rr = ramcloud.RejectRules.exactly(999)
                    self.assertEqual(reject_rules, rr)
                    exp_blob = txramcloud.pack(30, now + 10, Opaques.datas)
                    self.assertEqual(blob, exp_blob)
                    raise ramcloud.VersionError(999, 1201)
            def mock_read_rr(table_id, key, reject_rules):
                counter.bump(0)
                return (Opaques.datas, 999)
            retries = MockRetry(self, expect_later=True)
            with txrc_setup(self, rc=MockRAMCloud(), retries=retries) as txrc:
                txrc.read_rr = mock_read_rr
                self.assertRaises(BreakException, txrc._mask_object,
                                  Opaques.table, Opaques.oid, 30, now + 10,
                                  ramcloud.RejectRules())

    """Testing strategy for _mask_objects:
    - test_mask_objects_normal tests the case where a few objects are masked
    without problems. It tests the arguments to _mask_object.
    - test_mask_objects_err tests the case where there is an error and we have
    to unmask some objects. It tests the arguments to _unmask_objects.
    """

    def setup_test_mask_objects(self):
        mt = txramcloud.MiniTransaction()
        txid = Opaques.txid
        timeout = Opaques.timeout
        expected = zip(range(100, 130), range(200, 230), range(300, 330))
        for (table, oid, version) in expected:
            rr = ramcloud.RejectRules.exactly(version)
            mt[(table, oid)] = txramcloud.MTOperation(rr)
        txrc = TxRAMCloud(0)
        return mt, txid, timeout, expected, txrc

    def test_mask_objects_normal(self):
        mt, txid, timeout, expected, txrc = self.setup_test_mask_objects()
        with Counter(self, 30) as counter:
            def mock_mask_object(table_id, key, txid, timeout, reject_rules):
                counter.bump()
                self.assertEquals(txid, Opaques.txid)
                self.assertEquals(timeout, Opaques.timeout)
                self.assertEquals(expected[counter.count],
                                  (table_id, key, reject_rules.given_version))
                return expected[counter.count][2] + 1
            txrc._mask_object = mock_mask_object
            r = txrc._mask_objects([(t,o) for (t,o,v) in expected],
                                   mt, txid, timeout)
            for (t, o, v) in expected:
                self.assertEquals(r[(t, o)], v + 1)

    def test_mask_objects_err(self):
        mt, txid, timeout, expected, txrc = self.setup_test_mask_objects()
        with Counter(self, 12) as counter:
            def mock_mask_object(table_id, key, txid, timeout, reject_rules):
                if counter.bump(range(11)) == 10:
                    raise BreakException
            def mock_unmask_objects(objects, txid):
                counter.bump(11)
                self.assertEquals(set(objects),
                                  set([(t,o) for (t,o,v) in expected[:10]]))
                self.assertEquals(txid, Opaques.txid)
            txrc._mask_object = mock_mask_object
            txrc._unmask_objects = mock_unmask_objects
            self.assertRaises(BreakException, txrc._mask_objects,
                              [(t,o) for (t,o,v) in expected],
                              mt, txid, timeout)

    """Testing strategy for _write_mt:
    - test_write_mt tests the case where the object does not exist. It tests the
    arguments to RAMCloud.write_rr.
    - test_write_mt_fail tests the case where the object already exists.
    """

    def test_write_mt(self):
        mt = txramcloud.MiniTransaction()
        tx_table_id = 9012
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, tx_table_id)
                    self.assertEqual(key, Opaques.txid)
                    self.assertEqual(blob, txramcloud.serialize(mt))
                    rr = ramcloud.RejectRules(object_exists=True)
                    self.assertEqual(reject_rules, rr)
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = tx_table_id
                txrc._write_mt(mt, Opaques.txid)

    def test_write_mt_fail(self):
        mt = txramcloud.MiniTransaction()
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump()
                    raise ramcloud.ObjectExistsError
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                self.assertRaises(TxRAMCloud.TransactionExpired,
                                  txrc._write_mt, mt, 5000)

    """Testing strategy for _delete_mt:
    - test_delete_mt tests the case where the object exists. It tests the
    arguments to RAMCloud.delete_rr.
    - test_delete_mt tests the case where the object has been modified.
    """

    def test_delete_mt(self):
        tx_table_id = 9012
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, tx_table_id)
                    self.assertEqual(key, Opaques.txid)
                    rr = ramcloud.RejectRules(version_gt_given=True,
                                              given_version=823)
                    self.assertEqual(reject_rules, rr)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = tx_table_id
                txrc._delete_mt(Opaques.txid, 823)

    def test_delete_mt_versionerr(self):
        tx_table_id = 9012
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    raise ramcloud.VersionError(850, 823)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = tx_table_id
                self.assertRaises(TxRAMCloud.InconsistencyError,
                                  txrc._delete_mt, Opaques.txid, 823)

    def test_delete_tombstone(self):
        tx_table_id = 9012
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump()
                    self.assertEqual(table_id, tx_table_id)
                    self.assertEqual(key, Opaques.txid)
                    rr = ramcloud.RejectRules()
                    self.assertEqual(reject_rules, rr)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc.tx_table = tx_table_id
                txrc._delete_tombstone(Opaques.txid)

    """Testing strategy for _apply_op:
    - test_apply_op_noop_full tests the case where the full object exists and is
    masked by our transaction, the op is a noop, then RAMCloud.write_rr suceeds.
    It tests the arguments to RAMCloud.read_rr and RAMCloud.write_rr.
    - test_apply_op_write tests the case where the full object exists and is
    masked by our transaction, the op is a write, then RAMCloud.write_rr
    suceeds.
    - test_apply_op_noop_seed tests the case where the seed object exists and is
    masked by our transaction, the op is a noop, then RAMCloud.delete_rr suceeds.
    It tests the arguments to RAMCloud.delete_rr.
    - test_apply_op_delete tests the case where the full object exists and is
    masked by our transaction, the op is a delete, then RAMCloud.delete_rr
    suceeds.
    - test_apply_op_noobject tests the case where the object does not exist.
    - test_apply_op_noobject tests the case where the object exists but is
    not taking part in our transaction.
    """

    def test_apply_op_noop_full(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules, MUST_EXIST)
                    return (txramcloud.pack(123, now - 10, Opaques.datas), 764)
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(blob, txramcloud.pack(0, 0, Opaques.datas))
                    self.assertEqual(reject_rules,
                                     ramcloud.RejectRules.exactly(764))
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                op = txramcloud.MTOperation(Opaques.reject_rules)
                txrc._apply_op(Opaques.table, Opaques.oid, 123, op)

    def test_apply_op_write(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(123, now - 10, Opaques.datas), 764)
                def write_rr(mockrc, txrc, table_id, key, blob, reject_rules):
                    counter.bump(1)
                    self.assertEqual(blob, txramcloud.pack(0, 0, "newdata"))
                    return Opaques.version
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                op = txramcloud.MTWrite("newdata", Opaques.reject_rules)
                txrc._apply_op(Opaques.table, Opaques.oid, 123, op)

    def test_apply_op_noop_seed(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(123, now - 10, None), 764)
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
                    self.assertEqual(table_id, Opaques.table)
                    self.assertEqual(key, Opaques.oid)
                    self.assertEqual(reject_rules,
                                     ramcloud.RejectRules.exactly(764))
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                op = txramcloud.MTOperation(Opaques.reject_rules)
                txrc._apply_op(Opaques.table, Opaques.oid, 123, op)

    def test_apply_op_delete(self):
        with Counter(self, 2) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(123, now - 10, Opaques.datas), 764)
                def delete_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(1)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                op = txramcloud.MTDelete(Opaques.reject_rules)
                txrc._apply_op(Opaques.table, Opaques.oid, 123, op)

    def test_apply_op_noobject(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    raise ramcloud.NoObjectError
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._apply_op(Opaques.table, Opaques.oid, 123, Opaques.op)

    def test_apply_op_badtxid(self):
        with Counter(self, 1) as counter:
            class MockRAMCloud(object):
                def read_rr(mockrc, txrc, table_id, key, reject_rules):
                    counter.bump(0)
                    return (txramcloud.pack(123, now - 10, Opaques.datas), 764)
            with txrc_setup(self, rc=MockRAMCloud()) as txrc:
                txrc._apply_op(Opaques.table, Opaques.oid, -1, Opaques.op)

    def test_apply_mt(self):
        observed = set()
        expected = set()
        mt = txramcloud.MiniTransaction()
        for i in range(5):
            op = Opaque()
            mt[(500 + i, 700 + i)] = op
            expected.add((500 + i, 700 + i, op))
        with Counter(self, 5) as counter:
            def mock_apply_op(table_id, key, txid, op):
                counter.bump()
                observed.add((table_id, key, op))
                self.assertEquals(txid, Opaques.txid)
            with txrc_setup(self) as txrc:
                txrc._apply_op = mock_apply_op
                txrc._apply_mt(mt, Opaques.txid)
                self.assertEquals(observed, expected)

    def test_finish_mt(self):
        with Counter(self, 2) as counter:
            def mock_apply_mt(mt, txid):
                counter.bump(0)
                self.assertEquals(mt, Opaques.mt)
                self.assertEquals(txid, Opaques.txid)
            def mock_delete_mt(txid, version):
                counter.bump(1)
                self.assertEquals(txid, Opaques.txid)
                self.assertEquals(version, Opaques.version)
            with txrc_setup(self) as txrc:
                txrc._apply_mt = mock_apply_mt
                txrc._delete_mt = mock_delete_mt
                txrc._finish_mt(Opaques.mt, Opaques.txid, Opaques.version)

    """Testing strategy for mt_commit:
    - test_mt_commit_clean tests the case where the objects are masked
    successfully and the transaction is fully committed successfully. It tests
    the arguments to _mask_objects, _write_mt, and _finish_mt.
    - test_mt_commit_mask_fail tests the case where some object is not able to
    be masked.
    - test_mt_commit_write_mt_fail tests the case where the minitransaction
    intent is blocked by a tombstone. It tests the arguments to _unmask_objects
    and _delete_tombstone.
    """

    def setup_test_mt_commit(self):
        mt = txramcloud.MiniTransaction()
        mt[(38, 2)] = txramcloud.MTOperation(Opaque())
        mt[(38, 3)] = txramcloud.MTWrite(Opaque(), Opaque())
        mt[(73, 4)] = txramcloud.MTDelete(Opaque())
        return mt

    def test_mt_commit_clean(self):
        mt = self.setup_test_mt_commit()
        with Counter(self, 3) as counter:
            def mock_mask_objects(objects, _mt, txid, timeout):
                counter.bump(0)
                self.assertEquals(objects, [(38, 2), (38, 3), (73, 4)])
                self.assertEquals(_mt, mt)
                self.assertEquals(txid, 48484)
                self.assert_(timeout > time.time() + 5)
                self.assert_(timeout < time.time() + 60)
                return {(38, 2): 11, (38, 3): 21, (73, 4): 31}
            def mock_write_mt(_mt, txid):
                counter.bump(1)
                self.assertEquals(_mt, mt)
                self.assertEquals(txid, 48484)
                return Opaques.version
            def mock_finish_mt(_mt, txid, version):
                counter.bump(2)
                self.assertEquals(_mt, mt)
                self.assertEquals(txid, 48484)
                self.assertEquals(version, Opaques.version)
            with txrc_setup(self) as txrc:
                txrc.txid_res = iter([48484])
                txrc._mask_objects = mock_mask_objects
                txrc._write_mt = mock_write_mt
                txrc._finish_mt = mock_finish_mt
                self.assertEquals(txrc.mt_commit(mt),
                                  {(38, 2): 12, (38, 3): 22, (73, 4): None})

    def test_mt_commit_mask_fail(self):
        mt = self.setup_test_mt_commit()
        em = ramcloud.NoObjectError()
        em.table = 73
        em.oid = 4
        with Counter(self, 1) as counter:
            def mock_mask_objects(objects, _mt, txid, timeout):
                counter.bump(0)
                raise em
            with txrc_setup(self) as txrc:
                txrc.txid_res = iter([48484])
                txrc._mask_objects = mock_mask_objects
                try:
                    txrc.mt_commit(mt)
                except txrc.TransactionRejected, e:
                    self.assertEquals(e.reasons, {(73, 4): em})
                else:
                    self.fail()

    def test_mt_commit_write_mt_fail(self):
        mt = self.setup_test_mt_commit()
        with Counter(self, 4) as counter:
            def mock_mask_objects(objects, _mt, txid, timeout):
                counter.bump(0)
                return {(38, 2): 11, (38, 3): 21, (73, 4): 31}
            def mock_write_mt(_mt, txid):
                counter.bump(1)
                raise BreakException
            def mock_unmask_objects(objects, txid):
                counter.bump(2)
                self.assertEquals(set(objects), set(mt.keys()))
                self.assertEquals(txid, 48484)
            def mock_delete_tombstone(txid):
                counter.bump(3)
                self.assertEquals(txid, 48484)
            with txrc_setup(self) as txrc:
                txrc.txid_res = iter([48484])
                txrc._mask_objects = mock_mask_objects
                txrc._write_mt = mock_write_mt
                txrc._unmask_objects = mock_unmask_objects
                txrc._delete_tombstone = mock_delete_tombstone
                self.assertRaises(BreakException, txrc.mt_commit, mt)

class TestMiniTransaction(unittest.TestCase):
    def assertSerializable(self, mt):
        mt_out = txramcloud.unserialize(txramcloud.serialize(mt))
        self.assertEquals(set(mt_out.keys()), set(mt.keys()))
        for key in mt_out:
            self.assertEquals(type(mt_out[key]), type(mt[key]))
            # TODO: we'd like to test operation equality, not type equality

    def test_mt_serializable(self):
        mt = txramcloud.MiniTransaction()
        mt[(38, 2)] = txramcloud.MTOperation(ramcloud.RejectRules())
        mt[(38, 3)] = txramcloud.MTWrite(Opaques.datas, ramcloud.RejectRules())
        mt[(73, 4)] = txramcloud.MTDelete(ramcloud.RejectRules())
        self.assertSerializable(mt)

if __name__ == '__main__':
    unittest.main()
