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

"""Implements client-side transactions for RAMCloud.

This module buys you fully ACID transactions, but you'll pay for it:

In non-transactional operations:
 - Blind (unconditional) operations are now about twice as slow as operations
   that depend on a specific version number.

In transactional operations:
 - Client clocks must be roughly synchronized for good performance, though clock
   skew of a few seconds is tolerated.
 - Performance is on the order of six times as slow for small transactions and
   twice as slow for large transactions, as compared to non-transactional
   operations.
 - Having an object in the read-set of a transaction bumps its version number.
 - No support for server-assigned object IDs.

Internals
=========

Glossary
--------
 - A I{seed} object is one that is taking part in a transaction but does not
 otherwise exist.
 - A I{masked} object has a header that points to the transaction ID that
 it is a part of. Masks are mutually exclusive: an object is masked by 0 or 1
 transactions at any given time.
 - A I{tombstone} object sits in place of a transaction intent object to block
 that transaction intent from being written.
 - The I{coordinator} is the client that is attempting to commit the
 transaction.

Algorithm Overview
------------------

Assuming initially object ID 1 contains A, 2 contains B, and 3 contains C.

If the transaction is successful, this is the process:

 1. Coordinator reserves object ID for T and uses this as the transaction ID.
 2. Coordinator adds masks to objects 1, 2, 3 which all point to T (T does not
 yet exist).
 3. Coordinator creates T with the transaction intent (write A' at 1 over
 version V1, write B' at 2 over version V2, write C' at 3 over version V3).
 4. Coordinator writes back A' into 1, B' into 2, C' into 3, unmasking the
 objects.
 5. Coordinator deletes T.

If some other client wants to abort T before the coordinator reaches step 3, the
other client may create a tombstone at T's object ID. This blocks the create in
step 3 and the coordinator will be forced to abort. However, if the coordinator
has already reached step 3, the other client's tombstone will be blocked by T
and the transaction is no longer abortable.

If some other client finds an object masked by a committed transaction object,
it can do the write-back (step 4) on behalf of the coordinator, even racing the
coordinator.

If some other client finds an object masked by a tombstone, it can remove the mask.

If some other client finds an object masked by a missing transaction object, it
can either wait for a transaction object to appear, or it can create a tombstone
and then remove the mask.

Consequences
------------
 - 1:A, 2:B, 3:C appear an extra time in the log (when they are masked).
 - A', B', C' appear an extra time in the log (in T).
 - It'd be hard to do this safely if we need to consider access control. (We
   don't with workspaces.)
"""

import struct
import time
import cPickle as pickle

import retries
import ramcloud
from oidres import OIDRes

RAMCloud = ramcloud.RAMCloud
RetryStrategy = retries.FuzzyExponentialBackoff

TXRAMCLOUD_VERSION = 1
TXRAMCLOUD_HEADER = "txramcloud"

TXID_RES_OID = 2**64 - 1

def serialize(data):
    """Pickle some Python object."""
    return pickle.dumps(data, protocol=2)

def unserialize(serialized):
    """Unpickle some serialized Python object."""
    return pickle.loads(serialized)

HeaderFmt = struct.Struct("14sBBQd")

def unpack(blob):
    """Split a binary blob into its txid, timeout, and data.

    L{pack} is the inverse of L{unpack}.

    @param blob: binary blob as produced by L{pack}
    @type blob: C{str}

    @return: see L{pack} parameters
    @rtype: (txid, timeout, data)
    """

    if len(blob) < 32:
        raise TxRAMCloud.InconsistencyError("Object too small")

    header, version, seed, txid, timeout = HeaderFmt.unpack(blob[:32])
    if header.rstrip('\0') != TXRAMCLOUD_HEADER:
        raise TxRAMCloud.InconsistencyError("Object header not set to " +
                                            "TXRAMCLOUD_HEADER")
    if version != TXRAMCLOUD_VERSION:
        raise TxRAMCloud.OutdatedClient("Encountered object with version %d" %
                                        version)
    if seed:
        if txid == 0:
            raise TxRAMCloud.InconsistencyError("Seed without transaction")
        data = None
    else:
        data = blob[32:]
    return (txid, timeout, data)

def pack(txid, timeout, data):
    """Pack a txid, timeout, and data into a binary blob.

    L{unpack} is the inverse of L{pack}.

    @param txid: transaction id, or 0 for no transaction
    @type  txid: C{int}

    @param timeout: when the transaction expires (in seconds after the UNIX
    epoch), or 0 for no transaction
    @type  timeout: C{float}

    @param data: the underlying object's contents, or L{None} if there is no
    underlying object (then txid must be set)
    @type  data: C{str} or C{None}

    @return: binary blob
    @rtype: C{str}
    """

    if data is None:
        assert txid != 0
        return HeaderFmt.pack(TXRAMCLOUD_HEADER, TXRAMCLOUD_VERSION,
                              True, txid, timeout)
    else:
        return HeaderFmt.pack(TXRAMCLOUD_HEADER, TXRAMCLOUD_VERSION,
                              False, txid, timeout) + data

class TxRAMCloud(RAMCloud):
    """Implements client-side transactions for RAMCloud.

    This module assumes all version numbers passed in refer to objects that are
    clean. Effectively, that means the user's application code shouldn't
    fabricate version numbers.

    Internal Details
    ================
    - L{read_rr} will only return once the object read is not masked.
      Thus, all version numbers returned from L{read_rr} are "safe".
    - L{write_rr} will never overwrite an object that's in a transaction.
    - L{delete_rr} will never delete an object that's in a transaction.
    - L{ramcloud.RAMCloud} will never handle raw user data; it will always be
      packed with L{pack}.
    """

    class InconsistencyError(Exception):
        """Raised when a malformed TxRAMCloud object is discovered."""
        pass

    class OutdatedClient(Exception):
        """Raised if this client needs to be updated."""
        pass

    class TransactionAborted(Exception):
        """Raised if the minitransaction was aborted.

        This would be raised if the minitransaction was aborted either due to
        reject rules or by another client.

        This exception should be considered abstract and should never be raised.
        Use L{TransactionRejected} and L{TransactionExpired}, its subclasses,
        instead.
        """

        pass

    class TransactionRejected(TransactionAborted):
        """Raised if the minitransaction was aborted due to reject rules.

        @ivar reasons: A mapping from (table_id, oid) to L{NoObjectError},
                       L{ObjectExistsError}, or L{VersionError}. Some
                       non-empty subset of the objects of the transaction
                       whose operation's reject rules have not been met
                       will be present.
        @type reasons: C{dict}
        """

        def __init__(self):
            TxRAMCloud.TransactionAborted.__init__(self)
            self.reasons = {}

    class TransactionExpired(TransactionAborted):
        """Raised if the minitransaction was aborted by another client."""
        pass

    def __init__(self, tx_table):
        """
        @param tx_table: a table reserved for this module's use
        @type  tx_table: C{int}
        """

        if RAMCloud == ramcloud.RAMCloud:
            RAMCloud.__init__(self)
        else:
            # unit tests are messing around in here!
            pass
        self.txid_res = OIDRes(rc=self, table=tx_table, oid=TXID_RES_OID)
        self.tx_table = tx_table

    def __del__(self):
        if RAMCloud == ramcloud.RAMCloud:
            RAMCloud.__init__(self)
        else:
            # unit tests are messing around in here!
            pass

    def insert(self, table_id, data):
        blob = pack(0, 0, data)
        return RAMCloud.insert(self, table_id, blob)

    def _write_tombstone(self, txid):
        """Force a transaction to abort.

        Writes a tombstone at txid if no object exists there.

        @param txid: The ID of the transaction to abort.
        @type  txid: C{int}

        @return: The version number of the tombstone written, or C{None} if an
        object already exists at txid.
        @rtype: C{int} or C{None}
        """

        rr = ramcloud.RejectRules(object_exists=True)
        blob = serialize(Tombstone())
        try:
            return RAMCloud.write_rr(self, self.tx_table, txid, blob, rr)
        except ramcloud.ObjectExistsError:
            return None

    def _clean(self, table_id, key, txid, timeout):
        """Clean a mask off of an object.

        This method won't necessarily do anything productive every time it's
        called, so it should be called inside a retry loop.

        @param table_id: The table containing the masked object.
        @type  table_id: C{int}

        @param key: The object ID of the masked object.
        @type  key: C{int}

        @param txid: The transaction which is masking the object.
        @type  txid: C{int}

        @param timeout: The time at which the mask expires.
        @type  timeout: C{int}
        """

        rr = ramcloud.RejectRules(object_doesnt_exist=True)
        try:
            mtdata, mtversion = RAMCloud.read_rr(self, self.tx_table, txid, rr)
        except ramcloud.NoObjectError:
            # txid doesn't exist, so wait or write tombstone
            if time.time() > timeout:
                # try to write a tombstone
                if self._write_tombstone(txid) is not None:
                    self._unmask_object(table_id, key, txid)
            # caller will retry if the object still needs cleaning
        else:
            try:
                mt = unserialize(mtdata)
            except pickle.BadPickleGet:
                raise self.InconsistencyError("Not a pickled object")
            if type(mt) == Tombstone:
                self._unmask_object(table_id, key, txid)
            elif type(mt) == MiniTransaction:
                self._finish_mt(mt, txid, mtversion)
            else:
                raise self.InconsistencyError("Not a MiniTransaction or " +
                                              "Tombstone")

    def _read_rr(self, table_id, key, user_reject_rules):
        # We can't reject for object_exists in RAMCloud.read_rr because of seed
        # objects, so we handle it later.
        reject_rules = ramcloud.RejectRules(object_doesnt_exist=True,
                            version_eq_given=user_reject_rules.version_eq_given,
                            version_gt_given=user_reject_rules.version_gt_given,
                            given_version=user_reject_rules.given_version)
        start = time.time()
        for retry in RetryStrategy():
            blob, version = RAMCloud.read_rr(self, table_id, key, reject_rules)
            txid, timeout, data = unpack(blob)
            if txid:
                # there's plenty of room for optimizing round trips here
                timeout = min(start + 1, timeout)
                self._clean(table_id, key, txid, timeout)
                retry.later()
            else:
                # not masked
                if user_reject_rules.object_exists:
                    raise ramcloud.ObjectExistsError()
                else:
                    return data, version

    def read_rr(self, table_id, key, reject_rules):
        # Yep, this appears to be redundant. The names of the arguments are
        # part of the API inherited from ramcloud.RAMCloud, so we relay to a
        # private method that can name its arguments as it pleases.
        return self._read_rr(table_id, key, reject_rules)

    def _delete_unsafe(self, table_id, key, user_reject_rules):
        # - Throws L{ramcloud.NoObjectError} if user_reject_rules specifies
        # object_doesnt_exist and there is no object to be deleted.
        # - Doesn't throw L{ramcloud.ObjectExists}.
        # - Throws L{ramcloud.VersionError} if user_reject_rules specifies
        # version_eq_given and the version read matches the version given
        # or is older than the version given.

        assert not user_reject_rules.object_exists
        assert not user_reject_rules.version_gt_given

        for retry in RetryStrategy():
            try:
                # self.read_rr() makes sure there is object is not masked
                version = self.read_rr(table_id, key, user_reject_rules)[1]
            except (ramcloud.VersionError):
                raise
            except ramcloud.NoObjectError:
                # If there was no object, we're done.
                if user_reject_rules.object_doesnt_exist:
                    raise
                else:
                    return

            # We can stop worrying about the user's given version now:
            # If user_reject_rules.version_eq_given, there is a given version
            # that we must reject. But if we've made it this far, the version
            # we read is greater than user_reject_rules.given_version.

            # However, we still have to worry about NoObjectError iff
            # user_reject_rules.doesnt_exist.

            # There was an object when we called read. We're definitely good if
            # we delete the exact version we read. If there is no object to
            # delete now, we want our delete rejected iff user_reject_rules has
            # object_doesnt_exist.
            reject_rules = ramcloud.RejectRules(
                    object_doesnt_exist=user_reject_rules.object_doesnt_exist,
                    version_gt_given=True, given_version=version)
            try:
                return RAMCloud.delete_rr(self, table_id, key, reject_rules)
            except ramcloud.NoObjectError:
                if user_reject_rules.object_doesnt_exist:
                    raise
                else:
                    return
            except ramcloud.VersionError:
                retry.later()

    def delete_rr(self, table_id, key, reject_rules):
        if reject_rules.object_exists or reject_rules.version_gt_given:
            # these cases are safe
            return RAMCloud.delete_rr(self, table_id, key, reject_rules)
        else:
            return self._delete_unsafe(table_id, key, reject_rules)

    def _update_unsafe(self, table_id, key, blob, user_reject_rules):
        # - Throws L{ramcloud.NoObjectError} if there is no object to be
        # updated.
        # - Doesn't throw L{ramcloud.ObjectExists}.
        # - Throws L{ramcloud.VersionError} if user_reject_rules specifies
        # version_eq_given and the version read matches the version given
        # or is older than the version given.

        assert user_reject_rules.object_doesnt_exist
        assert not user_reject_rules.object_exists
        assert not user_reject_rules.version_gt_given

        for retry in RetryStrategy():
            # self.read_rr() makes sure the object is not masked
            try:
                version = self.read_rr(table_id, key, user_reject_rules)[1]
            except (ramcloud.NoObjectError, ramcloud.VersionError):
                raise

            # We can stop worrying about the user's given version now
            # but still have to worry about NoObjectError.

            reject_rules = ramcloud.RejectRules.exactly(version)
            try:
                return RAMCloud.write_rr(self, table_id, key, blob,
                                         reject_rules)
            except ramcloud.NoObjectError:
                raise
            except ramcloud.VersionError:
                retry.later()

    def _write_unsafe(self, table_id, key, blob, user_reject_rules):
        assert not user_reject_rules.object_doesnt_exist
        assert not user_reject_rules.object_exists
        assert not user_reject_rules.version_gt_given

        for retry in RetryStrategy():
            try:
                # self.read_rr() makes sure the object is not masked
                version = self.read_rr(table_id, key, user_reject_rules)[1]
            except ramcloud.NoObjectError:
                # If there was no object, we demand that there be no object.
                reject_rules = ramcloud.RejectRules(object_exists=True)
            else:
                # If there was an object, we demand that there be either no
                # object or this particular version.
                reject_rules = ramcloud.RejectRules(version_gt_given=True,
                                                    given_version=version)
            try:
                return RAMCloud.write_rr(self, table_id, key, blob,
                                         reject_rules)
            except (ramcloud.ObjectExistsError, ramcloud.VersionError):
                retry.later()

    def write_rr(self, table_id, key, data, reject_rules):
        blob = pack(0, 0, data)
        if reject_rules.object_exists or reject_rules.version_gt_given:
            # these cases are safe
            return RAMCloud.write_rr(self, table_id, key, blob, reject_rules)
        else:
            if reject_rules.object_doesnt_exist:
                return self._update_unsafe(table_id, key, blob, reject_rules)
            else:
                return self._write_unsafe(table_id, key, blob, reject_rules)

    # begin coordinator:

    def _unmask_object(self, table_id, key, txid):
        """Ensure an object is not masked by a particular transaction.

        This operation is idempotent.

        @param table_id: The table containing the possibly masked object.
        @type  table_id: C{int}

        @param key: The object ID of the possibly masked object.
        @type  key: C{int}

        @param txid: The transaction which may be masking the object.
        @type  txid: C{int}

        """
        for retry in RetryStrategy():

            rr_read = ramcloud.RejectRules(object_doesnt_exist=True)
            try:
                blob, version = RAMCloud.read_rr(self, table_id, key, rr_read)
            except ramcloud.NoObjectError:
                # the object doesn't exist, so it's most certainly not masked
                return

            otxid, otimeout, data = unpack(blob)
            if otxid == 0 or otxid != txid:
                # the object is already not masked by this transaction
                return

            rr_change = ramcloud.RejectRules(version_gt_given=True,
                                             given_version=version)
            if data is None:
                # seed object
                try:
                    RAMCloud.delete_rr(self, table_id, key, rr_change)
                except ramcloud.VersionError:
                    retry.later()
            else:
                # full object
                blob = pack(0, 0, data)
                try:
                    RAMCloud.write_rr(self, table_id, key, blob, rr_change)
                except ramcloud.VersionError:
                    retry.later()

    def _unmask_objects(self, objects, txid):
        """Ensure several objects are not masked by a particular transaction.

        This operation is idempotent.

        @param objects: A list of (table_id, key) tuples of possibly masked
                        objects.
        @type  objects: C{list}

        @param txid: The transaction which may be masking the object.
        @type  txid: C{int}

        """
        for (table_id, key) in objects:
            self._unmask_object(table_id, key, txid)

    def _mask_object(self, table_id, key, txid, timeout, user_reject_rules):
        """Mask an object by a particular transaction.

        If the object is already masked, this method will wait for it to become
        unmasked.

        @warning: Unlike L{_unmask_object}, this operation is B{not} idempotent.

        @param table_id: The table containing the object to mask.
        @type  table_id: C{int}

        @param key: The object ID to mask.
        @type  key: C{int}

        @param txid: The transaction which will mask the object.
        @type  txid: C{int}

        @param timeout: The time at which to expire the mask.
        @type  timeout: C{int}

        @param user_reject_rules: The reasons for which to abort masking the
                                  object.
        @type  user_reject_rules: L{ramcloud.RejectRules}

        @raise Exception: If the object could not be masked because of
                          C{user_reject_rules}. Specifically, this is one of:
                          L{ramcloud.NoObjectError},
                          L{ramcloud.ObjectExistsError}, or
                          L{ramcloud.VersionError}.
                          Additionally, the exception will have the attributes
                          C{table} and C{oid} set to parameter C{table_id} and
                          C{key}.

        @return: The new version of the newly masked object.
        @rtype: C{int}
        """

        for retry in RetryStrategy():

            # self.read_rr() requires object_doesnt_exist to be set, but we can
            # still use the user's other reject rules.
            rr_read = ramcloud.RejectRules(
                            object_doesnt_exist=True,
                            object_exists=user_reject_rules.object_exists,
                            version_eq_given=user_reject_rules.version_eq_given,
                            version_gt_given=user_reject_rules.version_gt_given,
                            given_version=user_reject_rules.given_version)
            try:
                data, version = self.read_rr(table_id, key, rr_read)
            except (ramcloud.ObjectExistsError, ramcloud.VersionError), e:
                # The user asked for a reject
                e.table = table_id
                e.oid = key
                raise
            except ramcloud.NoObjectError, e:
                if user_reject_rules.object_doesnt_exist:
                    # The user asked for a reject
                    e.table = table_id
                    e.oid = key
                    raise
                else:
                    # The user didn't ask for a reject, but the object doesn't
                    # exist. We need to create a seed and must fail if an
                    # object is already present.
                    rr_write = ramcloud.RejectRules(object_exists=True)
                    blob = pack(txid, timeout, data=None)
            else:
                # There was an object when we called read. It has no mask, and
                # it isn't a seed. We need to write over the exact version we
                # read.
                rr_write = ramcloud.RejectRules.exactly(version)
                blob = pack(txid, timeout, data)

            # Now, blob and rr_write are set and we can attempt a write.
            try:
                return RAMCloud.write_rr(self, table_id, key, blob, rr_write)
            except (ramcloud.ObjectExistsError, ramcloud.NoObjectError,
                    ramcloud.VersionError):
                retry.later()

    def _mask_objects(self, objects, mt, txid, timeout):
        """Mask the objects in a transaction.

        If any exceptions are raised, this method will try unmask all the
        objects that it has masked before raising it to the caller.

        @warning: This method masks the objects in the order given in
                  C{objects}. If the object is already masked, this method will
                  wait for it to become unmasked. The caller is in charge of
                  ensuring there won't be deadlock.

        @warning: Unlike L{_unmask_objects}, this operation is B{not}
        idempotent.

        @param objects: a list of (table, key) pairs of objects to mask
        @type  objects: list

        @param mt: The transaction with which to mask C{objects}.
        @type  mt: L{MiniTransaction}

        @param txid: The transaction ID.
        @type  txid: C{int}

        @param timeout: The time at which to expire the mask.
        @type  timeout: C{int}

        @raise Exception: If some object could not be masked because of
                          C{user_reject_rules}. See L{_mask_object}.

        @return: A mapping from (table_id, oid) to the version number of the
                 object with the mask added.
        @rtype:  C{dict}
        """
        masked_versions = {}
        try:
            for (table_id, key) in objects:
                op = mt[(table_id, key)]
                masked_version = self._mask_object(table_id, key, txid, timeout,
                                                   op.reject_rules)
                masked_versions[(table_id, key)] = masked_version
        except:
            # the order to _unmask_objects shouldn't matter
            self._unmask_objects(masked_versions.keys(), txid)
            raise
        return masked_versions

    def _write_mt(self, mt, txid):
        """Write out the minitransaction intent to RAMCloud.

        Uses the table identified by C{txramcloud.tx_table} and the transaction
        id stored in C{mt}.

        @param mt: the minitransaction
        @type  mt: L{MiniTransaction}

        @param txid: the object ID at which to write the minitransaction
        @type  txid: C{int}

        @return: the version of the object written
        @rtype:  L{int}

        @raise TransactionExpired: A tombstone is in the way.
        """

        rr = ramcloud.RejectRules(object_exists=True)
        try:
            return RAMCloud.write_rr(self, self.tx_table, txid,
                                     serialize(mt), rr)
        except ramcloud.ObjectExistsError:
            # looks like a tombstone is here
            raise self.TransactionExpired()

    def _delete_mt(self, txid, version):
        """Ensure a minitransaction intent has been deleted from RAMCloud.

        @precondition: No references exist to C{txid} in the RAMCloud.

        @param txid: the transaction id of the minitransaction to delete
        @type  txid: C{int}

        @param version: the version number of the mt when it was read
        @type  version: C{int}
        """

        rr = ramcloud.RejectRules(version_gt_given=True, given_version=version)
        try:
            RAMCloud.delete_rr(self, self.tx_table, txid, rr)
        except ramcloud.VersionError:
            raise self.InconsistencyError("txid has been modified")

    def _delete_tombstone(self, txid):
        """Ensure a tombstone has been deleted from RAMCloud.

        @warning: No one should ever commit a transaction under txid following
        this operation.

        @param txid: the transaction id of the tombstone to delete
        @type  txid: C{int}
        """

        rr = ramcloud.RejectRules()
        RAMCloud.delete_rr(self, self.tx_table, txid, rr)

    def _apply_op(self, table_id, key, txid, op):
        """Apply the operation to its masked object.

        @param table_id: The table containing the possibly masked object.
        @type  table_id: C{int}

        @param key: The object ID of the possibly masked object.
        @type  key: C{int}

        @param op: the operation to apply
        @type  op: L{MTOperation}
        """

        read_rr = ramcloud.RejectRules(object_doesnt_exist=True)
        try:
            blob, version = RAMCloud.read_rr(self, table_id, key, read_rr)
        except ramcloud.NoObjectError:
            # does not exist, so not masked by txid
            return
        otxid, otimeout, odata = unpack(blob)
        if otxid == 0 or otxid != txid:
            # not masked by txid
            return

        # So, txid is indeed masking the object we read.

        if type(op) == MTOperation:
            # no op
            # remove mask / delete seed
            data = odata
        elif type(op) == MTWrite:
            data = op.data
        elif type(op) == MTDelete:
            data = None
        else:
            raise TxRAMCloud.InconsistencyError("Unknown type in MT: %s" %
                                                type(op))

        # Now, (data is None) determines whether we need to delete or update the
        # object at version.

        update_rr = ramcloud.RejectRules.exactly(version)
        if data is None:
            # delete the object at version
            try:
                RAMCloud.delete_rr(self, table_id, key, update_rr)
            except (ramcloud.NoObjectError, ramcloud.VersionError):
                # we must be racing another client to clean this up
                return
        else:
            # update the object at version with data
            blob = pack(0, 0, data)
            try:
                RAMCloud.write_rr(self, table_id, key, blob, update_rr)
            except (ramcloud.NoObjectError, ramcloud.VersionError):
                # we must be racing another client to clean this up
                return

    def _apply_mt(self, mt, txid):
        """Apply the minitransaction's operations to its masked objects.

        @param mt: the minitransaction to apply
        @type  mt: L{MiniTransaction}

        @param txid: the transaction ID which masks the objects in the transaction
        @type  txid: C{int}
        """
        for ((table_id, key), op) in mt.items():
            self._apply_op(table_id, key, txid, op)

    def _finish_mt(self, mt, txid, version):
        """Complete/clean up a minitransaction after the point of no return.

        @postcondition: No object will be masked by the minitransaction and the
        mt will not exist in the system.

        @param mt: the minitransaction to clean up
        @type  mt: L{MiniTransaction}

        @param txid: the transaction ID which masks the objects
        @type  txid: C{int}

        @param version: the version number of mt when it was read
        @type  version: C{int}
        """

        self._apply_mt(mt, txid)
        self._delete_mt(txid, version)

    def mt_commit(self, mt):
        """Execute and commit a prepared minitransaction.

        @param mt: the prepared minitransaction to be executed
        @type  mt: L{MiniTransaction}

        @raise TransactionRejected: The minitransaction aborted due to reject
        rules.
        @raise TransactionExpired: The minitransaction was aborted by another
        client.

        @return: A mapping from (table_id, oid) to version numbers. There will
        be one entry for each object in the minitransaction:
         - Those objects that were deleted will have None as the verison number.
         - Those objects that were written will have their new version number.
         - Those objects that had no operation applied (unfortunately) also
           changed version numbers, and their new version is returned here.
        @rtype: C{dict}
        """

        # reserve a new transaction ID. 0 is not valid.
        txid = 0
        while txid == 0:
            txid = self.txid_res.next()
        timeout = time.time() + 30

        # mask objects (in sorted order to guarantee no deadlock)
        objects = sorted(mt.keys())
        try:
            masked_versions = self._mask_objects(objects, mt, txid, timeout)
        except (ramcloud.NoObjectError, ramcloud.ObjectExistsError,
                ramcloud.VersionError), e:
            a = self.TransactionRejected()
            a.reasons[(e.table, e.oid)] = e
            raise a
        assert len(masked_versions) == len(objects)

        # TODO: optimization: add masked_versions to mt intent, then use them in
        # _unmask_objects and _finish_mt to save the RAMCloud.read_rr call in
        # _unmask_object.

        # write out minitransaction intent
        try:
            version = self._write_mt(mt, txid)
        except:
            self._unmask_objects(objects, txid)
            self._delete_tombstone(txid)
            raise

        # no turning back now
        self._finish_mt(mt, txid, version)

        # Assume unmasked version is 1 greater than masked version,
        # unless the operation was a delete.
        unmasked_versions = {}
        for ((t, o), v) in masked_versions.items():
            if type(mt[(t, o)]) == MTDelete:
                unmasked_versions[(t, o)] = None
            else:
                assert type(mt[(t, o)]) in [MTOperation, MTWrite]
                unmasked_versions[(t, o)] = v + 1
        return unmasked_versions

# struct
class Tombstone(object):
    pass

# struct
class MiniTransaction(dict):
    pass

# struct
class MTOperation(object):
    def __init__(self, reject_rules):
        self.reject_rules = reject_rules

# struct
class MTWrite(MTOperation):
    def __init__(self, data, reject_rules):
        MTOperation.__init__(self, reject_rules)
        self.data = data

# struct
class MTDelete(MTOperation):
    pass

def main():
    global r
    r = TxRAMCloud(7)
    print "Client: 0x%x" % r.client
    r.connect()
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.get_table_id("test")
    print "with id %s" % table

    r.create(table, 0, "Hello, World, from Python")
    print "Inserted to table"
    value, got_version = r.read(table, 0)
    print value
    key = r.insert(table, "test")
    print "Inserted value and got back key %d" % key
    r.update(table, key, "test")

    bs = "binary\00safe?"
    oid = r.insert(table, bs)
    assert r.read(table, oid)[0] == bs

    r.drop_table("test")

if __name__ == '__main__':
    main()
