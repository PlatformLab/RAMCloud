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

"""Client-side, safe, concurrent object ID allocation."""

import ctypes
import ramcloud
import retries

OIDRES_VERSION = 1
OIDRES_HEADER = "oidres\0" + chr(OIDRES_VERSION)

def unpack(blob):
    """Unserialize a RAMCloud object into next_avail."""
    header = blob[:8]
    next_avail = blob[8:]

    assert OIDRES_HEADER == header

    next_avail = ctypes.create_string_buffer(next_avail)
    next_avail = ctypes.c_uint64.from_address(ctypes.addressof(next_avail))
    next_avail = next_avail.value

    return next_avail

def pack(next_avail):
    """Serialize next_avail into a RAMCloud object."""

    next_avail = ctypes.c_uint64(next_avail)
    sb = ctypes.create_string_buffer(8)
    ctypes.memmove(ctypes.addressof(sb), ctypes.addressof(next_avail), 8)
    next_avail = sb.raw

    return '%s%s' % (OIDRES_HEADER, next_avail)

class LazyOID:
    """An object which knows how to reserve and object ID from an L{OIDRes}.

    This is useful when you know you possibly need to reserve an object ID. You
    don't want to reserve one too early and waste it if you end up not needing
    it.

    L{LazyOID} objects should not be created directly. Use
    L{OIDRes.reserve_lazily}.

    Example usage:

    >>> my_oidres = ...
    >>> loid = my_oidres.reserve_lazily()
    >>> try:
    ...     for ... in ...:
    ...         if ...:
    ...             write object pointing to int(loid)
    ... except:
    ...     clean up dangling references to int(loid)
    ... else:
    ...     write int(loid) object

    """

    def __init__(self, oidres):
        """
        @type oidres: L{OIDRes}
        @param oidres: the reservation object to call if an object ID is
                       requested

        """
        self._oidres = oidres
        self._oid = None

    def __int__(self):
        """Return the reserved object ID.

        The first time C{int()} is called on this object, an object ID will be
        reserved. Calling C{int()} on this object additional times has no
        side-effects.

        @rtype: int
        """
        if self._oid is None:
            self._oid = self._oidres.next()
        return self._oid

class OIDRes:
    """Client-side, safe, concurrent object ID allocation.

    This class uses a given RAMCloud object to reserve object IDs in batches.

    A batch is reserved only when its use is first required. Simply
    instantiating this class is free.

    Once a batch of object IDs is reserved, none of those object IDs can ever
    be assigned again. Thus, every time a client exits (gracefully or
    otherwise), it wastes up to L{delta} object IDs. As long as L{delta} is
    relatively small, the object ID space will not be exhausted. However, the
    object IDs that end up in use will not be entirely dense when using this
    class.

    As L{delta} defines the batch size, in expectation, each object ID reserved
    costs M{2 / L{delta}} round trips to the RAMCloud server. Different clients
    reserving from the same RAMCloud object may have different values of
    L{delta}.

    The RAMCloud object used holds the next unreserved object ID. If it does
    not exist, it is assumed to have a value of 0 and will be created during
    the first reservation.

    @type delta: int
    @ivar delta: size of a batch of object IDs

        Subclasses are free to implement L{delta} as a variable or property
        that changes over time.

    """

    def __init__(self, rc, table, oid, delta=10):
        """
        @type rc: L{ramcloud.RAMCloud}

        @type  table: int
        @param table: table handle with which to access the reservation object

        @type  oid: int
        @param oid: object ID in which to access the reservation object

        @type  delta: int
        @param delta: see L{delta}

        """
        self._rc = rc
        self._table = table
        self._oid = oid
        self._reserved = []
        self.delta = delta

    def _read(self):
        """Wrapper for L{ramcloud.RAMCloud.read}"""
        data, version = self._rc.read(self._table, self._oid)
        return (unpack(data), version)

    def _update(self, next_avail, version):
        """Wrapper for L{ramcloud.RAMCloud.update}"""
        data = pack(next_avail)
        self._rc.update(self._table, self._oid, data, version)

    def _create(self, next_avail):
        """Wrapper for L{ramcloud.RAMCloud.create}"""
        data = pack(next_avail)
        self._rc.create(self._table, self._oid, data)

    def next(self, retry_strategy=retries.FuzzyExponentialBackoff):
        """Return the next reserved object ID.

        If necessary, L{next} will reserve another batch of object IDs from the
        server.

        @type  retry_strategy: callable that returns L{ImmediateRetry} instance
        @param retry_strategy: how to execute the retry loop

            Defaults to L{retries.FuzzyExponentialBackoff}, which is
            reasonable. This is here for testing purposes.

        @rtype: int
        """

        if self._reserved:
            return self._reserved.pop()

        for retry in retry_strategy():
            try:
                (next_avail, version) = self._read()
            except ramcloud.NoObjectError:
                next_avail = 0
                try:
                    self._create(next_avail + self.delta)
                except ramcloud.ObjectExistsError:
                    retry.later()
            else:
                try:
                    self._update(next_avail + self.delta, version)
                except (ramcloud.NoObjectError, ramcloud.VersionError):
                    retry.later()

        self._reserved = range(next_avail + self.delta - 1, next_avail, -1)
        return next_avail

    def reserve_lazily(self):
        """Return a L{LazyOID} which knows how to reserve an object ID.

        @rtype: L{LazyOID}

        """
        return LazyOID(self)

__all__ = ["OIDRes"]
