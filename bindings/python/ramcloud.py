# Copyright (c) 2009-2010 Stanford University
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

# Note in order for this module to work you must have libramcloud.so
# somewhere in a system library path and have run /sbin/ldconfig since
# installing it

import ctypes
from ctypes.util import find_library
import itertools

def load_so():
    not_found = ImportError("Couldn't find libramcloud.so, ensure it is " +
                            "installed and that you have registered it with " +
                            "/sbin/ldconfig")
    path = find_library('ramcloud')
    if not path:
        raise not_found
    try:
        so = ctypes.cdll.LoadLibrary(path)
    except OSError:
        raise not_found

    def int_errcheck(result, func, arguments):
        if result != 0:
            # If and when rc_last_error() takes a client pointer,
            # we can use arguments[0].
            msg = so.rc_last_error()
            raise RCException(msg)

    def malloc_errcheck(result, func, arguments):
        if result == 0:
            raise MemoryError()
        return result

    # ctypes.c_bool was introduced in Python 2.6
    if not hasattr(ctypes, 'c_bool'):
        class c_bool_compat(ctypes.c_uint8):
            def __init__(self, value=None):
                if value:
                    ctypes.c_uint8.__init__(self, 1)
                else:
                    ctypes.c_uint8.__init__(self, 0)

            @staticmethod
            def from_param(param):
                if param:
                    return ctypes.c_uint8(1)
                else:
                    return ctypes.c_uint8(0)
        ctypes.c_bool = c_bool_compat

    from ctypes import POINTER

    # argument types aliased to their names for sanity
    # alphabetical order
    buf                 = ctypes.c_void_p
    client              = ctypes.c_void_p
    count               = ctypes.c_uint32
    err                 = ctypes.c_int
    error_msg           = ctypes.c_char_p
    inclusive           = ctypes.c_bool
    index_entries_buf   = ctypes.c_void_p
    index_entries_len   = ctypes.c_uint64
    index_id            = ctypes.c_uint16
    index_key           = ctypes.c_void_p
    index_key_len       = ctypes.c_uint64
    index_type          = ctypes.c_int
    key                 = ctypes.c_uint64
    keys_buf_len        = ctypes.c_uint64
    keys_buf_p          = ctypes.c_void_p
    len                 = ctypes.c_uint64
    more                = ctypes.c_bool
    multi_lookup_args   = ctypes.c_void_p
    name                = ctypes.c_char_p
    oid                 = key
    oid_present         = ctypes.c_bool
    oids_buf_len        = ctypes.c_uint64
    oids_buf_p          = POINTER(ctypes.c_uint64)
    range_query_args    = ctypes.c_void_p
    range_queryable     = ctypes.c_bool
    table               = ctypes.c_uint64
    unique              = ctypes.c_bool
    version             = ctypes.c_uint64

    so.rc_connect.argtypes = [client]
    so.rc_connect.restype  = err
    so.rc_connect.errcheck = int_errcheck

    so.rc_disconnect.argtypes = [client]
    so.rc_disconnect.restype  = None

    so.rc_ping.argtypes = [client]
    so.rc_ping.restype  = err
    so.rc_ping.errcheck = int_errcheck

    so.rc_write.argtypes = [client, table, key, version, POINTER(version), buf,
                            len, index_entries_buf, index_entries_len]
    so.rc_write.restype  = err
    so.rc_write.errcheck = int_errcheck

    so.rc_insert.argtypes = [client, table, buf, len, POINTER(key),
                             index_entries_buf, index_entries_len]
    so.rc_insert.restype  = err
    so.rc_insert.errcheck = int_errcheck

    so.rc_delete.argtypes = [client, table, key, version, POINTER(version)]
    so.rc_delete.restype  = err
    so.rc_delete.errcheck = int_errcheck

    so.rc_read.argtypes = [client, table, key, version, POINTER(version), buf,
                           POINTER(len), index_entries_buf,
                           POINTER(index_entries_len)]
    so.rc_read.restype  = err
    so.rc_read.errcheck = int_errcheck

    so.rc_create_table.argtypes = [client, name]
    so.rc_create_table.restype  = err
    so.rc_create_table.errcheck = int_errcheck

    so.rc_open_table.argtypes = [client, name, POINTER(table)]
    so.rc_open_table.restype  = err
    so.rc_open_table.errcheck = int_errcheck

    so.rc_drop_table.argtypes = [client, name]
    so.rc_drop_table.restype  = err
    so.rc_drop_table.errcheck = int_errcheck

    so.rc_create_index.argtypes = [client, table, index_type, unique,
                                   range_queryable, POINTER(index_id)]
    so.rc_create_index.restype  = err
    so.rc_create_index.errcheck = int_errcheck

    so.rc_drop_index.argtypes = [client, table, index_id]
    so.rc_drop_index.restype  = err
    so.rc_drop_index.errcheck = int_errcheck

    so.rc_unique_lookup.argtypes = [client, table, index_id, index_key,
                                    index_key_len, POINTER(oid_present),
                                    POINTER(oid)]
    so.rc_unique_lookup.restype  = err
    so.rc_unique_lookup.errcheck = int_errcheck

    so.rc_multi_lookup_args_new.argtypes = []
    so.rc_multi_lookup_args_new.restype  = multi_lookup_args
    so.rc_multi_lookup_args_new.errcheck = malloc_errcheck

    so.rc_multi_lookup_args_free.argtypes = [multi_lookup_args]
    so.rc_multi_lookup_args_free.restype  = None

    so.rc_multi_lookup_set_index.argtypes = [multi_lookup_args, table, index_id]
    so.rc_multi_lookup_set_index.restype = None

    so.rc_multi_lookup_set_key.argtypes = [multi_lookup_args, index_key,
                                           index_key_len]
    so.rc_multi_lookup_set_key.restype = None

    so.rc_multi_lookup_set_start_following_oid.argtypes = [multi_lookup_args,
                                                           oid]
    so.rc_multi_lookup_set_start_following_oid.restype = None

    so.rc_multi_lookup_set_result_buf.argtypes = [multi_lookup_args,
                                                  POINTER(count),
                                                  oids_buf_p,
                                                  POINTER(more)]
    so.rc_multi_lookup_set_result_buf.restype = None

    so.rc_multi_lookup.argtypes = [client, multi_lookup_args]
    so.rc_multi_lookup.restype  = err
    so.rc_multi_lookup.errcheck = int_errcheck

    so.rc_range_query_args_new.argtypes = []
    so.rc_range_query_args_new.restype  = range_query_args
    so.rc_range_query_args_new.errcheck = malloc_errcheck

    so.rc_range_query_args_free.argtypes = [range_query_args]
    so.rc_range_query_args_free.restype  = None

    so.rc_range_query_set_index.argtypes = [range_query_args, table, index_id]
    so.rc_range_query_set_index.restype  = None

    so.rc_range_query_set_key_start.argtypes = [range_query_args, index_key,
                                                index_key_len, inclusive]
    so.rc_range_query_set_key_start.restype  = None

    so.rc_range_query_set_key_end.argtypes = [range_query_args, index_key,
                                              index_key_len, inclusive]
    so.rc_range_query_set_key_end.restype  = None

    so.rc_range_query_set_start_following_oid.argtypes = [range_query_args, oid]
    so.rc_range_query_set_start_following_oid.restype  = None

    so.rc_range_query_set_result_bufs.argtypes = [range_query_args,
                                                  POINTER(count),
                                                  oids_buf_p,
                                                  POINTER(oids_buf_len),
                                                  keys_buf_p,
                                                  POINTER(keys_buf_len),
                                                  POINTER(more)]
    so.rc_range_query_set_result_bufs.restype  = None

    so.rc_range_query.argtypes = [client, range_query_args]
    so.rc_range_query.restype  = err
    so.rc_range_query.errcheck = int_errcheck


    so.rc_new.argtypes = []
    so.rc_new.restype  = client
    so.rc_new.errcheck = malloc_errcheck

    so.rc_free.argtypes = [client]
    so.rc_free.restype  = None

    so.rc_last_error.argtypes = []
    so.rc_last_error.restype  = error_msg

    so.RCRPC_VERSION_ANY = ctypes.c_uint64.in_dll(so, "rcrpc_version_any")

    return so

def _ctype_copy(addr, var, width):
    ctypes.memmove(addr, ctypes.addressof(var), width)
    return addr + width

class _RCRPC_INDEX_TYPE(object):
    class IndexType:
        def __init__(self, type_id, width, ctype):
            self.type_id = type_id
            self.width = width
            self.ctype = ctype
            self.name = 'unknown'

        def __repr__(self):
            return 'RCRPC_INDEX_TYPE.%s' % self.name

    def __init__(self):
        id = itertools.count(0)
        self._lookup = {}

        # Keep this in sync with src/shared/rcrpc.h
        self.SINT8   = self.IndexType(id.next(), 1,    ctypes.c_int8)
        self.UINT8   = self.IndexType(id.next(), 1,    ctypes.c_uint8)
        self.SINT16  = self.IndexType(id.next(), 2,    ctypes.c_int16)
        self.UINT16  = self.IndexType(id.next(), 2,    ctypes.c_uint16)
        self.SINT32  = self.IndexType(id.next(), 4,    ctypes.c_int32)
        self.UINT32  = self.IndexType(id.next(), 4,    ctypes.c_uint32)
        self.SINT64  = self.IndexType(id.next(), 8,    ctypes.c_int64)
        self.UINT64  = self.IndexType(id.next(), 8,    ctypes.c_uint64)
        self.FLOAT32 = self.IndexType(id.next(), 4,    ctypes.c_float)
        self.FLOAT64 = self.IndexType(id.next(), 8,    ctypes.c_double)
        self.BYTES8  = self.IndexType(id.next(), None, ctypes.c_char_p)
        self.BYTES16 = self.IndexType(id.next(), None, ctypes.c_char_p)
        self.BYTES32 = self.IndexType(id.next(), None, ctypes.c_char_p)
        self.BYTES64 = self.IndexType(id.next(), None, ctypes.c_char_p)

    def __getitem__(self, i):
        return self._lookup[i]

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        if isinstance(value, self.IndexType):
            value.name = name
            self._lookup[value.type_id] = value

RCRPC_INDEX_TYPE = _RCRPC_INDEX_TYPE()

class RCException(Exception):
    pass


class RAMCloud(object):
    def __init__(self):
        self.client = so.rc_new()

    def __del__(self):
        so.rc_free(self.client)
        self.client = None

    def connect(self):
        so.rc_connect(self.client)

    def ping(self):
        so.rc_ping(self.client)

    def _indexes_buf_len(self, indexes):
        buf_len = 0
        for index_id, (index_type, data) in indexes:
            buf_len += 8 + 4 + 4 # len, index_id, index_type
            if index_type.width: # data
                buf_len += index_type.width
            else: # string
                buf_len += len(data)
        return buf_len

    def _indexes_fill(self, addr, indexes):
        for index_id, (index_type, data) in indexes:

            # len
            if index_type.width:
                addr = _ctype_copy(addr, ctypes.c_uint64(index_type.width), 8)
            else: # string
                addr = _ctype_copy(addr, ctypes.c_uint64(len(data)), 8)

            # index_id
            addr = _ctype_copy(addr, ctypes.c_uint32(index_id), 4)

            # index_type
            addr = _ctype_copy(addr, ctypes.c_uint32(index_type.type_id), 4)

            # data
            if index_type.width:
                addr = _ctype_copy(addr, index_type.ctype(data), index_type.width)
            else: # string
                addr = _ctype_copy(addr, ctypes.create_string_buffer(data), len(data))

    def _indexes_to_buf(self, indexes=None):
        if indexes:
            buf_len = self._indexes_buf_len(indexes)
            idx_buf = ctypes.create_string_buffer(buf_len)
            self._indexes_fill(ctypes.addressof(idx_buf), indexes)
            return ctypes.byref(idx_buf), ctypes.c_uint64(buf_len)
        else:
            return ctypes.c_void_p(None), ctypes.c_uint64(0)

    def write(self, table_id, key, data, want_version=None, indexes=None):
        idx_bufp, idx_buf_len = self._indexes_to_buf(indexes)
        got_version = ctypes.c_uint64()
        if want_version != None:
            want_version = ctypes.c_uint64(want_version)
        else:
            want_version = so.RCRPC_VERSION_ANY
        so.rc_write(self.client,
                    ctypes.c_uint64(table_id),
                    ctypes.c_uint64(key),
                    want_version,
                    ctypes.byref(got_version),
                    ctypes.c_char_p(data),
                    ctypes.c_uint64(len(data)),
                    idx_bufp,
                    idx_buf_len)
        if (want_version.value != so.RCRPC_VERSION_ANY.value and
            got_version.value != want_version.value):
            raise RCException("mismatched version")
        return got_version.value

    def insert(self, table_id, data, indexes=None):
        idx_bufp, idx_buf_len = self._indexes_to_buf(indexes)
        key = ctypes.c_uint64()
        so.rc_insert(self.client,
                     ctypes.c_uint64(table_id),
                     ctypes.c_char_p(data),
                     ctypes.c_uint64(len(data)),
                     ctypes.byref(key),
                     idx_bufp,
                     idx_buf_len)
        return key.value

    def delete(self, table_id, key, want_version=None):
        got_version = ctypes.c_uint64()
        if want_version != None:
            want_version = ctypes.c_uint64(want_version)
        else:
            want_version = so.RCRPC_VERSION_ANY
        so.rc_delete(self.client,
                     ctypes.c_uint64(table_id),
                     ctypes.c_uint64(key),
                     want_version,
                     ctypes.byref(got_version))
        if (want_version.value != so.RCRPC_VERSION_ANY.value and
            got_version.value != want_version.value):
            raise RCException("mismatched version")
        return got_version.value

    def _buf_to_indexes(self, addr, indexes_len):
        if addr and indexes_len:
            indexes = []
            while indexes_len > 0:
                # len
                len = ctypes.c_uint64.from_address(addr).value
                addr += 8

                # index_id
                index_id = ctypes.c_uint32.from_address(addr).value
                addr += 4

                # index_type
                type_id = ctypes.c_uint32.from_address(addr).value
                index_type = RCRPC_INDEX_TYPE[type_id]
                addr += 4

                # data
                if index_type.width:
                    data = index_type.ctype.from_address(addr).value
                    assert len == index_type.width
                else:
                    data_buf = ctypes.create_string_buffer(len)
                    ctypes.memmove(ctypes.addressof(data_buf), addr, len)
                    data = data_buf.value
                addr += len

                indexes.append((index_id, (index_type, data)))
                indexes_len -= 8 + 4 + 4 + len
            assert indexes_len == 0

            return indexes
        else:
            return []

    def read(self, table_id, key, want_version=None):
        buf = ctypes.create_string_buffer(10240)
        l = ctypes.c_uint64()
        got_version = ctypes.c_uint64()
        if want_version != None:
            want_version = ctypes.c_uint64(want_version)
        else:
            want_version = so.RCRPC_VERSION_ANY
        idx_buf = ctypes.create_string_buffer(10240)
        idx_buf_len = ctypes.c_uint64(len(idx_buf))
        so.rc_read(self.client,
                   ctypes.c_uint64(table_id),
                   ctypes.c_uint64(key),
                   want_version,
                   ctypes.byref(got_version),
                   ctypes.byref(buf),
                   ctypes.byref(l),
                   ctypes.byref(idx_buf),
                   ctypes.byref(idx_buf_len))
        if (want_version.value != so.RCRPC_VERSION_ANY.value and
            got_version.value != want_version.value):
            raise RCException("mismatched version")
        #print repr(idx_buf.raw[:idx_buf_len.value])
        indexes = self._buf_to_indexes(ctypes.addressof(idx_buf), idx_buf_len.value)
        return (buf.raw[0:l.value], got_version.value, indexes)

    def create_table(self, name):
        so.rc_create_table(self.client, name)

    def open_table(self, name):
        handle = ctypes.c_uint64()
        so.rc_open_table(self.client, name, ctypes.byref(handle))
        return handle.value

    def drop_table(self, name):
        so.rc_drop_table(self.client, name)

    def create_index(self, table_id, type, unique, range_queryable):
        index_id = ctypes.c_uint16()
        so.rc_create_index(self.client,
                           ctypes.c_uint64(table_id),
                           int(type.type_id),
                           bool(unique),
                           bool(range_queryable),
                           ctypes.byref(index_id))
        return index_id.value

    def drop_index(self, table_id, index_id):
        so.rc_drop_index(self.client,
                         ctypes.c_uint64(table_id),
                         ctypes.c_uint16(index_id))

    def unique_lookup(self, table_id, index_id, index_type, key):
        if index_type.width:
            width = index_type.width
            key_buf = index_type.ctype(key)
        else:
            # variable-length key type (BYTES)
            key_buf = ctypes.create_string_buffer(key)
            width = len(key)
        oid_present = ctypes.c_bool()
        oid = ctypes.c_uint64()
        so.rc_unique_lookup(self.client,
                            ctypes.c_uint64(table_id),
                            ctypes.c_uint16(index_id),
                            ctypes.byref(key_buf),
                            ctypes.c_uint64(width),
                            ctypes.byref(oid_present),
                            ctypes.byref(oid))
        if bool(oid_present.value):
            return oid.value
        else:
            raise KeyError()

    def multi_lookup(self, table_id, index_id, index_type, limit, key,
                     start_following_oid=None):
        args = so.rc_multi_lookup_args_new()
        so.rc_multi_lookup_set_index(args, ctypes.c_uint64(table_id),
                                     ctypes.c_uint16(index_id))
        if index_type.width:
            width = index_type.width
            key_buf = index_type.ctype(key)
        else:
            # variable-length key type (BYTES)
            key_buf = ctypes.create_string_buffer(key)
            width = len(key)
        so.rc_multi_lookup_set_key(args, ctypes.byref(key_buf),
                                   ctypes.c_uint64(width))

        if start_following_oid is not None:
            so.rc_multi_lookup_set_start_following_oid(args,
                    ctypes.c_uint64(start_following_oid))

        more = ctypes.c_bool()
        count = ctypes.c_uint32(limit)
        oids = (ctypes.c_uint64 * limit)()
        so.rc_multi_lookup_set_result_buf(args, ctypes.byref(count),
                                          oids, ctypes.byref(more))
        try:
            so.rc_multi_lookup(self.client, args)
        finally:
            so.rc_multi_lookup_args_free(args)
        return (oids[:count.value], bool(more.value))

    def range_query(self, table_id, index_id, index_type, limit,
                    key_start=None, key_start_inclusive=True,
                    key_end=None, key_end_inclusive=False,
                    start_following_oid=None):
        args = so.rc_range_query_args_new()
        so.rc_range_query_set_index(args, ctypes.c_uint64(table_id),
                                    ctypes.c_uint16(index_id))

        if key_start is not None:
            if index_type.width:
                width = index_type.width
                key_start_buf = index_type.ctype(key_start)
            else:
                # variable-length key type (BYTES)
                key_start_buf = ctypes.create_string_buffer(key_start)
                width = len(key_start)
            so.rc_range_query_set_key_start(args, ctypes.byref(key_start_buf),
                                            ctypes.c_uint64(width),
                                            bool(key_start_inclusive))

        if key_end is not None:
            if index_type.width:
                width = index_type.width
                key_end_buf = index_type.ctype(key_end)
            else:
                # variable-length key type (BYTES)
                key_end_buf = ctypes.create_string_buffer(key_end)
                width = len(key_end)
            so.rc_range_query_set_key_end(args, ctypes.byref(key_end_buf),
                                          ctypes.c_uint64(width),
                                          ctypes.c_bool(key_end_inclusive))
        if start_following_oid is not None:
            so.rc_range_query_set_start_following_oid(args,
                    ctypes.c_uint64(start_following_oid))

        more = ctypes.c_bool()
        count = ctypes.c_uint32(limit)
        oids = (ctypes.c_uint64 * limit)()
        oids_buf_len = ctypes.c_uint64(limit * 8)
        if index_type.width:
            keys = (index_type.ctype * limit)()
            keys_buf_len = ctypes.c_uint64(index_type.width * limit)
        else:
            # variable-length key type (BYTES)
            if index_type == RCRPC_INDEX_TYPE.BYTES8:
                maxlen = (2**8 - 1) * limit
            elif index_type == RCRPC_INDEX_TYPE.BYTES16:
                maxlen = (2**16 - 1) * limit
            elif index_type == RCRPC_INDEX_TYPE.BYTES32:
                maxlen = (2**32 - 1) * limit
            elif index_type == RCRPC_INDEX_TYPE.BYTES64:
                maxlen = (2**64 - 1) * limit
            else:
                assert False, "unknown variable-length key type"
            if maxlen > 1024 * 1024 * 10:
                print "warning: possible result size too big..."
                print "warning: using smaller buffer that may overflow"
                maxlen = 1024 * 1024 * 10
            keys = ctypes.create_string_buffer(maxlen)
            keys_buf_len = ctypes.c_uint64(len(keys))
        so.rc_range_query_set_result_bufs(args, ctypes.byref(count),
                                          oids, ctypes.byref(oids_buf_len),
                                          keys, ctypes.byref(keys_buf_len),
                                          ctypes.byref(more))
        try:
            so.rc_range_query(self.client, args)
        finally:
            so.rc_range_query_args_free(args)

        pairs = [] # (key, oid)
        addr = ctypes.addressof(keys)
        for i in range(count.value):
            if index_type.width:
                key = keys[i]
            else:
                # variable-length key type (BYTES)
                if index_type == RCRPC_INDEX_TYPE.BYTES8:
                    l = ctypes.c_uint8.from_address(addr).value
                    addr += 1
                elif index_type == RCRPC_INDEX_TYPE.BYTES16:
                    l = ctypes.c_uint16.from_address(addr).value
                    addr += 2
                elif index_type == RCRPC_INDEX_TYPE.BYTES32:
                    l = ctypes.c_uint32.from_address(addr).value
                    addr += 4
                elif index_type == RCRPC_INDEX_TYPE.BYTES64:
                    l = ctypes.c_uint64.from_address(addr).value
                    addr += 8
                else:
                    assert False, "unknown variable-length key type"
                buf = ctypes.create_string_buffer(l)
                ctypes.memmove(ctypes.addressof(buf), addr, l)
                addr += l
                key = buf.value
            oid = oids[i]
            pairs.append((key, oid))
        return (pairs, bool(more.value))

def main():
    r = RAMCloud()
    print "Client: 0x%x" % r.client
    r.connect()
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.open_table("test")
    print "with id %s" % table

    index_id = r.create_index(table, RCRPC_INDEX_TYPE.UINT64, True, True)
    print "Created index id %d" % index_id
    str_index_id = r.create_index(table, RCRPC_INDEX_TYPE.BYTES8, True, True)
    print "Created index id %d" % str_index_id
    multi_index_id = r.create_index(table, RCRPC_INDEX_TYPE.SINT32, False, True)
    print "Created index id %d" % multi_index_id

    r.write(table, 0, "Hello, World, from Python", None, [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4592)),
        (str_index_id, (RCRPC_INDEX_TYPE.BYTES8, "write")),
        (multi_index_id, (RCRPC_INDEX_TYPE.SINT32, 2)),
    ])
    print "Inserted to table"
    value, got_version, indexes = r.read(table, 0)
    print value
    print indexes
    key = r.insert(table, "test", [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4723)),
        (str_index_id, (RCRPC_INDEX_TYPE.BYTES8, "insert")),
        (multi_index_id, (RCRPC_INDEX_TYPE.SINT32, 2)),
    ])
    print "Inserted value and got back key %d" % key
    r.write(table, key, "test", None, [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4899)),
        (str_index_id, (RCRPC_INDEX_TYPE.BYTES8, "rewrite")),
        (multi_index_id, (RCRPC_INDEX_TYPE.SINT32, 2)),
    ])

    pairs, more = r.range_query(table_id=table, index_id=index_id,
                                index_type=RCRPC_INDEX_TYPE.UINT64,
                                limit=5,
                                key_start=4000, key_start_inclusive=True,
                                key_end=5000, key_end_inclusive=True,
                                start_following_oid=None)
    print pairs, more

    pairs, more = r.range_query(table_id=table, index_id=str_index_id,
                                index_type=RCRPC_INDEX_TYPE.BYTES8,
                                limit=5, key_start="m")
    print pairs, more

    print r.unique_lookup(table_id=table, index_id=str_index_id,
                          index_type=RCRPC_INDEX_TYPE.BYTES8, key="rewrite")

    oids, more = r.multi_lookup(table_id=table, index_id=multi_index_id,
                                index_type=RCRPC_INDEX_TYPE.SINT32,
                                limit=10, key=2)
    print oids, more

    bs = "binary\00safe?"
    oid = r.insert(table, bs)
    assert r.read(table, oid)[0] == bs

    r.drop_index(table, str_index_id)
    r.drop_index(table, index_id)
    r.drop_table("test")

so = load_so()

if __name__ == '__main__':
    main()
