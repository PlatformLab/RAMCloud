# Copyright (c) 2009 Stanford University
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

class IN_ADDR(ctypes.Structure):
    _fields_ = [('s_addr', ctypes.c_uint)]

    def __repr__(self):
        return "IN_ADDR{'s_addr': %s}" % repr(self.s_addr)

class SOCKADDR_IN(ctypes.Structure):
    _fields_ = [('sin_family', ctypes.c_ushort),
                ('sin_port', ctypes.c_ushort),
                ('sin_addr', IN_ADDR),
                ('sin_zero', ctypes.c_char * 8)]

    def __repr__(self):
        return "SOCKADDR_IN{'sin_family': %s, 'sin_port': %s, 'sin_addr': %s, 'sin_zero': %s}" % (
            repr(self.sin_family),
            repr(self.sin_port),
            repr(self.sin_addr),
            repr(self.sin_zero))

class NET(ctypes.Structure):
# TODO(stutsman) wrong widths on c_int, but doesn't matter since data is opaque
#    _fields_ = [('is_server', ctypes.c_int),
#                ('fd', ctypes.c_int),
#                ('connected', ctypes.c_int),
#                ('srcsin', SOCKADDR_IN),
#                ('dstsin', SOCKADDR_IN)]
    _fields_ = [('is_server', ctypes.c_char * 4),
                ('fd', ctypes.c_char * 4),
                ('connected', ctypes.c_char * 4),
                ('srcsin', SOCKADDR_IN),
                ('dstsin', SOCKADDR_IN)]

    def __repr__(self):
        return "NET{'is_server': %s, 'fd': %s, 'connected': %s, 'srcsin': %s, 'dstsin': %s}" % (
            repr(self.is_server),
            repr(self.fd),
            repr(self.connected),
            repr(self.srcsin),
            repr(self.dstsin))

# size 44 gels up with the C implementation
class CLIENT(ctypes.Structure):
    _fields_ = [('net', NET)]

    def __repr__(self):
        return "CLIENT{'net': %s}" % repr(self.net)

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
        def gen():
            i = 0
            while True:
                yield i
                i += 1
        id = gen()

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
        self.STRING  = self.IndexType(id.next(), None, ctypes.c_char_p) # \0-terminated

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
        path = find_library('ramcloud')
        if not path:
            raise """Couldn't find libramcloud.so, ensure it is
        installed that you have registered it with /sbin/ldconfig"""

        self.so = ctypes.cdll.LoadLibrary(path)
        self.so.rc_last_error.restype = ctypes.c_char_p

        self.so.rc_multi_lookup_args_new.restype = ctypes.c_voidp
        self.so.rc_multi_lookup_args_free.restype = None
        self.so.rc_multi_lookup_set_index.restype = None
        self.so.rc_multi_lookup_set_key.restype = None
        self.so.rc_multi_lookup_set_start_following_oid.restype = None
        self.so.rc_multi_lookup_set_result_buf.restype = None

        self.so.rc_range_query_args_new.restype = ctypes.c_void_p
        self.so.rc_range_query_args_free.restype = None
        self.so.rc_range_query_set_index.restype = None
        self.so.rc_range_query_set_key_start.restype = None
        self.so.rc_range_query_set_key_end.restype = None
        self.so.rc_range_query_set_start_following_oid.restype = None
        self.so.rc_range_query_set_result_bufs.restype = None

        self.client = CLIENT()

    def raise_error(self):
        msg = self.so.rc_last_error()
        raise RCException(msg)

    def connect(self):
        r = self.so.rc_connect(ctypes.byref(self.client))
        if r != 0:
            self.raise_error()
    
    def ping(self):
        r = self.so.rc_ping(ctypes.byref(self.client))
        if r != 0:
            self.raise_error()

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
                ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint64(index_type.width)), 8)
                addr += 8
            else: # string
                ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint64(len(data))), 8)
                addr += 8

            # index_id
            ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint32(index_id)), 4)
            addr += 4

            # index_type
            ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint32(index_type.type_id)), 4)
            addr += 4

            # data
            if index_type.width:
                ctypes.memmove(addr, ctypes.addressof(index_type.ctype(data)), index_type.width)
                addr += index_type.width
            else: # string
                ctypes.memmove(addr, ctypes.addressof(ctypes.create_string_buffer(data)), len(data))
                addr += len(data)

    def _indexes_to_buf(self, indexes=None):
        if indexes:
            buf_len = self._indexes_buf_len(indexes)
            idx_buf = ctypes.create_string_buffer(buf_len)
            self._indexes_fill(ctypes.addressof(idx_buf), indexes)
            return ctypes.byref(idx_buf), ctypes.c_uint64(buf_len)
        else:
            return ctypes.c_void_p(None), ctypes.c_uint64(0)

    def write(self, table_id, key, data, indexes=None):
        idx_bufp, idx_buf_len = self._indexes_to_buf(indexes)
        r = self.so.rc_write(ctypes.byref(self.client),
                             ctypes.c_uint64(table_id),
                             ctypes.c_uint64(key),
                             ctypes.c_char_p(data),
                             ctypes.c_uint64(len(data)),
                             idx_bufp,
                             idx_buf_len)
        if r != 0:
            self.raise_error()

    def insert(self, table_id, data, indexes=None):
        idx_bufp, idx_buf_len = self._indexes_to_buf(indexes)
        key = ctypes.c_uint64()
        r = self.so.rc_insert(ctypes.byref(self.client),
                              ctypes.c_uint64(table_id),
                              ctypes.c_char_p(data),
                              ctypes.c_uint64(len(data)),
                              ctypes.byref(key),
                              idx_bufp,
                              idx_buf_len)
        if r != 0:
            self.raise_error()
        return key.value

    def read(self, table_id, key):
        return self.read_full(table_id, key)[0]

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

    def read_full(self, table_id, key):
        buf = ctypes.create_string_buffer(2048)
        l = ctypes.c_int()
        idx_buf = ctypes.create_string_buffer(2048)
        idx_buf_len = ctypes.c_int(len(idx_buf))
        r = self.so.rc_read(ctypes.byref(self.client),
                            int(table_id),
                            int(key),
                            ctypes.byref(buf),
                            ctypes.byref(l),
                            ctypes.byref(idx_buf),
                            ctypes.byref(idx_buf_len))
        #print repr(idx_buf.raw[:idx_buf_len.value])
        indexes = self._buf_to_indexes(ctypes.addressof(idx_buf), idx_buf_len.value)
        if r != 0:
            self.raise_error()
        return (buf.value[0:l.value], indexes)

    def create_table(self, name):
        r = self.so.rc_create_table(ctypes.byref(self.client), name)
        if r != 0:
            self.raise_error()

    def open_table(self, name):
        handle = ctypes.c_int()
        r = self.so.rc_open_table(ctypes.byref(self.client), name, ctypes.byref(handle))
        if r != 0:
            self.raise_error()
        return handle.value

    def drop_table(self, name):
        r = self.so.rc_drop_table(ctypes.byref(self.client), name)
        if r != 0:
            self.raise_error()

    def create_index(self, table_id, type, unique, range_queryable):
        index_id = ctypes.c_int()
        r = self.so.rc_create_index(ctypes.byref(self.client),
                                    int(table_id),
                                    int(type.type_id),
                                    bool(unique),
                                    bool(range_queryable),
                                    ctypes.byref(index_id))
        if r != 0:
            self.raise_error()
        return index_id.value

    def drop_index(self, table_id, index_id):
        r = self.so.rc_drop_index(ctypes.byref(self.client),
                                  int(table_id),
                                  int(index_id))
        if r != 0:
            self.raise_error()

    def unique_lookup(self, table_id, index_id, index_type, key):
        if index_type.width:
            width = index_type.width
            key_buf = index_type.ctype(key)
        else:
            # variable-length key type (STRING)
            key_buf = ctypes.create_string_buffer(key)
            width = len(key)
        oid_present = ctypes.c_int()
        oid = ctypes.c_uint64()
        r = self.so.rc_unique_lookup(ctypes.byref(self.client),
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
        args = self.so.rc_multi_lookup_args_new()
        self.so.rc_multi_lookup_set_index(args, ctypes.c_uint64(table_id),
                                          ctypes.c_uint16(index_id))
        if index_type.width:
            width = index_type.width
            key_buf = index_type.ctype(key)
        else:
            # variable-length key type (STRING)
            key_buf = ctypes.create_string_buffer(key)
            width = len(key)
        self.so.rc_multi_lookup_set_key(args, ctypes.byref(key_buf),
                                        ctypes.c_uint64(width))

        if start_following_oid is not None:
            self.so.rc_multi_lookup_set_start_following_oid(args,
                    ctypes.c_uint64(start_following_oid))

        more = ctypes.c_int()
        count = ctypes.c_uint32(limit)
        oids = (ctypes.c_uint64 * limit)()
        self.so.rc_multi_lookup_set_result_buf(args, ctypes.byref(count),
                                               ctypes.byref(oids),
                                               ctypes.byref(more))
        r = self.so.rc_multi_lookup(ctypes.byref(self.client), args)
        self.so.rc_multi_lookup_args_free(args)
        if r != 0:
            self.raise_error()
        return (oids[:count.value], bool(more.value))

    def range_query(self, table_id, index_id, index_type, limit,
                    key_start=None, key_start_inclusive=True,
                    key_end=None, key_end_inclusive=False,
                    start_following_oid=None):
        args = self.so.rc_range_query_args_new()
        self.so.rc_range_query_set_index(args, ctypes.c_uint64(table_id),
                                         ctypes.c_uint16(index_id))

        if key_start is not None:
            if index_type.width:
                width = index_type.width
                key_start_buf = index_type.ctype(key_start)
            else:
                # variable-length key type (STRING)
                key_start_buf = ctypes.create_string_buffer(key_start)
                width = len(key_start)
            self.so.rc_range_query_set_key_start(args, ctypes.byref(key_start_buf),
                                                 ctypes.c_uint64(width),
                                                 bool(key_start_inclusive))

        if key_end is not None:
            if index_type.width:
                width = index_type.width
                key_end_buf = index_type.ctype(key_end)
            else:
                # variable-length key type (STRING)
                key_end_buf = ctypes.create_string_buffer(key_end)
                width = len(key_end)
            self.so.rc_range_query_set_key_end(args, ctypes.byref(key_end_buf),
                                               ctypes.c_uint64(width),
                                               ctypes.c_int(bool(key_end_inclusive)))
        if start_following_oid is not None:
            self.so.rc_range_query_set_start_following_oid(args,
                    ctypes.c_uint64(start_following_oid))

        more = ctypes.c_int()
        count = ctypes.c_uint32(limit)
        oids = (ctypes.c_uint64 * limit)()
        oids_buf_len = ctypes.c_uint64(limit * 8)
        if index_type.width:
            keys = (index_type.ctype * limit)()
            keys_buf_len = ctypes.c_uint64(index_type.width * limit)
        else:
            # variable-length key type (STRING)
            keys = ctypes.create_string_buffer(256 * limit)
            keys_buf_len = ctypes.c_uint64(len(keys))
            # TODO(ongaro): should we limit strings to 255 characters in general?
        self.so.rc_range_query_set_result_bufs(args, ctypes.byref(count),
                                               ctypes.byref(oids), ctypes.byref(oids_buf_len),
                                               ctypes.byref(keys), ctypes.byref(keys_buf_len),
                                               ctypes.byref(more))
        r = self.so.rc_range_query(ctypes.byref(self.client), args)
        self.so.rc_range_query_args_free(args)
        if r != 0:
            self.raise_error()

        pairs = [] # (key, oid)
        addr = ctypes.addressof(keys)
        for i in range(count.value):
            if index_type.width:
                key = keys[i]
            else:
                # variable-length key type (STRING)
                l = ctypes.c_uint8.from_address(addr).value
                addr += 1
                buf = ctypes.create_string_buffer(l)
                ctypes.memmove(ctypes.addressof(buf), addr, l)
                addr += l
                key = buf.value
            oid = oids[i]
            pairs.append((key, oid))
        return (pairs, bool(more.value))

def main():
    r = RAMCloud()
    r.connect()
    print r.client
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.open_table("test")
    print "with id %s" % table

    index_id = r.create_index(table, RCRPC_INDEX_TYPE.UINT64, True, True)
    print "Created index id %d" % index_id
    str_index_id = r.create_index(table, RCRPC_INDEX_TYPE.STRING, True, True)
    print "Created index id %d" % str_index_id
    multi_index_id = r.create_index(table, RCRPC_INDEX_TYPE.SINT32, False, True)
    print "Created index id %d" % multi_index_id

    r.write(table, 0, "Hello, World, from Python", [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4592)),
        (str_index_id, (RCRPC_INDEX_TYPE.STRING, "write")),
        (multi_index_id, (RCRPC_INDEX_TYPE.SINT32, 2)),
    ])
    print "Inserted to table"
    value, indexes = r.read_full(table, 0)
    print value
    print indexes
    key = r.insert(table, "test", [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4723)),
        (str_index_id, (RCRPC_INDEX_TYPE.STRING, "insert")),
        (multi_index_id, (RCRPC_INDEX_TYPE.SINT32, 2)),
    ])
    print "Inserted value and got back key %d" % key
    r.write(table, key, "test", [
        (index_id, (RCRPC_INDEX_TYPE.UINT64, 4899)),
        (str_index_id, (RCRPC_INDEX_TYPE.STRING, "rewrite")),
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
                                index_type=RCRPC_INDEX_TYPE.STRING,
                                limit=5, key_start="m")
    print pairs, more

    print r.unique_lookup(table_id=table, index_id=str_index_id,
                          index_type=RCRPC_INDEX_TYPE.STRING, key="rewrite")

    oids, more = r.multi_lookup(table_id=table, index_id=multi_index_id,
                                index_type=RCRPC_INDEX_TYPE.SINT32,
                                limit=10, key=2)
    print oids, more

    r.drop_index(table, str_index_id)
    r.drop_index(table, index_id)
    r.drop_table("test")

if __name__ == '__main__': main()
