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
                buf_len += len(data) + 1
        return buf_len

    def _indexes_fill(self, addr, indexes):
        for index_id, (index_type, data) in indexes:

            # len
            if index_type.width:
                ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint64(index_type.width)), 8)
                addr += 8
            else: # string
                ctypes.memmove(addr, ctypes.addressof(ctypes.c_uint64(len(data) + 1)), 8)
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
                ctypes.memmove(addr, ctypes.addressof(ctypes.create_string_buffer(data)), len(data) + 1)
                addr += len(data) + 1 # data

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

def main():
    r = RAMCloud()
    r.connect()
    print r.client
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.open_table("test")
    print "with id %s" % table

    index_id = r.create_index(table, RCRPC_INDEX_TYPE.UINT16, True, False)
    print "Created index id %d" % index_id

    index_id2 = r.create_index(table, RCRPC_INDEX_TYPE.STRING, False, True)
    print "Created index2 id %d" % index_id2

    r.write(table, 0, "Hello, World, from Python", [
        (index_id, (RCRPC_INDEX_TYPE.UINT16, 4592)),
        (index_id2, (RCRPC_INDEX_TYPE.STRING, 'roflctoper'))
    ])
    print "Wrote key 0 to table"
    value, indexes = r.read_full(table, 0)
    print value
    print indexes
    key = r.insert(table, "test")
    print "Inserted value and got back key %d" % key

    r.drop_index(table, index_id)
    r.drop_index(table, index_id2)
    r.drop_table("test")

if __name__ == '__main__': main()
