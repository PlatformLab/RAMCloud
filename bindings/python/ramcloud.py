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

class RAMCloud(object):
    def __init__(self):
        path = find_library('ramcloud')
        if not path:
            raise """Couldn't find libramcloud.so, ensure it is
        installed that you have registered it with /sbin/ldconfig"""

        self.so = ctypes.cdll.LoadLibrary(path)
        self.client = CLIENT()

    def connect(self):
        r = self.so.rc_connect(ctypes.byref(self.client))
        if r != 0:
            raise "error connecting"
    
    def ping(self):
        r = self.so.rc_ping(ctypes.byref(self.client))
        if r != 0:
            raise "error creating table"

    def write(self, table_id, key, data):
        r = self.so.rc_write(ctypes.byref(self.client),
                             int(table_id),
                             int(key),
                             data,
                             len(data))
        if r != 0:
            raise "error reading"

    def insert(self, table_id, data):
        key = ctypes.c_uint()
        r = self.so.rc_insert(ctypes.byref(self.client),
                             int(table_id),
                             data,
                             len(data),
                             ctypes.byref(key))
        if r != 0:
            raise "error reading"
        return key.value

    def read(self, table_id, key):
        buf = ctypes.create_string_buffer(2048)
        l = ctypes.c_int()
        r = self.so.rc_read(ctypes.byref(self.client),
                            int(table_id),
                            int(key),
                            ctypes.byref(buf),
                            ctypes.byref(l))
        if r != 0:
            raise "error reading"
        return buf.value[0:l.value]

    def create_table(self, name):
        r = self.so.rc_create_table(ctypes.byref(self.client), name)
        if r != 0:
            raise "error creating table"
        return r

    def open_table(self, name):
        r = self.so.rc_open_table(ctypes.byref(self.client), name)
        if r != 0:
            raise "error opening table"
        return r

    def drop_table(self, name):
        r = self.so.rc_drop_table(ctypes.byref(self.client), name)
        if r != 0:
            raise "error dropping table"

def main():
    r = RAMCloud()
    r.connect()
    print r.client
    r.ping()
    table = r.create_table("test")
    print "Created table with id %s" % table
    r.write(table, 0, "Hello, World, from Python")
    value = r.read(table, 0)
    print value
    key = r.insert(table, "test")
    print "Inserted value and got back key %d" % key
    r.drop_table("test")

if __name__ == '__main__': main()
