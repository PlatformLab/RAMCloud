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

class RejectRules(ctypes.Structure):
    _fields_ = [("object_doesnt_exist", ctypes.c_uint8, 1),
                ("object_exists", ctypes.c_uint8, 1),
                ("version_eq_given", ctypes.c_uint8, 1),
                ("version_gt_given", ctypes.c_uint8, 1),
                ("given_version", ctypes.c_uint64)]

    @staticmethod
    def exactly(want_version):
        return RejectRules(object_doesnt_exist=True, version_gt_given=True,
                           given_version=want_version)

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
        if result == -1:
            # If and when rc_last_error() takes a client pointer,
            # we can use arguments[0].
            msg = so.rc_last_error()
            raise RCException(msg)
        return result

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
    err                 = ctypes.c_int
    error_msg           = ctypes.c_char_p
    key                 = ctypes.c_uint64
    len                 = ctypes.c_uint64
    name                = ctypes.c_char_p
    reject_rules        = POINTER(RejectRules)
    table               = ctypes.c_uint64
    version             = ctypes.c_uint64

    so.rc_connect.argtypes = [client]
    so.rc_connect.restype  = err
    so.rc_connect.errcheck = int_errcheck

    so.rc_disconnect.argtypes = [client]
    so.rc_disconnect.restype  = None

    so.rc_ping.argtypes = [client]
    so.rc_ping.restype  = err
    so.rc_ping.errcheck = int_errcheck

    so.rc_write.argtypes = [client, table, key, reject_rules, POINTER(version),
                            buf, len]
    so.rc_write.restype  = err
    so.rc_write.errcheck = int_errcheck

    so.rc_insert.argtypes = [client, table, buf, len, POINTER(key)]
    so.rc_insert.restype  = err
    so.rc_insert.errcheck = int_errcheck

    so.rc_delete.argtypes = [client, table, key, reject_rules,
                             POINTER(version)]
    so.rc_delete.restype  = err
    so.rc_delete.errcheck = int_errcheck

    so.rc_read.argtypes = [client, table, key, reject_rules, POINTER(version),
                           buf, POINTER(len)]
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

    so.rc_new.argtypes = []
    so.rc_new.restype  = client
    so.rc_new.errcheck = malloc_errcheck

    so.rc_free.argtypes = [client]
    so.rc_free.restype  = None

    so.rc_last_error.argtypes = []
    so.rc_last_error.restype  = error_msg

    return so

def _ctype_copy(addr, var, width):
    ctypes.memmove(addr, ctypes.addressof(var), width)
    return addr + width

class RCException(Exception):
    pass

class NoObjectError(Exception):
    pass

class ObjectExistsError(Exception):
    pass

class VersionError(Exception):
    def __init__(self, want_version, got_version):
        Exception.__init__(self, "Bad version: want %d but got %d" %
                (want_version, got_version))
        self.want_version = want_version
        self.got_version = got_version

class FabricatedVersionError(Exception):
    def __init__(self, want_version, got_version):
        Exception.__init__(self, "Fabricated version: requested %d but at %d" %
                (want_version, got_version))
        self.want_version = want_version
        self.got_version = got_version

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

    def write_rr(self, table_id, key, data, reject_rules):
        got_version = ctypes.c_uint64()
        r = so.rc_write(self.client, table_id, key, ctypes.byref(reject_rules),
                        ctypes.byref(got_version), data, len(data))
        assert r in range(6)
        if r == 1: raise NoObjectError()
        if r == 2: raise ObjectExistsError()
        if r == 3 or r == 4: raise VersionError(want_version, got_version.value)
        if r == 5: raise FabricatedVersionError(want_version, got_version.value)
        return got_version.value

    def write(self, table_id, key, data, want_version=None):
        if want_version:
            reject_rules = RejectRules(version_gt_given=True,
                                       given_version=want_version)
        else:
            reject_rules = RejectRules()
        return self.write_rr(table_id, key, data, reject_rules)

    def create(self, table_id, key, data):
        reject_rules = RejectRules(object_exists=True)
        return self.write_rr(table_id, key, data, reject_rules)

    def update(self, table_id, key, data, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.write_rr(table_id, key, data, reject_rules)

    def insert(self, table_id, data):
        key = ctypes.c_uint64()
        so.rc_insert(self.client, table_id, data, len(data), ctypes.byref(key))
        return key.value

    def delete_rr(self, table_id, key, reject_rules):
        got_version = ctypes.c_uint64()
        r = so.rc_delete(self.client, table_id, key,
                         ctypes.byref(reject_rules), ctypes.byref(got_version))
        assert r in range(6)
        if r == 1: raise NoObjectError()
        if r == 2: raise ObjectExistsError()
        if r == 3 or r == 4: raise VersionError(want_version, got_version.value)
        if r == 5: raise FabricatedVersionError(want_version, got_version.value)

    def delete(self, table_id, key, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.delete_rr(table_id, key, reject_rules)

    def read_rr(self, table_id, key, reject_rules):
        buf = ctypes.create_string_buffer(10240)
        l = ctypes.c_uint64()
        got_version = ctypes.c_uint64()
        reject_rules.object_doesnt_exist = True
        r = so.rc_read(self.client, table_id, key, ctypes.byref(reject_rules),
                       ctypes.byref(got_version), ctypes.byref(buf),
                       ctypes.byref(l))
        assert r in range(6)
        if r == 1: raise NoObjectError()
        if r == 2: raise ObjectExistsError()
        if r == 3 or r == 4: raise VersionError(want_version, got_version.value)
        if r == 5: raise FabricatedVersionError(want_version, got_version.value)
        return (buf.raw[0:l.value], got_version.value)

    def read(self, table_id, key, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.read_rr(table_id, key, reject_rules)

    def create_table(self, name):
        so.rc_create_table(self.client, name)

    def open_table(self, name):
        handle = ctypes.c_uint64()
        so.rc_open_table(self.client, name, ctypes.byref(handle))
        return handle.value

    def drop_table(self, name):
        so.rc_drop_table(self.client, name)

def main():
    r = RAMCloud()
    print "Client: 0x%x" % r.client
    r.connect()
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.open_table("test")
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

so = load_so()

if __name__ == '__main__':
    main()
