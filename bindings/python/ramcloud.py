# Copyright (c) 2009-2013 Stanford University
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
import os

class RejectRules(ctypes.Structure):
    _fields_ = [("given_version", ctypes.c_uint64),
                ("object_doesnt_exist", ctypes.c_uint8, 8),
                ("object_exists", ctypes.c_uint8, 8),
                ("version_eq_given", ctypes.c_uint8, 8),
                ("version_gt_given", ctypes.c_uint8, 8),
                ]

    def _as_tuple(self):
        return (self.object_doesnt_exist, self.object_exists,
                self.version_eq_given, self.version_gt_given,
                self.given_version)

    def __cmp__(self, other):
        return cmp(self._as_tuple(), other._as_tuple())

    def __repr__(self):
        return 'ramcloud.RejectRules(%s)' % str(self._as_tuple())

    @staticmethod
    def exactly(want_version):
        return RejectRules(object_doesnt_exist=True, version_gt_given=True,
                           given_version=want_version)


def load_so():
    not_found = ImportError("Couldn't find libramcloud.so, ensure it is " +
                            "installed and that you have registered it with " +
                            "/sbin/ldconfig")

    # try to find the overridden path first, if possible using
    # LD_LIBRARY_PATH which means we don't have to install the so
    # during devel

    path = None
    if 'LD_LIBRARY_PATH' in os.environ:
        for search_dir in os.environ['LD_LIBRARY_PATH'].split(':'):
            test_path = os.path.join(search_dir, 'libramcloud.so')
            if os.path.exists(test_path):
                path = test_path
                break

    # couldn't find the so in LD_LIBRARY_PATH, so try the usual approach
    if not path:
        path = find_library('ramcloud')

    if not path:
        raise not_found
    try:
        so = ctypes.cdll.LoadLibrary(path)
    except OSError, e:
        if 'No such file or directory' in str(e):
            raise not_found
        else:
            raise

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
    address             = ctypes.c_char_p
    buf                 = ctypes.c_void_p
    client              = ctypes.c_void_p
    key                 = ctypes.c_char_p
    keyLength           = ctypes.c_uint16
    len                 = ctypes.c_uint32
    name                = ctypes.c_char_p
    nanoseconds         = ctypes.c_uint64
    nonce               = ctypes.c_uint64
    rejectRules         = POINTER(RejectRules)
    serviceLocator      = ctypes.c_char_p
    status              = ctypes.c_int
    table               = ctypes.c_uint64
    version             = ctypes.c_uint64
    serverId            = ctypes.c_uint64

    so.rc_connect.argtypes = [address, address, POINTER(client)]
    so.rc_connect.restype  = status

    so.rc_disconnect.argtypes = [client]
    so.rc_disconnect.restype  = None

    so.rc_createTable.argtypes = [client, name]
    so.rc_createTable.restype  = status

    so.rc_dropTable.argtypes = [client, name]
    so.rc_dropTable.restype  = status

    so.rc_getTableId.argtypes = [client, name, POINTER(table)]
    so.rc_getTableId.restype  = status

    so.rc_read.argtypes = [client, table, key, keyLength, rejectRules, POINTER(version),
                           buf, len, POINTER(len)]
    so.rc_read.restype  = status

    so.rc_remove.argtypes = [client, table, key, keyLength, rejectRules,
                             POINTER(version)]
    so.rc_remove.restype  = status

    so.rc_write.argtypes = [client, table, key, keyLength, buf, len, rejectRules,
                            POINTER(version)]
    so.rc_write.restype  = status

    so.rc_testing_kill.argtypes = [client, table, key, keyLength]
    so.rc_testing_kill.restype  = status

    so.rc_testing_fill.argtypes = [client, table, key, keyLength,
                                   ctypes.c_uint32, ctypes.c_uint32]
    so.rc_testing_fill.restype  = status

    so.rc_testing_get_server_id.argtypes = [client, table, key, keyLength,
                                            POINTER(serverId)]
    so.rc_testing_get_server_id.restype  = status

    so.rc_testing_get_service_locator.argtypes = [client, table, key, keyLength,
                                                  serviceLocator, ctypes.c_size_t]
    so.rc_testing_get_service_locator.restype  = status

    so.rc_set_runtime_option.argtypes = [client,
                                                 ctypes.c_char_p,
                                                 ctypes.c_char_p]
    so.rc_set_runtime_option.restype = status

    so.rc_testing_wait_for_all_tablets_normal.argtypes = [client, table,
                                                          nanoseconds]
    so.rc_testing_wait_for_all_tablets_normal.restype = None

    so.rc_set_log_file.argtypes = [ctypes.c_char_p]
    so.rc_set_log_file.restype = None

    return so

def _ctype_copy(addr, var, width):
    ctypes.memmove(addr, ctypes.addressof(var), width)
    return addr + width

def get_key(id):
    if type(id) is int:
        return str(id)
    else:
        return id

def get_keyLength(id):
    if type(id) is int:
        return len(str(id))
    else:
        return len(id)

class RCException(Exception):
    def __init__(self, status):
        Exception.__init__(self, 'RAMCloud error ' + str(status))
        self.status = status
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

class RAMCloud(object):
    def __init__(self):
        self.client = ctypes.c_void_p()
        self.hook = lambda: None

    def __del__(self):
        if self.client.value != None:
            so.rc_disconnect(self.client)

    def handle_error(self, status, actual_version=0):
        if status == 0:
            return
        if status == 2:
            raise NoObjectError()
        if status == 3:
            raise ObjectExistsError()
        if status == 4:
            raise VersionError(reject_rules.given_version, actual_version)
        raise RCException(status)

    def connect(self, serverLocator='fast+udp:host=127.0.0.1,port=12242',
                clusterName='main'):
        s = so.rc_connect(serverLocator, clusterName,
                          ctypes.byref(self.client))
        self.handle_error(s)

    def create(self, table_id, id, data):
        reject_rules = RejectRules(object_exists=True)
        return self.write_rr(table_id, id, data, reject_rules)

    def create_table(self, name, serverSpan = 1):
        s = so.rc_createTable(self.client, name, serverSpan)
        self.handle_error(s)

    def delete(self, table_id, id, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.delete_rr(table_id, id, reject_rules)

    def delete_rr(self, table_id, id, reject_rules):
        got_version = ctypes.c_uint64()
        self.hook()
        s = so.rc_remove(self.client, table_id, get_key(id), get_keyLength(id),
                         ctypes.byref(reject_rules), ctypes.byref(got_version))
        self.handle_error(s, got_version.value)
        return got_version.value

    def drop_table(self, name):
        s = so.rc_dropTable(self.client, name)
        self.handle_error(s)

    def get_table_id(self, name):
        handle = ctypes.c_uint64()
        s = so.rc_getTableId(self.client, name, ctypes.byref(handle))
        self.handle_error(s)
        return handle.value

    def ping(self, serviceLocator, nonce, nanoseconds):
        result = ctypes.c_uint64();
        s = so.rc_ping(self.client, serviceLocator, nonce, nanoseconds,
                       ctypes.byref(result))
        self.handle_error(s)
        return result

    def read(self, table_id, id, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.read_rr(table_id, id, reject_rules)

    def read_rr(self, table_id, id, reject_rules):
        max_length = 1024 * 1024 * 2
        buf = ctypes.create_string_buffer(max_length)
        actual_length = ctypes.c_uint32()
        got_version = ctypes.c_uint64()
        reject_rules.object_doesnt_exist = True
        self.hook()
        s = so.rc_read(self.client, table_id, get_key(id), get_keyLength(id),
                       ctypes.byref(reject_rules),
                       ctypes.byref(got_version), ctypes.byref(buf), max_length,
                       ctypes.byref(actual_length))
        self.handle_error(s, got_version.value)
        return (buf.raw[0:actual_length.value], got_version.value)

    def update(self, table_id, id, data, want_version=None):
        if want_version:
            reject_rules = RejectRules.exactly(want_version)
        else:
            reject_rules = RejectRules(object_doesnt_exist=True)
        return self.write_rr(table_id, id, data, reject_rules)

    def write(self, table_id, id, data, want_version=None):
        if want_version:
            reject_rules = RejectRules(version_gt_given=True,
                                       given_version=want_version)
        else:
            reject_rules = RejectRules()
        return self.write_rr(table_id, id, data, reject_rules)

    def write_rr(self, table_id, id, data, reject_rules):
        got_version = ctypes.c_uint64()
        self.hook()
        s = so.rc_write(self.client, table_id, get_key(id), get_keyLength(id),
                        data, len(data),
                        ctypes.byref(reject_rules), ctypes.byref(got_version))
        self.handle_error(s, got_version.value)
        return got_version.value

    def testing_kill(self, table_id, id):
        s = so.rc_testing_kill(self.client, table_id,
                               get_key(id), get_keyLength(id))
        self.handle_error(s)

    def testing_fill(self, table_id, id, object_count, object_size):
        s = so.rc_testing_fill(self.client, table_id,
                               get_key(id), get_keyLength(id),
                               object_count, object_size)
        self.handle_error(s)

    def testing_get_server_id(self, table_id, id):
        cserver_id = ctypes.c_uint64()
        s = so.rc_testing_get_server_id(self.client, table_id, get_key(id),
                                        get_keyLength(id),
                                        ctypes.byref(cserver_id))
        self.handle_error(s)
        return cserver_id.value

    def testing_get_service_locator(self, table_id, id):
        max_len = 128
        buffer = ctypes.create_string_buffer(max_len)
        s = so.rc_testing_get_service_locator(self.client,
                                              table_id, get_key(id),
                                              get_keyLength(id),
                                              buffer, max_len)
        self.handle_error(s)
        return buffer.value

    def testing_set_runtime_option(self, option, value):
        so.rc_set_runtime_option(self.client, option, value)

    def testing_wait_for_all_tablets_normal(self, table_id, timeoutNs=2**64 - 1):
        so.rc_testing_wait_for_all_tablets_normal(self.client, table_id, timeoutNs)

    def set_log_file(self, path):
        so.rc_set_log_file(path)

def main():
    r = RAMCloud()
    r.connect()
    print "Client: 0x%x" % r.client.value
    r.ping()

    r.create_table("test")
    print "Created table 'test'",
    table = r.get_table_id("test")
    print "with id %s" % table

    r.create(table, 0, "Hello, World, from Python")
    print "Created object 0 in table"
    value, got_version = r.read(table, 0)
    print value
    id = r.insert(table, "test")
    print "Inserted value and got back id %d" % id
    print "Value read back: %s" % r.read(table, id)[0]
    r.update(table, id, "test")

    bs = "binary\00safe?"
    oid = r.insert(table, bs)
    value = r.read(table, oid)[0]
    assert value == bs

    r.drop_table("test")

    # these don't belong here, but they're testing a bug in which table names
    # were truncated to 8 characters
    r.create_table("01234567890123456789A")
    r.create_table("01234567890123456789B")
    assert (r.get_table_id("01234567890123456789A") !=
            r.get_table_id("01234567890123456789B"))
    r.drop_table("01234567890123456789A")
    r.drop_table("01234567890123456789B")

so = load_so()

if __name__ == '__main__':
    main()
