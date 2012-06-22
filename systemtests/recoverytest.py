#!/usr/bin/env python

# Copyright (c) 2012 Stanford University
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

from __future__ import division, print_function
from ramcloudtest import *
import ramcloud
import cluster

class RecoveryTestCase(ContextManagerTestCase):
    def __enter__(self):
        require_hosts(7)
        self.cluster = cluster.Cluster()
        self.cluster.__enter__()

        try:
            self.cluster.start_coordinator(hosts[0])
            for host in hosts[:7]:
                self.cluster.start_server(host)
            self.cluster.ensure_servers()

            self.rc = ramcloud.RAMCloud()
            self.rc.connect(self.cluster.coordinator_locator)

            self.rc.create_table('test')
            self.table = self.rc.get_table_id('test')
            self.rc.write(self.table, 'testKey', 'testValue')
        except:
            self.cluster.__exit__()
            raise
        return self

    def __exit__(self, *args):
        self.cluster.__exit__()
        return False # rethrow exception, if any

    @timeout()
    def test_simple_recovery(self):
        """Store a value on a master, crash that master, wait for recovery,
        then read a value from the recovery master.
        """
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_kill(0, '0')
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_600M_recovery(self):
        """Store 600 MB of objects on a master, crash that master,
        wait for recovery, then read a value from the recovery master.
        """
        self.assertEqual(0, self.table)
        self.rc.testing_fill(self.table, '0', 592415, 1024)
        expectedValue = (chr(0xcc) * 1024, 2)
        value = self.rc.read(self.table, '0')
        self.assertEqual(expectedValue, value)
        self.rc.testing_kill(0, '0')
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, '0')
        self.assertEqual(expectedValue, value)

    @timeout()
    def test_recovery_master_failure(self):
        """Cause a recovery where one of the recovery masters fails which
        is remedied by a follow up recovery.
        """
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '1')
        self.rc.testing_kill(0, '0')
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_repeated_recovery_master_failures(self):
        """Cause a recovery where one of the recovery masters fails which,
        the followup recovery has its recovery master fail as well, then
        on the third recovery things work out.
        """
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '1 1')
        self.rc.testing_kill(0, '0')
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_multiple_recovery_master_failures(self):
        """Cause a recovery where two of the recovery masters fail which
        is remedied by a follow up recovery.
        TODO(stutsman): This test doesn't work right yet because the
        original master currently only has one table on it.
        """
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '2')
        self.rc.testing_kill(0, '0')
        self.rc.testing_fill(0, '0', 1000, 1000)
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_only_one_recovery_master_for_many_partitions(self):
        """Cause a recovery when there is only one recovery master available
        and make sure that eventually all of the partitions of the will are
        recovered on that recovery master.
        """
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '2')
        self.rc.testing_kill(0, '0')
        self.rc.testing_fill(0, '0', 1000, 1000)
        self.rc.testing_wait_for_all_tablets_normal()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

def removeAllTestsExcept(klass, name):
    for k in dir(klass):
        if k.startswith('test') and not k == name:
            delattr(klass, k)

import unittest
suite = unittest.TestLoader().loadTestsFromTestCase(RecoveryTestCase)

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 0:
        removeAllTestsExcept(RecoveryTestCase, sys.argv[1])
        suite = unittest.TestLoader().loadTestsFromTestCase(RecoveryTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

