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
        num_hosts = 8
        require_hosts(num_hosts)
        self.servers = []
        self.cluster = cluster.Cluster()
        self.cluster.log_level = 'DEBUG'
        self.cluster.transport = 'infrc'
        self.cluster.__enter__()

        try:
            self.cluster.start_coordinator(hosts[0])
            for host in hosts[:num_hosts]:
                self.servers.append(self.cluster.start_server(host))
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
    def test_multiple_recovery_master_failures_in_one_recovery(self):
        """Cause a recovery where two of the recovery masters fail which
        is remedied by a follow up recovery.
        """
        # One table already created.
        # Stroke the round-robin table creation until we get back to the
        # server we care about.
        for t in range(len(self.servers) - 1):
            self.rc.create_table('junk%d' % t)
        self.rc.create_table('test2')
        table2 = self.rc.get_table_id('test2')
        self.rc.write(table2, 'testKey', 'testValue2')

        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        value = self.rc.read(table2, 'testKey')
        self.assertEqual(('testValue2', 1), value)

        self.rc.testing_set_runtime_option('failRecoveryMasters', '2')
        self.rc.testing_kill(0, '0')
        self.rc.testing_wait_for_all_tablets_normal()

        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        value = self.rc.read(table2, 'testKey')
        self.assertEqual(('testValue2', 1), value)

    @timeout()
    def test_only_one_recovery_master_for_many_partitions(self):
        """Cause a recovery when there is only one recovery master available
        and make sure that eventually all of the partitions of the will are
        recovered on that recovery master.
        """
        pass

    @timeout()
    def test_one_backup_fails_during_recovery(self):
        # Create a second table, due to round robin this will be on a different
        # server than the first.
        self.rc.create_table('elsewhere')
        self.elsewhereTable = self.rc.get_table_id('elsewhere')

        # Ensure the key was stored ok.
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

        # Kill a backup.
        self.rc.testing_kill(self.elsewhereTable, '0')

        # Crash the master
        self.rc.testing_kill(self.table, '0')
        self.rc.testing_wait_for_all_tablets_normal()

        # Ensure the key was recovered.
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

