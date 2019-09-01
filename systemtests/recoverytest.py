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
import log
import time

def extractLocatorFromCommand(command):
    """Given an command line to start a RAMCloud server return the
    service locator specified for that server.
    """
    tokens = command.split()
    dashL = tokens.index('-L')
    locator = tokens[dashL + 1]
    return locator

def sync():
    """Decorator which can be applied to functions which adds a field 'sync'
    to them. Recovery tests use this to determine if the servers should be
    started with the --sync mode flag for backups.
    """
    def decorate(f):
        def new_f(*args, **kwargs):
            return f(*args, **kwargs)
        new_f.func_name = f.func_name
        new_f.sync = True
        return new_f
    return decorate

class RecoveryTestCase(ContextManagerTestCase):
    def __enter__(self):
        self.last_unused_port = 12247
        import random
        self.clusterName = ''.join([chr(random.choice(range(ord('a'), ord('z'))))
                                    for c in range(8)])
        self.num_hosts = 8
        require_hosts(self.num_hosts)
        self.servers = []
        self.cluster = cluster.Cluster()
        #self.cluster.verbose = True
        self.cluster.enable_logcabin = False
        self.cluster.log_level = 'DEBUG'
        self.cluster.transport = 'infrc'
        self.cluster.__enter__()

        try:
            self.cluster.start_coordinator(hosts[0])
            # Hack below allows running with an existing coordinator
            #self.cluster.coordinator_host = hosts[0]
            #self.cluster.coordinator_locator = cluster.coord_locator(self.cluster.transport,
            #                                                         self.cluster.coordinator_host)
            syncArgs = ''
            if hasattr(getattr(self, self._testMethodName), 'sync'):
                syncArgs = '--sync'
            for host in hosts[:self.num_hosts]:
                self.servers.append(
                    self.cluster.start_server(
                        host, args=syncArgs))
                # Hack below can be used to use different ports for all servers
                #self.servers.append(
                #    self.cluster.start_server(host,
                #        port=self.last_unused_port,
                #        args='--clusterName=%s' % self.clusterName))
                #self.last_unused_port += 1
            self.cluster.ensure_servers()

            self.rc = ramcloud.RAMCloud()
            print('%s ... ' % self.cluster.log_subdir, end='', file=sys.stderr)
            self.rc.set_log_file(os.path.join(self.cluster.log_subdir,
                                              'client.log'))
            self.rc.connect(self.cluster.coordinator_locator)
        except:
            self.cluster.__exit__()
            raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is self.failureException:
            log_info = log.scan(self.cluster.log_subdir, ["WARNING", "ERROR"])
            if len(log_info) > 0:
                print(log_info)
        self.cluster.__exit__()
        return False # rethrow exception, if any

    def createTestValue(self):
        self.rc.create_table('test')
        self.table = self.rc.get_table_id('test')
        self.rc.write(self.table, 'testKey', 'testValue')

    @timeout()
    def test_01_simple_recovery(self):
        """Store a value on a master, crash that master, wait for recovery,
        then read a value from the recovery master.
        """
        self.createTestValue()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_kill(self.table, 'testKey')
        self.rc.testing_wait_for_all_tablets_normal(self.table)
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_02_200M_recovery(self):
        """Store 200 MB of objects on a master, crash that master,
        wait for recovery, then read a value from the recovery master.
        """
        self.createTestValue()
        self.assertEqual(1, self.table)
        self.rc.testing_fill(self.table, '0', 197650, 1024)
        expectedValue = (chr(0xcc) * 1024, 2)
        value = self.rc.read(self.table, '0')
        self.assertEqual(expectedValue, value)
        self.rc.testing_kill(self.table, '0')
        self.rc.testing_wait_for_all_tablets_normal(self.table)
        value = self.rc.read(self.table, '0')
        self.assertEqual(expectedValue, value)

    @timeout()
    def test_03_recovery_master_failure(self):
        """Cause a recovery where one of the recovery masters fails which
        is remedied by a follow up recovery.
        """
        self.createTestValue()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '1')
        self.rc.testing_kill(self.table, 'testKey')
        self.rc.testing_wait_for_all_tablets_normal(self.table)
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def test_04_repeated_recovery_master_failures(self):
        """Cause a recovery where one of the recovery masters fails which,
        the followup recovery has its recovery master fail as well, then
        on the third recovery things work out.
        """
        self.createTestValue()
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        self.rc.testing_set_runtime_option('failRecoveryMasters', '1 1')
        self.rc.testing_kill(self.table, 'testKey')
        self.rc.testing_wait_for_all_tablets_normal(self.table)
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout(30)
    def test_05_multiple_recovery_master_failures_in_one_recovery(self):
        """Cause a recovery where two of the recovery masters fail which
        is remedied by a follow up recovery.
        """
        self.createTestValue()
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
        self.assertEqual(('testValue2', 2), value)

        self.rc.testing_set_runtime_option('failRecoveryMasters', '2')
        self.rc.testing_kill(self.table, 'testKey')
        self.rc.testing_wait_for_all_tablets_normal(self.table)

        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)
        value = self.rc.read(table2, 'testKey')
        self.assertEqual(('testValue2', 2), value)

    @timeout()
    def test_06_one_backup_fails_during_recovery(self):
        # Create a second table, due to round robin this will be on a different
        # server than the first.
        self.createTestValue()
        self.rc.create_table('elsewhere')
        self.elsewhereTable = self.rc.get_table_id('elsewhere')
        self.rc.write(self.elsewhereTable, 'elsewhereKey', 'testValue')

        # Ensure the key was stored ok.
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

        # Kill a backup.
        self.rc.testing_kill(self.elsewhereTable, 'elsewhereKey')

        # Crash the master
        self.rc.testing_kill(self.table, 'testKey')
        self.rc.testing_wait_for_all_tablets_normal(self.table)

        # Ensure the key was recovered.
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @timeout()
    def _test_07_only_one_recovery_master_for_many_partitions(self):
        """Cause a recovery when there is only one recovery master available
        and make sure that eventually all of the partitions of the will are
        recovered on that recovery master.
        """
        pass

    def addServerInfo(self, tables):
        """Augment a Sandbox.Process for a RAMCloud server with the service
        locator given to that server on start as well as the server id of
        that server, if available. A server id can only be associated with
        the process if it is serving some RAMCloud tablet.
        """
        server_ids = {}
        for table in tables:
            server_id = self.rc.testing_get_server_id(table, '0')
            locator = self.rc.testing_get_service_locator(table, '0')
            server_ids[locator] = server_id
        for server in self.servers:
            locator = extractLocatorFromCommand(server.command)
            server.service_locator = locator
            if locator in server_ids:
                server.server_id = server_ids[locator]

    def restart(self, process):
        """Kill process and restart a server on the same host with the same
        arguments."""
        self.cluster.sandbox.kill(process)
        self.servers.remove(process)
        host = process.host
        for h in hosts:
            if host == h[0]:
                host = h
                break
        self.servers.append(
            self.cluster.start_server(host,
                                      args='--clusterName=%s' % self.clusterName))
        # Hack below can be used to use different ports for all servers
        #self.servers.append(
        #    self.cluster.start_server(host,
        #        port=self.last_unused_port,
        #        args='--clusterName=%s' % self.clusterName))
        #self.last_unused_port += 1

    @timeout(180)
    def test_08_restart(self):
        # We'll want two flavors of this test, I think.
        # In this one backups re-enlist. Another test where they do not would
        # be interesting, as it should be stable as well as long as crashes
        # come slowly enough.
        print()
        self.createTestValue()
        self.rc.testing_fill(self.table, '0', 592950, 1024)
        print('Restarting ', end='')
        for x in range(20):
            server = self.servers[0]
            print(server.host, end= ' ')
            sys.stdout.flush()
            self.restart(server)
            time.sleep(5)
            self.rc.testing_wait_for_all_tablets_normal(self.table, 10 * 10**9)
            value = self.rc.read(self.table, 'testKey')
            self.assertEqual(('testValue', 1), value)
        print()

    @timeout(240)
    def _test_09_restart_large(self):
        #self.addServerInfo([self.table])

        # - Setup -
        # One table already created.
        # Stroke the round-robin table creation until we get back to the
        # server we care about.
        numObjectsPerTable = 592950
        objectSize = 1024
        timeToWaitForRecovery = 5
        numCrashes = 1

        tables = []
        for t in range(len(self.servers)):
            name = 'test%d' % t
            self.rc.create_table(name)
            table_id = self.rc.get_table_id(name)
            tables.append(table_id)
            print('Filling %s with %d objects of %d bytes' %
                    (name, numObjectsPerTable, objectSize))
            self.rc.testing_fill(table_id, '0', numObjectsPerTable, objectSize)
            self.rc.write(table_id, 'testKey', 'testValue')

        # - Restart a server at-a-time and check recovery -
        print('Restarting ')
        for x in range(numCrashes):
            server = self.servers[0]
            print('Restarting %s' % server.host)
            self.restart(server)
            time.sleep(timeToWaitForRecovery)
            self.rc.testing_wait_for_all_tablets_normal(self.table, 240 * 10**9)
            print('Doing test reads')
            for table in tables:
                # These messages may be stale since they precede the actual read.
                server_id = self.rc.testing_get_server_id(table, 'testKey')
                locator = self.rc.testing_get_service_locator(table, 'testKey')
                print('Key is on %d %s' % (server_id, locator))
                value = self.rc.read(table, 'testKey')
                self.assertEqual(('testValue', numObjectsPerTable + 1), value)
            print('Looks good')

        print('Waiting for GC to catch up')
        time.sleep(20)

    @sync()
    @timeout(120)
    def test_10_cold_start(self):
        # We're only cold-start-safe if data is synchronously written,
        # otherwise open replicas for the head won't be found on backup
        # restart.
        self.createTestValue()
        self.rc.testing_fill(self.table, '0', 592950, 1024)
        print('Killing all servers')
        for server in self.servers:
            self.cluster.sandbox.kill(server)
        self.servers = []
        print('Starting all new servers')
        for host in hosts[:self.num_hosts]:
            self.servers.append(
                self.cluster.start_server(host,
                    port=self.last_unused_port,
                    args='--clusterName=%s' % self.clusterName))
            self.last_unused_port += 1
        server_count = len(self.servers)
        print('Waiting for new servers to enlist')
        self.cluster.ensure_servers(server_count, server_count)
        print('Giving servers a little time to come back')
        time.sleep(10)
        print('Attempting read')
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @sync()
    @timeout(120)
    def test_11_cold_deadservers(self):
        # Cold start with two servers not restarted.
        # Since replicas are written to three backups, RAMClould should
        # restart.
        self.createTestValue()
        self.rc.testing_fill(self.table, '0', 592950, 1024)
        print('Killing all %d servers' % self.num_hosts)
        for server in self.servers:
            self.cluster.sandbox.kill(server)
        self.servers = []
        start_servers = self.num_hosts - 2
        print('Starting %d servers' % start_servers)
        for host in hosts[:start_servers]:
            self.servers.append(
                self.cluster.start_server(host,
                    port=self.last_unused_port,
                    args='--clusterName=%s' % self.clusterName))
            self.last_unused_port += 1
        server_count = len(self.servers)
        print('Waiting for new %d servers to enlist' % server_count)
        self.cluster.ensure_servers(server_count, server_count)
        print('Giving servers a little time to come back')
        time.sleep(10)
        print('Attempting read')
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

    @sync()
    @timeout(120)
    def test_12_cold_deadservers_large(self):
        # Cold start with two servers not restarted with larger
        # 1k of 600KB objects.
        # Since replicas are written to three backups, RAMClould should
        # restart.
        self.createTestValue()
        self.rc.testing_fill(self.table, '0', 1024, 614400)
        print('Killing all %d servers' % self.num_hosts)
        for server in self.servers:
            self.cluster.sandbox.kill(server)
        self.servers = []
        start_servers = self.num_hosts - 2
        print('Starting %d servers' % start_servers)
        for host in hosts[:start_servers]:
            self.servers.append(
                self.cluster.start_server(host,
                    port=self.last_unused_port,
                    args='--clusterName=%s' % self.clusterName))
            self.last_unused_port += 1
        server_count = len(self.servers)
        print('Waiting for new %d servers to enlist' % server_count)
        self.cluster.ensure_servers(server_count, server_count)
        print('Giving servers a little time to come back')
        time.sleep(10)
        print('Attempting read')
        value = self.rc.read(self.table, 'testKey')
        self.assertEqual(('testValue', 1), value)

#   No @sync() : asynchronous
    @timeout(120)
    def test_13_cold_deadservers_large_async(self):
        # Cold start with two servers not restarted with larger
        # 1k of 600KB objects.
        # Since replicas are written to three backups, RAMClould should
        # restart.
        self.createTestValue()
        self.rc.testing_fill(self.table, '0', 1024, 614400)
        print('Killing all %d servers' % self.num_hosts)
        for server in self.servers:
            self.cluster.sandbox.kill(server)
        self.servers = []
        start_servers = self.num_hosts - 2
        print('Starting %d servers' % start_servers)
        for host in hosts[:start_servers]:
            self.servers.append(
                self.cluster.start_server(host,
                    port=self.last_unused_port,
                    args='--clusterName=%s' % self.clusterName))
            self.last_unused_port += 1
        server_count = len(self.servers)
        print('Waiting for new %d servers to enlist' % server_count)
        self.cluster.ensure_servers(server_count, server_count)
        print('Giving servers a little time to come back')
        time.sleep(10)
        print('Attempting read')
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
    if len(sys.argv) > 1:
        removeAllTestsExcept(RecoveryTestCase, sys.argv[1])
        suite = unittest.TestLoader().loadTestsFromTestCase(RecoveryTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

