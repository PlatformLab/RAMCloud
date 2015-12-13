#!/usr/bin/env python

# Copyright (c) 2010-2015 Stanford University
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

"""Runs a RAMCloud.

Used to exercise a RAMCloud cluster (e.g., for performance measurements)
by running a collection of servers and clients.
"""

from __future__ import division, print_function
from common import *
import itertools
import log
import os
import random
import pprint
import re
import subprocess
import sys
import time
from optparse import OptionParser

# Locations of various RAMCloud executables.
coordinator_binary = '%s/coordinator' % obj_path
server_binary = '%s/server' % obj_path
ensure_servers_bin = '%s/ensureServers' % obj_path
# valgrind
valgrind_command = ''

# Info used to construct service locators for each of the transports
# supported by RAMCloud.  In some cases the locator for the coordinator
# needs to be different from that for the servers.
server_locator_templates = {
    'tcp': 'tcp:host=%(host)s,port=%(port)d',
    'tcp-1g': 'tcp:host=%(host1g)s,port=%(port)d',
    'basic+udp': 'basic+udp:host=%(host)s,port=%(port)d',
    'basic+udp-1g': 'basic+udp:host=%(host1g)s,port=%(port)d',
    'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
    'fast+udp-1g': 'fast+udp:host=%(host1g)s,port=%(port)d',
    'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
    'infrc': 'infrc:host=%(host)s,port=%(port)d',
    'basic+infud': 'basic+infud:host=%(host1g)s',
    'fast+infud': 'fast+infud:host=%(host1g)s',
    'unreliable+infud': 'unreliable+infud:host=%(host1g)s',
    'fast+infeth': 'fast+infeth:mac=00:11:22:33:44:%(id)02x',
    'unreliable+infeth': 'unreliable+infeth:mac=00:11:22:33:44:%(id)02x',
}
coord_locator_templates = {
    'tcp': 'tcp:host=%(host)s,port=%(port)d',
    'tcp-1g': 'tcp:host=%(host1g)s,port=%(port)d',
    'basic+udp': 'basic+udp:host=%(host)s,port=%(port)d',
    'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
    'basic+udp-1g': 'basic+udp:host=%(host1g)s,port=%(port)d',
    'fast+udp-1g': 'fast+udp:host=%(host1g)s,port=%(port)d',
    'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
    'infrc': 'infrc:host=%(host)s,port=%(port)d',
    # Coordinator uses udp even when rest of cluster uses infud
    # or infeth.
    'basic+infud': 'basic+udp:host=%(host)s,port=%(port)d',
    'fast+infud': 'fast+udp:host=%(host)s,port=%(port)d',
    'unreliable+infud': 'fast+udp:host=%(host)s,port=%(port)d',
    'fast+infeth': 'fast+udp:host=%(host)s,port=%(port)d',
    'unreliable+infeth': 'fast+udp:host=%(host)s,port=%(port)d',
}

def server_locator(transport, host, port=server_port):
    """Generate a service locator for a master/backup process.

    @param transport: A transport name (e.g. infrc, fast+udp, tcp, ...)
    @type  transport: C{str}

    @param host: A 3-tuple of (hostname, ip, id).
    @type  host: C{(str, str, int)}

    @param port: Port which should be part of the locator (if any).
                 Allows multiple services to be started on the same host.
    @type  port: C{int}

    @return: A service locator.
    @rtype: C{str}
    """
    locator = (server_locator_templates[transport] %
               {'host': host[1],
                'host1g': host[0],
                'port': port,
                'id': host[2]})
    return locator

def coord_locator(transport, host):
    """Generate a service locator for a coordinator process.

    @param transport: A transport name (e.g. infrc, fast+udp, tcp, ...)
    @type  transport: C{str}

    @param host: A 3-tuple of (hostname, ip, id).
    @type  host: C{(str, str, int)}

    @return: A service locator.
    @rtype: C{str}
    """
    locator = (coord_locator_templates[transport] %
               {'host': host[1],
                'host1g': host[0],
                'port': coordinator_port,
                'id': host[2]})
    return locator

class Cluster(object):
    """Helper context manager for scripting and coordinating RAMCloud on a
    cluster.  Useful for configuring and running experiments.  See run() for
    a simpler interface useful for running single-shot experiments where the
    cluster configuration is (mostly) static throughout the experiment.

    === Configuration/Defaults ===
    The fields below control various aspects of the run as well as some
    of the defaults that are used when creating processes in the cluster.
    Most users of this class will want to override some of these after
    an instance of Cluster is created but before any operations are
    performed with it.
    @ivar log_level: Log level to use for spawned servers. (default: NOTICE)
    @ivar verbose: If True then print progress of starting clients/servers.
                   (default: False)
    @ivar transport: Transport name to use for servers
                     (see server_locator_templates) (default: infrc).
    @ivar replicas: Replication factor to use for each log segment. (default: 3)
    @ivar disk: Server args for specifying the storage device to use for
                backups (default: default_disk1 taken from {,local}config.py).
    @ivar disjunct: Disjunct (not collocate) entities on each server.

    === Other Stuff ===
    @ivar coordinator: None until start_coordinator() is run, then a
                       Sandbox.Process corresponding to the coordinator process.
    @ivar servers: List of Sandbox.Process corresponding to each of the
                   server processes started with start_server().
    @ivar masters_started: Number of masters started via start_server(). Notice,
                           ensure_servers() uses this, so if you kill processes
                           in the cluster ensure this is consistent.
    @ivar backups_started: Number of backups started via start_server(). Notice,
                           ensure_servers() uses this, so if you kill processes
                           in the cluster ensure this is consistent.
    @ivar log_subdir: Specific directory where all the processes created by
                      this cluster will log.
    @ivar sandbox: Nested context manager that cleans up processes when the
                   the context of this cluster is exited.
    """

    def __init__(self, log_dir='logs', log_exists=False,
                    cluster_name_exists=False):
        """
        @param log_dir: Top-level directory in which to write log files.
                        A separate subdirectory will be created in this
                        directory for the log files from this run. This can
                        only be overridden by passing it to __init__() since
                        that method creates the subdirectory.
                        (default: logs)
        @param log_exists:
                        Indicates whether the log directory already exists.
                        This will be true for cluster objects that are
                        created after starting the clusterperf test.
                        (default: False)
        @param cluster_name_exists:
                        Indicates whether a cluster name already exists as
                        part of this test. Backups that are started/restarted
                        using the same cluster name will read data from the
                        replicas
                        (default: False)
        """
        self.log_level = 'NOTICE'
        self.verbose = False
        self.transport = 'infrc'
        self.replicas = 3
        self.disk = default_disk1
        self.disjunct = False

        if cluster_name_exists: # do nothing if it exists
            self.cluster_name = None
            if self.verbose:
                print ('Cluster name exists')
        else:
            self.cluster_name = 'cluster_' +  ''.join([chr(random.choice(
                                 range(ord('a'), ord('z'))))
                                    for c in range(20)])
        if self.verbose:
            print ('Cluster name is %s' % (self.cluster_name))

        self.coordinator = None
        self.next_server_id = 1
        self.next_client_id = 1
        self.masters_started = 0
        self.backups_started = 0

        self.coordinator_host= getHosts()[0]
        self.coordinator_locator = coord_locator(self.transport,
                                                 self.coordinator_host)
        self.log_subdir = log.createDir(log_dir, log_exists)

        # Create a perfcounters directory under the log directory.
        os.mkdir(self.log_subdir + '/perfcounters')
        if not log_exists:
            self.sandbox = Sandbox()
        else:
            self.sandbox = Sandbox(cleanup=False)
        # create the shm directory to store shared files
        try:
            os.mkdir('%s/logs/shm' % os.getcwd())
        except:
            pass
        f = open('%s/logs/shm/README' % os.getcwd(), 'w+')
        f.write('This directory contains files that correspond to'
                'different server processes that were started during'
                'the last run of clusterperf. Filename is\n'
                '"<hostname>_<pid>". Each of these files stores'
                'the service locator of the respective server which is'
                'used to give information to the client.\nThe existence'
                'of this file at the end of a clusterperf run  means'
                'that processes were not cleaned up properly the last'
                ' time. So one can use these pids during manual clean up')
        if not cluster_name_exists:
            # store the name of the cluster by creating an empty file with
            # the appropriate file name in shm so that new backups when
            # created using a different cluster object can use it to read
            # data from their disks
            f = open('%s/logs/shm/%s' % (os.getcwd(), self.cluster_name),
                     'w+')

    def start_coordinator(self, host, args=''):
        """Start a coordinator on a node.
        @param host: (hostname, ip, id) tuple describing the node on which
                     to start the RAMCloud coordinator.
        @param args: Additional command-line args to pass to the coordinator.
                     (default: '')
        @return: Sandbox.Process representing the coordinator process.
        """
        if self.coordinator:
            raise Exception('Coordinator already started')
        self.coordinator_host = host
        self.coordinator_locator = coord_locator(self.transport,
                                                 self.coordinator_host)
        if not self.enable_logcabin:
            command = (
                '%s %s -C %s -l %s --logFile %s/coordinator.%s.log %s' %
                (valgrind_command,
                 coordinator_binary, self.coordinator_locator,
                 self.log_level, self.log_subdir,
                 self.coordinator_host[0], args))

            self.coordinator = self.sandbox.rsh(self.coordinator_host[0],
                        command, bg=True, stderr=subprocess.STDOUT)
        else:
            # currently hardcoding logcabin server because ankita's logcabin
            # scripts are not on git.
            command = (
                '%s %s -C %s -z logcabin21:61023 -l %s '
                '--logFile %s/coordinator.%s.log %s' %
                (valgrind_command,
                 coordinator_binary, self.coordinator_locator,
                 self.log_level, self.log_subdir,
                 self.coordinator_host[0], args))

            self.coordinator = self.sandbox.rsh(self.coordinator_host[0],
                        command, bg=True, stderr=subprocess.STDOUT)

            # just wait for coordinator to start
            time.sleep(1)
            # invoke the script that restarts the coordinator if it dies
            restart_command = ('%s/restart_coordinator %s/coordinator.%s.log'
                               ' %s %s logcabin21:61023' %
                                (scripts_path, self.log_subdir,
                                 self.coordinator_host[0],
                                 obj_path, self.coordinator_locator))

            restarted_coord = self.sandbox.rsh(self.coordinator_host[0],
                        restart_command, kill_on_exit=True, bg=True,
                        stderr=subprocess.STDOUT)

        self.ensure_servers(0, 0)
        if self.verbose:
            print('Coordinator started on %s at %s' %
                   (self.coordinator_host[0], self.coordinator_locator))
            print('Coordinator command line arguments %s' %
                   (command))
        return self.coordinator

    def start_server(self,
                     host,
                     args='',
                     master=True,
                     backup=True,
                     disk=None,
                     port=server_port,
                     kill_on_exit=True
                     ):
        """Start a server on a node.
        @param host: (hostname, ip, id) tuple describing the node on which
                     to start the RAMCloud server.
        @param args: Additional command-line args to pass to the server.
                     (default: '')
        @param master: If True then the started server provides a master
                       service. (default: True)
        @param backup: If True then the started server provides a backup
                       service. (default: True)
        @param disk: If backup is True then the started server passes these
                     additional arguments to select the storage type and
                     location. (default: self.disk)
        @param port: The port the server should listen on.
                     (default: see server_locator())
        @param kill_on_exit:
                     If False, this server process is not reaped at the end
                     of the clusterperf test.
                     (default: True)
        @return: Sandbox.Process representing the server process.
        """
        log_prefix = '%s/server%d.%s' % (
                      self.log_subdir, self.next_server_id, host[0])

        command = ('%s %s -C %s -L %s -r %d -l %s --clusterName __unnamed__ '
                   '--logFile %s.log --preferredIndex %d %s' %
                   (valgrind_command,
                    server_binary, self.coordinator_locator,
                    server_locator(self.transport, host, port),
                    self.replicas,
                    self.log_level,
                    log_prefix,
                    self.next_server_id,
                    args))

        self.next_server_id += 1
        if master and backup:
            pass
        elif master:
            command += ' --masterOnly'
        elif backup:
            command += ' --backupOnly'
        else:
            raise Exception('Cannot start a server that is neither a master '
                            'nor backup')

        if backup:
            if not disk:
                disk = self.disk
            command += ' %s' % disk
            self.backups_started += 1

        if master:
            self.masters_started += 1

        # Adding redirection for stdout and stderr.
        stdout = open(log_prefix + '.out', 'w')
        stderr = open(log_prefix + '.err', 'w')
        if not kill_on_exit:
            server = self.sandbox.rsh(host[0], command, is_server=True,
                                      locator=server_locator(self.transport,
                                                             host, port),
                                      kill_on_exit=False, bg=True,
                                      stdout=stdout,
                                      stderr=stderr)
        else:
            server = self.sandbox.rsh(host[0], command, is_server=True,
                                      locator=server_locator(self.transport,
                                                             host, port),
                                      bg=True,
                                      stdout=stdout,
                                      stderr=stderr)

        if self.verbose:
            print('Server started on %s at %s: %s' %
                  (host[0],
                   server_locator(self.transport, host, port), command))
        return server

    def kill_server(self, locator):
        """Kill a running server.
        @param locator: service locator for the server that needs to be
                        killed.
        """

        path = '%s/logs/shm' % os.getcwd()
        files = sorted([f for f in os.listdir(path)
           if os.path.isfile( os.path.join(path, f) )])

        for file in files:
            f = open('%s/logs/shm/%s' % (os.getcwd(), file),'r')
            service_locator = f.read()

            if (locator in service_locator):
                to_kill = '1'
                mhost = file
                subprocess.Popen(['ssh', mhost.split('_')[0],
                                  '%s/killserver' % scripts_path,
                                  to_kill, os.getcwd(), mhost])
                f.close()
                try:
                    os.remove('%s/logs/shm/%s' % (os.getcwd(), file))
                except:
                    pass
            else:
                f.close()


    def ensure_servers(self, numMasters=None, numBackups=None, timeout=30):
        """Poll the coordinator and block until the specified number of
        masters and backups have enlisted. Useful for ensuring that the
        cluster is in the expected state before experiments begin.
        If the expected state isn't acheived within 5 seconds the call
        will throw an exception.

        @param numMasters: Number of masters that must be part of the
                           cluster before this call returns successfully.
                           If unspecified then wait until all the masters
                           started with start_servers() have enlisted.
        @param numBackups: Number of backups that must be part of the
                           cluster before this call returns successfully.
                           If unspecified then wait until all the backups
                           started with start_servers() have enlisted.
        """
        if not numMasters:
            numMasters = self.masters_started
        if not numBackups:
            numBackups = self.backups_started
        self.sandbox.checkFailures()
        try:
            ensureCommand = ('%s -C %s -m %d -b %d -l 1 --wait %d '
                             '--logFile %s/ensureServers.log' %
                             (ensure_servers_bin, self.coordinator_locator,
                             numMasters, numBackups, timeout,
                             self.log_subdir))
            if self.verbose:
                print("ensureServers command: %s" % ensureCommand)
            self.sandbox.rsh(self.coordinator_host[0], ensureCommand)
        except:
            # prefer exceptions from dead processes to timeout error
            self.sandbox.checkFailures()
            raise

    def start_clients(self, hosts, client):
        """Start a client binary on a set of nodes.
        @param hosts: List of (hostname, ip, id) tuples describing the
                      nodes on which to start the client binary.
                      Each binary is launch with a --numClients and
                      --clientIndex argument.
        @param client: Path to the client binary to run along with any
                       args to pass to each client.
        @return: Sandbox.Process representing the client process.
        """
        num_clients = len(hosts)
        args = client.split(' ')
        client_bin = args[0]
        client_args = ' '.join(args[1:])
        clients = []
        for i, client_host in enumerate(hosts):
            command = ('%s %s -C %s --numClients %d --clientIndex %d '
                       '--logFile %s/client%d.%s.log %s' %
                       (valgrind_command,
                        client_bin, self.coordinator_locator, num_clients,
                        i, self.log_subdir, self.next_client_id,
                        client_host[0], client_args))
            self.next_client_id += 1
            clients.append(self.sandbox.rsh(client_host[0], command, bg=True))
            if self.verbose:
                print('Client %d started on %s: %s' % (i, client_host[0],
                        command))
        return clients

    def wait(self, processes, timeout=30):
        """Wait for a set of processes to exit.

        @param processes: List of Sandbox.Process instances as returned by
                          start_coordinator, start_server, and start_clients
                          whose exit should be waited on.
        @param timeout: Seconds to wait for exit before giving up and throwing
                        an exception. (default: 30)
        """
        start = time.time()
        for i, p in enumerate(processes):
            while p.proc.returncode is None:
                self.sandbox.checkFailures()
                time.sleep(.1)
                if time.time() - start > timeout:
                    raise Exception('timeout exceeded %s' % self.log_subdir)
            if self.verbose:
                print('%s finished' % p.sonce)

    def remove_empty_files(self):
        """Remove blank files and empty directories within the log directory.
        """
        root = self.log_subdir
        for item in os.listdir(root):
           path = os.path.join(root, item)
           if os.path.isfile(path):
             if os.path.getsize(path) == 0:
                os.remove(path)
           elif os.path.isdir(path):
             try:
                os.rmdir(path)
             except:
                None

    def shutdown():
        """Kill all remaining processes started as part of this cluster and
        wait for their exit. Usually called implicitly if 'with' keyword is
        used with the cluster."""
        self.__exit__(None, None, None)

    def __enter__(self):
        self.sandbox.__enter__()
        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        self.sandbox.__exit__(exc_type, exc_value, exc_tb)
        self.remove_empty_files()
        return False # rethrow exception, if any

def run(
        num_servers=4,             # Number of hosts on which to start
                                   # servers (not including coordinator).
        backups_per_server=1,      # Number of backups to run on each
                                   # server host (0, 1, or 2).
        replicas=3,                # Replication factor to use for each
                                   # log segment.
        disk1=default_disk1,       # Server arguments specifying the
                                   # backing device for the first backup
                                   # on each server.
        disk2=default_disk2,       # Server arguments specifying the
                                   # backing device for the first backup
                                   # on each server (if backups_per_server= 2).
        timeout=20,                # How many seconds to wait for the
                                   # clients to complete.
        coordinator_args='',       # Additional arguments for the
                                   # coordinator.
        master_args='',            # Additional arguments for each server
                                   # that runs a master
        backup_args='',            # Additional arguments for each server
                                   # that runs a backup.
        log_level='NOTICE',        # Log level to use for all servers.
        log_dir='logs',            # Top-level directory in which to write
                                   # log files.  A separate subdirectory
                                   # will be created in this directory
                                   # for the log files from this run.
        client=None,               # Command-line to invoke for each client
                                   # additional arguments will be prepended
                                   # with configuration information such as
                                   # -C.
        num_clients=1,             # Number of client processes to run.
                                   # They will all run on separate
                                   # machines, if possible, but if there
                                   # aren't enough available machines then
                                   # multiple clients will run on some
                                   # machines.
        client_hosts=None,         # An explicit list of hosts (in
                                   # host, ip, id triples) on which clients
                                   # should be run. If this is set and
                                   # share_hosts is set then share_hosts is
                                   # ignored.
        share_hosts=False,         # True means clients can be run on
                                   # machines running servers, if needed.
        transport='infrc',         # Name of transport to use for servers.
        verbose=False,             # Print information about progress in
                                   # starting clients and servers
        debug=False,               # If True, pause after starting all
                                   # to allow for debugging setup such as
                                   # attaching gdb.
        old_master_host=None,      # Pass a (hostname, ip, id) tuple to
                                   # construct a large master on that host
                                   # before the others are started.  Useful
                                   # for creating the old master for
                                   # recoveries.
        old_master_args='',        # Additional arguments to run on the
                                   # old master (e.g. total RAM).
        enable_logcabin=False,     # Do not enable logcabin.
        valgrind=False,		   # Do not run under valgrind
        valgrind_args='',	   # Additional arguments for valgrind
        disjunct=False,            # Disjunct entities on a server
        coordinator_host=None
        ):
    """
    Start a coordinator and servers, as indicated by the arguments.
    Then start one or more client processes and wait for them to complete.
    @return: string indicating the path to the log files for this run.
    """

    if not client:
        raise Exception('You must specify a client binary to run '
                        '(try obj.master/client)')

    if verbose:
        print('num_servers=(%d), available hosts=(%d) defined in config.py'
              % (num_servers, len(getHosts())))
        print ('disjunct=', disjunct)

# When disjunct=True, disjuncts Coordinator and Clients on Server nodes.
    if disjunct:
        if num_servers + num_clients + 1 > len(getHosts()):
            raise Exception('num_servers (%d)+num_clients (%d)+1(coord) exceeds the available hosts (%d)'
                            % (num_servers, num_clients, len(getHosts())))
    else:
        if num_servers > len(getHosts()):
            raise Exception('num_servers (%d) exceeds the available hosts (%d)'
                            % (num_servers, len(getHosts())))

    if not share_hosts and not client_hosts:
        if (len(getHosts()) - num_servers) < 1:
            raise Exception('Asked for %d servers without sharing hosts with %d '
                            'clients, but only %d hosts were available'
                            % (num_servers, num_clients, len(getHosts())))

    masters_started = 0
    backups_started = 0

    global valgrind_command
    if valgrind:
        valgrind_command = ('valgrind %s' % valgrind_args)

    with Cluster(log_dir) as cluster:
        cluster.log_level = log_level
        cluster.verbose = verbose
        cluster.transport = transport
        cluster.replicas = replicas
        cluster.timeout = timeout
        cluster.disk = disk1
        cluster.enable_logcabin = enable_logcabin
        cluster.disjunct = disjunct
        cluster.hosts = getHosts()

        if not coordinator_host:
            coordinator_host = cluster.hosts[len(cluster.hosts)-1]
        coordinator = cluster.start_coordinator(coordinator_host,
                                                coordinator_args)
        if disjunct:
            cluster.hosts.pop(0)

        if old_master_host:
            oldMaster = cluster.start_server(old_master_host,
                                             old_master_args,
                                             backup=False)
            oldMaster.ignoreFailures = True
            masters_started += 1
            cluster.ensure_servers(timeout=60)

        for host in cluster.hosts[:num_servers]:
            backup = False
            args = master_args
            if backups_per_server > 0:
                backup = True
                args += ' %s' % backup_args
                backups_started += 1
            cluster.start_server(host, args, backup=backup)
            masters_started += 1

        if disjunct:
            cluster.hosts = cluster.hosts[num_servers:]

        if backups_per_server == 2:
            for host in cluster.hosts[:num_servers]:
                cluster.start_server(host, backup_args,
                                     master=False,
                                     disk=disk2, port=second_backup_port)
                backups_started += 1
            if disjunct:
                cluster.hosts = cluster.hosts[num_servers:]

        if debug:
            print('Servers started; pausing for debug setup.')
            raw_input('Type <Enter> to continue: ')
        if masters_started > 0 or backups_started > 0:
            cluster.ensure_servers()
            if verbose:
                print('All servers running')

        if client:
            # Note: even if it's OK to share hosts between clients and servers,
            # don't do it unless necessary.
            if not client_hosts:
                if disjunct:
                    host_list = cluster.hosts[:]
                else:
                    host_list = cluster.hosts[num_servers:]
                    if share_hosts:
                        host_list.extend(cluster.hosts[:num_servers])

                client_hosts = [host_list[i % len(host_list)]
                                for i in range(num_clients)]
            assert(len(client_hosts) == num_clients)

            clients = cluster.start_clients(client_hosts, client)
            cluster.wait(clients, timeout)

        return cluster.log_subdir

if __name__ == '__main__':
    parser = OptionParser(description=
            'Start RAMCloud servers and run a client application.',
            conflict_handler='resolve')
    parser.add_option('--backupArgs', metavar='ARGS', default='',
            dest='backup_args',
            help='Additional command-line arguments to pass to '
                 'each backup')
    parser.add_option('-b', '--backups', type=int, default=1,
            metavar='N', dest='backups_per_server',
            help='Number of backups to run on each server host '
                 '(0, 1, or 2)')
    parser.add_option('--client', metavar='ARGS',
            help='Command line to invoke the client application '
                 '(additional arguments will be inserted at the beginning '
                 'of the argument list)')
    parser.add_option('-n', '--clients', type=int, default=1,
            metavar='N', dest='num_clients',
            help='Number of instances of the client application '
                 'to run')
    parser.add_option('--coordinatorArgs', metavar='ARGS', default='',
            dest='coordinator_args',
            help='Additional command-line arguments to pass to the '
                 'cluster coordinator')
    parser.add_option('--debug', action='store_true', default=False,
            help='Pause after starting servers but before running '
                 'clients to enable debugging setup')
    parser.add_option('--disk1', default=default_disk1,
            help='Server arguments to specify disk for first backup')
    parser.add_option('--disk2', default=default_disk2,
            help='Server arguments to specify disk for second backup')
    parser.add_option('-l', '--logLevel', default='NOTICE',
            choices=['DEBUG', 'NOTICE', 'WARNING', 'ERROR', 'SILENT'],
            metavar='L', dest='log_level',
            help='Controls degree of logging in servers')
    parser.add_option('-d', '--logDir', default='logs',
            metavar='DIR',
            dest='log_dir',
            help='Top level directory for log files; the files for '
                 'each invocation will go in a subdirectory.')
    parser.add_option('--masterArgs', metavar='ARGS', default='',
            dest='master_args',
            help='Additional command-line arguments to pass to '
                 'each master')
    parser.add_option('-r', '--replicas', type=int, default=3,
            metavar='N',
            help='Number of disk backup copies for each segment')
    parser.add_option('-s', '--servers', type=int, default=4,
            metavar='N', dest='num_servers',
            help='Number of hosts on which to run servers')
    parser.add_option('--shareHosts', action='store_true', default=False,
            dest='share_hosts',
            help='Allow clients to run on machines running servers '
                 '(by default clients run on different machines than '
                 'the servers, though multiple clients may run on a '
                 'single machine)')
    parser.add_option('-t', '--timeout', type=int, default=20,
            metavar='SECS',
            help="Abort if the client application doesn't finish within "
            'SECS seconds')
    parser.add_option('-T', '--transport', default='infrc',
            help='Transport to use for communication with servers')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='Print progress messages')
    parser.add_option('--valgrind', action='store_true', default=False,
            help='Run all the processes under valgrind')
    parser.add_option('--valgrindArgs', metavar='ARGS', default='',
            dest='valgrind_args',
            help='Arguments to pass to valgrind')
    parser.add_option('--disjunct', action='store_true', default=False,
            help='Disjunct entities (disable collocation) on each server')

    (options, args) = parser.parse_args()

    status = 0
    try:
        run(**vars(options))
    finally:
        logInfo = log.scan("logs/latest", ["WARNING", "ERROR"])
        if len(logInfo) > 0:
            print(logInfo, file=sys.stderr)
            status = 1
    quit(status)
