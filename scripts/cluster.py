#!/usr/bin/env python

# Copyright (c) 2010-2011 Stanford University
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

from __future__ import division
from common import *
import itertools
import log
import os
import pprint
import re
import subprocess
import sys
import time
from optparse import OptionParser

#------------------------------------------------------------------
# End of site-specific configuration.
#------------------------------------------------------------------

# Locations of various RAMCloud executables.
coordinator_binary = '%s/coordinator' % obj_path
server_binary = '%s/server' % obj_path
ensure_servers_bin = '%s/ensureServers' % obj_path

# Info used to construct service locators for each of the transports
# supported by RAMCloud.  In some cases the locator for the coordinator
# needs to be different from that for the servers.
server_locator_templates = {
    'tcp': 'tcp:host=%(host)s,port=%(port)d',
    'tcp-1g': 'tcp:host=%(host1g)s,port=%(port)d',
    'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
    'fast+udp-1g': 'fast+udp:host=%(host1g)s,port=%(port)d',
    'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
    'infrc': 'infrc:host=%(host)s,port=%(port)d',
    'fast+infud': 'fast+infud:',
    'unreliable+infud': 'unreliable+infud:',
    'fast+infeth': 'fast+infeth:mac=00:11:22:33:44:%(id)02x',
    'unreliable+infeth': 'unreliable+infeth:mac=00:11:22:33:44:%(id)02x',
}
coord_locator_templates = {
    'tcp': 'tcp:host=%(host)s,port=%(port)d',
    'tcp-1g': 'tcp:host=%(host1g)s,port=%(port)d',
    'fast+udp': 'fast+udp:host=%(host)s,port=%(port)d',
    'fast+udp-1g': 'fast+udp:host=%(host1g)s,port=%(port)d',
    'unreliable+udp': 'unreliable+udp:host=%(host)s,port=%(port)d',
    'infrc': 'infrc:host=%(host)s,port=%(port)d',
    # Coordinator uses udp even when rest of cluster uses infud
    # or infeth.
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
        client='echo',             # Command-line to invoke for each client
                                   # additional arguments will be prepended
                                   # with configuration information such as
                                   # -C.
        num_clients=1,             # Number of client processes to run.
                                   # They will all run on separate
                                   # machines, if possible, but if there
                                   # aren't enough available machines then
                                   # multiple clients will run on some
                                   # machines.
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
        old_master_args=""         # Additional arguments to run on the
                                   # old master (e.g. total RAM).
        ):       
    """
    Start a coordinator and servers, as indicated by the arguments.
    Then start one or more client processes and wait for them to complete.
    @return: string indicating the path to the log files for this run.
    """

    if num_servers > len(hosts):
        raise Exception("num_servers (%d) exceeds the available hosts (%d)"
                        % (num_servers, len(hosts)))

    # Create a subdirectory of the log directory for this run
    log_subdir = log.createDir(log_dir)

    coordinator = None
    servers = []
    clients = []
    with Sandbox() as sandbox:
        def ensure_servers(numMasters, numBackups):
            sandbox.checkFailures()
            try:
                sandbox.rsh(hosts[0][0], '%s -C %s -m %d -b %d -l 1 --wait 5 '
                            '--logFile %s/ensureServers.log' %
                            (ensure_servers_bin, coordinator_locator,
                             numMasters, numBackups, log_subdir))
            except:
                # prefer exceptions from dead processes to timeout error
                sandbox.checkFailures()
                raise

        # Start coordinator
        if num_servers > 0:
            coordinator_host = hosts[0]
            coordinator_locator = coord_locator(transport, coordinator_host)
            coordinator = sandbox.rsh(coordinator_host[0],
                      ('%s -C %s -l %s --logFile %s/coordinator.%s.log %s' %
                       (coordinator_binary, coordinator_locator, log_level,
                        log_subdir, coordinator_host[0], coordinator_args)),
                      bg=True, stderr=subprocess.STDOUT)
            ensure_servers(0, 0)
            if verbose:
                print "Coordinator started on %s at %s" % (coordinator_host[0],
                        coordinator_locator)

        # Track how many services are registered with the coordinator
        # for ensure_servers
        masters_started = 0
        backups_started = 0

        # Start old master - a specialized master for recovery with lots of data
        if old_master_host:
            host = old_master_host
            command = ('%s -C %s -L %s -M -r %d -l %s '
                       '--logFile %s/oldMaster.%s.log %s' %
                       (server_binary, coordinator_locator,
                        server_locator(transport, host),
                        replicas, log_level, log_subdir, host[0],
                        old_master_args))
            servers.append(sandbox.rsh(host[0], command, ignoreFailures=True,
                           bg=True, stderr=subprocess.STDOUT))
            masters_started += 1
            ensure_servers(masters_started, 0)

        # Start servers
        for i in range(num_servers):
            # First start the main server on this host, which runs a master
            # and possibly a backup.  The first server shares the same machine
            # as the coordinator.
            host = hosts[i];
            command = ('%s -C %s -L %s -r %d -l %s '
                       '--logFile %s/server.%s.log %s' %
                       (server_binary, coordinator_locator,
                        server_locator(transport, host),
                        replicas, log_level, log_subdir, host[0],
                        master_args))
            if backups_per_server > 0:
                command += ' %s %s' % (disk1, backup_args)
                masters_started += 1
                backups_started += 1
            else:
                command += ' -M'
                masters_started += 1
            servers.append(sandbox.rsh(host[0], command, bg=True,
                           stderr=subprocess.STDOUT))
            if verbose:
                print "Server started on %s at %s" % (host[0],
                                                      server_locator(transport,
                                                                     host))
            
            # Start an extra backup server in this host, if needed.
            if backups_per_server == 2:
                command = ('%s -C %s -L %s -B %s -l %s '
                           '--logFile %s/backup.%s.log %s' %
                           (server_binary, coordinator_locator,
                            server_locator(transport, host, second_backup_port),
                            disk2, log_level, log_subdir, host[0],
                            backup_args))
                servers.append(sandbox.rsh(host[0], command, bg=True,
                                           stderr=subprocess.STDOUT))
                backups_started += 1
                if verbose:
                    print "Extra backup started on %s at %s" % (host[0],
                            server_locator(transport, host, second_backup_port))
        if debug:
            print "Servers started; pausing for debug setup."
            raw_input("Type <Enter> to continue: ")
        if masters_started > 0 or backups_started > 0:
            ensure_servers(masters_started, backups_started)
            if verbose:
                print "All servers running"

        # Start clients
        args = client.split(" ")
        client_bin = args[0]
        client_args = " ".join(args[1:])
        host_index = num_servers
        for i in range(num_clients):
            if host_index >= len(hosts):
                if share_hosts or num_servers >= len(hosts):
                    host_index = 0
                else:
                    host_index = num_servers
            client_host = hosts[host_index]
            command = ('%s -C %s --numClients %d --clientIndex %d '
                       '--logFile %s/client%d.%s.log %s' %
                       (client_bin, coordinator_locator, num_clients,
                        i, log_subdir, i, client_host[0], client_args))
            clients.append(sandbox.rsh(client_host[0], command, bg=True))
            if verbose:
                print "Client %d started on %s: %s" % (i, client_host[0],
                        command)
            host_index += 1

        # Wait for all of the clients to complete
        start = time.time()
        for i in range(num_clients):
            while clients[i].returncode is None:
                sandbox.checkFailures()
                time.sleep(.1)
                if time.time() - start > timeout:
                    raise Exception('timeout exceeded')
            if verbose:
                print "Client %d finished" % i

        return log_subdir

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
    parser.add_option('--client', metavar='ARGS', default='echo',
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
    parser.add_option('-d', '--logDir', default='logs', metavar='DIR',
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
    (options, args) = parser.parse_args()

    status = 0
    try:
        run(**vars(options))
    finally:
        logInfo = log.scan("logs/latest", ["WARNING", "ERROR"])
        if len(logInfo) > 0:
            print >>sys.stderr, logInfo
            status = 1
    quit(status)
