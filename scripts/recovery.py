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

"""Runs a recovery of a master."""

from __future__ import division, print_function
from common import *
import cluster
import config
import log
import recoverymetrics
import os
import pprint
import re
import subprocess
import sys
import time
from optparse import OptionParser

def recover(num_servers,
            backups_per_server,
            object_size,
            num_objects,
            num_overwrites,
            replicas,
            coordinator_args='',
            master_args='',
            backup_args='',
            master_ram=None,
            old_master_ram=None,
            num_removals=0,
            timeout=100,
            log_level='NOTICE',
            log_dir='logs',
            transport='infrc',
            verbose=False,
            debug=False):
    """Run a recovery on the cluster specified in the config module and
    return statisitics about the recovery.

    See the config module for more parameters that affect how the recovery
    will be run.  In particular config.hosts and config.old_master_host which
    select which hosts in the cluster recovery will be run on.

    @param num_servers: Number of hosts on which to run Masters.
    @type  num_servers: C{int}

    @param backups_per_server: Number of Backups to colocate on the same host
                               with each Master.  If this is 1 the Backup is
                               run in the same process as the Master.  If this
                               is 2 then an additional process is started to
                               run the second Backup.
    @type  backups_per_server: C{int}

    @param object_size: Size of objects to fill old Master with before crash.
    @type  object_size: C{int}

    @param num_objects: Number of objects to fill old Master with before crash.
    @type  num_objects: C{int}

    @param num_overwrites: Number of writes on same key while filling old Master.
    @type  num_overwrites: C{int}

    @param replicas: Number of times each segment is replicated to different
                     backups in the cluster.
    @type  replicas: C{int}

    @param coordinator_args: Additional command-line arguments to pass to
                             the coordinator.
    @type  coordinator_args: C{str}

    @param master_args: Additional command-line arguments to pass to
                        each server that acts as a master.
    @type  master_args: C{str}

    @param backup_args: Additional command-line arguments to pass to
                        each server that acts as a backup.
    @type  backup_args: C{str}

    @param master_ram: Megabytes of space allocated in each of the Masters
                       (except the old Master, see old_master_ram).  If left
                       unspecified same sane default will be attempted based
                       on num_objects, object_size and num_overwrites.
    @type  master_ram: C{int}

    @param old_master_ram: Megabytes of space allocated in the old Master that
                           will eventually be crashed.  If left unspecified same
                           sane default will be attempted based on num_objects,
                           num_overwrites and object_size.
    @type  old_master_ram: C{int}

    @param num_removals: Number of erases to do after inserts.  Allows
                         recoveries where some of the log contains tombstones.
    @type  num_removals: C{int}

    @param timeout: Seconds to wait before giving up and declaring the recovery
                    to have failed.
    @type  timeout: C{int}

    @param log_level: Log level to use for all servers.
                      DEBUG, NOTICE, WARNING, or ERROR.
    @type  log_level: C{str}

    @param log_dir: Top-level directory in which to write log files.
                    A separate subdirectory will be created in this
                    directory for the log files from this run.
    @type  log_dir: C{str}

    @param transport: A transport name (e.g. infrc, fast+udp, tcp, ...)
    @type  transport: C{str}

    @param verbose: Print information about progress in starting clients
                    and servers.
    @type  verbose: C{bool}

    @param debug: If True, pause after starting all to allow for debugging
                  setup such as attaching gdb.
    @type  debug: C{bool}

    @return: A recoverymetrics stats struct (see recoverymetrics.parseRecovery).
    """
    server_binary = '%s/server' % obj_path
    client_binary = '%s/recovery' % obj_path
    ensure_servers_bin = '%s/ensureServers' % obj_path

    args = {}
    args['num_servers'] = num_servers
    args['backups_per_server'] = backups_per_server
    args['replicas'] = replicas
    args['timeout'] = timeout
    args['log_level'] = log_level
    args['log_dir'] = log_dir
    args['transport'] = transport
    args['verbose'] = verbose
    args['debug'] = debug
    args['coordinator_host'] = config.old_master_host
    args['coordinator_args'] = coordinator_args
    if backup_args:
        args['backup_args'] += backup_args;
    else:
        args['backup_args'] = '--maxNonVolatileBuffers 1000'
    # Allocate enough memory on recovery masters to handle several
    # recovery partitions (most recoveries will only have one recovery
    # partition per master, which is about 500 MB).
    args['master_args'] = '-d -D -t 5000'
    if master_args:
        args['master_args'] += ' ' + master_args;
    args['client'] = ('%s -f -n %d -r %d -s %d '
                      '-k %d -l %s -o %d' % (client_binary,
                      num_objects, num_removals, object_size,
                      num_servers, log_level, num_overwrites))
    args['old_master_host'] = config.old_master_host
    args['client_hosts'] = [config.old_master_host]
    if old_master_ram:
        args['old_master_args'] = '-d -t %d' % old_master_ram
    else:
        # Estimate how much log space the old master will need to hold
        # all of the data.
        old_master_ram = (200 + (1.3 * num_objects / object_size * num_overwrites))
        if args['coordinator_host'][0] == 'rcmaster' and old_master_ram > 42000:
            print('Warning: pushing the limits of rcmaster; '
                  'limiting RAM to 42000 MB to avoid knocking it over; '
                  'use rcmonster if you need more')
            old_master_ram = 42000
        args['old_master_args'] = '-d -D -t %d' % old_master_ram
    recovery_logs = cluster.run(**args)

    # Collect metrics information.
    stats = {}
    stats['metrics'] = recoverymetrics.parseRecovery(recovery_logs)
    report = recoverymetrics.makeReport(stats['metrics']).jsonable()
    f = open('%s/metrics' % recovery_logs, 'w')
    getDumpstr().print_report(report, file=f)
    f.close()
    stats['run'] = recovery_logs
    stats['count'] = num_objects
    stats['size'] = object_size
    stats['ns'] = stats['metrics'].client.recoveryNs
    stats['report'] = report
    return stats

def insist(*args, **kwargs):
    """Keep trying recoveries until the damn thing succeeds"""
    while True:
        try:
            return recover(*args, **kwargs)
        except KeyboardInterrupt, e:
            raise
        except Exception, e:
            print('Recovery failed:', e)
            print('Trying again...')
        time.sleep(0.1)

if __name__ == '__main__':
    parser = OptionParser(description=
            'Run a recovery on a RAMCloud cluster.',
            usage='%prog [options] ...',
            conflict_handler='resolve')
    parser.add_option('--backupArgs', metavar='ARGS', default='',
            dest='backup_args',
            help='Additional command-line arguments to pass to '
                 'each backup')
    parser.add_option('--coordinatorArgs', metavar='ARGS', default='',
            dest='coordinator_args',
            help='Additional command-line arguments to pass to the '
                 'cluster coordinator')
    parser.add_option('--debug', action='store_true', default=False,
            help='Pause after starting servers but before running '
                 'to enable debugging setup')
    parser.add_option('-d', '--logDir', default='logs',
            metavar='DIR',
            dest='log_dir',
            help='Top level directory for log files; the files for '
                 'each invocation will go in a subdirectory.')
    parser.add_option('-l', '--logLevel', default='NOTICE',
            choices=['DEBUG', 'NOTICE', 'WARNING', 'ERROR', 'SILENT'],
            metavar='L', dest='log_level',
            help='Controls degree of logging in servers')
    parser.add_option('-b', '--numBackups', type=int, default=1,
            metavar='N', dest='backups_per_server',
            help='Number of backups to run on each server host '
                 '(0, 1, or 2)')
    parser.add_option('--masterArgs', metavar='ARGS', default='',
            dest='master_args',
            help='Additional command-line arguments to pass to '
                 'each master')
    parser.add_option('-r', '--replicas', type=int, default=3,
            metavar='N',
            help='Number of disk backup copies for each segment')
    parser.add_option('--servers', type=int, default=10,
            metavar='N', dest='num_servers',
            help='Number of hosts on which to run servers for use '
                 "as recovery masters; doesn't include crashed server")
    parser.add_option('-s', '--size', type=int, default=1024,
            help='Object size in bytes')
    parser.add_option('-n', '--numObjects', type=int,
            metavar='N', dest='num_objects', default=0,
            help='Total number of objects on crashed master (default value '
                 'will create enough data for one partition on each recovery '
                 'master)')
    parser.add_option('-o', '--numOverwrite', type=int,
            metavar='N', dest='num_overwrites', default=1,
            help='Number of writes per key')
    parser.add_option('-t', '--timeout', type=int, default=100,
            metavar='SECS',
            help="Abort if the client application doesn't finish within "
                 'SECS seconds')
    parser.add_option('-T', '--transport', default='infrc',
            help='Transport to use for communication with servers')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='Print progress messages')
    parser.add_option('--masterRam', type=int,
            metavar='N', dest='master_ram',
            help='Megabytes to allocate for the log per recovery master')
    parser.add_option('--oldMasterRam', type=int,
            metavar='N', dest='old_master_ram',
            help='Megabytes to allocate for the log of the old master')
    parser.add_option('--removals', type=int,
            metavar='N', dest='num_removals', default=0,
            help='Perform this many removals after filling the old master')
    parser.add_option('--trend',
            dest='trends', action='append',
            help='Add to dumpstr trend line (may be repeated)')
    (options, args) = parser.parse_args()

    args = {}
    args['num_servers'] = options.num_servers
    args['backups_per_server'] = options.backups_per_server
    args['object_size'] = options.size
    if options.num_objects == 0:
        # The value below depends on Recovery::PARTITION_MAX_BYTES
        # and various other RAMCloud parameters
        options.num_objects = 480000*options.num_servers
    args['num_objects'] = options.num_objects
    args['num_overwrites'] = options.num_overwrites
    args['replicas'] = options.replicas
    args['master_ram'] = options.master_ram
    args['old_master_ram'] = options.old_master_ram
    args['num_removals'] = options.num_removals
    args['timeout'] = options.timeout
    args['log_level'] = options.log_level
    args['log_dir'] = options.log_dir
    args['transport'] = options.transport
    args['verbose'] = options.verbose
    args['debug'] = options.debug
    args['coordinator_args'] = options.coordinator_args
    args['master_args'] = options.master_args
    args['backup_args'] = options.backup_args

    try:
        stats = recover(**args)

        # set up trend points for dumpstr
        trends = ['recovery']
        if options.trends is not None:
            for trend in options.trends:
                if trend not in trends:
                    trends.append(trend)
        trends = zip(trends,
                     [stats['ns'] / 1e9] * len(trends))

        # print and upload dumpstr report
        dumpstr = getDumpstr()
        dumpstr.print_report(stats['report'])
        s = dumpstr.upload_report('recovery', stats['report'], trends=trends)
        print('You can view your report at %s' % s['url'])

        # write the dumpstr URL to the metrics log file
        f = open('%s/metrics' % stats['run'], 'a')
        print('You can view your report at %s' % s['url'], file=f)
        f.close()

    finally:
        log_info = log.scan("%s/latest" % (options.log_dir),
                            ["WARNING", "ERROR"],
                            ["Ping timeout", "Pool destroyed", "told to kill",
                             "is not responding", "failed to exchange",
                             "timed out waiting for response",
                             "received nonce", "Couldn't open session",
                             "verifying cluster membership"])
        if len(log_info) > 0:
            print(log_info)
