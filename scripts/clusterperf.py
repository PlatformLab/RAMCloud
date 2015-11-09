#!/usr/bin/env python

# Copyright (c) 2011-2015 Stanford University
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

"""
Runs one or more cluster benchmarks for RAMCloud, using cluster.py to
set up the cluster and ClusterPerf.cc to implement the details of the
benchmark.
"""

# TO ADD A NEW BENCHMARK:
# 1. Decide on a symbolic name for the new test.
# 2. Write code for the test in ClusterPerf.cc using the same test name (see
#    instructions in ClusterPerf.cc for details).
# 3. If needed, create a driver function for the test (named after the test)
#    in the "driver functions" section below.  Many tests can just use the
#    function "default".  If you need to provide special arguments to
#    cluster.run for your tests, or if the running of your test is unusual
#    in some way (e.g., you call cluster.run several times or collect
#    results from unusual places) then you'll need to write a test-specific
#    driver function.
# 4. Create a new Test object in one of the tables simple_tests or
#    graph_tests below, depending on the kind of test.

from __future__ import division, print_function
from common import *
import cluster
import config
import log
import glob
import os
import pprint
import re
import sys
import time
from optparse import OptionParser

# Each object of the following class represents one test that can be
# performed by this program.
class Test:
    def __init__(self,
            name,                 # Symbolic name for the test, used on the
                                  # command line to run the test.  This same
                                  # name is normally used for the
                                  # corresponding test in ClusterPerf.cc.
            function              # Python driver function for the test.
            ):
        """
        Construct a Test object.
        """

        self.name = name
        self.function = function

def flatten_args(args):
    """
    Given a dictionary of arguments, produce a string suitable for inclusion
    in a command line, such as "--name1 value1 --name2 value2"
    """
    return " ".join(["%s %s" % (name, value)
            for name, value in args.iteritems()])

def get_client_log(
        index = 1                 # Client index (1 for first client,
                                  # which is usually the one that's wanted)
        ):
    """
    Given the index of a client, read the client's log file
    from the current log directory and return its contents,
    ignoring RAMCloud log messages (what's left should be a
    summary of the results from a test.
    """
    globResult = glob.glob('%s/latest/client%d.*.log' %
            (options.log_dir, index))
    if len(globResult) == 0:
        raise Exception("couldn't find log file for client %d" % (index))
    result = "";
    for line in open(globResult[0], 'r'):
        if not re.match('([0-9]+\.[0-9]+) ', line):
            result += line
    return result

def print_cdf_from_log(
        index = 1                 # Client index (0 for first client,
                                  # which is usually the one that's wanted)
        ):
    """
    Given the index of a client, print in gnuplot format a cumulative
    distribution of the data in the client's log file (where "data" consists
    of comma-separated numbers stored in all of the lines of the log file
    that are not RAMCloud log messages). Each line in the printed output
    will contain a fraction and a number, such that the given fraction of all
    numbers in the log file have values less than or equal to the given number.
    """

    # Read the log file into an array of numbers.
    numbers = []
    globResult = glob.glob('%s/latest/client%d.*.log' %
            (options.log_dir, index))
    if len(globResult) == 0:
        raise Exception("couldn't find log file for client %d" % (index))
    result = "";
    for line in open(globResult[0], 'r'):
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                numbers.append(float(value))

    # Generate a CDF from the array.
    numbers.sort()
    result = []
    print("%8.2f    %8.3f" % (0.0, 0.0))
    print("%8.2f    %8.3f" % (numbers[0], 1/len(numbers)))
    for i in range(1, 100):
        print("%8.2f    %8.3f" % (numbers[int(len(numbers)*i/100)], i/100))
    print("%8.2f    %8.3f" % (numbers[int(len(numbers)*999/1000)], .999))
    print("%8.2f    %9.4f" % (numbers[int(len(numbers)*9999/10000)], .9999))
    print("%8.2f    %8.3f" % (numbers[-1], 1.0))

def print_rcdf_from_log(
        index = 1                 # Client index (1 for first client,
                                  # which is usually the one that's wanted)
        ):
    """
    Given the index of a client, print in gnuplot format a reverse cumulative
    distribution of the data in the client's log file (where "data" consists
    of comma-separated numbers stored in all of the lines of the log file
    that are not RAMCloud log messages). Each line in the printed output
    will contain a fraction and a number, such that the given fraction of all
    numbers in the log file have values less than or equal to the given number.
    """

    # Read the log file into an array of numbers.
    numbers = []
    globResult = glob.glob('%s/latest/client%d.*.log' %
            (options.log_dir, index))
    if len(globResult) == 0:
        raise Exception("couldn't find log file for client %d" % (index))
    result = "";
    for line in open(globResult[0], 'r'):
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                numbers.append(float(value))

    # Generate a RCDF from the array.
    numbers.sort()
    result = []
    print("%8.2f    %11.6f" % (numbers[0], 1.0))
    for i in range(1, len(numbers)-1):
        if (numbers[i] != numbers[i-1] or numbers[i] != numbers[i+1]):
            print("%8.2f    %11.6f" % (numbers[i], 1-(i/len(numbers))))
    print("%8.2f    %11.6f" % (numbers[-1], 1/len(numbers)))

def run_test(
        test,                     # Test object describing the test to run.
        options                   # Command-line options.
        ):
    """
    Run a given test.  The main value provided by this function is to
    prepare a candidate set of options for cluster.run and another set
    for the ClusterPerf clients, based on the command-line options.
    """
    cluster_args = {
        'debug':       options.debug,
        'log_dir':     options.log_dir,
        'log_level':   options.log_level,
        'backups_per_server': options.backups_per_server,
        'num_servers': options.num_servers,
        'replicas':    options.replicas,
        'timeout':     options.timeout,
        'share_hosts': True,
        'transport':   options.transport,
        'replicas':    options.replicas,
        'disjunct':    options.disjunct,
        'verbose':     options.verbose
    }
    client_args = {}
    # Provide a default value for num_servers here.  This is better
    # than defaulting it in the OptionParser below, because tests can
    # see whether or not an actual value was specified and provide a
    # test-specific default.
    if cluster_args['num_servers'] == None:
        # Make sure there are enough servers to meet replica requirements.
        cluster_args['num_servers'] = options.replicas+1
    if options.num_clients != None:
        cluster_args['num_clients'] = options.num_clients
    if options.master_args != None:
        cluster_args['master_args'] = options.master_args
    if options.count != None:
        client_args['--count'] = options.count
    if options.size != None:
        client_args['--size'] = options.size
    if options.numObjects != None:
        client_args['--numObjects'] = options.numObjects
    if options.numTables != None:
        client_args['--numTables'] = options.numTables
    if options.warmup != None:
        client_args['--warmup'] = options.warmup
    if options.workload != None:
        client_args['--workload'] = options.workload
    if options.targetOps != None:
        client_args['--targetOps'] = options.targetOps
    if options.txSpan != None:
        client_args['--txSpan'] = options.txSpan
    if options.numIndexlet != None:
        client_args['--numIndexlet'] = options.numIndexlet
    if options.numIndexes != None:
        client_args['--numIndexes'] = options.numIndexes
    if options.numVClients != None:
        client_args['--numVClients'] = options.numVClients
    test.function(test.name, options, cluster_args, client_args)

#-------------------------------------------------------------------
# Driver functions follow below.  These functions are responsible for
# invoking ClusterPerf via cluster.py, and they collect and print
# result data.  Simple tests can just use the "default" driver function.
#-------------------------------------------------------------------

def default(
        name,                      # Name of this test; passed through
                                   # to ClusterPerf verbatim.
        options,                   # The full set of command-line options.
        cluster_args,              # Proposed set of arguments to pass to
                                   # cluster.run (extracted from options).
                                   # Individual tests can override as
                                   # appropriate for the test.
        client_args,               # Proposed set of arguments to pass to
                                   # ClusterPerf (via cluster.run).
                                   # Individual tests can override as
                                   # needed for the test.
        ):
    """
    This function is used as the invocation function for most tests;
    it simply invokes ClusterPerf via cluster.run and prints the result.
    """
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def basic(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 4000'
    if cluster_args['timeout'] < 250:
        cluster_args['timeout'] = 250
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def broadcast(name, options, cluster_args, client_args):
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 10
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def indexBasic(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 2 --totalMasterMemory 1500'
    if cluster_args['timeout'] < 200:
        cluster_args['timeout'] = 200
    # Ensure at least 5 hosts for optimal performance
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def indexRange(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 2 --totalMasterMemory 1500'
    if cluster_args['timeout'] < 360:
        cluster_args['timeout'] = 360

    if '--numObjects' not in client_args:
        client_args['--numObjects'] = 1000
    if '--warmup' not in client_args:
        client_args['--warmup'] = 10
    if '--count' not in client_args:
        client_args['--count'] = 90

    # Ensure at least 5 hosts for optimal performance
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def indexMultiple(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 2'
    if cluster_args['timeout'] < 360:
        cluster_args['timeout'] = 360
    # Ensure atleast 15 hosts for optimal performance
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())

    # use a maximum of 10 secondary keys
    if len(getHosts()) <= 10:
        # Hack until synchronization bug in write RPC handler
        # in MasterService is resolved. This bug prevents us from using more
        # than 1 MasterSerivice thread. However, we need to use more than 1
        # service thread, otherwise if a tablet and its corresponding
        # indexlet end up on the same server, we will have a deadlock.
        # For now, make sure that we never wrap around the server list
        # Once the bug is resolved, we should be able to use len(getHosts())
        # for numIndexes
        client_args['--numIndexes'] = len(getHosts()) - 1
    else:
        client_args['--numIndexes'] = 10

    cluster.run(client='%s/ClusterPerf %s %s' %
               (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def indexScalability(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 3'
    if cluster_args['timeout'] < 360:
        cluster_args['timeout'] = 360
    cluster_args['backups_per_server'] = 0
    cluster_args['replicas'] = 0
    # Number of concurrent rpcs to do per indexlet
    if '--count' not in client_args:
        client_args['--count'] = 20
    # Number of objects per read request
    if '--numObjects' not in client_args:
        client_args['--numObjects'] = 1

    # Ensure at least 15 hosts for optimal performance
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 10
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def indexWriteDist(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 2 --totalMasterMemory 1500'
    if cluster_args['timeout'] < 200:
        cluster_args['timeout'] = 200

    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())

    if '--count' not in client_args:
        client_args['--count'] = 10000

    if '--numObjects' not in client_args:
        client_args['--numObjects'] = 1000000

    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)

    print("# Cumulative distribution of time for a single client to write\n"
          "# %d %d-byte objects to a table with one index and %d\n"
          "# initial objects. Each object has two 30-byte keys and a 100\n"
          "# byte value. Each line indicates that a given fraction of all\n"
          "# reads took at most a given time to complete.\n"
          "#\n"
          "# Generated by 'clusterperf.py readDist'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % (client_args['--count'], options.size, client_args['--numObjects'] ))
    print_cdf_from_log()


def indexReadDist(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '--maxCores 2 --totalMasterMemory 1500'
    if cluster_args['timeout'] < 200:
        cluster_args['timeout'] = 200

    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())

    if '--count' not in client_args:
        client_args['--count'] = 10000

    if '--numObjects' not in client_args:
        client_args['--numObjects'] = 1000000

    if '--warmup' not in client_args:
        client_args['--warmup'] = 100

    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)

    print("# Cumulative distribution of time for a single client to read\n"
          "# %d %d-byte objects to a table with one index and %d\n"
          "# initial objects. Each object has two 30-byte keys and a 100\n"
          "# byte value. Each line indicates that a given fraction of all\n"
          "# reads took at most a given time to complete.\n"
          "#\n"
          "# Generated by 'clusterperf.py readDist'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % (client_args['--count'], options.size, client_args['--numObjects'] ))
    print_cdf_from_log()

def transactionDist(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 2000'
    if options.numTables == None:
        client_args['--numTables'] = 1
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path,  flatten_args(client_args), name),
            **cluster_args)
    print("# Cumulative distribution of time for a single client to commit a\n"
          "# transactional read-write on a single %d-byte object from a\n"
          "# single server.  Each line indicates that a given fraction of all\n"
          "# commits took at most a given time to complete.\n"
          "# Generated by 'clusterperf.py %s'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % (options.size, name))
    if (options.rcdf):
        print_rcdf_from_log()
    else:
        print_cdf_from_log()

def transactionThroughput(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 2000'
    if cluster_args['timeout'] < 250:
        cluster_args['timeout'] = 250
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = len(getHosts()) - cluster_args['num_servers']
    if cluster_args['num_clients'] < 2:
        print("Not enough machines in the cluster to run the '%s' benchmark"
                % name)
        print("Need at least 2 machines in this configuration")
        return
    if options.numTables == None:
        client_args['--numTables'] = 1
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    for i in range(1, cluster_args['num_clients'] + 1):
        print(get_client_log(i), end='')

def multiOp(name, options, cluster_args, client_args):
    if cluster_args['timeout'] < 100:
        cluster_args['timeout'] = 100
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    client_args['--numTables'] = cluster_args['num_servers'];
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name),
            **cluster_args)
    print(get_client_log(), end='')

def netBandwidth(name, options, cluster_args, client_args):
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 2*len(config.getHosts())
    if options.num_servers == None:
        cluster_args['num_servers'] = cluster_args['num_clients']
        if cluster_args['num_servers'] > len(config.getHosts()):
            cluster_args['num_servers'] = len(config.getHosts())
    if options.size != None:
        client_args['--size'] = options.size
    else:
        client_args['--size'] = 1024*1024;
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def readAllToAll(name, options, cluster_args, client_args):
    cluster_args['backups_per_server'] = 0
    cluster_args['replicas'] = 0
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = len(getHosts())
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    client_args['--numTables'] = cluster_args['num_servers'];
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def readDist(name, options, cluster_args, client_args):
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path,  flatten_args(client_args), name),
            **cluster_args)
    print("# Cumulative distribution of time for a single client to read a\n"
          "# single %d-byte object from a single server.  Each line indicates\n"
          "# that a given fraction of all reads took at most a given time\n"
          "# to complete.\n"
          "# Generated by 'clusterperf.py readDist'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % options.size)
    print_cdf_from_log()

def readDistRandom(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 1000'
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path,  flatten_args(client_args), name),
            **cluster_args)
    print("# Cumulative distribution of time for a single client to read a\n"
          "# random %d-byte object from a single server.  Each line indicates\n"
          "# that a given fraction of all reads took at most a given time\n"
          "# to complete.\n"
          "# Generated by 'clusterperf.py readDist'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % options.size)
    if (options.rcdf):
        print_rcdf_from_log()
    else:
        print_cdf_from_log()

def readLoaded(name, options, cluster_args, client_args):
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 20
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def readRandom(name, options, cluster_args, client_args):
    cluster_args['backups_per_server'] = 0
    cluster_args['replicas'] = 0
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 16
    if options.num_servers == None:
        cluster_args['num_servers'] = 1
    client_args['--numTables'] = cluster_args['num_servers'];
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

# This method is also used for multiReadThroughput and
# linearizableWriteThroughput
def readThroughput(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 2000'
    if cluster_args['timeout'] < 250:
        cluster_args['timeout'] = 250
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = len(getHosts()) - cluster_args['num_servers']
    if cluster_args['num_clients'] < 2:
        print("Not enough machines in the cluster to run the '%s' benchmark"
                % name)
        print("Need at least 2 machines in this configuration")
        return
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def txCollision(name, options, cluster_args, client_args):
    if cluster_args['timeout'] < 100:
        cluster_args['timeout'] = 100
    if options.num_servers == None:
        cluster_args['num_servers'] = len(getHosts())
    #client_args['--numTables'] = cluster_args['num_servers'];
    if 'num_clients' not in cluster_args:
        cluster_args['num_clients'] = 5
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path, flatten_args(client_args), name), **cluster_args)
    print(get_client_log(), end='')

def writeDist(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 2000'
    cluster_args['disjunct'] = True
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path,  flatten_args(client_args), name),
            **cluster_args)
    print("# Cumulative distribution of time for a single client to write a\n"
          "# single %d-byte object from a single server.  Each line indicates\n"
          "# that a given fraction of all writes took at most a given time\n"
          "# to complete.\n"
          "# Generated by 'clusterperf.py %s'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % (options.size, name))
    if (options.rcdf):
        print_rcdf_from_log()
    else:
        print_cdf_from_log()

def workloadDist(name, options, cluster_args, client_args):
    if 'master_args' not in cluster_args:
        cluster_args['master_args'] = '-t 2000'
    cluster_args['disjunct'] = True
    cluster.run(client='%s/ClusterPerf %s %s' %
            (obj_path,  flatten_args(client_args), name),
            **cluster_args)
    print("# Cumulative distribution latencies for operations specified by\n"
          "# the benchmark.\n#\n"
          "# Generated by 'clusterperf.py %s'\n#\n"
          "# Time (usec)  Cum. Fraction\n"
          "#---------------------------"
          % (name))
    if (options.rcdf):
        print_rcdf_from_log()
    else:
        print_cdf_from_log()

#-------------------------------------------------------------------
#  End of driver functions.
#-------------------------------------------------------------------

# The following tables define all of the benchmarks supported by this program.
# The benchmarks are divided into two groups:
#   * simple_tests describes tests that output one or more individual
#     performance metrics
#   * graph_tests describe tests that generate one graph per test;  the graph
#     output is in gnuplot format with comments describing the data.

simple_tests = [
    Test("basic", basic),
    Test("broadcast", broadcast),
    Test("netBandwidth", netBandwidth),
    Test("readAllToAll", readAllToAll),
    Test("readNotFound", default)
]

graph_tests = [
    Test("indexBasic", indexBasic),
    Test("indexRange", indexRange),
    Test("indexMultiple", indexMultiple),
    Test("indexScalability", indexScalability),
    Test("indexReadDist", indexReadDist),
    Test("indexWriteDist", indexWriteDist),
    Test("multiRead_general", multiOp),
    Test("multiRead_generalRandom", multiOp),
    Test("multiRead_oneMaster", multiOp),
    Test("multiRead_oneObjectPerMaster", multiOp),
    Test("multiReadThroughput", readThroughput),
    Test("multiWrite_oneMaster", multiOp),
    Test("transaction_oneMaster", multiOp),
    Test("transactionDistRandom", transactionDist),
    Test("transactionThroughput", transactionThroughput),
    Test("transactionContention", transactionThroughput),
    Test("readDist", readDist),
    Test("readDistRandom", readDistRandom),
    Test("readDistWorkload", workloadDist),
    Test("readLoaded", readLoaded),
    Test("readRandom", readRandom),
    Test("readThroughput", readThroughput),
    Test("readVaryingKeyLength", default),
    Test("transaction_oneMaster", multiOp),
    Test("transaction_collision", txCollision),
    Test("writeAsyncSync", default),
    Test("writeVaryingKeyLength", default),
    Test("writeDist", writeDist),
    Test("writeDistRandom", writeDist),
    Test("writeDistWorkload", workloadDist),
    Test("writeThroughput", readThroughput),
    Test("workloadThroughput", readThroughput),
]

if __name__ == '__main__':
    parser = OptionParser(description=
            'Run one or more performance benchmarks on a RAMCloud cluster.  Each '
            'test argument names one test to run (default: run a selected subset '
            'of useful benchmarks; "all" means run all benchmarks).  Not all options '
            'are used by all benchmarks.',
            usage='%prog [options] test test ...',
            conflict_handler='resolve')
    parser.add_option('-n', '--clients', type=int,
            metavar='N', dest='num_clients',
            help='Number of instances of the client application '
                 'to run')
    parser.add_option('-c', '--count', type=int,
            metavar='N', dest='count',
            help='Number of times to perform the operation')
    parser.add_option('--disjunct', action='store_true', default=False,
            metavar='True/False',
            help='Do not colocate clients on a node (servers are never '
                  'colocated, regardless of this option)')
    parser.add_option('--debug', action='store_true', default=False,
            help='Pause after starting servers but before running '
                 'clients to enable debugging setup')
    parser.add_option('-d', '--logDir', default='logs', metavar='DIR',
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
    parser.add_option('-r', '--replicas', type=int, default=3,
            metavar='N',
            help='Number of disk backup copies for each segment')
    parser.add_option('--servers', type=int,
            metavar='N', dest='num_servers',
            help='Number of hosts on which to run servers')
    parser.add_option('-s', '--size', type=int, default=100,
            help='Object size in bytes')
    parser.add_option('--numObjects', type=int,
            help='Number of objects per operation.')
    parser.add_option('--numTables', type=int,
            help='Number of tables involved.')
    parser.add_option('-t', '--timeout', type=int, default=30,
            metavar='SECS',
            help="Abort if the client application doesn't finish within "
                 'SECS seconds')
    parser.add_option('-m', '--masterArgs', metavar='mARGS',
            dest='master_args',
            help='Additional command-line arguments to pass to '
                 'each master')
    parser.add_option('-T', '--transport', default='infrc',
            help='Transport to use for communication with servers')
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='Print progress messages')
    parser.add_option('-w', '--warmup', type=int,
            help='Number of times to execute operating before '
            'starting measurements')
    parser.add_option('--workload', default='YCSB-A',
            choices=['YCSB-A', 'YCSB-B', 'YCSB-C', 'WRITE-ONLY'],
            help='Name of workload to run on extra clients to generate load')
    parser.add_option('--targetOps', type=int,
            help='Operations per second that each load generating client '
            'will try to achieve')
    parser.add_option('--txSpan', type=int,
                    help='Number servers a transaction should span.')
    parser.add_option('-i', '--numIndexlet', type=int,
            help='Number of indexlets for measuring index scalability ')
    parser.add_option('-k', '--numIndexes', type=int,
            help='Number of secondary keys/object to measure index operations')
    parser.add_option('--numVClients', type=int,
            metavar='N', dest='numVClients',
            help='Number of virtual clients each client instance should '
                 'simulate')
    parser.add_option('--rcdf', action='store_true', default=False,
            dest='rcdf',
            help='Output reverse CDF data instead.')
    (options, args) = parser.parse_args()

    # Invoke the requested tests (run all of them if no tests were specified)
    try:
        if len(args) == 1 and args[0] == 'all':
            # Run all of the tests.

            for test in simple_tests:
                run_test(test, options)
            for test in graph_tests:
                run_test(test, options)
        else:
            if len(args) == 0:
                # Provide a default set of tests to run (the most useful ones).
                args = ["basic",
                        "multiRead_oneMaster",
                        "multiRead_oneObjectPerMaster",
                        "multiReadThroughput",
                        "multiWrite_oneMaster",
                        "readDistRandom",
                        "readThroughput",
                        "readVaryingKeyLength",
                        "writeVaryingKeyLength"
                ]
            for name in args:
                for test in simple_tests:
                    if test.name == name:
                        run_test(test, options)
                        break
                else:
                    for test in graph_tests:
                        if test.name == name:
                            run_test(test, options)
                            break
                    else:
                        print("No clusterperf test named '%s'" % (name))
    finally:
        logInfo = log.scan("%s/latest" % (options.log_dir),
                ["WARNING", "ERROR"],
                ["starting new cluster from scratch",
                 "Ping timeout to server"])
        if len(logInfo) > 0:
            print(logInfo, file=sys.stderr)
