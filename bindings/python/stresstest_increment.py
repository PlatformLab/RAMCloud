#!/usr/bin/env python

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

"""Repeatedly execute a transaction that increments multiple objects.

WARNING: This file does not create a new client instance per worker process.
Your client library needs a mutex in shared memory around the networking code.
See RAM-39.

This is a stress test and something of a microbenchmark for RAMCloud.

Run this program with --help for usage."""

import os
import sys
import random
import time
from optparse import OptionParser

import multiprocessing

from retries import ImmediateRetry as RetryStrategy
import ramcloud
import txramcloud
from testutil import BreakException

txramcloud.RetryStrategy = RetryStrategy

class Stats(object):
    INCREMENTS = 0
    ABORTS = 1
    CRASHES = 2

    NUM = 3
    LABELS = ['increments', 'aborts', 'crashes']

    @classmethod
    def to_str(cls, stats):
        pairs = ["'%s': %d" % (l, v) for (l, v) in zip(cls.LABELS, stats)]
        return '{%s}' % ', '.join(pairs)

class CountdownHook(object):
    def __init__(self, count):
        self.count = count
    def __call__(self):
        if self.count == 0:
            raise BreakException
        else:
            self.count -= 1

class Test(object):
    def __init__(self, txrc, table, oids, stats, die, options, args):
        self.txrc = txrc
        self.table = table
        self.oids = oids
        self.global_stats = stats
        self.die = die
        self.options = options
        self.args = args

    def algo(self):
        raise NotImplementedError()

    def __call__(self):
        # Called by the child in its address space
        # self.global_stats, self.die are in shared memory

        # detach the child from the parent's TTY
        os.setsid()

        if self.options.crash:
            when = random.randint(0, self.options.crash)
            self.txrc.hook = CountdownHook(when)

        self.local_stats = [0] * Stats.NUM

        self.cache = {}
        for oid in self.oids:
            self.cache[oid] = None

        i = 0
        try:
            while (self.options.num_increments == 0 or
                   i < self.options.num_increments):
                for retry in RetryStrategy():
                    if die.value:
                        return
                    for oid in self.oids:
                        if self.cache[oid] is None:
                            blob, version = self.txrc.read(self.table, oid)
                            value = int(blob)
                            self.cache[oid] = (value, version)
                    if not self.algo():
                        retry.later()
                i += 1
        except BreakException:
            print "PID %d: crash after %s" % (os.getpid(),
                                              Stats.to_str(self.local_stats))
            self.local_stats[Stats.CRASHES] += 1
        else:
            print "PID %d: done after %s" % (os.getpid(),
                                             Stats.to_str(self.local_stats))
        finally:
            # update global stats
            for i, v in enumerate(self.local_stats):
                self.global_stats[i] += v

class TestWrite(Test):
    def algo(self):
        for oid in self.oids:
            value, version = self.cache[oid]
            try:
                version = self.txrc.update(self.table, oid, str(value + 1),
                                           version)
            except:
                self.cache[oid] = None
                self.local_stats[Stats.ABORTS] += 1
                return False
            else:
                self.cache[oid] = (value + 1, version)
                self.local_stats[Stats.INCREMENTS] += 1
                return True

class TestMT(Test):
    def algo(self):
        mt = txramcloud.MiniTransaction()
        for oid in self.oids:
            value, version = self.cache[oid]
            rr = ramcloud.RejectRules.exactly(version)
            mt[(self.table, oid)] = txramcloud.MTWrite(str(value + 1), rr)
        try:
            result = self.txrc.mt_commit(mt)
        except txramcloud.TxRAMCloud.TransactionRejected, e:
            for ((table, oid), reason) in e.reasons.items():
                self.cache[oid] = None
            self.local_stats[Stats.ABORTS] += 1
            return False
        except txramcloud.TxRAMCloud.TransactionExpired, e:
            self.local_stats[Stats.ABORTS] += 1
            return False
        else:
            for ((table, oid), version) in result.items():
                self.cache[oid] = (self.cache[oid][0] + 1, version)
            self.local_stats[Stats.INCREMENTS] += 1
            return True

if __name__ == '__main__':
    parser = OptionParser()
    parser.set_description(__doc__.split('\n\n', 1)[0])
    parser.add_option("-p", "--num-processes",
                      dest="num_procs", type="int", default=1,
                      help="spawn NUM processes, defaults to 1",
                      metavar="NUM")
    parser.add_option("-i", "--num-increments",
                      dest="num_increments", type="int", default=0,
                      help=("increment NUM times per process per object, " +
                            "defaults to infinity"),
                      metavar="NUM")
    parser.add_option("-o", "--num-objects",
                      dest="num_objects", type="int", default=1,
                      help=("increment across NUM objects, defaults to 1"),
                      metavar="NUM")
    parser.add_option("-c", "--crash",
                      dest="crash", type="int", default=0,
                      help=("crash randomly by the NUM-th RAMCloud "
                            "operation, defaults to not crashing"),
                      metavar="NUM")
    parser.add_option("-u", "--unsafe", action="store_true",
                      dest="unsafe", default=False,
                      help="don't use transactions")
    (options, args) = parser.parse_args()
    assert not args

    r = txramcloud.TxRAMCloud(7)
    r.connect()

    r.create_table("test")
    table = r.get_table_id("test")

    oids = range(options.num_objects)

    for oid in oids:
        r.create(table, oid, str(0))

    stats = multiprocessing.Array('i', Stats.NUM)
    die = multiprocessing.Value('i', 0, lock=False)

    if options.unsafe:
        target_class = TestWrite
    else:
        target_class = TestMT
    target = target_class(r, table, oids, stats, die, options, args)

    procs = []
    for i in range(options.num_procs):
        procs.append(multiprocessing.Process(target=target))

    start = time.time()
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        # a process can be joined multiple times
        die.value = 1
        for p in procs:
            p.join()
    end = time.time()

    print "wall time: %0.02fs" % (end - start)
    print "stats:", Stats.to_str(stats[:])
    for oid in oids:
        blob, version = r.read(table, oid)
        value = int(blob)
        print 'oid %d: value=%d, version=%d' % (oid, value, version)

