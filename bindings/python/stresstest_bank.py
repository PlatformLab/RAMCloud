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

"""Repeatedly execute bank transfers.

WARNING: This file does not create a new client instance per worker process.
Your client library needs a mutex in shared memory around the networking code.
See RAM-39.

This is a stress test for RAMCloud.

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

        i = 1
        try:
            while True:
                if i % 10**6 == 0:
                    print "PID %d: continuing after %s" % (os.getpid(),
                          Stats.to_str(self.local_stats))

                accts = self.choose_accts()

                for retry in RetryStrategy():
                    if die.value:
                        print "PID %d: done after %s" % (os.getpid(),
                              Stats.to_str(self.local_stats))
                        return
                    try:
                        for oid in accts:
                            if self.cache[oid] is None:
                                blob, version = self.txrc.read(self.table, oid)
                                value = int(blob)
                                self.cache[oid] = (value, version)
                        if not self.algo(accts):
                            retry.later()
                    except BreakException:
                        print "PID %d: crash after %s" % (os.getpid(),
                              Stats.to_str(self.local_stats))
                        self.local_stats[Stats.CRASHES] += 1
                        for oid in self.cache:
                            self.cache[oid] = None
                        when = random.randint(0, self.options.crash)
                        self.txrc.hook = CountdownHook(when)
                        # and keep going
                i += 1
        finally:
            # update global stats
            for i, v in enumerate(self.local_stats):
                self.global_stats[i] += v

    def choose_accts(self):
        assert len(self.oids) >= 2

        max_num_accts = len(self.oids)
        if (self.options.max_num_accts_per_tx and
            self.options.max_num_accts_per_tx < max_num_accts):
            max_num_accts = self.options.max_num_accts_per_tx
        num_accts = random.randint(2, max_num_accts)
        accts = list(oids)
        random.shuffle(accts)
        accts = accts[:num_accts]
        return accts

    def algo(self, accts):
        mt = txramcloud.MiniTransaction()

        new_values = {}
        for oid in accts:
            value, version = self.cache[oid]
            rr = ramcloud.RejectRules.exactly(version)
            if oid == accts[0]:
                value -= len(accts[1:])
            else:
                value += 1
            new_values[oid] = value
            mt[(self.table, oid)] = txramcloud.MTWrite(str(value), rr)
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
                self.cache[oid] = (new_values[oid], version)
            self.local_stats[Stats.INCREMENTS] += 1
            return True

if __name__ == '__main__':
    parser = OptionParser()
    parser.set_description(__doc__.split('\n\n', 1)[0])
    parser.add_option("-p", "--num-processes",
                      dest="num_procs", type="int", default=1,
                      help="spawn NUM processes, defaults to 1",
                      metavar="NUM")
    parser.add_option("-o", "--num-objects",
                      dest="num_objects", type="int", default=2,
                      help=("increment across NUM objects, defaults to 2"),
                      metavar="NUM")
    parser.add_option("-m", "--max-tx",
                      dest="max_num_accts_per_tx", type="int", default=0,
                      help=("the maximum NUM of accounts to involve in a " +
                            "single transaction, defaults to infinity"),
                      metavar="NUM")
    parser.add_option("-c", "--crash",
                      dest="crash", type="int", default=0,
                      help=("crash randomly by the NUM-th RAMCloud "
                            "operation, defaults to not crashing"),
                      metavar="NUM")
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

    target = Test(r, table, oids, stats, die, options, args)

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
    sum = 0
    for oid in oids:
        blob, version = r.read(table, oid)
        value = int(blob)
        sum += value
        print 'oid %d: value=%d, version=%d' % (oid, value, version)
    print 'sum: %d' % sum
    assert sum == 0

