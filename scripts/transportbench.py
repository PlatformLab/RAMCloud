#!/usr/bin/env python

# Copyright (c) 2011 Stanford University
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

from __future__ import print_function
from cluster import *
from functools import *
from optparse import OptionParser

def runTransportBench(numObjects, objectSize, uncached,
                      sandbox, run, coordinator, ensureServers):
    host = hosts[0][0]
    binary = os.path.join(obj_path, 'TransportBench')
    return sandbox.rsh(host,
                 ('%s -C %s -n %d -S %d -m %s' %
                  (binary,
                   coordinator,
                   numObjects,
                   objectSize,
                   '-u' if uncached else '')),
                 bg=True, stderr=subprocess.STDOUT,
                 stdout=open('%s/mcp.%s.log' % (run, host), 'w'))

def transportBench(numObjects, objectSize, transport, uncached=False):
    f = partial(runTransportBench, numObjects, objectSize, uncached)
    stats = cluster(f,
                    numBackups=0,
                    replicas=0,
                    transport=transport,
                    timeout=240)
    for line in open('%s/mcp.%s.log' % (stats['run'], hosts[0][0])):
         m = re.match('.*METRICS: (.*)$', line)
         if m:
             stats.update(eval(m.group(1)))
    return stats

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-S', '--size',
        dest='objectSize', type='int', default=128,
        help='Object size to read/write from the table')
    parser.add_option('-n', '--number',
        dest='numObjects', type='int', default=10000,
        help='Number of times to read the object from the table')
    parser.add_option('-t', '--transport',
        dest='transport', default='fast+infud', type='string',
        help='Name of the transport to use for the run')
    parser.add_option('-c', '--cached',
        dest='cached', default=False, action="store_true",
        help='Read the same object repeatedly')
    options, args = parser.parse_args()
    if options.transport not in locators['coordinator']:
        print('First argument must be a transport, one of:',
              locators['coordinator'].keys())
        sys.exit(-1)

    print('Running', options.transport, 'with',
          options.numObjects, options.objectSize, 'byte objects')
    transportBench(options.numObjects, options.objectSize,
                   options.transport, not options.cached)
