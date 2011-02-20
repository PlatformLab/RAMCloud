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

"""Generates data for a recovery performance graph.

Keeps partition size constant and scales the number of objects.
"""

from __future__ import division
from common import *
import recovery
import subprocess

dat = open('%s/recovery/objectsize_scale.data' % top_path, 'w')

for objectSize in (8192, 4096, 2048, 1024, 512, 256, 128):
    args = {}
    args['numBackups'] = 6
    args['numPartitions'] = 1
    args['objectSize'] = objectSize
    args['disk'] = True
    numObjectsPerMb = 2**20 / (objectSize + 40)
    print('Running with %d objects of size %d' % (numObjectsPerMb * 400, objectSize))
    while True:
        try:
            r= recovery.recover(
                oldMasterArgs='-m 3000',
                numObjects=int(numObjectsPerMb * 400),
                **args)
        except subprocess.CalledProcessError, e:
            print e
        else:
            break
    print 'Result', r
    dat.write('%d\t%d\n' % (objectSize, r['ns']))
    dat.flush()
