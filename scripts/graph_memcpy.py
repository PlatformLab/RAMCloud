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

"""A wrapper around the misc/memcpy benchmark that generates graphs.

Graphs will be dropped into a memcpy/ directory by default, which can be
overridden by specifying a name on the command line.
"""

from __future__ import division
from common import *
import errno
import os
import re
import sys

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

if len(sys.argv) == 2:
    dirname = sys.argv[1]
else:
    dirname = 'memcpy'

try:
    os.mkdir(dirname)
except OSError, e:
    if e.errno != errno.EEXIST:
        raise

output = captureSh('%s/misc/memcpy' % obj_dir)

open('%s/data.txt' % dirname, 'w').write(output)

open('%s/memcpy.html' % dirname, 'w').write("""
<img src="memcpy.png" />

<p>
Raw data:
</p>
<pre>
%s
</pre>
""" % output)

lines = output.split('\n')
data = OrderedDict()
category = None
for line in lines:
    if not line:
        continue
    m = re.search('^=== (.*) Memcpy ===$', line)
    if m is not None:
        category = data[m.group(1)] = []
        continue
    if 'bytes' in line:
        cbytes, _, cavg, _, cstddev, _, cmin, _, cmax, _ = line.split()
        category.append((cbytes,
                         cavg,
                         cmin,
                         cmax,
                         float(cbytes) / float(cavg) * (10**9 / 2**30),
                         float(cbytes) / float(cmin) * (10**9 / 2**30),
                         float(cbytes) / float(cmax) * (10**9 / 2**30)))
        continue
    raise Exception, line

for label, points in data.items():
    dat = open('%s/%s.dat' % (dirname, label.lower()), 'w')
    for point in points:
        dat.write('%s\n' % '\t'.join(map(str, point)))
    dat.close()
for name, columns in [('absolute', '1:2:3:4'), ('bandwidth', '1:5:6:7')]:
    cmds = []
    for label in data.keys():
        cmds.append(('"%s.dat" using %s title "%s" with yerrorlines' %
                     (label.lower(), columns, label)))
    gnu = open('%s/%s.gnu' % (dirname, name), 'w')
    gnu.write('plot %s\n' % (', \\\n     '.join(cmds)))
    gnu.close()

cmd = 'cd %s; gnuplot %s/scripts/graph_memcpy.gnu' % (dirname, os.getcwd())
try:
    if float(captureSh('gnuplot --version').split()[1]) < 4.2:
        raise Exception, "gnuplot version too old"
except:
    print 'Run the following command with a recent version of gnuplot:'
    print cmd
else:
    sh(cmd)
