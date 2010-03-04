#!/usr/bin/env python

# Copyright (c) 2010 Stanford University
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

"""Filter Doxygen warning output."""

import sys
import os
import subprocess
import re

cwd = os.getcwd() + '/'

lines = []
pragma_settings = {}
prev = False

for line in sys.stdin.readlines():

    if not line.startswith(cwd):
        if prev:
            lines.append(line)
        continue
    line = line[len(cwd):]

    filename = line.split(':', 1)[0]

    if filename not in pragma_settings:
        p = subprocess.Popen('./pragmas.py -q DOXYGEN %s' % filename,
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        assert p.wait() == 0
        pragma_settings[filename] = int(p.stdout.read())

    if pragma_settings[filename] >= int(sys.argv[1]):
        lines.append(line)
        prev = True
    else:
        prev = False

sys.stdout.write(''.join(lines))
sys.exit(1 if lines else 0)
