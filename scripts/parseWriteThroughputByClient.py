#!/usr/bin/env python

# Copyright (c) 2016 Stanford University
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
Scan the time trace data in a log file; find all records containing
a given string, and output only those records, renormalized in time
so that the first record is at time 0.
Usage: ttgrep.py string file
"""

from __future__ import division, print_function
from glob import glob
from optparse import OptionParser
import math
import os
import re
import string
import sys
import numpy as np

def scan(f):
    """
    Scan the log file given by 'f' (handle for an open file) and output
    all-time trace records containing string, with times renormalized
    relative to the time of the first matching record.
    """
    lineCount = 0
    batchSize = 0
    throughputByClient = [[] for i in range(50)]
    lines = [[] for i in range(50)]
    for line in f:
        if lineCount < 9: # Print header again.
            print(line.rstrip())
        lineCount += 1
        
#                print("  %d  \t%7.3f  \t%7.3f  \t%7.3f  \t%7.3f" % (batchSize, np.mean(throughputList), np.amin(throughputList), np.amax(throughputList), np.median(throughputList)))
        match = re.match('\s+([0-9]+)\s+([0-9.]+)\s+([0-9.]+)\s+.*', line)
        if not match:
            continue
        throughputByClient[int(match.group(1))].append(float(match.group(2)))
#        lines[int(match.group(1))].append(line)
        lines[int(match.group(1))].append(match.group(0))
        
#    print(str(lines))
    for i in range(len(lines)):
        if len(throughputByClient[i]) < 1:
            continue
        median = np.median(throughputByClient[i])
#        print("Client index %d , median: %f, list: %s" %(i, median, str(throughputByClient[i])))
        for j in range(len(throughputByClient[i])):
            if throughputByClient[i][j] == median:
                print(lines[i][j])
    #print("  %d  \t%7.3f  \t%7.3f  \t%7.3f  \t%7.3f" % (batchSize, np.mean(throughputList), np.amin(throughputList), np.amax(throughputList), np.median(throughputList)))
    

if len(sys.argv) != 2:
    print("Usage: %s stringFrom stringTo logFile" % (sys.argv[0]))
    sys.exit(1)

scan(open(sys.argv[1]))