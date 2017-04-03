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
    print("# RAMCloud WriteThroughput varying WitnessTracker::syncBatchSize.");
    print("# Batch  Throughput    \tMin      \tMax     \tMedian");
    batchSize = 0
    throughputList = []
    for line in f:
        matchBatchSize = re.match('batchSize: ([0-9]+)', line)
        if matchBatchSize:
            if len(throughputList) != 0:
                #Print out.
                print("  %d  \t%7.3f  \t%7.3f  \t%7.3f  \t%7.3f" % (batchSize, np.mean(throughputList), np.amin(throughputList), np.amax(throughputList), np.median(throughputList)))
            throughputList = []
            batchSize = int(matchBatchSize.group(1))
            continue
        match = re.match('\s+([0-9]+)\s+([0-9.]+)\s+([0-9.]+)\s+.*', line)
        if not match:
            continue
        throughputList.append(float(match.group(2)))
    print("  %d  \t%7.3f  \t%7.3f  \t%7.3f  \t%7.3f" % (batchSize, np.mean(throughputList), np.amin(throughputList), np.amax(throughputList), np.median(throughputList)))
    

if len(sys.argv) != 2:
    print("Usage: %s stringFrom stringTo logFile" % (sys.argv[0]))
    sys.exit(1)

scan(open(sys.argv[1]))