#!/usr/bin/env python

# Copyright (c) 2015 Stanford University
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
This program generates the header file RpcLevelNums.h, which assigns a
call depth to each RPC opcode, in order to avoid distributed deadlocks.
This program should be run from the top-level RAMCloud source directory.
"""

from __future__ import division, print_function
from glob import glob
from optparse import OptionParser
import math
import os
import re
import string
import sys

# This variable contains information about which RPCs invoke which
# other RPCs (e.g., WRITE invokes BACKUP_WRITE to replicate the new
# data). The table must be updated manually when new RPCs are
# added or existing ones are modified. Each entry in the dictionary
# describes all of the lower-level RPCs invoked (directly) by the handler
# for a particular RPC.  For example, the following entry indicates the
# the implementation of the REMOVE RPC may invoke BACKUP_WRITE and
# REMOVE_INDEX_ENTRY RPCs:
#          "REMOVE":         ["BACKUP_WRITE", "REMOVE_INDEX_ENTRY"],
# The names in the dictionary must be the same as the names used in
# the Opcode enum in WireFormat.h.

callees = {
    "COORD_SPLIT_AND_MIGRATE_INDEXLET":
                             ["SPLIT_AND_MIGRATE_INDEXLET"],
    "CREATE_INDEX":          ["TAKE_INDEXLET_OWNERSHIP",
                              "TAKE_TABLET_OWNERSHIP"],
    "CREATE_TABLE":          ["TAKE_TABLET_OWNERSHIP"],
    "DROP_INDEX":            ["DROP_TABLET_OWNERSHIP"],
    "DROP_TABLE":            ["TAKE_TABLET_OWNERSHIP"],
    "FILL_WITH_TEST_DATA":   ["BACKUP_WRITE"],
    "GET_HEAD_OF_LOG":       ["BACKUP_WRITE"],
    "HINT_SERVER_CRASHED":   ["PING"],
    "INCREMENT":             ["BACKUP_WRITE"],
    "INSERT_INDEX_ENTRY":    ["BACKUP_WRITE"],
    "MIGRATE_TABLET":        ["RECEIVE_MIGRATION_DATA",
                              "REASSIGN_TABLET_OWNERSHIP"],
    "MULTI_OP":              ["BACKUP_WRITE", "INSERT_INDEX_ENTRY",
                              "REMOVE_INDEX_ENTRY"],
    "RECEIVE_MIGRATION_DATA":["BACKUP_WRITE"],
    "RECOVER":               ["BACKUP_GETRECOVERYDATA", "BACKUP_WRITE"],
    "REMOVE":                ["BACKUP_WRITE", "REMOVE_INDEX_ENTRY"],
    "REMOVE_INDEX_ENTRY":    ["BACKUP_WRITE"],
    "SERVER_CONTROL_ALL":    ["SERVER_CONTROL"],
    "TAKE_TABLET_OWNERSHIP": ["BACKUP_WRITE"],
    "TX_DECISION":           ["BACKUP_WRITE"],
    "TX_HINT_FAILED":        ["BACKUP_WRITE"],
    "TX_PREPARE":            ["BACKUP_WRITE"],
    "TX_REQUEST_ABORT":      ["BACKUP_WRITE"],
    "WRITE":                 ["BACKUP_WRITE", "INSERT_INDEX_ENTRY",
                              "REMOVE_INDEX_ENTRY"],
}

# The following dictionary maps from the name of an opcode to its
# numerical value, as defined in WireFormat.h.
opcodes = {}

# The following dictionary maps from the name of an opcode to its RPC
# level number (the depth of nested RPCs under it: 0 means this RPC
# invokes no other RPCS; 1 means it invokes one or more RPCs, but none
# of them invoke other RPCS; etc.)
levels = {}

# Read in WireFormat.h to get a list of valid RPC names and their opcodes.
foundEnum = False
foundEnd = False
for line in open("src/WireFormat.h"):
    # Strip newline
    line = line[0:-1]

    # Skip lines before the Opcode definition.
    if not foundEnum:
        if "enum Opcode {" in line:
            foundEnum = True
        continue

    # Check for the end of the Opcode definition.
    if "};" in line:
        foundEnd = True
        break

    # Record one Opcode value.
    match = re.match(' *([^ ]*) *= *([0-9]*),', line)
    if not match:
        sys.stderr.write("Unrecognized line in WireFormat.h: '%s'\n" % (line))
        sys.exit(1)
    opcodes[match.group(1)] = int(match.group(2))

if not foundEnum:
    sys.stderr.write("Couldn't find \"enum Opcode\" line in WireFormat.h\n")
    sys.exit(1)
if not foundEnd:
    sys.stderr.write("Couldn't find end of Opcode declaration in WireFormat.\n")
    sys.exit(1)

# Verify that all of the names in callees are legal opcodes.
error = False
for caller in callees:
    if not caller in opcodes:
        sys.stderr.write("Unknown name in callees table: %s\n" % (caller))
        error = True
    for callee in callees[caller]:
        if not callee in opcodes:
            sys.stderr.write("Unknown name in callees table: %s\n" % (callee))
            error = True
if error:
    sys.exit(1)

# Compute the calling level for each RPC, in multiple passes. In the
# first pass, find all leaf RPCs (those not listed in callees).
for op in opcodes.keys():
    if not op in callees:
        levels[op] = 0
        # print("%-25s: level 0" % (op))

# Now make additional passes, where in each pass we assign a level to
# all RPCs whose callees have levels.
finished = False
passCount = 0
while (passCount < 20) and not finished:
    finished = True

    # Check all opcodes for which a level has not yet been assigned
    for op in opcodes.keys():
        if op in levels:
            continue
        level = 0

        # See if all of the callees' levels have been assigned; if so,
        # pick a level 1 higher than the largest of the callees.
        for callee in callees[op]:
            if not callee in levels:
                level = -1
                break
            if levels[callee] > level:
                level = levels[callee]
        if level >= 0:
            levels[op] = level+1
            # print("%-25s: level %d" % (op, level+1))
        else:
            finished = False
    passCount += 1

if not finished:
    # If we get here it means that there is a circularity in the call graph.
    # Print out the opcodes that don't yet have assigned levels.
    for op in opcodes.keys():
        if not op in levels:
            sys.stderr.write("Couldn't assign level for %s: circularity "
                    "in call graph?\n" % (op))
    sys.exit(1)

# Finally, generate output, consisting of the guts of a C++ array
# (everything between the '[' and the ']').

print("// This file was generated automatically by genLevels.py.")
print("// Do not modify by hand.")
outputInfo = []
for op in levels:
    outputInfo.append([op, opcodes[op], levels[op]])
outputInfo.sort(key=lambda item: item[1]);
nextOp = 0
for info in outputInfo:
    while info[1] > nextOp:
        print("NO_LEVEL,   // undefined RPC")
        nextOp += 1
    print("%8d,   // %s" % (info[2], info[0]))
    nextOp += 1
