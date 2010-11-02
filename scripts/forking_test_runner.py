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

"""Runs each unit test in a separate process.

This is useful for finding which tests cause crashes or enter infinite loops.

Pass any arguments to output timing statistics.
"""
import os
import re
import signal
import subprocess
import sys
import time

FAIL_AFTER_SECONDS = 2.0

ignore = \
"""terminate called after throwing an instance of 'std::invalid_argument'
  what():  No test named <%s> found in test <All Tests>."""

cppunit_fail_header = """!!!FAILURES!!!
Test Results:
Run:  1   Failures: 1   Errors: 0


1) test: RAMCloud::%s::%s (F) """

signals = dict([(getattr(signal, name), name)
                for name in dir(signal) if name.startswith('SIG')])

p = subprocess.Popen(['git', 'symbolic-ref', '-q', 'HEAD'],
                     stdout=subprocess.PIPE)
p.wait()
git_branch = re.search('^refs/heads/(.*)$', p.stdout.read())
if git_branch is None:
    obj_dir = 'obj'
else:
    git_branch = git_branch.group(1)
    obj_dir = 'obj.%s' % git_branch

tests = []
for name in os.listdir('src/'):
    if name.endswith('Test.in.cc') or name.endswith('Test.cc'):
        suite = None
        for line in open('src/%s' % name):
            m = re.match('\s*CPPUNIT_TEST_SUITE\((.*)\);', line)
            if m:
                suite = m.group(1)
                continue
            m = re.match('\s*CPPUNIT_TEST\((.*)\);', line)
            if m:
                test = m.group(1)
                tests.append((suite, test))
                continue

print 'Running %d tests...' % len(tests)

ok = 0
failed = 0
suite_times = {}
test_times = {}
for (suite, test) in tests:
    start = time.time()
    process = subprocess.Popen(['./%s/test' % obj_dir,
                                '-t', 'RAMCloud::%s::%s' % (suite, test)],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    rc = None
    while True:
        rc = process.poll()
        now = time.time()
        if rc is not None:
            break
        if now - start > FAIL_AFTER_SECONDS:
            print "Killing %s::%s" % (suite, test)
            process.kill()
            break
    if rc != 0:
        output = process.stdout.read().strip()
        if output == (ignore % test):
            print "Ignored: RAMCloud::%s::%s" % (suite, test)
            continue
        if rc is None:
            why = ' by taking too long (over %ss)' % FAIL_AFTER_SECONDS
        elif rc == 1:
            why = '' # usual CPPUNIT failure
        elif rc > 1:
            why = ' with return value %d' % rc
        elif rc < 0:
            why = ' from signal %s' % signals[-rc]
        cfh = cppunit_fail_header % (suite, test)
        if output.startswith(cfh):
            output = output[len(cfh):]
        print '%s::%s failed%s%s\n' % (suite, test, why,
                                     ':\n%s' % output if output else '')
        failed += 1
    else:
        if suite in suite_times:
            suite_times[suite] += now - start
        else:
            suite_times[suite] = now - start
        suite_test = '%s::%s' % (suite, test)
        if suite_test in test_times:
            test_times[suite_test] += now - start
        else:
            test_times[suite_test] = now - start
        ok += 1

print '%d tests passed, %d failed' % (ok, failed)

def print_timing(title, times, num=None):
    print title
    print '=' * len(title)
    l = times.items()
    l.sort(key=lambda x: x[1], reverse=True)
    if num is not None:
        l = l[:num]
    max_name_length = max([len(name) for name, t in l])
    for name, t in l:
        print '%s%s' % (name.ljust(max_name_length),
                        ('%0.02fms' % (t * 1000)).rjust(8))

if len(sys.argv) > 1:
    print
    print 'Total time: %0.02fms' % (sum(suite_times.values()) * 1000)
    print
    print_timing('Suite Timing', suite_times)
    print
    print_timing('Test Timing (top 20)', test_times, num=20)
