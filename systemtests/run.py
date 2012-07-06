#!/usr/bin/env python

# Copyright (c) 2012 Stanford University
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

"""Runs RAMCloud system tests. Should be run from the top-level directory
of the RAMCloud source tree.

Notice these tests run on multiple machines throughout the cluster which
has a few implcations. First, you'll need a passwordless ssh logins to
work for your user before you can run these. Second, you'll need ssh
master mode configured to run them reliably.
See https://ramcloud.stanford.edu/wiki/display/ramcloud/Running+Recoveries+with+recovery.py
Finally, they take a long time to run; just starting and stopping the processes
may take several seconds for each test.

Tests use config.py/localconfig.py to find a list of hosts where the tests
should run. Make sure you've coordinated with anyone else who might be
using the machines you have listed there before running these tests.

This runner attempts to automatically discover tests. It imports
all the modules in the directory and looks for a 'suite' field. If it exists
the tests in the suite are added and run.
"""

from __future__ import division, print_function

import sys
import os
suites = []
for module in os.listdir(os.path.dirname(__file__)):
    if module == 'run.py' or module == '__init__.py' or module[-3:] != '.py':
        continue
    module_name = module[:-3]
    __import__(module_name, locals(), globals())
    try:
        mod = sys.modules['systemtests.%s' % module_name]
    except KeyError:
        mod = sys.modules[module_name]
    try: 
        suites.append(mod.suite)
    except AttributeError:
        print('No test suite found in %s' % module_name)
    else:
        print('Adding tests from %s' %  module_name)

import unittest
alltests = unittest.TestSuite(suites)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(alltests)
