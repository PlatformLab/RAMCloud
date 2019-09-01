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

"""Miscellaneous bits and utilities for RAMCloud system tests.
"""

import sys
sys.path.append('scripts')
sys.path.append('bindings/python')

import common
import os
os.environ['LD_LIBRARY_PATH'] = common.obj_dir + ':' + os.environ['LD_LIBRARY_PATH']
import unittest
import signal

hosts = common.getHosts()

def require_hosts(n):
    """Raise an exception if config.py (or localconfig.py) doesn't list enough
    hosts to perform the specified test.
    """
    if len(hosts) < n:
        raise Exception('Not enough hosts to perform test')

def timeout(secs=10):
    """Decorator which can be applied to functions which will abort calls
    to them if they take too long.

    @param secs: Seconds to allow the decorated function to run before
                 throwing TimeoutException.
    """
    class TimeoutException(Exception):
        pass
    def handler(signum, frame):
        raise TimeoutException()
    def decorate(f):
        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            signal.alarm(secs)
            try:
                result = f(*args, **kwargs)
            finally:
                signal.signal(signal.SIGALRM, old)
            signal.alarm(0)
            return result
        new_f.func_name = f.func_name
        return new_f
    return decorate

class ContextManagerTestCase(unittest.TestCase):
    """Replaces unittest test running behavior to make it much more
    exception/signal safe. Tests which subclass this class should
    implemented __enter__ and __exit__ rather than setUp() and
    tearDown(). Those methods will be called using the Python's
    'with' semantics, treating the test instance as a context
    manager. If a keyboard interrupt occurs during a unit test
    the run of the test is aborted but __exit__ still performs
    cleanup before re-raising the interrupt.
    """
    def run(self, result=None):
        if result is None: result = self.defaultTestResult()
        result.startTest(self)
        testMethod = getattr(self, self._testMethodName)
        try:
            ok = False
            try:
                with self as test:
                    testMethod()
                ok = True
            except KeyboardInterrupt:
                raise
            except self.failureException:
                result.addFailure(self, sys.exc_info())
            except:
                result.addError(self, sys.exc_info())
                return
            if ok: result.addSuccess(self)
        finally:
            result.stopTest(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False # rethrow exception, if any

