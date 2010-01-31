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

"""Utilities for unit tests."""

import unittest
import cPickle as pickle

class Opaque(object):
    """A serializable object that equals only itself."""

    def __init__(self):
        self._id = id(self)
    def __hash__(self):
        return self._id
    def __cmp__(self, other):
        return cmp(self._id, other._id)

class TestOpaque(unittest.TestCase):
    """Unit tests for L{Opaque}."""

    def test_equality(self):
        x = Opaque()
        y = Opaque()
        self.assertEqual(x, x)
        self.assertNotEqual(x, y)
        self.assert_(x == x)
        self.assert_(x != y)

    def test_pickle(self):
        x1 = Opaque()
        x2 = pickle.loads(pickle.dumps(x1))
        x3 = pickle.loads(pickle.dumps(x2, protocol=2))
        self.assertEqual(x1, x2)
        self.assertEqual(x1, x3)
        self.assertEqual(x2, x3)

if __name__ == '__main__':
    unittest.main()
