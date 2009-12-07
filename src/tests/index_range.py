#!/usr/bin/env python

# Copyright (c) 2009 Stanford University
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

import sys
import re

class Output:
    def __init__(self):
        self.indentlevel = 0
        self.buffer = []

    def blank(self):
        self.buffer.append('')

    def line(self, s):
        self.buffer.append('%s%s' % ('    ' * self.indentlevel, s))

    def raw(self, s):
        self.buffer.append(s)

    def contents(self):
        return '\n'.join(self.buffer + [''])

def list_kv_tuples(s):
    items = []
    s = s.strip()
    if not s:
        return []
    for item in s.split(','):
        key, value = item.split(':')
        key = key.strip()
        value = value.strip()
        items.append((key, value))
    return items

def list_v(s):
    items = []
    s = s.strip()
    if not s:
        return []
    for value in s.split(','):
        value = value.strip()
        items.append(value)
    return items

def range_query_assert(line):
    m = re.search('^\s*RANGE_QUERY_ASSERT\s*\(\s*"\s*(\[|\()\s*(-?\d+)\s*,\s*(-?\d+)(\]|\))\s*=>\s*{(.*)}\s*"\s*\)\s*;\s*$', line)
    assert m is not None, line

    left = m.group(1)
    start = int(m.group(2))
    stop = int(m.group(3))
    right = m.group(4)
    result_set = list_kv_tuples(m.group(5))
    buf_size = len(result_set) + 2

    # buffers
    out.line('int    keybuf[%d];' % (buf_size))
    out.line('double valbuf[%d];' % (buf_size))
    out.line('memset(keybuf, 0xAB, sizeof(keybuf));')
    out.line('memset(valbuf, 0xCD, sizeof(valbuf));')

    # execute RangeQuery, make sure it returned the right number of pairs
    out.line('RAMCloud::RangeQueryArgs<int, double> rq;')
    out.line('bool more;')
    out.line('rq.setKeyStart(%d, %s);' % (start, 'true' if left  == '[' else 'false'))
    out.line('rq.setKeyEnd(%d, %s);'  % (stop,  'true' if right == ']' else 'false'))
    out.line('rq.setLimit(%d);' % (len(result_set) + 1))
    out.line('rq.setResultBuf(keybuf + 1, valbuf + 1);')
    out.line('rq.setResultMore(&more);')
    out.line('CPPUNIT_ASSERT(index->RangeQuery(&rq) == %d);' % len(result_set))
    out.line('CPPUNIT_ASSERT(!more);')

    # make sure the result didn't clobber the first or last element
    out.line('CPPUNIT_ASSERT(keybuf[0]  == static_cast<int>(0xABABABAB));')
    out.line('CPPUNIT_ASSERT(keybuf[%d] == static_cast<int>(0xABABABAB));' % (len(result_set) + 1))
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0])  == 0xCDCDCDCDCDCDCDCD);')
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[%d]) == 0xCDCDCDCDCDCDCDCD);' % (len(result_set) + 1))

    # make sure the buffers contain the right pairs in the right order
    i = 1
    for (k,v) in result_set:
        out.line('CPPUNIT_ASSERT(%s == keybuf[%d]);' % (k, i))
        out.line('CPPUNIT_ASSERT_DOUBLES_EQUAL(%s, valbuf[%d], D);' % (v, i))
        i += 1

def multi_lookup_assert(line):
    m = re.search('^\s*MULTI_LOOKUP_ASSERT\s*\(\s*"\s*(-?\d+)\s*=>\s*{(.*)}\s*"\s*\)\s*;\s*$', line)
    assert m is not None, line

    key = int(m.group(1))
    result_set = list_v(m.group(2))
    buf_size = len(result_set) + 2

    # buffers
    out.line('double valbuf[%d];' % (buf_size))
    out.line('memset(valbuf, 0xCD, sizeof(valbuf));')

    # execute Lookup, make sure it returned the right number of values
    out.line('RAMCloud::MultiLookupArgs<int, double> ml;')
    out.line('bool more;')
    out.line('ml.setKey(%d);' % key)
    out.line('ml.setLimit(%d);' % (len(result_set) + 1))
    out.line('ml.setResultBuf(valbuf + 1);')
    out.line('ml.setResultMore(&more);')
    out.line('CPPUNIT_ASSERT(index->Lookup(&ml) == %d);' % len(result_set))
    out.line('CPPUNIT_ASSERT(!more);')

    # make sure the result didn't clobber the first or last element
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0])  == 0xCDCDCDCDCDCDCDCD);')
    out.line('CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[%d]) == 0xCDCDCDCDCDCDCDCD);' % (len(result_set) + 1))

    # make sure the buffers contain the right values in the right order
    i = 1
    for v in result_set:
        out.line('CPPUNIT_ASSERT_DOUBLES_EQUAL(%s, valbuf[%d], D);' % (v, i))
        i += 1

out = Output()
out.indentlevel += 1

lines = sys.stdin.readlines()
i = 0
prev = ''
for line in lines:
    i += 1
    if len(line) > 1 and line[-2] == '\\':
        prev += line[:-2].rstrip()
        continue
    else:
        if prev:
            line = prev + line.lstrip()
            prev = ''
    if 'RANGE_QUERY_ASSERT' in line:
        handler = range_query_assert
    elif 'MULTI_LOOKUP_ASSERT' in line:
        handler = multi_lookup_assert
    else:
        out.raw(line[:-1])
        continue

    line = line.replace('""', '')

    out.blank()
    out.line('{ // %s' % line.strip())
    out.indentlevel += 1

    handler(line)

    out.indentlevel -= 1
    out.line('}')


open(sys.argv[1], 'w').write(out.contents())
