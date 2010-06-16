#!/usr/bin/env python

# Copyright (c) 2009-2010 Stanford University
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

class MockParseRange(object):
    def __init__(self, lineno, line):
        m = re.search('^( *)BEGIN_MOCK\(\s*(\w+)\s*,\s*(\w+)\s*\)', line)
        if m is None:
            raise ValueError
        out.indentlevel = len(m.group(1)) / 4
        self.lines = []
        self.mock_class = m.group(2)
        self.base_class = m.group(3)

    def add(self, lineno, line):
        if 'END_MOCK' in line:
            out.line('// begin generated code from %s' % sys.argv[0])
            self.handler()
            out.line('// end generated code')
            return None
        else:
            self.lines.append(line)
            return self

    def handler(self):
        steps = []
        step_method = None
        step_lines = None
        step_whitespace = None

        prototypes = PROTOTYPES[self.base_class]

        for line in self.lines:
            line = line.rstrip()
            if not line:
                continue
            if step_method is None:
                m = re.match('^(\s*)(\w+)\((.*)\)\s*{\s*$', line)
                step_whitespace = m.group(1)
                step_method = m.group(2)
                assert step_method in prototypes
                step_lines = []
                for assertion in m.group(3).split(','):
                    assertion = assertion.strip()
                    if re.match('^[A-Za-z0-9_]+$', assertion) is None:
                        step_lines.append('    CPPUNIT_ASSERT(%s);' % assertion)
            else:
                if line == step_whitespace + '}':
                    step_lines.append('    break;')
                    steps.append((step_method, step_lines))
                    step_method = None
                    step_lines = None
                    step_whitespace = None
                else:
                    assert line.startswith(step_whitespace)
                    step_lines.append(line[len(step_whitespace):])

        methods = {}
        for i, (method, lines) in enumerate(steps):
            if method not in methods:
                methods[method] = ([prototypes[method] + ' {',
                                    '    ++state;',
                                    '    switch (state) {'],
                                   [],
                                   ['        default:',
                                    '            CPPUNIT_ASSERT(false);',
                                    '            throw 0;',
                                    '    }',
                                    '}'])
            methods[method][1].extend(['case %d: {' % (i + 1)] + lines + ['}'])

        out.line('class %s : public %s {' % (self.mock_class, self.base_class))

        out.line('  public:')
        out.indentlevel += 1
        out.line('%s() : state(0) {' % self.mock_class)
        out.line('}')
        out.line('~%s() {' % self.mock_class)
        out.line('    CPPUNIT_ASSERT(state == %d);' % len(steps))
        out.line('}')
        for first, middle, last in methods.values():
            for l in first:
                out.line(l)
            out.indentlevel += 2
            for l in middle:
                out.line(l)
            out.indentlevel -= 2
            for l in last:
                out.line(l)
        out.indentlevel -= 1

        out.line('  private:')
        out.indentlevel += 1
        out.line('int state;')
        out.indentlevel -= 1

        out.line('};')

class StubParseRange(object):
    def __init__(self, lineno, line):
        m = re.search('^( *)BEGIN_STUB\(\s*(\w+)\s*,\s*([\w:]+)\s*\)', line)
        if m is None:
            raise ValueError
        out.indentlevel = len(m.group(1)) / 4
        self.lines = []
        self.stub_class = m.group(2)
        self.base_class = m.group(3)

    def add(self, lineno, line):
        if 'END_STUB' in line:
            out.line('// begin generated code from %s' % sys.argv[0])
            self.handler()
            out.line('// end generated code')
            return None
        else:
            if not line.strip().endswith(';'):
                raise ValueError
            self.lines.append(line)
            return self

    def handler(self):
        prototypes = {}
        PROTOTYPES[self.stub_class] = prototypes

        out.line('class %s : public %s {' % (self.stub_class, self.base_class))
        out.line('  public:')
        out.indentlevel += 1
        out.line('struct NotImplementedException {};')
        for line in self.lines:
            prototype = line.strip()[:-1] # strip and drop semicolon

            m = re.search('\s(\w+)\(', prototype)
            method_name = m.group(1)
            prototypes[method_name] = prototype

            out.line(prototype + ' __attribute__ ((noreturn)) {')
            out.line('    throw NotImplementedException();')
            out.line('}')

        out.indentlevel -= 1
        out.line('};')

out = Output()
out.line('// This file was automatically generated, so don\'t edit it.')
out.line('// RAMCloud pragma [CPPLINT=0]')

PROTOTYPES = {}

lines = sys.stdin.readlines()
prev = ''
parse_range = None
for i, line in enumerate(lines):
    if len(line) > 1 and line[-2] == '\\':
        prev += line[:-2]
        continue
    else:
        if prev:
            line = prev + line.lstrip()
            prev = ''

    if parse_range is None:
        for range_type in [MockParseRange, StubParseRange]:
            try:
                parse_range = range_type(i, line)
            except ValueError:
                continue
        if parse_range is None:
            out.raw(line[:-1])
    else:
        parse_range = parse_range.add(i, line)

sys.stdout.write(out.contents())
