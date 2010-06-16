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
        self.lineno = 1
        self.input_filename = None
        self.buffer = []

    def blank(self):
        self.linectrl()
        self.buffer.append('')

    def line(self, s):
        self.linectrl()
        self.buffer.append('%s%s' % ('    ' * self.indentlevel, s))

    def raw(self, s):
        self.linectrl()
        self.buffer.append(s)

    def contents(self):
        return '\n'.join(self.buffer + [''])

    def linectrl(self):
        if self.input_filename:
            self.buffer.append('#line %d "%s"' % (self.lineno,
                                                  self.input_filename))
        else:
            self.buffer.append('#line %d' % self.lineno)

class MockParseRange(object):
    def __init__(self, lineno, line):
        m = re.search('^( *)BEGIN_MOCK\(\s*(\w+)\s*,\s*(\w+)\s*\)', line)
        if m is None:
            raise ValueError
        out.indentlevel = len(m.group(1)) / 4
        self.begin_lineno = lineno
        self.lines = []
        self.mock_class = m.group(2)
        self.base_class = m.group(3)

    def add(self, lineno, line):
        if 'END_MOCK' in line:
            self.end_lineno = lineno
            out.lineno = self.begin_lineno
            out.line('// begin generated code from %s' % out.input_filename)
            self.handler()
            out.lineno = self.end_lineno
            out.line('// end generated code')
            return None
        else:
            self.lines.append((lineno, line))
            return self

    def handler(self):

        steps = []
        step_method = None
        step_lines = None
        step_whitespace = None

        prototypes = PROTOTYPES[self.base_class]

        for lineno, line in self.lines:
            line = line.rstrip()
            if not line:
                continue
            if step_method is None:
                m = re.match('^(\s*)(\w+)\((.*)\)\s*{\s*$', line)
                step_lineno = lineno
                step_whitespace = m.group(1)
                step_method = m.group(2)
                assert step_method in prototypes
                step_lines = []
                for assertion in m.group(3).split(','):
                    assertion = assertion.strip()
                    if not assertion:
                        continue
                    if re.match('^[A-Za-z0-9_]+$', assertion) is None:
                        step_lines.append((lineno,
                                           '    CPPUNIT_ASSERT(%s);' % assertion))
            else:
                if line == step_whitespace + '}':
                    step_lines.append((step_lineno, '    break;'))
                    steps.append((step_lineno, step_method, step_lines))
                    step_lineno = None
                    step_method = None
                    step_lines = None
                    step_whitespace = None
                else:
                    assert line.startswith(step_whitespace)
                    step_lines.append((lineno, line[len(step_whitespace):]))

        methods = {}
        for i, (lineno, method, lines) in enumerate(steps):
            if method not in methods:
                methods[method] = (lineno,
                                   ([prototypes[method] + ' {',
                                     '    ++state;',
                                     '    switch (state) {'],
                                    [],
                                    ['        default:',
                                     '            CPPUNIT_ASSERT(false);',
                                     '            throw 0;',
                                     '    }',
                                     '}']))
            methods[method][1][1].extend([(lineno, 'case %d: {' % (i + 1))] +
                                      lines +
                                      [(lineno, '}')])

        out.lineno = self.begin_lineno
        out.line('class %s : public %s {' % (self.mock_class, self.base_class))

        out.line('  public:')
        out.indentlevel += 1
        out.line('%s() : state(0) {' % self.mock_class)
        out.line('}')
        out.line('~%s() {' % self.mock_class)
        out.line('    CPPUNIT_ASSERT(state == %d);' % len(steps))
        out.line('}')
        for method_lineno, (first, middle, last) in methods.values():
            out.lineno = method_lineno
            for l in first:
                out.line(l)

            out.indentlevel += 2
            for line_lineno, l in middle:
                out.lineno = line_lineno
                out.line(l)
            out.indentlevel -= 2

            out.lineno = method_lineno
            for l in last:
                out.line(l)
        out.indentlevel -= 1

        out.lineno = self.end_lineno
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
        self.begin_lineno = lineno
        self.lines = []
        self.stub_class = m.group(2)
        self.base_class = m.group(3)

    def add(self, lineno, line):
        if 'END_STUB' in line:
            self.end_lineno = lineno
            out.lineno = self.begin_lineno
            out.line('// begin generated code from %s' % out.input_filename)
            self.handler()
            out.lineno = self.end_lineno
            out.line('// end generated code')
            return None
        else:
            if not line.strip().endswith(';'):
                raise ValueError
            self.lines.append((lineno, line))
            return self

    def handler(self):
        prototypes = {}
        PROTOTYPES[self.stub_class] = prototypes

        out.lineno = self.begin_lineno
        out.line('class %s : public %s {' % (self.stub_class, self.base_class))
        out.line('  public:')
        out.indentlevel += 1
        out.line('struct NotImplementedException {};')
        for lineno, line in self.lines:
            prototype = line.strip()[:-1] # strip and drop semicolon

            m = re.search('\s(\w+)\(', prototype)
            method_name = m.group(1)
            prototypes[method_name] = prototype

            out.lineno = lineno
            out.line(prototype + ' __attribute__ ((noreturn)) {')
            out.line('    throw NotImplementedException();')
            out.line('}')

        out.indentlevel -= 1
        out.lineno = self.end_lineno
        out.line('};')



PROTOTYPES = {}

infile = open(sys.argv[1])
outfile = open(sys.argv[2], 'w')

out = Output()
out.input_filename = infile.name
out.line('// This file was automatically generated, so don\'t edit it.')

prev = ''
prev_start_lineno = None
parse_range = None
for i, line in enumerate(infile.readlines()):
    i += 1
    out.lineno = i
    if len(line) > 1 and line[-2] == '\\':
        if not prev:
            prev_start_lineno = i
        prev += line[:-2]
        continue
    else:
        if prev:
            line = prev + line.lstrip()
            i = prev_start_lineno
            prev = ''
            prev_start_lineno = None

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

outfile.write(out.contents())
