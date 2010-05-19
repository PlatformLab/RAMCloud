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

PROTOTYPES = {
    'TestSyscalls': {
        'accept': 'int accept(int sockfd, struct sockaddr* addr, ' +
                             'socklen_t* addrlen)',
        'bind': 'int bind(int sockfd, const struct sockaddr* addr, ' +
                         'socklen_t addrlen)',
        'close': 'int close(int fd)',
        'connect': 'int connect(int sockfd, const struct sockaddr* addr, ' +
                               'socklen_t addrlen)',
        'listen': 'int listen(int sockfd, int backlog)',
        'recv': 'ssize_t recv(int sockfd, void* buf, size_t len, int flags)',
        'sendmsg': 'ssize_t sendmsg(int sockfd, const struct msghdr* msg, ' +
                                   'int flags)',
        'setsockopt': 'int setsockopt(int sockfd, int level, int optname, ' +
                                     'const void* optval, socklen_t optlen)',
        'socket': 'int socket(int domain, int type, int protocol)',
    },
    'TestServerSocket': {
        'init': 'void init(TCPTransport::ListenSocket* listenSocket)',
        'recv': 'void recv(Buffer* payload)',
        'send': 'void send(const Buffer* payload)',
    },
    'TestClientSocket': {
        'init': 'void init(const char* ip, uint16_t port)',
        'recv': 'void recv(Buffer* payload)',
        'send': 'void send(const Buffer* payload)',
    },
}

def handler(mock_class, base_class, lines):
    steps = []
    step_method = None
    step_lines = None
    step_whitespace = None

    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        if step_method is None:
            m = re.match('^(\s*)(\w+)\((.*)\)\s*{\s*$', line)
            step_whitespace = m.group(1)
            step_method = m.group(2)
            assert step_method in PROTOTYPES[base_class]
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
            methods[method] = ([PROTOTYPES[base_class][method] + ' {',
                                '    ++state;',
                                '    switch (state) {'],
                               [],
                               ['        default:',
                                '            CPPUNIT_ASSERT(false);',
                                '            throw 0;',
                                '    }',
                                '}'])
        methods[method][1].extend(['case %d: {' % (i + 1)] + lines + ['}'])

    out.line('class %s : public %s {' % (mock_class, base_class))

    out.line('  public:')
    out.indentlevel += 1
    out.line('%s() : state(0) {' % mock_class)
    out.line('}')
    out.line('~%s() {' % mock_class)
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

out = Output()
out.line('// This file was automatically generated, so don\'t edit it.')
out.line('// RAMCloud pragma [CPPLINT=0]')
out.indentlevel += 1

lines = sys.stdin.readlines()
prev = ''
mock_lines = None
mock_class = None
base_class = None
for i, line in enumerate(lines):
    if len(line) > 1 and line[-2] == '\\':
        prev += line[:-2].rstrip()
        continue
    else:
        if prev:
            line = prev + line.lstrip()
            prev = ''

    if mock_lines is None:
        m = re.search('BEGIN_MOCK\(\s*(\w+)\s*,\s*(\w+)\s*\)', line)
        if m is not None:
            mock_lines = []
            mock_class = m.group(1)
            base_class = m.group(2)
        else:
            out.raw(line[:-1])
    else:
        if 'END_MOCK' in line:
            out.indentlevel += 1
            out.line('// begin generated code from %s' % sys.argv[0])
            handler(mock_class, base_class, mock_lines)
            out.line('// end generated code')
            out.indentlevel -= 1
            mock_lines = None
            mock_class = None
            base_class = None
        else:
            mock_lines.append(line)

open(sys.argv[1], 'w').write(out.contents())
