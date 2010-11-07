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

"""Read the pragma directives from a source file.

Run this program with --help for usage.
"""

import sys
import re
from optparse import OptionParser


class PragmaDefinition(dict):

    def __init__(self, doc, default):
        self.doc = doc
        self.default = int(default)
        self.name = None

    def get_default(self):
        return self.default

    def __str__(self):
        items = self.items()
        if self.default not in self:
            items.append((self.default, '(unknown)'))
        values = []
        for (value, doc) in sorted(items):
            if self.default == value:
                default = ' (default)'
            else:
                default = ''
            values.append('  %d: %s%s' % (value, doc, default))
        values = '\n'.join(values)
        if len(values):
            values = '\n' + values
        return '%s:%s' % (self.name, values)


class PragmaDefinitions(dict):

    def __setitem__(self, k, v):
        if v.default not in v:
            raise ValueError('Default value must be documented')
        v.name = k
        dict.__setitem__(self, k, v)

    def __str__(self):
        return '\n'.join([str(v) for (k, v) in sorted(self.items())])


def read_pragmas(stream, definitions, magic_pattern):
    pragmas = {}
    for line in stream.readlines():
        m = re.search(magic_pattern, line, re.IGNORECASE)
        if m is None:
            continue
        for pragma in re.split('[,;]*', m.group(1)):
            if len(pragma) == 0:
                continue
            try:
                k, v = re.split('\s*=\s*', pragma, 1)
            except ValueError:
                raise Exception('Bad pragma %s in %s' % (pragma, stream))
            if k not in definitions:
                raise Exception('Unknown pragma %s in %s' % (k, stream))
            try:
                v = int(v)
            except ValueError:
                raise Exception('Bad value %s for %s in %s' % (v, k, stream))
            if v not in definitions[k]:
                raise Exception('Undefined value %s for %s in %s' %
                                (v, k, stream))
            if k in pragmas:
                raise Exception('Duplicate pragma %s defined in %s' %
                                (k, stream))
            pragmas[k] = v
    return pragmas

if __name__ == '__main__':
    usage = '%prog [OPTIONS] FILENAMES\n       %prog -l'
    parser = OptionParser(usage=usage)
    parser.set_description(__doc__.split('\n\n', 1)[0])
    parser.add_option('-f', '--filter', type='str', dest='filt',
                      default=None, metavar='PRAGMA:VALUE',
                      help='filter filenames for PRAGMA with VALUE')
    parser.add_option('-q', '--query', type='str', dest='query', default=None,
                      metavar='PRAGMA',
                      help='print the setting for PRAGMA only')
    parser.add_option('-l', '--list', action='store_true',
                      dest='list_settings', default=False,
                      help='print the list of pragmas definitions')
    parser.add_option('-d', '--def', type='str', dest='defs_file',
                      default='pragmas.conf.py', metavar='FILE',
                      help='use pragma definitions from FILE')
    (options, filenames) = parser.parse_args()

    definitions = PragmaDefinitions()
    globals_ = {'PragmaDefinition': PragmaDefinition,
                'definitions': definitions}
    execfile(options.defs_file, globals_)
    magic_pattern = globals_['magic_pattern']

    if options.list_settings:
        print definitions
        sys.exit(0)


    if options.query is not None:
        if options.query not in definitions:
            raise Exception('Unknown pragma %s to query' % (options.query))

    for filename in filenames:
        pragmas = read_pragmas(open(filename), definitions, magic_pattern)

        if options.filt:
            p, v = options.filt.split(':')
            try:
                if pragmas[p] == int(v):
                    print filename
            except KeyError:
                if definitions[p].default == int(v):
                    print filename
        else:
            if options.query is not None:
                try:
                    print pragmas[options.query]
                except KeyError:
                    print definitions[options.query].default
            else:
                for k, definition in definitions.items():
                    try:
                        v = pragmas[k]
                    except KeyError:
                        v = definition.default
                    print '%s=%d' % (k, v)
