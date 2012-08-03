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
#
# Installation:
#
# Install termcolor available here or through your distro:
# http://pypi.python.org/pypi/termcolor
# 
# Then just create a script to pipe your junk through colorize.
# make $* 2>&1 | python colorize.py
# exit ${PIPESTATUS[0]}

import termcolor
import re
import fileinput
import os
import sys

# Most of the 'configuration'.
# Default substitutions, colors, etc.

def color(*args, **kwargs):
    """Return a function that takes a string and colors it."""
    return lambda s: termcolor.colored(s, *args, **kwargs)

markup = [ ('g++',              color('yellow'))
         , ('ccache',           color('yellow'))
         , ('ar',               color('yellow'))
         , ('perl',             color('yellow'))
         , ('mock.py',          color('yellow'))
         , ('cpplint',          color('yellow'))
         , ('make:',            color('green'))
         , ('error:',           color('red', attrs=['bold']))
         , ('Error',            color('red', attrs=['bold']))
         , ('!!!FAILURES!!!',   color('cyan', attrs=['bold']))
         , ('undefined',        color('cyan', attrs=['bold']))
         , ('assertion',        color('cyan', attrs=['bold']))
         , ('within',           color('cyan'))
         , ('note:',            color('white'))
         , ('warning:',         color('yellow'))
         , ('instantiated',     color('green'))
         , ('from',             color('green'))
         , ('In',               color('green'))
         , ('',                 color())
         ]

ss = 'std::basic_string<char, std::char_traits<char>, std::allocator<char> >'
substs = [ (ss, 'string')
         , ('boost::intrusive_ptr<RAMCloud::Transport::Session>',
            'Transport::SessionRef')
         , (r'^perl .*$', 'perl ...')
         , (r'^python cpplint\.py.*$', 'cpplint')
         ]

prefixesToStrip = [ os.getcwd() + '/src/'
                  , os.getcwd() + '/'
                  ]
prefixesToStrip.sort(key=len, reverse=True) # sort prefixes by length desc

def cleanup(line):
    line = line.strip()
    line = elideCompiles(['ccache', 'g++', 'ar'], line)
    line = stripPaths(line)
    line = applySubsts(line)
    markLine(line)

# Many utility functions for slicing and dicing lines.

def stripPaths(line):
    """Kill all prefixesToStrip from a line"""
    def stripWord(word):
        for prefix in prefixesToStrip:
            if word.startswith(prefix):
                return word[len(prefix):]
        return word
    return ' '.join([stripWord(word) for word in line.split()])

def elideCompiles(words, line):
    """If the first word of line is in words then delete all but the first
       and last word in the line."""
    for word in words:
        if line.startswith(word):
            lwords = line.split()
            return '%s %s' % (lwords[0], lwords[-1])
    return line

def applySubsts(line):
    """Apply pairs from substs on the line.  The first string in each pair "
    is a regular expression pattern which is replaced with the second "
    string from the pair.  The pairs are applied in order."""
    for left, right in substs:
       line = re.sub(left, right, line)
    return line

def markLine(line):
    """Apply pairs from markup on the line.  For a string from the pair
       that matches the corresponding markup from the pair is performed
       on the line.  Pairs are applied in order; the lastmost match is
       the action that gets performed."""
    for word, action in markup:
        if word in line:
            print(action(line))
            sys.stdout.flush()
            return

def main():
   while True:
       line = sys.stdin.readline()
       if line == "":
           break
       cleanup(line)

if __name__ == '__main__': main()
