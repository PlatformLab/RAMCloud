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
#
# This is perhaps not the most idiomatic Python; it was once written
# in Haskell.

from termcolor import cprint
from functools import partial
import re
import fileinput
import os
import sys

# Most of the 'configuration'.
# Default substitutions, colors, etc.

markup = [ ('g++',              lambda s: cprint(s, 'yellow'))
         , ('ar',               lambda s: cprint(s, 'yellow'))
         , ('perl',             lambda s: cprint(s, 'yellow'))
         , ('mock.py',          lambda s: cprint(s, 'yellow'))
         , ('cpplint',          lambda s: cprint(s, 'yellow'))
         , ('make:',            lambda s: cprint(s, 'green'))
         , ('error:',           lambda s: cprint(s, 'red', attrs=['bold']))
         , ('Error',            lambda s: cprint(s, 'red', attrs=['bold']))
         , ('!!!FAILURES!!!',   lambda s: cprint(s, 'cyan', attrs=['bold']))
         , ('undefined',        lambda s: cprint(s, 'cyan', attrs=['bold']))
         , ('assertion',        lambda s: cprint(s, 'cyan', attrs=['bold']))
         , ('within',           lambda s: cprint(s, 'cyan'))
         , ('note:',            lambda s: cprint(s, 'white'))
         , ('warning:',         lambda s: cprint(s, 'yellow'))
         , ('instantiated',     lambda s: cprint(s, 'green'))
         , ('from',             lambda s: cprint(s, 'green'))
         , ('In',               lambda s: cprint(s, 'green'))
         ]

ss = 'std::basic_string<char, std::char_traits<char>, std::allocator<char> >'
substs = [ (ss, 'string')
         , ('python cpplint.py', 'cpplint')
         , ('boost::intrusive_ptr<RAMCloud::Transport::Session>',
            'Transport::SessionRef')
         ]

prefixsToStrip = [ os.getcwd() + '/src/'
                 , os.getcwd() + '/'
                 ]

def cleanup(line):
    markLine(
        applySubsts(
            stripPaths(
                partial(killFollowing, 'python cpplint.py')(
                    partial(killFollowing, 'perl')(
                        partial(elideCompiles, ['g++', 'ar'])(line.strip())
                    )
                )
            )
        )
    )

# Many utility functions for slicing and dicing lines.

def stripPaths(line):
    """Kill all prefixesToStrip from a line"""
    words = line.split()
    for i, word in enumerate(words):
        longest = 0
        for prefix in prefixsToStrip:
            if word.startswith(prefix):
                if len(prefix) > longest:
                    longest = len(prefix)
            words[i] = word[longest:]
    return ' '.join(words)

def elideCompiles(words, line):
    """If the first word of line is in words then delete all but the first
       and last word in the line."""
    for word in words:
        if line.startswith(word):
            lwords = line.split()
            return ' '.join([lwords[0], lwords[-1]])
    return line

def killFollowing(word, line):
    """Replace all words in the line beyond the first with "..." if the
       first word matches the given word."""
    if line.startswith(word):
        return '%s ...' % word
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
       that matches the corresponding action from the pair is performed
       on the line.  Pairs are applied in order; the lastmost match is
       the action that gets performed."""
    for word, action in markup:
        if word in line:
            action(line)
            return
    sys.stdout.flush()

def main():
    for line in fileinput.input():
        cleanup(line)

if __name__ == '__main__': main()
