#!/usr/bin/python

# Copyright (c) 2010-2014 Stanford University
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

import smux
import sys
import os
from time import sleep

import getopt



def create(serverRanges, superuser):
    if "," in serverRanges:
       serverRanges = serverRanges.split(",")
    else:
       serverRanges = [serverRanges]

    servers = []
    for serverRange in serverRanges:
       if '-' in serverRange:
          left,right = serverRange.split("-")
          left,right = int(left),int(right)
          for x in xrange(left, right + 1):
            if superuser:
              servers.append(["ssh root@rc%02d" % x])
            else:
              servers.append(["ssh rc%02d" % x])
       else:
         if superuser:
           x = int(serverRange)
           servers.append(["ssh root@rc%02d" % x])
         else:
           x = int(serverRange)
           servers.append(["ssh rc%02d" % x])
    smux.create(len(servers), servers, executeBeforeAttach=lambda : smux.tcmd("setw synchronize-panes on"))


def usage():
    doc_string = '''
    Usage: clusterreplay.py <serverlist>

    ServerList is specified as a comma-delmited list of ranges, such as 1-3,5-7,10-12

    Options:

      -h, --help       Prints this help
      -s, --superuser  Log in as root.


    '''
    print doc_string
    sys.exit(1)

def main():
    if len(sys.argv) < 2: usage()
    superuser = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hs', ['help', 'superuser'])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    for o, a in opts:
      if o in ('--help', '-h') : usage()
      if o == "--superuser":
        superuser = True

    if len(args) == 0: usage()
    create(args[0], superuser)


if __name__ == "__main__": main()
