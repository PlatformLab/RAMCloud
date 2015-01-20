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
import re
import sys
import os
import time

import getopt


DEFAULT_LOG_SUBDIR = "logs"

def findServerAndCommand(guts, regex, serverAndCommand):
    while True:
        match = re.search(regex, 
                          guts, re.MULTILINE)
        if not match: break
        
        # Found the server
        server = match.group(1)
        command = match.group(2)
        serverAndCommand.append((server, command))

        guts = guts[match.end():]
    return guts
   
def getTS():
    return time.strftime('%Y%m%d%H%M%S')
      
def adjustLogDirs(serverAndCommand, logDir):
    # Set up log subdirectory, assuming all servers share the same log directory
    ts = getTS()
    fullPath = None
    if not logDir: 
      logDir = DEFAULT_LOG_SUBDIR

    fullPath = os.path.join(logDir, ts)
    latest = os.path.join(logDir, 'latest')

    os.makedirs(fullPath)
    try:
      os.remove(latest)
    except:
      pass 

    os.symlink(ts, latest)

    retVal = []
    for server, command in serverAndCommand:
       match = re.search("--logFile ([^ ]+)", command)
       if not match:
         raise Exception("logFile not found in command string")
       logFileName = match.group(1)
       basename = os.path.basename(logFileName)

       newName = os.path.join(fullPath, basename)
       command = re.sub(logFileName, newName, command)
       retVal.append((server, command))
    return retVal
         
    
def replay(file, numPanesPerWindow = 4, logDir = None, dryrun = False):
    serverAndCommand = []
    guts = file.read()

    # Coordinator finding
    guts = findServerAndCommand(guts,
        'Coordinator started on ([a-z0-9]+) .*\n' +
        'Coordinator command line arguments (.*)',
        serverAndCommand)

    # Master finding
    guts = findServerAndCommand(guts, 
            'Server started on ([a-z0-9]+) at [^ ]+: (.*)$', serverAndCommand)
    # Client finding
    guts = findServerAndCommand(guts, 
            'Client [^ ]+ started on ([a-z0-9]+): (.*)$', serverAndCommand)

    # Modify the log directory
    serverAndCommand = adjustLogDirs(serverAndCommand, logDir)

    finalCommands = [['ssh ' + x, 'cd ' + os.getcwd(), y] for x,y in serverAndCommand]

    # Generate smux input instead
    if dryrun:
        SEP = '-' * 10
        print "PANES_PER_WINDOW = %d" % numPanesPerWindow
        print SEP
        for server in finalCommands:
          for cmd in server:
             print cmd
          print SEP
        return
       
    smux.create(numPanesPerWindow, finalCommands)

def usage():
    doc_string = '''
    Usage: clusterreplay.py [options] <cluster.py_verbose_output> 

    Rerun a prevoius run of cluster.py in interactive mode using tmux terminals. 
    The user should pass in the name of a file containing the output of a
    previous run of cluster.py with the --verbose flag, and optionally pass an
    integer representing the number of panes they would like to have open in
    each tmux window.

    Options:
      
      -h, --help   Prints this help
      -p, --panes  Set the number of panes per window [default to 4]
      -d, --logDir Specify the log directory prefix, exactly as in cluster.py.
      -n, --dryrun Print the commands being run in each window instead of
                   running it. The output can be saved to a file and passed to smux.py as an
                   option.


    '''
    print doc_string
    sys.exit(1)

def main():
    # Default to 4 for now
    if len(sys.argv) < 2: usage()
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hp:d:n', ['help', 'panes=', 'logDir=', 'dryrun'])
    except getopt.GetoptError as err:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    dryrun = False
    logDir = None
    numPanesPerWindow = 4
    for o, a in opts:
      if o in ('--help', '-h') : usage()
      elif o in ('-p', '--panes'):
        numPanesPerWindow = int(a)
      elif o in ('-d', '--logDir'):
        logDir = a
      elif o in ('-n', '--dryrun'):
        dryrun = True

    if len(args) == 0: usage()
    with open(args[0]) as f:
      replay(f, numPanesPerWindow, logDir, dryrun)
        
if __name__ == "__main__": main()
