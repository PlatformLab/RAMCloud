#! /usr/bin/python
import re
import sys

from datetime import datetime

if __name__ == '__main__':
  if len(sys.argv) < 3 or sys.argv[1] in ("--help"):
    print \
"""This script transform a human-readable NanoLog log (one that's been
decompressed) to a log that matches the TimeTrace output format.
This allows for TimeTrace helper scripts like ttsum.py to be used.
"""
    print "Usage:"
    print "\t%s <inputFile> <outputFile>\r\n" % sys.argv[0]
    sys.exit(1)

  logStmt = re.compile("(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).(\d{9}) [^:]+:\d+ [^\[]+\[\d+\]: (.*)")
  with open(sys.argv[1], "r") as iFile, open(sys.argv[2], "w") as oFile:
    count = 0
    for line in iFile.readlines():
      match = logStmt.match(line)

      if match:
        dateTime = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        nanoseconds = int(match.group(2))
        logMsg = match.group(3)


        if count == 0:
          firstDateTime = dateTime
          firstNanoseconds = nanoseconds

          lastDateTime = dateTime
          lastNanoseconds = nanoseconds


        deltaStart = (dateTime - firstDateTime).total_seconds()*1.0e9
        deltaStart += nanoseconds - firstNanoseconds

        deltaLast = (dateTime - lastDateTime).total_seconds()*1.0e9
        deltaLast += nanoseconds - lastNanoseconds
        oFile.write("%8.1f ns (+%6.1f ns): %s\r\n"
                        % (deltaStart, deltaLast, logMsg))

        lastDateTime = dateTime
        lastNanoseconds = nanoseconds
        count += 1