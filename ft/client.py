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

import itertools
import random
import sys

from driver import UDPDriver as Driver
from transport import Transport, Buffer, TEST_ADDRESS

def main():
    d = Driver()
    t = Transport(d, isServer=False)

    for i in itertools.count(1):
        # gotta make sure those ack responses still fit
        totalFrags = random.randrange(1, (d.MAX_PAYLOAD_SIZE - 64) * 8)
        totalFrags = random.randrange(1, 500)
        requestBuffer = Buffer(['a' * t.dataPerFragment() for i in range(totalFrags)])
        responseBuffer = Buffer()
        r = t.clientSend(TEST_ADDRESS, requestBuffer, responseBuffer)
        r.getReply()
        resp = responseBuffer.getRange(0, responseBuffer.getTotalLength())
        req = requestBuffer.getRange(0, requestBuffer.getTotalLength())
        assert len(req) == len(resp), (len(req), len(resp), req[:10], resp[:10],
                                       req[-10:], resp[-10:])
        assert req == resp, (req, resp)
        print "Message %d with %d frags OK" % (i, totalFrags)

    #responseBuffer1 = []
    #responseBuffer2 = []
    #responseBuffer3 = []
    #r1 = t.clientSend(TEST_ADDRESS, sys.argv[1:], responseBuffer1)
    #r2 = t.clientSend(TEST_ADDRESS, sys.argv[2:], responseBuffer2)
    #r3 = t.clientSend(TEST_ADDRESS, sys.argv[3:], responseBuffer3)
    #r3.getReply()
    #r1.getReply()
    #r2.getReply()
    #print ''.join(responseBuffer1)
    #print ''.join(responseBuffer2)
    #print ''.join(responseBuffer3)

if __name__ == '__main__':
    main()
