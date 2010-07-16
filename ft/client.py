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
from util import gettime

class Service(object):
    def __init__(self, address, session):
        self.address = address
        self.session = session

class Services(object):
    def __init__(self, transport):
        self._transport = transport
        self._services = {}

    def getService(self, address):
        if address in self._services:
            return self._services[address]
        else:
            session = self._transport.getClientSession()
            service = Service(address, session)
            session.connect(service)
            self._services[address] = service
            return service


def main():
    random.seed(0)

    d = Driver()
    t = Transport(d, isServer=False)
    services = Services(t)
    s = services.getService(TEST_ADDRESS)

    for i in itertools.count(1):
        #totalFrags = random.randrange(1, 2**16 - 1)
        totalFrags = random.randrange(1, 500)
        #totalFrags = 1000
        requestBuffer = Buffer(['a' * t.dataPerFragment() for j in range(totalFrags)])
        responseBuffer = Buffer()
        start = gettime()
        r = t.clientSend(s, requestBuffer, responseBuffer)
        r.getReply()
        elapsedNs = gettime() - start
        resp = responseBuffer.getRange(0, responseBuffer.getTotalLength())
        req = requestBuffer.getRange(0, requestBuffer.getTotalLength())
        assert len(req) == len(resp), (len(req), len(resp), req[:10], resp[:10],
                                       req[-10:], resp[-10:])
        assert req == resp, (req, resp)
        print
        print "Message %d with %d frags OK in %dms" % (i, totalFrags,
                                                       elapsedNs / 1000000)
        d.stat()

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
