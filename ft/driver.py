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

import socket

class Driver(object):
    MAX_PAYLOAD_SIZE = None

    def sendPacket(self, address, payloadBuffer):
        """Blocks until the NIC has the packet data."""
        raise NotImplementedError

    def tryRecvPacket(self):
        """Try to receive a packet off the network.

        Returns (payload, length, address), or None if there was no packet available.
        The caller must call release() with this payload and length later.
        """
        raise NotImplementedError

    def release(self, payload, length):
        """Free the memory returned in a previous call to tryRecvPacket."""
        raise NotImplementedError

class UDPDriver(Driver):
    # The maximum number of bytes of data (including the RPC Header) that would fit
    # in a single packet.
    MAX_PAYLOAD_SIZE = 1400

    def __init__(self, address=None):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if address is not None:
            self._socket.bind(address)
        self._packetBufsUtilized = 0

    def sendPacket(self, address, payloadBuffer):
        payloadLen = payloadBuffer.getTotalLength()
        assert payloadLen <= self.MAX_PAYLOAD_SIZE
        payload = payloadBuffer.getRange(0, payloadLen)
        sent = self._socket.sendto(payload, socket.MSG_WAITALL, address)
        assert sent == payloadLen

    def tryRecvPacket(self):
        try:
            payload, address = self._socket.recvfrom(self.MAX_PAYLOAD_SIZE,
                                                     socket.MSG_DONTWAIT)
            self._packetBufsUtilized += 1
            return (payload, len(payload), address)
        except socket.error:
            return None

    def release(self, data, length):
        self._packetBufsUtilized -= 1

    def stat(self):
        print 'Driver: packetBufsUtilized = %d' % self._packetBufsUtilized
