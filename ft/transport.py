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

import random
import struct

from util import gettime, Buffer, BitVector

TEST_ADDRESS = ('127.0.0.1', 12242)

# TODO(ongaro): Should timeouts be on the RPC object?

#### Client only

TIMEOUT_NS = 10 * 1000 * 1000 # 10ms

TIMEOUTS_UNTIL_ABORTING = 500 # >= 5s

# The server will advertise that n channels are available on session open.
# The client can then use any subset of those.
MAX_NUM_CHANNELS_PER_SESSION = 8

#### Server only

# The maximum number of sessions in the session table.
# Must be a power of two.
# TODO(ongaro): It sounds like we probably want the session table to have no
# fixed size (e.g., using a C++ vector).
NUM_SERVER_SESSIONS = 1 << 10

# The maximum number of channels to allocate per session.
NUM_CHANNELS_PER_SESSION = 8


#### Client and Server (must be same)

# The width in bits of the RPC ID field.
RPCID_WIDTH = 32

#### Client and Server (may differ)

# The fraction of packets that will be dropped on transmission.
# This should be 0 for production!
PACKET_LOSS = 0.25

MAX_BURST_SIZE = 10
REQ_ACK_AFTER = 5
assert 0 < REQ_ACK_AFTER <= MAX_BURST_SIZE

DEBUGGING = True

"""
Naming conventions:

_transport refers to the Transport object.

_session refers to a Session, either a ClientSession or a ServerSession. Often
only one of those makes sense for the context.

_channel refers to a Channel, either a ClientChannel or a ServerChannel. Often
only one of those makes sense for the context.

_state is present in many of the classes that act as state machines and will
refer to one of the class's _*_STATE members.

_lastActivityTime is the time immediately after the latest packet (of any type)
was sent to or received from the other end.
"""

def debug(s):
    if DEBUGGING:
        print s

newObjects = []

def new(x):
    newObjects.append(x)
    return x

def delete(x):
    newObjects.remove(x)

class Header(object):
    """
    A binary header that goes at the start of every message (same for request
    and response).

    This would be implemented as a simple struct in C++.

    Wire format::
      <---------------32 bits -------------->
      +-------------------------------------+
      |            sessionToken             |
      +-------------------------------------+
      |        sessionToken (cont.)         |
      +-------------------------------------+
      |                rpcId                |
      +-------------------------------------+
      |   fragNumber    |   totalFrags      |
      +-------------------------------------+
      |   sessionId     | channelId | flags |
      +-------------------------------------+
    Everything is encoded in little-endian, NOT network byte order.

    @cvar LENGTH: The size in bytes of a Header.
    """
    # TODO(ongaro): Change sessionId (16 bits) to serverSessionHint (32 bits).
    # Add clientSessionHint (32 bits).

    _PACK_FORMAT = 'QIHHHBB'
    assert RPCID_WIDTH <= 32

    LENGTH = struct.calcsize(_PACK_FORMAT)

    # direction
    CLIENT_TO_SERVER = 0
    SERVER_TO_CLIENT = 1

    # flags
    PAYLOAD_TYPE_MASK = 0xF0
    DIRECTION_MASK    = 0x01
    REQUEST_ACK_MASK  = 0x02
    PLEASE_DROP_MASK  = 0x04

    # payload types
    PT_DATA         = 0x00
    PT_ACK          = 0x10
    PT_SESSION_OPEN = 0x20
    PT_RESERVED_1   = 0x30
    PT_BAD_SESSION  = 0x40
    PT_RETRY_WITH_NEW_RPCID = 0x50
    PT_RESERVED_2   = 0x60
    PT_RESERVED_3   = 0x70

    @classmethod
    def fromString(cls, string):
        """Unpack a Header from a string."""
        unpacked = struct.unpack(cls._PACK_FORMAT, string)
        flags = unpacked[-1]
        return cls(sessionToken=unpacked[0],
                   rpcId=unpacked[1],
                   fragNumber=unpacked[2],
                   totalFrags=unpacked[3],
                   sessionId=unpacked[4],
                   channelId=unpacked[5],
                   payloadType=(flags & cls.PAYLOAD_TYPE_MASK),
                   direction=(flags & cls.DIRECTION_MASK),
                   requestAck=bool((flags & cls.REQUEST_ACK_MASK) != 0),
                   pleaseDrop=bool((flags & cls.PLEASE_DROP_MASK) != 0))

    def __init__(self,
                 sessionToken=None,
                 rpcId=None,
                 fragNumber=0,
                 totalFrags=1,
                 sessionId=None,
                 channelId=None,
                 payloadType=None,
                 direction=None,
                 requestAck=False,
                 pleaseDrop=False):
        self.sessionToken = sessionToken
        self.rpcId = rpcId
        self.fragNumber = fragNumber
        self.totalFrags = totalFrags
        self.sessionId = sessionId
        self.channelId = channelId
        self.payloadType = payloadType
        self.direction = direction
        self.requestAck = requestAck
        self.pleaseDrop = pleaseDrop

    def __str__(self):
        assert self.sessionToken is not None
        assert self.rpcId is not None
        assert self.fragNumber is not None
        assert self.totalFrags is not None
        assert self.sessionId is not None
        assert self.channelId is not None
        assert self.fragNumber < self.totalFrags
        if self.requestAck:
            assert self.payloadType == self.PT_DATA
        if self.direction == self.CLIENT_TO_SERVER:
            assert self.payloadType in [self.PT_DATA, self.PT_ACK,
                                        self.PT_SESSION_OPEN]

        flags = 0
        if self.direction:   flags |= self.DIRECTION_MASK
        if self.requestAck:  flags |= self.REQUEST_ACK_MASK
        if self.pleaseDrop:  flags |= self.PLEASE_DROP_MASK
        flags |= self.payloadType
        return struct.pack(self._PACK_FORMAT,
                           self.sessionToken,
                           self.rpcId,
                           self.fragNumber,
                           self.totalFrags,
                           self.sessionId,
                           self.channelId,
                           flags)

class AckResponse(object):
    """
    The format of the payload of messages of type Header.PT_ACK.

    This could be implemented in C++ as a struct with a variable-sized bit
    vector at the end of it. It could still be stack-allocated using a
    pre-processor macro.

    @ivar numberFrags:
        The total number of frags to ACK or NACK. Same as len(ackedFrags).
    @ivar ackedFrags:
        A BitVector where a bit set signifies the corresponding packet has
        arrived.
    """
    PACK_FORMAT = 'H'
    LENGTH = struct.calcsize(PACK_FORMAT)

    @classmethod
    def fromString(cls, string):
        """Unpack an AckResponse from a string."""
        (numberFrags,) = struct.unpack(cls.PACK_FORMAT, string[:2])
        return cls(numberFrags, seq=string[2:])

    def __init__(self, numberFrags, seq=None):
        self.numberFrags = numberFrags
        self.ackedFrags = BitVector(numberFrags, seq=seq)

    def __str__(self):
        b = Buffer()
        self.fillBuffer(b)
        return b.getRange(0, b.getTotalLength())

    def fillBuffer(self, bufferToFill):
        self.ackedFrags.fillBuffer(bufferToFill)
        bufferToFill.insert(0, struct.pack(self.PACK_FORMAT, self.numberFrags))

class InboundMessage(object):
    """
    A partially-received data message (either a request or a response).

    This handles assembling the message fragments and responding to ACK
    requests. It is used in ServerChannel for the client's request and in
    ClientChannel for the server's response.

    In the C++ port, keep in mind when allocating this class that it has a
    variable-sized array (_data) depending on the totalFrags argument to its
    constructor.

    There's really no reason to have the states explicit like this, since
    _numMissingFrags easily serves to distinguish them. I'll leave it until I'm
    convinced this class isn't changing much more, though.

    @ivar _transport: x
    @ivar _session: x
    @ivar _channel: x
    @ivar _state:
        Start in RECEIVING and move to COMPLETE upon receipt of all the message
        fragments.
    @ivar _lastActivityTime: x
    @ivar _totalFrags:
        The number of fragments that make up the message to be received. 
    @ivar _numMissingFrags:
        The number of fragments of the message that have not yet been received.
        Invariant: same as the number of None entries in _data.
    @ivar _data:
        An array of length _totalFrags, indexed by fragment number, and
        pointing to None for fragments that have not yet been received or
        packet buffers for those that have. TODO: The memory management for
        these packet buffers will need to be worked out.
    @ivar _dataBuffer:
        An empty Buffer to fill with the contents of the message.

    @cvar _RECEIVING_STATE:
        There are still fragments missing from the message.
    @cvar _COMPLETE_STATE:
        All fragments have been received and the message has been added to
        _dataBuffer. _numMissingFrags is 0, and _data contains all non-None
        entries.
    """

    _RECEIVING_STATE = 0
    _COMPLETE_STATE = 1

    def __init__(self, transport, session, channel, totalFrags, dataBuffer):
        self._transport = transport
        self._session = session
        self._channel = channel
        self._state = self._RECEIVING_STATE
        self._totalFrags = totalFrags
        self._numMissingFrags = self._totalFrags
        self._data = [None] * self._totalFrags
        self._dataBuffer = dataBuffer
        self._lastActivityTime = 0

    def getLastActivityTime(self):
        return self._lastActivityTime

    def _sendAck(self):
        """Send the server an ACK for the received response packets.

        Caller should update _lastActivityTime.
        """
        header = Header()
        self._channel.fillHeader(header)
        header.payloadType = Header.PT_ACK
        ackResponse = AckResponse(self._totalFrags)
        for frag, data in enumerate(self._data):
            if data is not None:
                ackResponse.ackedFrags.setBit(frag)
        payloadBuffer = Buffer()
        ackResponse.fillBuffer(payloadBuffer)
        self._transport._sendOne(self._session.getAddress(), header,
                                 payloadBuffer)

    def processReceivedData(self, fragNumber, totalFrags, data, sendAck):
        """
        @param sendAck:
            Whether the other end is requesting an ACK along with the data it's
            sent.
        @return:
            Whether the full message has been received and added to the
            dataBuffer.
        """
        if totalFrags != self._totalFrags:
            # The other end is retarded?
            return
        if self._data[fragNumber] is None:
            self._data[fragNumber] = data
            self._numMissingFrags -= 1
            if self._numMissingFrags == 0:
                for data in self._data:
                    # In C++, there will be some sort of Chunk type wrapping
                    # 'data'.
                    self._dataBuffer.append(data)
                self._state = self._COMPLETE_STATE
        # TODO(ongaro): Have caller call self.sendAck() instead.
        if sendAck:
            self._sendAck()
        self._lastActivityTime = gettime()
        return (self._state == self._COMPLETE_STATE)
        # TODO(ongaro): Change from self._state to boolean.

    def timeout(self):
        # Gratuitously ACK the received packets.
        self._sendAck()
        self._lastActivityTime = gettime()

class OutboundMessage(object):
    """
    A partially-transmitted data message (either a request or a response).

    This handles flow control and requesting processing ACKs from the other end
    of the channel. It is used in ServerChannel for the server's response and
    in ClientChannel for the client's request.

    @ivar _transport: x
    @ivar _session: x
    @ivar _channel: x
    @ivar _lastActivityTime: x
    @ivar _sendBuffer:
        The Buffer containing the message to send. This is set on the
        transition to SENDING and is None while IDLE.
    @ivar _maxSentFrag:
        The largest fragment number already sent.
    @ivar _totalFrags:
        The total number of fragments in the message to send.
    @ivar _fragsInFlight:
        Why: To make sure you don't send a fragment that's already on the wire.

        A set of request data fragments that have been recently sent to the
        server but have not yet been acknowledged by the server. Fragments
        expire from this set after TIMEOUT_NS (and we consider it OK to resend
        those). This is implemented as an unordered array of (time sent, frag
        number) or None, of size MAX_BURST_SIZE.
    @ivar _packetsSinceAckReq:
        The number of data packets sent on the wire since the last ACK request.
    @ivar _state:
        Start in IDLE and move to SENDING once sending fragments of the message
        has begun. The clear() method will return to the IDLE state.

    @cvar _IDLE_STATE:
        There is no message to transmit currently.
    @cvar _SENDING_STATE:
        Transmission of the message has at least begun.
    """
    # TODO(ongaro): Can probably drop _state.

    _IDLE_STATE = 0
    _SENDING_STATE = 1

    def __init__(self, transport, session, channel):
        self._transport = transport
        self._session = session
        self._channel = channel
        self._fragsInFlight = [None] * MAX_BURST_SIZE
        self.clear()

    def clear(self):
        self._state = self._IDLE_STATE
        self._sendBuffer = None
        self._maxSentFrag = 0
        self._totalFrags = 0
        self._lastActivityTime = 0
        self._packetsSinceAckReq = 0
        for i in range(MAX_BURST_SIZE):
            self._fragsInFlight[i] = None

    def getLastActivityTime(self):
        return self._lastActivityTime

    def _sendOneData(self, fragNumber, forceRequestAck=False):
        """Send a single data fragment.

        Caller should update state such as _lastActivityTime, _fragsInFlight,
        and _maxSentFrag as appropriate.
        """
        requestAck = (forceRequestAck or
                      (self._packetsSinceAckReq == REQ_ACK_AFTER - 1))
        header = Header()
        self._channel.fillHeader(header)
        header.fragNumber = fragNumber
        header.totalFrags = self._totalFrags
        header.requestAck = requestAck
        header.payloadType = Header.PT_DATA
        dataPerFragment = self._transport.dataPerFragment()
        payloadBuffer = Buffer([self._sendBuffer.getRange(fragNumber *
                                                          dataPerFragment,
                                                          dataPerFragment)])
        # TODO(ongaro): Driver sould take
        # (void *header, uint32_t headerLength,
        #  Buffer *payload, uint32_t payloadOffset, uint32_t payloadLength)
        # or
        # (void *header, uint32_t headerLength,
        #  BufferIterator *payloadFromOffsetThrough)
        self._transport._sendOne(self._session.getAddress(), header,
                                 payloadBuffer)
        if requestAck:
            self._packetsSinceAckReq = 0
        else:
            self._packetsSinceAckReq += 1

    # TODO(ongaro): Create a method to send the next fragments and
    # do the Right Thing.

    def beginSending(self, messageBuffer):
        # TODO(ongaro): Pass in the messageBuffer to clear() instead and rename
        # it (to "reset" or "reinit"?).
        """Start sending the message.

        This will send as many fragments of the message as is allowed by
        MAX_BURST_SIZE. ACKs from the other end will cause transmission to
        continue beyond that (see processReceivedAck() below).
        """
        assert self._state == self._IDLE_STATE
        self._state = self._SENDING_STATE
        self._sendBuffer = messageBuffer
        self._totalFrags = self._transport.numFrags(self._sendBuffer)

        # send out the first burst of fragments
        for fragNumber in range(min(MAX_BURST_SIZE, self._totalFrags)):
            self._sendOneData(fragNumber)
        self._maxSentFrag = fragNumber
        now = gettime()
        self._lastActivityTime = now
        for fragNumber in range(min(MAX_BURST_SIZE, self._totalFrags)):
            self._fragsInFlight[fragNumber] = (now, fragNumber)

    def processReceivedAck(self, ack):
        """
        Based on the information in an acknowledgement from the other side,
        send more packets. Tihs could be iether due to flow control or
        retransmission.
        """

        """Process an ACK response from the other end for the message.

        This will often free up send slots, allowing more fragments to be sent
        to the server.

        @param ack:
            An AckResponse object from the server for the request. It may
            acknowledge all packets, in which case this method won't send
            anything.
        @return:
            Whether all fragments have been acknowledged by the server.
        """
        if self._state != self._SENDING_STATE:
            debug("OutboundMessage droppped ack because not SENDING")
            return False

        # expire old frags in flight and remove those received
        numFragsInFlight = 0
        now = gettime()
        for i in range(MAX_BURST_SIZE):
            if self._fragsInFlight[i] is None:
                continue
            timeSent, fragNumber = self._fragsInFlight[i]
            if (ack.ackedFrags.getBit(fragNumber) or
                timeSent + TIMEOUT_NS < now):
                self._fragsInFlight[i] = None
            else:
                numFragsInFlight += 1
        assert numFragsInFlight == len([x for x in self._fragsInFlight
                                        if x is not None])

        # a list of fragments to send out
        toSend = [None] * (MAX_BURST_SIZE - numFragsInFlight)
        numFragsToSend = 0

        # queue fragments we've never sent before
        while (numFragsInFlight + numFragsToSend < MAX_BURST_SIZE and
               self._maxSentFrag < self._totalFrags - 1):
            self._maxSentFrag += 1
            toSend[numFragsToSend] = self._maxSentFrag
            numFragsToSend += 1

        arrivedFrags = enumerate(ack.ackedFrags.iterBits())
        while numFragsInFlight + numFragsToSend < MAX_BURST_SIZE:
            try:
                fragNumber, arrived = arrivedFrags.next()
            except StopIteration:
                # no more bits in arrivedFrags
                break
            # already ACKed?
            if arrived:
                continue
            # already queued for transmission?
            if fragNumber in toSend:
                continue
            # already in flight?
            for i in range(MAX_BURST_SIZE):
                if (self._fragsInFlight[i] is not None and
                    self._fragsInFlight[i][1] == fragNumber):
                    break
            else: # not in flight already
                # maxSentFrag is already totalFrags - 1,
                # so don't need to set it
                toSend[numFragsToSend] = fragNumber
                numFragsToSend += 1

        for i, fragNumber in enumerate(toSend[:numFragsToSend]):
            self._sendOneData(fragNumber)

        now = gettime()
        self._lastActivityTime = now
        for fragNumber in toSend[:numFragsToSend]:
            j = self._fragsInFlight.index(None)
            self._fragsInFlight[j] = (now, fragNumber)

        return (ack.ackedFrags.ffs() is None)

    def timeout(self):
        if self._state == self._IDLE_STATE:
            return

        # expire old frags in flight
        now = gettime()
        for i in range(MAX_BURST_SIZE):
            if self._fragsInFlight[i] is None:
                continue
            timeSent, fragNumber = self._fragsInFlight[i]
            if timeSent + TIMEOUT_NS < now:
                self._fragsInFlight[i] = None

        # Ask the other end to send back an ACK.
        fragNumber = min(self._maxSentFrag + 1, self._totalFrags - 1)
        # TODO: If _maxSentFrag was totalFrags - 1, we should resend one
        # that just expired instead.
        if fragNumber not in self._fragsInFlight:
            # At least one frag in flight must have expired if we've gotten
            # a timeout.
            evict = self._fragsInFlight.index(None)
            self._fragsInFlight[evict] = (now, fragNumber)
            self._maxSentFrag = fragNumber
        debug("timeout: request ack with %d" % fragNumber)
        self._sendOneData(fragNumber, forceRequestAck=True)
        self._lastActivityTime = gettime()

class Channel(object):
    """A Channel is a connection within an established Session on which a
    sequence of RPCs travel.

    This is a base class for ClientChannel and ServerChannel."""

    def fillHeader(self, header):
        """Set Header fields according to this channel and its containing
        session.

        This will set the rpcId, channelId, sessionId, sessionToken, and
        direction.
        """
        raise NotImplementedError

class Session(object):
    """A session encapsulates the state of communication between a particular
    client and a particular server.

    At the cost of a session open handshake (during which the server
    authenticates the client and allocates state for the client's session),
    sessions allow the client to open new channels for free.
    """

    def fillHeader(self, header):
        """Set Header fields according to this session.

        This will set the sessionId, sessionToken, and direction.
        """
        raise NotImplementedError

    def getAddress(self):
        """Return the address of the node to which this Session
        communicates."""
        raise NotImplementedError

class ServerChannel(Channel):
    """A channel on the server.

    @ivar _transport: x
    @ivar _session: x
    @ivar _channelId:
        The ID of the channel (as scoped to _session).
    @ivar _rpcId:
        The current RPC ID that is being processed.
        None if IDLE, or an int RPC ID otherwise. This is set by advance().
    @ivar _currentRpc:
        The current ServerRPC object active on this channel.
        None if IDLE or DISCARDED. Otherwise (if RECEIVING, PROCESSING, or
        SENDING_WAITING), a Transport.ServerRPC object that is dynamically
        allocated in processReceivedData() once the first data fragment of the
        request arrives.

        This could almost be allocated inline as part of the channel, but that
        has two problems. Firstly, _discard() currently orphans _currentRpc if
        the server handler is currently processing it, which would need some
        other solution. Secondly, this would increase the size of idle channels
        to a few KB.
    @ivar _inboundMsg:
        An InboundMessage to assemble the RPC request.
        None if IDLE or DISCARDED. Otherwise (if RECEIVING, PROCESSING, or
        SENDING_WAITING), an InboundMessage object that is dynamically
        allocated in processReceivedData() once the first data fragment of the
        request arrives.

        This basically shares a lifetime with _currentRpc, so they could be
        allocated together.
    @ivar _outboundMsg:
        An OutboundMessage to transmit the RPC response.
    @ivar _state:
        Start at IDLE and move to RECEIVING once advance() assigns the channel an RPC ID.
        Move from RECEIVING to PROCESSING once the request is fully assembled
        in processReceivedData(). Move from PROCESSING to DISCARDED if the
        handler ignored the request (rpcIgnored()) or to SENDING_WAITING if it
        produced a response (beginSending()). Move from SENDING_WAITING to
        DISCARDED if the client happened to ACK the entire response. At any
        time, destroy() moves back to IDLE.

        The server would also be free to discard the response after some period
        of time to reclaim space, but this is not currently implemented.

    @cvar _IDLE_STATE:
        The channel is waiting to be assigned an RPC ID.
    @cvar _RECEIVING_STATE:
        The channel has been assigned an RPC ID and is awaiting data fragments
        for it. Zero or more (but not all) such fragments have arrived.
    @cvar _PROCESSING_STATE:
        The RPC is waiting in _transport._serverReadyQueue for processing or is
        being actively processed by the server handler.
    @cvar _SENDING_WAITING_STATE:
        Still have the last RPC response.
    @cvar _DISCARDED_STATE:
        Discarded the last RPC's data.
    """

    _IDLE_STATE = 0
    _RECEIVING_STATE = 1
    _PROCESSING_STATE = 2
    _SENDING_WAITING_STATE = 3
    _DISCARDED_STATE = 4

    def __init__(self, transport, session, channelId):
        self._state = self._IDLE_STATE
        self._transport = transport
        self._session = session
        self._channelId = channelId
        self._rpcId = None
        self._currentRpc = None
        self._inboundMsg = None
        self._outboundMsg = OutboundMessage(self._transport, self._session,
                                            self)

    def __del__(self):
        self._discard()

    def _discard(self):
        """Discard all state except for the current rpcId."""
        if self._state == self._IDLE_STATE:
            return
        try:
            if self._state == self._PROCESSING_STATE:
                # TODO: Use a doubly-linked list with link pointers inside
                # self._currentRpc instead of a Python "list" type. That'd make
                # this O(1).
                try:
                    q = self._transport._serverReadyQueue
                    del q[q.index(self._currentRpc)]
                except ValueError:
                    # self._currentRpc has already been popped off the queue,
                    # so there's no stopping the server from processing it now.
                    # We'll just tell it to abort once the server tries to send
                    # the reply.
                    self._currentRpc.abort() # The RPC will delete() itself now.
                    self._currentRpc = None
                    return
        finally:
            self._state = self._DISCARDED_STATE
            if self._currentRpc is not None:
                delete(self._currentRpc)
                self._currentRpc = None
            if self._inboundMsg is not None:
                delete(self._inboundMsg)
                self._inboundMsg = None
            self._outboundMsg.clear()

    def fillHeader(self, header):
        header.rpcId = self._rpcId
        header.channelId = self._channelId
        self._session.fillHeader(header)

    def getRpcId(self):
        return self._rpcId

    def getLastActivityTime(self):
        t = 0
        if self._inboundMsg is not None:
            t = max(t, self._inboundMsg.getLastActivityTime())
        t = max(t, self._outboundMsg.getLastActivityTime())
        return t

    def advance(self, rpcId):
        """Begin receiving on a new rpcId."""
        self._discard()
        self._rpcId = rpcId
        self._state = self._RECEIVING_STATE

    def processReceivedData(self, fragNumber, totalFrags, data, sendAck):
        """Process inbound received data for the client's request."""
        if self._state == self._IDLE_STATE:
            return
        elif self._state == self._DISCARDED_STATE:
            header = Header()
            self.fillHeader(header)
            header.payloadType = PT_RETRY_WITH_NEW_RPCID
            self._transport._sendOne(self._session.getAddress(), header,
                                     Buffer([]))
            return
        elif self._state == self._RECEIVING_STATE:
            if self._inboundMsg is None: # maybe this should be another state?
                self._currentRpc = new(Transport.ServerRPC(self._transport,
                                                           self._session,
                                                           self))
                requestBuffer = self._currentRpc.recvPayload
                self._inboundMsg = new(InboundMessage(self._transport,
                                                      self._session, self,
                                                      totalFrags,
                                                      requestBuffer))
            if self._inboundMsg.processReceivedData(fragNumber, totalFrags,
                                                    data, sendAck):
                self._transport._serverReadyQueue.append(self._currentRpc)
                self._state = self._PROCESSING_STATE
        else: # PROCESSING or SENDING/WAITING
            if sendAck:
                # could also fake an all-1s response, possibly more cheaply
                self._inboundMsg.processReceivedData(fragNumber, totalFrags,
                                                     data, sendAck)

    def rpcIgnored(self):
        """The server handler chose to ignore this RPC request and will not
        produce a response for it."""
        assert self._state == self._PROCESSING_STATE
        self._discard()

    def beginSending(self):
        """The server handler has finished producing the response; begin
        sending the response data."""
        assert self._state == self._PROCESSING_STATE
        self._state = self._SENDING_WAITING_STATE
        responseBuffer = self._currentRpc.replyPayload
        self._outboundMsg.beginSending(responseBuffer)

    def processReceivedAck(self, ack):
        """Process an ACK from the client for the response."""
        if self._state != self._SENDING_WAITING_STATE:
            return
        isCompletelyAcked = self._outboundMsg.processReceivedAck(ack)
        if isCompletelyAcked:
            # Probably uncommon with this client implementation, but who knows?
            self._discard()

    def destroy(self):
        """The session is being destroyed, either because the client is not
        responding or to clean up state."""
        if self._state == self._IDLE_STATE:
            return
        self._discard()
        self._state = self._IDLE_STATE
        self._rpcId = None

class ServerSession(Session):
    """A session on the server.

    @ivar _transport: x
    @ivar _lastActivityTime: x
    @ivar _id:
        The offset into the server's session table for this session.
    @ivar _channels:
        An array of ServerChannel objects of size NUM_CHANNELS_PER_SESSION.
    @ivar _token:
        A large integer that disambiguates this session from others before and
        after it on the same server with the same _id. None if IDLE.
    @ivar _address:
        The address of the client to which this session is connected.
        None if IDLE.
    @ivar _state:
        Start in IDLE, and startSession() moves from IDLE to ACTIVE. Then
        destroy() moves back to IDLE.

    @cvar _IDLE_STATE:
        Not connected to a client.
    @cvar _ACTIVE_STATE:
    """

    _IDLE_STATE = 0
    _ACTIVE_STATE = 1

    def __init__(self, transport, sessionId):
        self._transport = transport
        self._id = sessionId
        self._state = self._IDLE_STATE
        self._channels = [ServerChannel(self._transport, self, i)
                          for i in range(NUM_CHANNELS_PER_SESSION)]
        self._token = None
        self._lastActivityTime = 0
        self._address = None

    def getChannel(self, channelId):
        if channelId < NUM_CHANNELS_PER_SESSION:
            return self._channels[channelId]
        else:
            return None

    def fillHeader(self, header):
        header.direction = Header.SERVER_TO_CLIENT
        header.sessionId = self._id
        header.sessionToken = self._token

    def getAddress(self):
        return self._address

    def isInUse(self):
        return (self._state == self._ACTIVE_STATE)

    def startSession(self, address):
        assert self._state == self._IDLE_STATE
        self._state = self._ACTIVE_STATE
        self._address = address
        self._token = random.randrange(0, 1 << 64)

        # send session open response
        header = Header()
        self.fillHeader(header)
        header.rpcId = 0
        # TODO(ongaro): Stop abusing the channelId field for this. Put it in
        # the payload instead.
        header.channelId = (NUM_CHANNELS_PER_SESSION - 1)
        header.payloadType = Header.PT_SESSION_OPEN
        self._transport._sendOne(self._address, header, Buffer([]))
        self._lastActivityTime = gettime()

    def destroy(self):
        if self._state == self._IDLE_STATE:
            return
        for channel in self._channels:
            channel.destroy()
        for i, rpc in enumerate(self._transport._serverReadyQueue):
            if rpc._session == self:
                self._transport._serverReadyQueue[i] = None
                delete(rpc)
        self._state = self._IDLE_STATE
        self._token = None
        self._lastActivityTime = 0

    def getLastActivityTime(self):
        t = self._lastActivityTime
        if self._state == self._ACTIVE_STATE:
            for channel in self._channels:
                t = max(t, channel.getLastActivityTime())
        return t

class ClientChannel(Channel):
    """A channel on the client.

    @ivar _rpcId:
        The RPC Id for next packet if IDLE or the active one if non-IDLE.
    @ivar _currentRpc:
        Pointer to external Transport.ClientRPC if non-IDLE. None if IDLE.
    @ivar _lastActivityTime:
        The time immediately after the latest packet (of any type) was
        sent to or received from the server.
    @ivar _numRetries:
        The number of ACK requests or responses we've sent due to timeouts
        since the last time the server has responded. This is cleared on the
        receipt of response data or an ACK for request data.
    """

    # TODO(ongaro): Maybe get rid of this class (replace with a struct).

    # start at IDLE.
    # destroy() transitions to IDLE.
    # beginSending() transitions from IDLE to SENDING.
    # processReceivedData() transitions from SENDING to RECEIVING.
    # retryWithNewRpcId() transitions from non-IDLE to SENDING.
    _IDLE_STATE = 0
    _SENDING_STATE = 1
    _RECEIVING_STATE = 2

    def __init__(self, transport, session, channelId):
        self._transport = transport
        self._session = session
        self._channelId = channelId
        self._rpcId = 0

        self._outboundMsg = OutboundMessage(self._transport, self._session,
                                            self)
        self._inboundMsg = None
        self._clear()

    def _clear(self):
        self._state = self._IDLE_STATE
        self._rpcId = (self._rpcId + 1) % (1 << RPCID_WIDTH)
        self._currentRpc = None

        self._outboundMsg.clear()

        if self._inboundMsg is not None:
            delete(self._inboundMsg)
            self._inboundMsg = None

        self._lastActivityTime = 0
        self._numRetries = 0

    def __del__(self):
        if self._inboundMsg is not None:
            delete(self._inboundMsg)

    def getRpcId(self):
        """Called by receive path to help in processing."""
        return self._rpcId

    def fillHeader(self, header):
        header.rpcId = self._rpcId
        header.channelId = self._channelId
        self._session.fillHeader(header)

    def getLastActivityTime(self):
        t = self._lastActivityTime
        t = max(t, self._outboundMsg.getLastActivityTime())
        if self._inboundMsg is not None:
            t = max(t, self._inboundMsg.getLastActivityTime())
        return t

    def retryWithNewRpcId(self):
        rpc = self._currentRpc
        self._clear()
        self.beginSending(rpc)

    def beginSending(self, rpc):
        assert self._state == self._IDLE_STATE
        self._state = self._SENDING_STATE
        self._currentRpc = rpc
        self._outboundMsg.beginSending(rpc.getRequestBuffer())

    def timeout(self):
        """Called by _checkTimers."""
        self._numRetries += 1
        if self._numRetries == TIMEOUTS_UNTIL_ABORTING:
            self._session.destroy(serverIsDead=True)
            return

        if self._state == self._SENDING_STATE:
            self._outboundMsg.timeout()
        elif self._state == self._RECEIVING_STATE:
            self._inboundMsg.timeout()
        else:
            pass

    def processReceivedData(self, fragNumber, totalFrags, data, sendAck):
        """Process inbound received data."""
        if self._state == self._IDLE_STATE:
            return

        self._numRetries = 0
        if self._state == self._SENDING_STATE:
            responseBuffer = self._currentRpc.getResponseBuffer()
            # TODO(ongaro): Find a way around this. Maybe use the storage
            # allocation mechanism in the Buffer.
            self._inboundMsg = new(InboundMessage(self._transport,
                                                  self._session, self,
                                                  totalFrags, responseBuffer))
            self._state = self._RECEIVING_STATE

        if self._inboundMsg.processReceivedData(fragNumber, totalFrags,
                                                data, sendAck):
            self._currentRpc.completed()
            self._clear()
            self._session.doneWithChannel(self)

    def processReceivedAck(self, ack):
        if self._state != self._SENDING_STATE:
            return
        self._numRetries = 0
        self._outboundMsg.processReceivedAck(ack)

    def destroy(self, serverIsDead):
        """Called by the session when it is destroyed."""
        if self._currentRpc is not None:
            if serverIsDead:
                self._currentRpc.aborted()
            else:
                self._currentRpc.findNewSession()
        self._clear()

class ClientSession(Session):
    """A session on the client."""

    def __init__(self, transport, address):
        self._transport = transport
        self._address = address
        self._channelQueue = []

        # A bit set in the vector signifies the corresponding channel is in
        # use.
        self._channelStatus = BitVector(MAX_NUM_CHANNELS_PER_SESSION,
                                        ones=True)

        self._numChannels = 0
        self._channels = None
        self._serverSessionId = None
        self._token = None

        self._sendSessionOpenRequest()
        # TODO(ongaro): Would it be safe to call poll here and wait?

    def fillHeader(self, header):
        header.direction = Header.CLIENT_TO_SERVER
        header.sessionId = self._serverSessionId
        header.sessionToken = self._token

    def getAddress(self):
        return self._address

    def _isConnected(self):
        return self._token is not None

    def _sendSessionOpenRequest(self):
        header = Header()
        self.fillHeader(header)
        header.rpcId = 0
        header.sessionId = 0
        header.sessionToken = 0
        header.channelId = 0
        header.payloadType = Header.PT_SESSION_OPEN
        self._transport._sendOne(self._address, header, Buffer([]))
        # TODO(ongaro): Would it be possible to open a session like other RPCs?
        # TODO: set up timer

    def processSessionOpenResponse(self, sessionId, sessionToken, channelId, data):
        """Process an inbound session open response."""
        if self._isConnected():
            return
        self._serverSessionId = sessionId
        self._token = sessionToken
        self._numChannels = min(channelId, MAX_NUM_CHANNELS_PER_SESSION)
        self._channels = new([ClientChannel(self._transport, self, i)
                              for i in range(self._numChannels)])
        for channel in self._channels:
            self.doneWithChannel(channel)
            # TODO(ongaro): Using doneWithChannel here is confusing.

    def getChannel(self, channelId):
        """Return the existing ClientChannel object corresponding to the given
        id, or None if the id is invalid.

        Used by the receive path to map an inbound packet to a channel.
        """
        if channelId < self._numChannels:
            return self._channels[channelId]
        else:
            return None

    def startRpc(self, rpc):
        """Queue an RPC for transmission on this session.

        This session will try its best to send the RPC. If the session is
        closed, the RPC's findNewSession method will be called.
        """
        # TODO(ongaro): What if finding a new session isn't going to do it,
        # because the server has crashed?
        channel = self._getAvailableChannel()
        if channel is None:
            # TODO(ongaro): Thread linked list through rpc.
            self._channelQueue.append(rpc)
        else:
            channel.beginSending(rpc)

    def _getAvailableChannel(self):
        """Return any available ClientChannel object or None."""
        if not self._isConnected():
            return None
        channelId = self._channelStatus.ffz()
        if channelId is None:
            return None
        self._channelStatus.setBit(channelId)
        return self._channels[channelId]

    def getActiveChannels(self):
        if not self._isConnected():
            return
        for channelId, isActive in enumerate(self._channelStatus.iterBits()):
            if isActive and channelId < self._numChannels:
                yield self._channels[channelId]

    def doneWithChannel(self, channel):
        """Mark a channel as available.

        If there's an RPC waiting on an available channel, it will be started.

        This method should only be called by self and one of self._channels.
        """
        # TODO(ongaro): Rename this.
        # TODO(ongaro): Maybe pass in a channelId.
        channelId = self._channels.index(channel)
        assert self._channelStatus.getBit(channelId)
        try:
            rpc = self._channelQueue.pop(0)
        except IndexError:
            self._channelStatus.clearBit(channelId)
        else:
            channel.beginSending(rpc)

    def destroy(self, serverIsDead=False):
        """Mark this session as invalid.

        This may be called at any time, e.g., when the server says the session
        is invalid or when someone wants to reclaim the space.
        """
        for channel in self._channels:
            channel.destroy(serverIsDead)
        for rpc in self._channelQueue:
            if serverIsDead:
                rpc.aborted()
            else:
                rpc.findNewSession()

class Transport(object):
    class TransportException(Exception):
        pass

    def __init__(self, driver, isServer):
        self._driver = driver
        self._isServer = isServer

        if self._isServer:
            # indexed by session IDs
            # values are ServerSession objects
            self._serverSessions = [ServerSession(self, i) for i in
                                    range(NUM_SERVER_SESSIONS)]

            # a list of dynamically allocated Transport.ServerRPC objects that
            # are ready for processing by server handlers
            self._serverReadyQueue = []

        # keyed by address
        # values are ClientSession objects
        # This should be merged with the Service concept (a Service has exactly
        # one ClientSession).
        self._clientSessions = {}

    def dataPerFragment(self):
        return (self._driver.MAX_PAYLOAD_SIZE - Header.LENGTH)

    def numFrags(self, dataBuffer):
        return ((dataBuffer.getTotalLength() + self.dataPerFragment() - 1) /
                self.dataPerFragment())

    def _sendOne(self, address, header, dataBuffer):
        """Dump a packet onto the wire."""
        assert header.fragNumber < header.totalFrags
        header.pleaseDrop = (random.random() < PACKET_LOSS)
        dataBuffer.insert(0, str(header))
        # TODO(ongaro): Will a sync API to Driver allow us to fully utilize
        # the NIC?
        self._driver.sendPacket(address, dataBuffer)

    def _checkTimers(self):
        now = gettime()

        # TODO(ongaro): This won't scale well to clients that have a large
        # number of sessions open. Maybe a linked list through the active
        # sessions instead?
        for session in self._clientSessions.values():
            for channel in session.getActiveChannels():
                if channel.getLastActivityTime() + TIMEOUT_NS < now:
                    channel.timeout()

        # server role:
        # If the client hasn't received our entire response, that's their
        # problem. No need to handle timeouts here.
        # TODO(ongaro): periodic cleanup of state?

    def _checkWire(self):
        x = self._driver.tryRecvPacket()
        if x is None:
            return False
        payload, address = x
        header = Header.fromString(payload[:Header.LENGTH])
        data = payload[Header.LENGTH:]
        if header.pleaseDrop:
            return True

        if header.direction == header.CLIENT_TO_SERVER:
            if not self._isServer:
                # This must be an old or malformed packet, so it is safe to drop.
                debug("drop -- not a server")
                return True
            session = self._serverSessions[header.sessionId &
                                           (NUM_SERVER_SESSIONS - 1)]
            if header.sessionToken == session._token:
                channel = session.getChannel(header.channelId)
                if channel is None:
                    # Invalid channel. A well-behaved client wouldn't ever do this,
                    # so it's safe to drop.
                    debug("drop due to invalid channel")
                    return True
                if channel.getRpcId() is None:
                    rpcIdIsOld = False
                    rpcIdIsNew = True
                else:
                    # TODO(ongaro): review modulo arithmetic
                    rpcIdMask = (1 << RPCID_WIDTH) - 1
                    diff = (header.rpcId - channel.getRpcId()) & rpcIdMask
                    rpcIdIsOld = (diff >= 10 * 1000 * 1000)
                    rpcIdIsNew = (0 < diff < 10 * 1000 * 1000)
                if rpcIdIsOld:
                    # This must be an old packet that the client's no longer
                    # waiting on, just drop it.
                    debug("drop old packet")
                    return True
                elif rpcIdIsNew:
                    if header.payloadType == Header.PT_DATA:
                        channel.advance(header.rpcId)
                        channel.processReceivedData(header.fragNumber,
                                                    header.totalFrags, data,
                                                    sendAck=header.requestAck)
                    else:
                        # A well-behaved client wouldn't ever do this, so it's safe
                        # to drop.
                        debug("drop new rpcId with non-data")
                        return True
                else: # header's RPC ID is same as channel's
                    if header.payloadType == Header.PT_DATA:
                        channel.processReceivedData(header.fragNumber,
                                                    header.totalFrags, data,
                                                    sendAck=header.requestAck)
                    elif header.payloadType == Header.PT_ACK:
                        channel.processReceivedAck(AckResponse.fromString(data))
                    else:
                        # A well-behaved client wouldn't ever do this, so it's safe
                        # to drop.
                        debug("drop current rpcId with bad type")
                        return True
            else:
                if header.payloadType == Header.PT_SESSION_OPEN:
                    oldestSession = self._serverSessions[0]
                    oldestTime = 0
                    for session in self._serverSessions:
                        if not session.isInUse():
                            session.startSession(address)
                            return True
                        t = session.getLastActivityTime()
                        if t > oldestTime:
                            oldestTime = t
                            oldestSession = session
                    oldestSession.destroy()
                    oldestSession.startSession(address)
                else:
                    replyHeader = Header()
                    replyHeader.sessionToken = header.sessionToken
                    replyHeader.rpcId = header.rpcId
                    replyHeader.sessionId = header.sessionId
                    replyHeader.channelId = header.channelId
                    replyHeader.payloadType = Header.PT_BAD_SESSION
                    replyHeader.direction = Header.SERVER_TO_CLIENT
                    self._sendOne(address, replyHeader, Buffer([]))

        else:
            try:
                session = self._clientSessions[address]
            except KeyError:
                return True
            channel = session.getChannel(header.channelId)
            if channel is None:
                if header.payloadType == Header.PT_SESSION_OPEN:
                    session.processSessionOpenResponse(header.sessionId,
                                                       header.sessionToken,
                                                       header.channelId,
                                                       data)
                return True
            if channel.getRpcId() == header.rpcId:
                if header.payloadType == Header.PT_DATA:
                    # TODO(ongaro): Just pass the Header through.
                    channel.processReceivedData(header.fragNumber,
                                                header.totalFrags, data,
                                                sendAck=header.requestAck)
                elif header.payloadType == Header.PT_ACK:
                    channel.processReceivedAck(AckResponse.fromString(data))
                elif header.payloadType == Header.PT_SESSION_OPEN:
                    # The session is already open (we have a channel),
                    # so just drop this.
                    return True
                elif header.payloadType == Header.PT_BAD_SESSION:
                    self._clientSessions.pop(address)
                    session.destroy()
                    delete(session)
                elif header.payloadType == Header.PT_RETRY_WITH_NEW_RPCID:
                    channel.retryWithNewRpcId()
            else:
                if (0 < channel.getRpcId() - header.rpcId < 1024 and
                    header.payloadType == Header.PT_DATA and header.requestAck):
                    raise NotImplementedError("faked full ACK response")
        return True

    def poke(self):
        # TODO(ongaro): Rename to "poll"
        """Check the wire and check timers. Do all possible work but don't
        wait."""

        while self._checkWire(): # TODO(ongaro): rename from "check"
            self._checkTimers()
        self._checkTimers()

    def _getClientSession(self, address):
        try:
            return self._clientSessions[address]
        except KeyError:
            session = new(ClientSession(self, address))
            self._clientSessions[address] = session
            return session

    class ClientRPC(object):
        _IN_PROGRESS_STATE = 0
        _COMPLETED_STATE = 1
        _ABORTED_STATE = 2

        def __init__(self, transport, address,
                     requestBuffer, responseBuffer):
            self._transport = transport
            self._address = address

            # pointers to buffers on client's stack
            self._requestBuffer = requestBuffer
            self._responseBuffer = responseBuffer

            self.findNewSession()

        def _hasCompleted(self):
            """Returns whether the response Buffer contains the full
            response."""
            return self._session is None

        def getRequestBuffer(self):
            return self._requestBuffer

        def getResponseBuffer(self):
            return self._responseBuffer

        def findNewSession(self):
            # TODO(ongaro): At least rename this to imply that it starts
            # sending.
            """A callback for when the session is no longer valid and this RPC
            needs to start over."""
            self._state = self._IN_PROGRESS_STATE
            self._session = self._transport._getClientSession(self._address)
            self._session.startRpc(self)

        def aborted(self):
            self._state = self._ABORTED_STATE
            self._session = None

        def completed(self):
            """A callback for when the response Buffer has been filled with the
            response."""
            self._state = self._COMPLETED_STATE
            self._session = None

        def getReply(self):
            try:
                while True:
                    if self._state == self._COMPLETED_STATE:
                        return
                    elif self._state == self._ABORTED_STATE:
                        raise self._transport.TransportException("RPC aborted")
                    self._transport.poke()
            finally:
                delete(self)

    def clientSend(self, address, requestBuffer, responseBuffer):
        # TODO(ongaro): Allocate the ClientRPC in requestBuffer or responseBuffer?
        rpc = new(Transport.ClientRPC(self, address, requestBuffer,
                                      responseBuffer))
        # TODO: Move the work out of the constructor to an rpc.start() or
        # similar
        return rpc

    class ServerRPC(object):
        _PROCESSING_STATE = 0
        _COMPLETED_STATE = 1
        _ABORTED_STATE = 2

        def __init__(self, transport, session, channel):
            self._state = self._PROCESSING_STATE
            self._transport = transport
            self._session = session
            self._channel = channel
            self.recvPayload = Buffer()
            self.replyPayload = Buffer()

        def sendReply(self):
            if self._state == self._ABORTED_STATE:
                delete(self)
            elif self._state == self._PROCESSING_STATE:
                self._state = self._COMPLETED_STATE
                self._channel.beginSending()
                # TODO: don't forget to delete(self) eventually
            else:
                assert False

        def ignore(self):
            if self._state == self._ABORTED_STATE:
                delete(self)
            elif self._state == self._PROCESSING_STATE:
                self._state = self._COMPLETED_STATE
                self._channel.rpcIgnored()
            else:
                assert False

        def abort(self):
            """This object takes responsibility for deleting itself."""
            self._state = self._ABORTED_STATE
            self._transport = None
            self._session = None
            self._channel = None

    def serverRecv(self):
        while True:
            self.poke()
            try:
                return self._serverReadyQueue.pop(0)
            except IndexError:
                pass
