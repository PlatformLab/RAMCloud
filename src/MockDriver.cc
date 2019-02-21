/* Copyright (c) 2010-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"

#include "MockDriver.h"

namespace RAMCloud {

/**
 * Construct a MockDriver with a custom serializer for the opaque header in
 * the outputLog.
 *
 * \param context
 *      RAMCloud context
 * \param headerToString
 *      A pointer to a function which serializes a Header into a format
 *      for prefixing packets in the outputLog.
 */
MockDriver::MockDriver(Context* context, HeaderToString headerToString)
            : Driver(context)
            , headerToString(headerToString)
            , outputLog()
            , sendPacketCount(0)
            , releaseCount(0)
            , incomingPackets()
            , transmitQueueSpace(10000)
{
}

MockDriver::~MockDriver()
{
    foreach (PacketBuf* packet, incomingPackets) {
        delete(packet);
    }
}

/**
 * Counts number of times sendPacket for unit tests and logs the sent
 * packet to outputLog.
 *
 * See Driver::release().
 */
void
MockDriver::sendPacket(const Address* addr,
                       const void* header,
                       uint32_t headerLen,
                       Buffer::Iterator* payload,
                       int priority,
                       TransmitQueueState* txQueueState)
{
    sendPacketCount++;
    lastTransmitTime = Cycles::rdtsc();
    uint32_t bytesSent = headerLen;
    if (payload != NULL) {
        bytesSent += payload->size();
    }
    if (downCast<int>(bytesSent) > transmitQueueSpace) {
        transmitQueueSpace = 0;
    } else {
        transmitQueueSpace -= bytesSent;
    }

    if (outputLog.length() != 0)
        outputLog.append(" | ");

    if (headerToString && header) {
        outputLog += headerToString(header, headerLen);
        if (payload && (payload->size() > 0)) {
            outputLog += " ";
        }
    }

    if (!payload)
        return;

    uint32_t length = payload->size();
    char buf[length];

    uint32_t off = 0;
    while (!payload->isDone()) {
        uint32_t l = payload->getLength();
        memcpy(&buf[off],
               const_cast<void*>(payload->getData()), l);
        off += l;
        payload->next();
    }

    uint32_t take = 10;
    if (length <= take) {
        outputLog += TestUtil::toString(buf, length);
    } else {
        outputLog += TestUtil::toString(buf, take);
        outputLog += format(" (+%u more)", length - take);
    }
}

/**
 * See Driver::getServiceLocator. 
 */
string
MockDriver::getServiceLocator()
{
    return "mock:";
}

// See documentation in Driver.h.
void
MockDriver::receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets)
{
    uint32_t count = 0;
    while ((count < maxPackets) && !incomingPackets.empty()) {
        PacketBuf* source = incomingPackets[0];
        receivedPackets->emplace_back(&source->address, this, source->length,
                source->payload);
        count++;
        incomingPackets.erase(incomingPackets.begin());
    }
}

/**
 * Construct a PacketBuf.
 *
 * \param sender
 *      String that will be converted into an Address for the sender.
 * \param header
 *      First byte of a header for the packet.
 * \param headerLength
 *      Number of bytes in the header.
 * \param body
 *      Null-terminated string that will be concatenated with the header
 *      to form the packet.  NULL means no body.
 */
MockDriver::PacketBuf::PacketBuf(const char* sender, const void* header,
                uint32_t headerLength, const char* body)
    : address(sender)
    , length()
{
    uint32_t bodyLength = 0;
    if (body != NULL) {
        bodyLength = downCast<uint32_t>(strlen(body));
    }
    assert(headerLength + bodyLength <= MAX_PAYLOAD_SIZE);
    memcpy(payload, header, headerLength);
    if (body != NULL) {
        memcpy(payload + headerLength, body, bodyLength);
    }
    length = headerLength + bodyLength;
}

}  // namespace RAMCloud
