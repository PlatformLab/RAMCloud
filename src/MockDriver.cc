/* Copyright (c) 2010-2015 Stanford University
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
 * Construct a MockDriver which does not include the header in the outputLog.
 */
MockDriver::MockDriver()
            : incomingPacketHandler()
            , headerToString(0)
            , outputLog()
            , sendPacketCount(0)
            , stealCount(0)
            , releaseCount(0)
            , packets()
{
}

/**
 * Construct a MockDriver with a custom serializer for the opaque header in
 * the outputLog.
 *
 * \param headerToString
 *      A pointer to a function which serializes a Header into a format
 *      for prefixing packets in the outputLog.
 */
MockDriver::MockDriver(HeaderToString headerToString)
            : incomingPacketHandler()
            , headerToString(headerToString)
            , outputLog()
            , sendPacketCount(0)
            , stealCount(0)
            , releaseCount(0)
            , packets()
{
}

MockDriver::~MockDriver()
{
    foreach (MockReceived* received, packets) {
        delete(received);
    }
}

void
MockDriver::connect(IncomingPacketHandler* incomingPacketHandler) {
    this->incomingPacketHandler.reset(incomingPacketHandler);
}

void
MockDriver::disconnect() {
}

/**
 * Counts number of times release is called to allow unit tests to check
 * that Driver resources are properly reclaimed.
 *
 * See Driver::release().
 */
void
MockDriver::release(char *payload)
{
    releaseCount++;
}

/**
 * Counts number of times sendPacket for unit tests and logs the sent
 * packet to outputLog.
 *
 * See Driver::release().
 */
void
MockDriver::sendPacket(const Address *addr,
                       const void *header,
                       uint32_t headerLen,
                       Buffer::Iterator *payload)
{
    sendPacketCount++;

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

/**
 * Simulates the arrival of a packet
 * 
 * \param received
 *      Structure describing the packet. Must be dynamically allocated,
 *      and becomes our property; it will get freed when the MockDriver
 *      is destroyed.
 */
void
MockDriver::receivePacket(MockReceived *received)
{
    packets.push_back(received);
    received->driver = received->mockDriver = this;
    incomingPacketHandler->handlePacket(received);
}

/**
 * Construct an object to hold an incoming packet.
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
MockDriver::MockReceived::MockReceived(const char* sender, const void* header,
        uint32_t headerLength, const char* body)
    : locator(sender)
    , senderAddress(&locator)
    , mockDriver(NULL)
{
    this->sender = &senderAddress;
    uint32_t bodyLength = 0;
    if (body != NULL) {
        bodyLength = downCast<uint32_t>(strlen(body));
    }
    payload = new char[headerLength + bodyLength];
    memcpy(payload, header, headerLength);
    if (body != NULL) {
        memcpy(payload + headerLength, body, bodyLength);
    }
    len = headerLength + bodyLength;
}

// See Driver::Received::steal.
char*
MockDriver::MockReceived::steal(uint32_t *length)
{
    mockDriver->stealCount++;
    *length = len;
    return payload;
}

MockDriver::MockReceived::~MockReceived()
{
    delete payload;
}

}  // namespace RAMCloud
