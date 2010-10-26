/* Copyright (c) 2010 Stanford University
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

#include "MockDriver.h"

#include "TestUtil.h"

namespace RAMCloud {

/**
 * Construct a MockDriver which does not include the header in the outputLog.
 */
MockDriver::MockDriver()
            : headerToString(0)
            , inputReceived(0)
            , outputLog()
            , sendPacketCount(0)
            , tryRecvPacketCount(0)
            , releaseCount(0)
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
            : headerToString(headerToString)
            , inputReceived(0)
            , outputLog()
            , sendPacketCount(0)
            , tryRecvPacketCount(0)
            , releaseCount(0)
{
}

/**
 * Counts number of times release is called to allow unit tests to check
 * that Driver resources are properly reclaimed.
 *
 * See Driver::release().
 */
void
MockDriver::release(char *payload, uint32_t len)
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

    // TODO(stutsman) Append target address as well once we settle on
    // format of addresses in the system?

    if (headerToString && header) {
        outputLog += headerToString(header, headerLen);
        outputLog += " ";
    }

    if (!payload)
        return;

    uint32_t length = payload->getTotalLength();
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
    if (length < take) {
        outputLog += toString(buf, length);
    } else {
        outputLog += toString(buf, take);
        char tmp[50];
        snprintf(tmp, sizeof(tmp), " (+%u more)", length - take);
        outputLog += tmp;
    }
}

/**
 * Wait for an incoming packet. This is a fake method that uses
 * a message explicitly provided by the test, or an empty
 * buffer if none was provided.
 *
 * See Driver::tryRecvPacket.
 */
bool
MockDriver::tryRecvPacket(Driver::Received *received)
{
    tryRecvPacketCount++;

    if (!inputReceived)
        return false;

    // dangerous, but only used in testing
    received->sender = inputReceived->sender;
    received->payload = inputReceived->payload;
    received->len = inputReceived->len;
    received->driver = this;

    inputReceived = 0;

    return true;
}

/**
 * This method is invoked by tests to provide a Received that will
 * be used to synthesize an input message the next time one is
 * needed (such as for a packet).
 *
 * \param received
 *      A Driver::Received to return from the next call to
 *      tryRecvPacket(); probably a MockReceived, even.
 */
void
MockDriver::setInput(Driver::Received* received)
{
    inputReceived = received;
}

}  // namespace RAMCloud
