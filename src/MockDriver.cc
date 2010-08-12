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

/**
 * \file
 * Implementation of a class that implements Driver for tests,
 * without an actual network.
 */

#include "MockDriver.h"

#include "TestUtil.h"

namespace RAMCloud {

// --- MockDriver ---

/**
 * Construct a MockDriver.
 */
MockDriver::MockDriver()
            : outputLog(), inputMessage(0), inputMessageLen(0),
              sendPacketCount(0), tryRecvPacketCount(0),
              releaseCount(0) {}

void
MockDriver::sendPacket(const sockaddr *addr,
                       socklen_t addrlen,
                       void *header,
                       uint32_t headerLen,
                       Buffer::Iterator *payload)
{
    sendPacketCount++;

    if (outputLog.length() != 0)
        outputLog.append(" | ");

    outputLog.append("sendPacket: ");
    // TODO(stutsman) Append target address as well once we settle on
    // format of addresses in the system

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
    if (length < take)
        bufToString(buf, length, outputLog);
    else {
        bufToString(buf, take, outputLog);
        char tmp[50];
        snprintf(tmp, sizeof(tmp), " (+%u more)", length - take);
        outputLog += tmp;
    }
}

/**
 * Wait for an incoming packet. This is a fake method that uses
 * a request message explicitly provided by the test, or an empty
 * buffer if none was provided.
 */
bool
MockDriver::tryRecvPacket(Driver::Received *received)
{
    tryRecvPacketCount++;

    if (!inputMessage)
        return false;

    received->addrlen = 0;
    received->payload = inputMessage;
    received->len = inputMessageLen;
    received->driver = this;

    return true;
}

void
MockDriver::release(char *payload, uint32_t len)
{
    releaseCount++;
}

/**
 * This method is invoked by tests to provide a string that will
 * be used to synthesize an input message the next time one is
 * needed (such as for a packet).
 *
 * \param msg
 *      A string representation of the contents of a buffer;
 *      see stringToBuffer documentation for details.
 * \param msgLen
 *      The length of msg.
 */
void
MockDriver::setInput(char *msg, uint32_t msgLen) {
    inputMessage = msg;
    inputMessageLen = msgLen;
}

/**
 * Append a printable representation of the contents of the buffer
 * to a string.
 *
 * \param buffer
 *      Convert the contents of this to ASCII.
 * \param[out] s
 *      Append the converted value here. The output format is intended
 *      to simplify testing: things that look like strings are output
 *      that way, and everything else is output as 4-byte decimal integers.
 */
void
MockDriver::bufferToString(Buffer *buffer, string& s) {
    uint32_t length = buffer->getTotalLength();
    char buf[length];
    buffer->copy(0, length, buf);
    bufToString(buf, length, s);
}

/**
 * Fill in the contents of the buffer from a textual description.
 *
 * \param s
 *      Describes what to put in the buffer. Consists of one or more
 *      substrings separated by spaces. If a substring starts with a
 *      digit or "-" is assumed to be a decimal number, which is converted
 *      to a 4-byte signed integer in the buffer. Otherwise the
 *      characters of the substrate are appended to the buffer, with
 *      an additional null terminating character.
 * \param[out] buffer
 *      Where to store the results of conversion; any existing contents
 *      are discarded.
 */
void
MockDriver::stringToBuffer(const char* s, Buffer *buffer) {
    buffer->truncateFront(buffer->getTotalLength());

    uint32_t i, length;
    length = strlen(s);
    for (i = 0; i < length; ) {
        char c = s[i];
        if ((c == '0') && (s[i+1] == 'x')) {
            // Hexadecimal number
            int value = 0;
            i += 2;
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                if (c <= '9') {
                    value = 16*value + (c - '0');
                } else if ((c >= 'a') && (c <= 'f')) {
                    value = 16*value + 10 + (c - 'a');
                } else {
                    value = 16*value + 10 + (c - 'A');
                }
            }
            *(new(buffer, APPEND) int32_t) = value;
        } else if ((c == '-') || ((c >= '0') && (c <= '9'))) {
            // Decimal number
            int value = 0;
            int sign = (c == '-') ? -1 : 1;
            if (c == '-') {
                sign = -1;
                i++;
            }
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                value = 10*value + (c - '0');
            }
            *(new(buffer, APPEND) int32_t) = value * sign;
        } else {
            // String
            while (i < length) {
                char c = s[i];
                i++;
                if (c == ' ') {
                    break;
                }
                *(new(buffer, APPEND) char) = c;
            }
            *(new(buffer, APPEND) char) = 0;
        }
    }
    string s2;
    bufferToString(buffer, s2);
}

}  // namespace RAMCloud
