/* Copyright (c) 2010-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_PROTOBUF_H
#define RAMCLOUD_PROTOBUF_H

#include <sstream>

#include "Common.h"
#include "Buffer.h"
#include "ClientException.h"

namespace RAMCloud {
namespace ProtoBuf {

/**
 * Serialize and append a protocol buffers message to a #Buffer.
 * Prefer #serializeToRequest and #serializeToResponse rather than calling this
 * directly.
 * \tparam M
 *      Type of the protocol buffers message.
 * \tparam E
 *      Type of exception to throw if \a message is malformed.
 */
template<typename M, typename E>
uint32_t
serializeToBuffer(Buffer* buffer, M* message)
{
    std::ostringstream ostream;
    if (!message->IsInitialized() ||
        !message->SerializePartialToOstream(&ostream)) {
        throw E(HERE);
    }
    string str(ostream.str());
    uint32_t length = downCast<uint32_t>(str.length());
    if (length > 0) {
        buffer->appendCopy(str.c_str(), length);
    }
    return length;
}

template<typename M>
uint32_t
serializeToRequest(Buffer* buffer, M* message)
{
    return serializeToBuffer<M, RequestFormatError>(buffer, message);
}

template<typename M>
uint32_t
serializeToResponse(Buffer* buffer, M* message)
{
    return serializeToBuffer<M, ResponseFormatError>(buffer, message);
}

/**
 * Parse a protocol buffers message out of a #Buffer.
 * Prefer #parseFromRequest and #parseFromResponse rather than calling this
 * directly.
 * \tparam M
 *      Type of the protocol buffers message.
 * \tparam E
 *      Type of exception to throw if the message is malformed.
 */
template<typename M, typename E>
void
parseFromBuffer(Buffer* buffer, uint32_t offset, uint32_t length, M* message)
{
    string str(static_cast<const char*>(buffer->getRange(offset, length)),
               length);
    std::istringstream istream(str);
    if (!message->ParsePartialFromIstream(&istream) ||
        !message->IsInitialized()) {
        message->Clear();
        throw E(HERE);
    }
}

template<typename M>
void
parseFromRequest(Buffer* buffer, uint32_t offset, uint32_t length, M* message)
{
    parseFromBuffer<M, RequestFormatError>(buffer, offset, length, message);
}

template<typename M>
void
parseFromResponse(Buffer* buffer, uint32_t offset, uint32_t length, M* message)
{
    parseFromBuffer<M, ResponseFormatError>(buffer, offset, length, message);
}

} // namespace ProtoBuf
} // namespace RAMCloud

#endif  // RAMCLOUD_PROTOBUF_H
