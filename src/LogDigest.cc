/* Copyright (c) 2009-2014 Stanford University
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

#include "Common.h"
#include "ShortMacros.h"
#include "LogDigest.h"

namespace RAMCloud {

/**
 * Create a new, empty log digest.
 */
LogDigest::LogDigest()
    : header(),
      segmentIds(),
      checksum()
{
    header.checksum = checksum.getResult();
}

/**
 * Create a new log digest and initialize it from a buffer containing a
 * previously serialized digest if the checksum is valid.
 */
LogDigest::LogDigest(const void* buffer, uint32_t length)
    : header(),
      segmentIds(),
      checksum()
{
    Crc32C actualChecksum;

    if (length < sizeof(header)) {
        LOG(WARNING, "buffer too small to hold header (length = %u)", length);
        throw LogDigestException(HERE, "buffer too small to hold header");
    }

    header = *reinterpret_cast<const Header*>(buffer);

    length -= sizeof32(header);
    if ((length % sizeof32(segmentIds.front())) != 0) {
        LOG(WARNING, "length left not even 64-bit multiple (%u)", length);
        throw LogDigestException(HERE, "length left not even 64-bit multiple");
    }

    const uint64_t* ids = reinterpret_cast<const uint64_t*>(
        reinterpret_cast<const uint8_t*>(buffer) + sizeof(header));
    for (uint32_t i = 0; i < length / sizeof32(segmentIds.front()); i++) {
        actualChecksum.update(&ids[i], sizeof(ids[i]));
        segmentIds.push_back(ids[i]);
    }

    if (actualChecksum.getResult() != header.checksum) {
        LOG(WARNING, "invalid digest checksum (computed 0x%08x, expect 0x%08x",
            actualChecksum.getResult(), header.checksum);
        throw LogDigestException(HERE, "invalid digest checksum");
    }
}

/**
 * Add a segment id to this LogDigest.
 */
void
LogDigest::addSegmentId(uint64_t id)
{
    segmentIds.push_back(id);
    checksum.update(&segmentIds.back(), sizeof(segmentIds.back()));
    header.checksum = checksum.getResult();
}

/**
 * Get the total number of segment ids in this LogDigest.
 */
uint32_t
LogDigest::size() const
{
    return downCast<uint32_t>(segmentIds.size());
}

/**
 * Return the N'th segment id stored in this digest.
 */
uint64_t
LogDigest::operator[](size_t index) const
{
    assert(index < segmentIds.size());
    return segmentIds[index];
}

/**
 * Append this digest to the given buffer, typically so that it can be sent
 * via RPC or appended to a log. This method does no copying, so it should
 * only be called after all ids have been added. This digest object must
 * also exist as long as the buffer it was appended to does, since it will
 * reference internal object memory.
 */
void
LogDigest::appendToBuffer(Buffer& buffer) const
{
    buffer.appendExternal(&header, sizeof(header));
    buffer.appendExternal(&segmentIds.front(),
        downCast<uint32_t>(sizeof(segmentIds.front()) * segmentIds.size()));
}

} // namespace RAMCloud
