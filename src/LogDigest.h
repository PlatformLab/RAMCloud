/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_LOGDIGEST_H
#define RAMCLOUD_LOGDIGEST_H

#include "Common.h"
#include "Crc32C.h"

namespace RAMCloud {

/// Exception thrown when deserializing a corrupt digest.
struct LogDigestException : public Exception {
    LogDigestException(const CodeLocation& where, std::string msg)
            : Exception(where, msg) {}
};

/**
 * The LogDigest is a special entry that is written to the front of every new
 * head segment. It simply contains a list of all segment IDs that are part
 * of the log as of that head's creation. This is used during recovery to
 * discover all needed segments and determine when data loss has occurred.
 * That is, once the latest head segment is found, the recovery process need
 * only find copies of all segments referenced by the head's LogDigest. If it
 * finds them all (and they pass checksums), it knows it has the complete log.
 *
 */
class LogDigest {
  public:
    LogDigest();
    LogDigest(const void* buffer, uint32_t length);
    void addSegmentId(uint64_t id);
    uint32_t size() const;
    uint64_t operator[](size_t index) const;
    void appendToBuffer(Buffer& buffer) const;

  PRIVATE:
    /**
     * When serialized in a buffer, the log digest starts with this header.
     */
    struct Header {
        Crc32C::ResultType checksum;
    } __attribute__((__packed__));

    /// Copy of the current header. Updated every time another segment id is
    /// added.
    Header header;

    /// Vector containing all of the segment ids added to this digest.
    vector<uint64_t> segmentIds;

    /// Current accumulated checksum. Updated each time a new segment id is
    /// added.
    Crc32C checksum;
};

} // namespace RAMCloud

#endif // RAMCLOUD_LOGDIGEST_H
