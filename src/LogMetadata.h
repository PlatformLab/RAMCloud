/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_LOGMETADATA_H
#define RAMCLOUD_LOGMETADATA_H

#include "Minimal.h"
#include "Crc32C.h"

// This header file contains definitions for several pieces of metadata
// related to logs. One of the reasons for putting this information here,
// rather than in other header files, is that client-visible headers need
// these definitions, but we'd rather not expose all of the other
// RAMCloud header files to clients.

namespace RAMCloud {

/**
 * Each segment's first entry is a header that contains vital metadata such
 * as the log the segment is a part of, the segment's identifier within that
 * log, and so on. The header is written automatically upon construction.
 */
struct SegmentHeader {
    SegmentHeader(uint64_t logId,
                  uint64_t segmentId,
                  uint32_t capacity)
        : logId(logId),
          segmentId(segmentId),
          capacity(capacity),
          checksum()
    {
        Crc32C segmentChecksum;
        segmentChecksum.update(this,
            static_cast<unsigned>(sizeof(*this) - sizeof(Crc32C::ResultType)));
        checksum = segmentChecksum.getResult();
    }

    /// ID of the Log this segment belongs to.
    uint64_t logId;

    /// Log-unique ID for this Segment.
    uint64_t segmentId;

    /// Total capacity of this segment in bytes.
    uint32_t capacity;

    /// Checksum cover all of the above fields in the SegmentHeader.
    Crc32C::ResultType checksum;
} __attribute__((__packed__));
static_assert(sizeof(SegmentHeader) == 24,
              "Header has unexpected padding");

/**
 * Indicates which portion of a segment contains valid data and
 * information to verify the integrity of the metadata of the segment.
 * Segments return these opaque certificates on calls to
 * getAppendedLength(). Calling checkMetadataIntegrity() with a certificate
 * guarantees (with some probability) the segment metadata hasn't been
 * corrupted and is iterable through the length given in the certificate.
 * This is used by SegmentIterators to ensure the portion of the segment
 * they intend to iterate across is intact. ReplicaManager transmits
 * certificates to backups along with segment data which backups store
 * for when the segment data is used during recovery. Because only the
 * portion of the segment that is covered by the certificate is used,
 * the certificate acts as a way to atomically commit segment data to
 * backups.
 *
 * Code outside of the Segment and SegmentIterator class should not
 * need to understand the internals, except to retrieve the segmentLength
 * field, and shouldn't attempt to use certificates other than through
 * the SegmentIterator or Segment code.
 */
class SegmentCertificate {
  public:
    SegmentCertificate()
        : segmentLength()
        , checksum()
    {}

    /**
     * Compare this SegmentCertificate with another. Returns true if
     * they're equal, else false.
     */
    bool
    operator==(const SegmentCertificate& other) const
    {
        return segmentLength == other.segmentLength &&
               checksum == other.checksum;
    }

    /**
     * Return a string representation of the certificate.
     */
    string
    toString() const
    {
        return format("<%u, 0x%08x>", segmentLength, checksum);
    }

    /// Number of valid bytes in the segment that #checksum covers.
    /// Determines how much of the segment should be checked for integrity
    /// and how much of the segment should be iterated over for
    /// SegmentIterator.
    uint32_t segmentLength;

  PRIVATE:
    /// Checksum covering all metadata in the segment: EntryHeaders and
    /// their corresponding variably-sized length fields, as well as fields
    /// above in this struct.
    Crc32C::ResultType checksum;

    friend class Segment;
    friend class SegmentIterator;
} __attribute__((__packed__));
static_assert(sizeof(SegmentCertificate) == 8,
              "Unexpected padding in SegmentCertificate");


/**
 * LogPosition is a (Segment Id, Segment Offset) tuple that represents a
 * position in the log. For example, it can be considered the logical time
 * at which something was appended to the Log. It can be used for things
 * like computing table partitions and obtaining a master's current log
 * position.
 */
class LogPosition {
  public:
    /**
     * Default constructor that creates a zeroed position. This refers to
     * the very beginning of a log.
     */
    LogPosition()
        : pos(0, 0)
    {
    }

    /**
     * Construct a position given a segment identifier and offset within
     * the segment.
     */
    LogPosition(uint64_t segmentId, uint64_t segmentOffset)
        : pos(segmentId, downCast<uint32_t>(segmentOffset))
    {
    }

    bool operator==(const LogPosition& other) const {return pos == other.pos;}
    bool operator!=(const LogPosition& other) const {return pos != other.pos;}
    bool operator< (const LogPosition& other) const {return pos <  other.pos;}
    bool operator<=(const LogPosition& other) const {return pos <= other.pos;}
    bool operator> (const LogPosition& other) const {return pos >  other.pos;}
    bool operator>=(const LogPosition& other) const {return pos >= other.pos;}

    /**
     * Return the segment identifier component of this position object.
     */
    uint64_t getSegmentId() const { return pos.first; }

    /**
     * Return the offset component of this position object.
     */
    uint32_t getSegmentOffset() const { return pos.second; }

  PRIVATE:
    std::pair<uint64_t, uint32_t> pos;
};

} // namespace RAMCloud

#endif // RAMCLOUD_LOGMETADATA_H
