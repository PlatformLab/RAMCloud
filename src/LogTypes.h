/* Copyright (c) 2009, 2010 Stanford University
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

#ifndef RAMCLOUD_LOGTYPES_H
#define RAMCLOUD_LOGTYPES_H

#include <functional>

#include "Common.h"
#include "Crc32C.h"

namespace RAMCloud {

/**
 * LogPosition is a (Segment Id, Segment Offset) tuple that represents a
 * position in the log. For example, it can be considered the logical time
 * at which something was appended to the Log. It can be used for things like
 * computing table partitions and obtaining a master's current log position.
 */
class LogPosition {
  public:
    LogPosition()
        : pos(0, 0)
    {
    }

    LogPosition(uint64_t segmentId, uint64_t segmentOffset)
        : pos(segmentId, downCast<uint32_t>(segmentOffset))
    {
    }

    bool operator==(const LogPosition& other) const { return pos == other.pos; }
    bool operator!=(const LogPosition& other) const { return pos != other.pos; }
    bool operator< (const LogPosition& other) const { return pos <  other.pos; }
    bool operator<=(const LogPosition& other) const { return pos <= other.pos; }
    bool operator> (const LogPosition& other) const { return pos >  other.pos; }
    bool operator>=(const LogPosition& other) const { return pos >= other.pos; }
    uint64_t segmentId() const { return pos.first; }
    uint32_t segmentOffset() const { return pos.second; }

  private:
    std::pair<uint64_t, uint32_t> pos;
};

/**
 * Each entry in the log has an 8-bit type field. In a disaster recovery
 * situation this doesn't give us much to go on if we're trying to recover
 * segments from random memory, however, the addition of a CRC gives us a
 * reliable means of validating a suspected entry.
 */
enum LogEntryType {
    LOG_ENTRY_TYPE_UNINIT    = 0x0,
    LOG_ENTRY_TYPE_INVALID   = 'I',
    LOG_ENTRY_TYPE_SEGHEADER = 'H',
    LOG_ENTRY_TYPE_SEGFOOTER = 'F',
    LOG_ENTRY_TYPE_OBJ       = 'O',
    LOG_ENTRY_TYPE_OBJTOMB   = 'T',
    LOG_ENTRY_TYPE_LOGDIGEST = 'D'
};

/// The class used to calculate segment checksums.
typedef Crc32C SegmentChecksum;

struct SegmentEntry {
    SegmentEntry(LogEntryType type, uint32_t entryLength)
        : type(downCast<uint8_t>(type)),
          mutableFields{0},
          length(entryLength),
          checksum(0)
    {
    }

    /// LogEntryType (enum is 32-bit, but we downcast for lack of billions of
    /// different types).
    uint8_t                     type;

    /// Log data is typically immutable - it absolutely never changes once
    /// written, despite cleaning, recovery, etc. However, there are some
    /// cases where we'd like to have a handful of bits that can change when
    /// objects move between machines, segments, etc. We use the following to
    /// enable SegmentEntry -> Segment base address calculations with variably
    /// sized segments.
    struct {
        /// log_2(Capacity of Segment in bytes). That is, encodes the
        /// power-of-two size of the Segment between 2^0 and 2^31.
        uint8_t                 segmentCapacityExponent;
    } mutableFields;

    /// Length of the entry described in by this header in bytes.
    uint32_t                    length;

    /// Checksum of the header and following entry data (with checksum and
    /// mutableFields zeroed out) and then XORed with the checksum of the
    /// mutableFields. This lets us back out the mutableFields value so it
    /// can be changed without recomputing the entire checksum. Users never
    /// see a checksum with the mutableFields included, since it's XORed out
    /// again when the checksum is retrieved via the #SegmentEntryHandle.
    /// This lets us compare checksums with entries of different mutableField
    /// values (e.g. from different machines or Segments). If the mutableField
    /// is corrupted, then we won't likely be able to back out to the correct
    /// checksum and will therefore detect corruption anywhere in the entry.
    SegmentChecksum::ResultType checksum;
} __attribute__((__packed__));
static_assert(sizeof(SegmentEntry) == 10,
              "SegmentEntry has unexpected padding");

struct SegmentHeader {
    /// ID of the Log this segment belongs to.
    uint64_t logId;

    /// Log-unique ID for this Segment.
    uint64_t segmentId;

    /// Total capacity of this segment in bytes.
    uint32_t segmentCapacity;

    /// If != INVALID_SEGMENT_ID, this Segment was created by the cleaner and
    /// the value here is the id of the head segment at the time cleaning
    /// started. Any data in this segment is guaranteed to have come from
    /// earlier segments.
    uint64_t headSegmentIdDuringCleaning;
} __attribute__((__packed__));
static_assert(sizeof(SegmentHeader) == 28,
              "SegmentHeader has unexpected padding");

struct SegmentFooter {
    SegmentChecksum::ResultType checksum;
} __attribute__((__packed__));
static_assert(sizeof(SegmentFooter) == sizeof(SegmentChecksum::ResultType),
              "SegmentFooter has unexpected padding");

/**
 * SegmentFooter together with its SegmentEntry. Declared explicitly
 * because this combination of footer with its entry is given by value
 * to the ReplicaManager on Segment::getCommittedLength() calls.
 * This is so the ReplicaManager can send a correct footer along with
 * each write call, while allowing the master to overwrite the footer
 * in its copy of the segment during writes that are concurrent with
 * the replication.
 */
struct SegmentFooterEntry {
    SegmentFooterEntry()
        : entry(LOG_ENTRY_TYPE_SEGFOOTER, sizeof(footer))
        , footer{0}
    {}
    explicit SegmentFooterEntry(SegmentChecksum::ResultType checksum)
        : entry(LOG_ENTRY_TYPE_SEGFOOTER, sizeof(footer))
        , footer{checksum}
    {}
    SegmentEntry entry;
    SegmentFooter footer;
} __attribute__((__packed__));

} // namespace RAMCloud

namespace std {

/**
 * Specialize std::hash<LogEntryType>, which allows LogEntryType to be used as
 * keys of std::unordered_map, etc.
 */
template<>
struct hash<RAMCloud::LogEntryType> {
    size_t operator()(RAMCloud::LogEntryType type) const {
        return std::hash<uint8_t>()(static_cast<uint8_t>(type));
    }
};

} // namespace std

#endif // !RAMCLOUD_LOGTYPES_H
