/* Copyright (c) 2012 Stanford University
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

#include "Common.h"
#include "Crc32C.h"
#include "Segment.h"

namespace RAMCloud {

/**
 * Each segment's first entry is a header that contains vital metadata such
 * as the log the segment is a part of, the segment's identifier within that
 * log, and so on. The header is written automatically upon construction.
 */
struct SegmentHeader {
    SegmentHeader(uint64_t logId,
                  uint64_t segmentId,
                  uint32_t capacity,
                  uint64_t headSegmentIdDuringCleaning)
        : logId(logId),
          segmentId(segmentId),
          capacity(capacity),
          headSegmentIdDuringCleaning(headSegmentIdDuringCleaning),
          checksum()
    {
        Crc32C segmentChecksum;
        segmentChecksum.update(this, sizeof(*this) - sizeof(Crc32C::ResultType));
        checksum = segmentChecksum.getResult();
    }

    /// ID of the Log this segment belongs to.
    uint64_t logId;

    /// Log-unique ID for this Segment.
    uint64_t segmentId;

    /// Total capacity of this segment in bytes.
    uint32_t capacity;

    /// If != INVALID_SEGMENT_ID, this Segment was created by the cleaner and
    /// the value here is the id of the head segment at the time cleaning
    /// started. Any data in this segment is guaranteed to have come from
    /// earlier segments. This allows us to logically order cleaner-generated
    /// survivor segments, even though they may have IDs larger than head
    /// segments containing newer data.
    uint64_t headSegmentIdDuringCleaning;

    /// Checksum cover all of the above fields in the SegmentHeader.
    Crc32C::ResultType checksum;
} __attribute__((__packed__));
static_assert(sizeof(SegmentHeader) == 32,
              "Header has unexpected padding");

struct LogDigestException : public Exception {
    LogDigestException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * The LogDigest is a special entry that is written to the front of every new
 * head Segment. It simply contains a list of all Segment IDs that are part
 * of the Log as of that head's creation. This is used during recovery to
 * discover all needed Segments and determine when data loss has occurred.
 * That is, once the latest head Segment is found, the recovery process need
 * only find copies of all Segments referenced by the head's LogDigest. If it
 * finds them all (and they pass checksums), it knows it has the complete Log.
 *
 * TODO(anyone): Should this not just be, or at least serialize to/from, a
 *               protocol buffer?
 */
class LogDigest {
  public:
    typedef uint64_t SegmentId;

    /**
     * Create a LogDigest and serialize it to the given buffer.
     * This is the method to call when creating a new LogDigest,
     * i.e. when addSegment() will be called. Calls to addSegment
     * will update the serialization in the provided buffer.
     *  
     * \param[in] maxSegmentCount
     *      The maximum number of SegmentIDs that are to be stored
     *      in this LogDigest
     * \param[in] base
     *      Base address of a buffer in which to serialise this
     *      LogDigest.
     * \param[in] length
     *      Length of the buffer pointed to by ``base'' in bytes.
     */
    LogDigest(size_t maxSegmentCount, void* base, uint32_t length)
        : ldd(static_cast<LogDigestData*>(base)),
          currentSegment(0),
          freeSlots((length - sizeof32(LogDigestData)) / sizeof32(SegmentId)),
          checksum()
    {
        if (length <= sizeof32(LogDigestData))
            throw LogDigestException(HERE, "length too small");

        if (freeSlots > maxSegmentCount)
            freeSlots = downCast<uint32_t>(maxSegmentCount);

        ldd->segmentCount = 0;
        for (uint32_t i = 0; i < freeSlots; i++) {
            ldd->segmentIds[i] = static_cast<SegmentId>(
                                            Segment::INVALID_SEGMENT_ID);
        }
        checksum.update(&ldd->segmentCount, sizeof(ldd->segmentCount));
        ldd->checksum = checksum.getResult(); 
    }

    /**
     * Create a LogDigest object from a previous one that was
     * serialised in the given buffer. This is the method to
     * call when accessing a previously-constructed and
     * serialised LogDigest.
     *
     * \param[in] base
     *      Base address of a buffer that contains a serialised
     *      LogDigest.
     * \param[in] length
     *      Length of the buffer pointed to by ``base'' in bytes.
     */
    LogDigest(const void* base, uint32_t length)
        : ldd(static_cast<LogDigestData*>(const_cast<void*>(base))),
          currentSegment(downCast<uint32_t>(ldd->segmentCount)),
          freeSlots(0),
          checksum()
    {
        if (length < getBytesFromCount(ldd->segmentCount))
            throw LogDigestException(HERE, "digest larger than given buffer");

        checksum.update(&ldd->segmentCount,
                        sizeof(ldd->segmentCount));
        for (uint32_t i = 0; i < ldd->segmentCount; i++) {
            checksum.update(&ldd->segmentIds[i],
                            sizeof(ldd->segmentIds[i]));    
        }

        if (checksum.getResult() != ldd->checksum)
            throw LogDigestException(HERE, "digest checksum invalid");
    }

    /**
     * Add a SegmentID to this LogDigest.
     */
    void
    addSegment(SegmentId id)
    {
        if (freeSlots == 0)
            throw LogDigestException(HERE, "out of space: can't add id");

        ldd->segmentIds[currentSegment] = id;
        checksum.update(&ldd->segmentIds[currentSegment],
                        sizeof(ldd->segmentIds[currentSegment]));
        ldd->checksum = checksum.getResult(); 
        currentSegment++;
        freeSlots--;
    }

    /**
     * Get the number of SegmentIDs in this LogDigest.
     */
    int
    getSegmentCount() const
    {
        return ldd->segmentCount;
    }

    /**
     * Get an array of SegmentIDs in this LogDigest. There
     * will be getSegmentCount() elements in the array.
     */
    const SegmentId*
    getSegmentIds() const
    {
        return ldd->segmentIds;
    }

    /**
     * Return the number of bytes needed to store a LogDigest
     * that contains ``segmentCount'' Segment IDs.
     */
    static uint32_t
    getBytesFromCount(size_t segmentCount)
    {
        return sizeof32(LogDigestData) +
            (downCast<uint32_t>(segmentCount) * sizeof32(SegmentId));
    }

    /**
     * Return a raw pointer to the memory passed in to the constructor.
     */
    const void*
    getRawPointer()
    {
        return static_cast<void*>(ldd);
    }

    /**
     * Return the number of bytes this LogDigest uses.
     */
    uint32_t
    getBytes()
    {
        return getBytesFromCount(ldd->segmentCount);
    }

  PRIVATE:
    struct LogDigestData {
        uint32_t segmentCount;
        Crc32C::ResultType checksum;
        SegmentId segmentIds[0];
    } __attribute__((__packed__));

    LogDigestData* ldd;
    uint32_t currentSegment;
    uint32_t freeSlots;
    Crc32C checksum;
};

} // namespace RAMCloud

#endif // RAMCLOUD_LOGMETADATA_H
