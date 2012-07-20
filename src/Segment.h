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

#ifndef RAMCLOUD_SEGMENT_H
#define RAMCLOUD_SEGMENT_H

#include <vector>

#include "Common.h"
#include "Buffer.h"
#include "Crc32C.h"
#include "LogEntryTypes.h"
#include "LargeBlockOfMemory.h"

namespace RAMCloud {

/**
 * An exception that is thrown when the Segment class is provided invalid
 * method arguments or mutating operations are attempted on a closed Segment.
 */
struct SegmentException : public Exception {
    SegmentException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * Methods and metadata for a segment stored in memory. Segments objects
 * wrap a contiguous region of memory that hold entries that were appended
 * there. This chunk of memory is self-describing, so the Segment object
 * is really just a series of operations on that memory and cached state
 * about it. The cached state could be recomputed if necessary and this
 * is effectively what happens during recovery and cold start.
 */
class Segment {
  public:
    enum { INVALID_SEGMENT_ID = ~(0ull) };

    enum { DEFAULT_SEGLET_SIZE = 128 * 1024 };

#ifdef VALGRIND
    // can't use more than 1M, see http://bugs.kde.org/show_bug.cgi?id=203877
    enum { DEFAULT_SEGMENT_SIZE = 1 * 1024 * 1024 };
#else
    enum { DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024 };
#endif

    class Allocator {
      public:
        virtual ~Allocator() { }
        virtual uint32_t getSegletsPerSegment() = 0;
        virtual uint32_t getSegletSize() = 0;
        virtual void* alloc() = 0;
        virtual void free(void* seglet) = 0;

        uint32_t
        getSegmentSize()
        {
            return getSegletSize() * getSegletsPerSegment();
        }
    };

    class DefaultHeapAllocator : public Allocator {
      public:
        uint32_t getSegletsPerSegment() { return 1; }
        uint32_t getSegletSize() { return DEFAULT_SEGMENT_SIZE; }
        void* alloc() { return new char[getSegmentSize()]; }
        void free(void* seglet) { delete[] reinterpret_cast<char*>(seglet); }
    };

  PRIVATE:
    /**
     * Every piece of data appended to a segment is described by one of these
     * structures. All this does is identify the type of data that follows
     * each header and how many bytes are needed to store the length of the
     * entry (not including the header itself).
     *
     * The Segment code allocates an entry by writing this single byte header,
     * and then appends the entry's length using between 1 and 4 bytes. The
     * actual entry content begins immediately after. This class does not
     * store or load the length. It only encapsulates the first byte header.
     * The rest is up to the Segment class.
     */
    struct EntryHeader {
        EntryHeader(LogEntryType type, uint32_t length)
            : lengthBytesAndType(downCast<uint8_t>(type))
        {
            assert((type & ~0x3f) == 0);

            if (length < 0x00000100)
                lengthBytesAndType |= (0 << 6);
            else if (length < 0x00010000)
                lengthBytesAndType |= (1 << 6);
            else if (length < 0x01000000)
                lengthBytesAndType |= (2 << 6);
            else
                lengthBytesAndType |= (3 << 6);
        }

        EntryHeader()
            : lengthBytesAndType(0)
        {
        }

        // Number of bytes needed to describe the entry's length and the type of
        // the entry (downcasted from the LogEntryType enum).
        //
        // The upper two bits indicate whether the entry's length consists of
        // 1, 2, 3, or 4 bytes. The lower 6 bits allow for 64 different possible
        // types.
        uint8_t lengthBytesAndType;

        LogEntryType
        getType() const
        {
            return static_cast<LogEntryType>(lengthBytesAndType & 0x3f);
        }

        uint8_t
        getLengthBytes() const
        {
            return downCast<uint8_t>((lengthBytesAndType >> 6) + 1);
        }
    } __attribute__((__packed__));
    static_assert(sizeof(EntryHeader) == 1,
                  "Unexpected padding in Segment::EntryHeader");

  public:
    Segment(Allocator& allocator);
    Segment();
    virtual ~Segment();
    bool append(LogEntryType type, Buffer& buffer, uint32_t offset, uint32_t length, uint32_t& outOffset);
    bool append(LogEntryType type, Buffer& buffer, uint32_t& outOffset);
    bool append(LogEntryType type, Buffer& buffer);
    bool append(LogEntryType type, const void* data, uint32_t length, uint32_t& outOffset);
    bool append(LogEntryType type, const void* data, uint32_t length);
    void free(uint32_t entryOffset);
    void close();
    uint32_t appendToBuffer(Buffer& buffer, uint32_t offset, uint32_t length);
    uint32_t appendToBuffer(Buffer& buffer);
    uint32_t appendEntryToBuffer(uint32_t offset, Buffer& buffer);
    LogEntryType getEntryTypeAt(uint32_t offset);
    uint32_t getTailOffset();
    uint32_t getSegletsAllocated();
    uint32_t getSegletsNeeded();

  PRIVATE:
    /**
     * Each segment's very last entry is a Footer. The footer denotes the end
     * of the segment and contains a checksum for checking segment metadata
     * integrity.
     */
    struct Footer {
        Footer(bool closed, Crc32C segmentChecksum)
            : closed(closed),
              checksum()
        {
            segmentChecksum.update(this,
                sizeof(*this) - sizeof(Crc32C::ResultType));
            checksum = segmentChecksum.getResult();
        }

        /// If true, this segment was closed by the log and therefore could
        /// not be the head of the log.
        bool closed;

        /// Checksum covering all metadata in the segment, including fields
        /// above in this struct.
        Crc32C::ResultType checksum;
    } __attribute__((__packed__));
    static_assert(sizeof(Footer) == 5, "Unexpected padding in Segment::Footer");

    typedef std::vector<void*> SegletVector;

    void appendFooter();
    uint32_t getEntryDataOffset(uint32_t offset);
    uint32_t getEntryDataLength(uint32_t offset);
    void* getAddressAt(uint32_t offset);
    uint32_t getContiguousBytesAt(uint32_t offset); 
    void* offsetToSeglet(uint32_t offset);
    uint32_t bytesLeft();
    uint32_t bytesNeeded(uint32_t length);
    void copyOut(uint32_t offset, void* buffer, uint32_t length);
    void copyIn(uint32_t offset, const void* buffer, uint32_t length);
    void copyInFromBuffer(uint32_t segmentOffset, Buffer& buffer, uint32_t bufferOffset, uint32_t length);

    /// The allocator our seglets are obtained from during construction and
    /// returned to during destruction.
    Allocator& allocator;

    /// Seglets that back this segment. The order here is the logical order.
    /// That is, seglets[0] will cover offset 0 through segletSize - 1.
    SegletVector seglets;

    /// Indicates whether or not this segment is allowed to allocate any more
    /// space.
    bool closed;

    /// Offset to the next free byte in Segment.
    uint32_t tail;

    /// Bytes freed in this Segment by invocation of the #free method on a
    /// particular entry handle.
    uint32_t bytesFreed;

    /// Sum of live datas' space-time products. I.e., for each entry multiply
    /// its timestamp by its size in bytes and add to this counter. This is
    /// used to compute an average age per byte in the Segment, which in turn
    /// is used for the cleaner's cost-benefit analysis.
    uint64_t spaceTimeSum;

    /// Latest Segment checksum (crc32c). This is a checksum of all metadata
    /// in the Segment (that is, every Segment::Entry, ::Header, and ::Footer).
    /// Any user data that is stored in the Segment is unprotected. Integrity
    /// is their responsibility.
    Crc32C checksum;

    /// The number of each type of entry present in the Segment. Note that this
    /// counts all entries, both dead and live.
    uint32_t entryCountsByType[TOTAL_LOG_ENTRY_TYPES];

    friend class SegmentIterator;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
