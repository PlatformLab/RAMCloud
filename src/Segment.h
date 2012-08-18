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
#include "Seglet.h"
#include "SegletAllocator.h"
#include "Tub.h"

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
 * Segments are simple, append-only containers for typed data blobs. Data can
 * be added to the end of a segment and later retrieved, but not mutated.
 * Segments are easily iterated and they contain internal consistency checks to
 * help ensure metadata integrity.
 *
 * Although similar, Segments differ from buffers in several ways. First, any
 * append data is always copied. Second, they understand some of the data that
 * is stored within them. For example, they maintain internal metadata that
 * differentiates entries and allows for iteration. Third, they protect metadata
 * integrity with checksums. Finally, although segments may consist of many
 * discontiguous pieces of memory called "seglets", each seglet in a given
 * segment is always the same size.
 *
 * Seglets only exist to make log cleaning more efficient and are almost
 * entirely hidden by the segment API. The exception is that custom allocators
 * may be used when constructing segments in order to control the size of
 * seglets and segments.
 *
 * The log ties many segments together to form a larger logical log. By using
 * many smaller segments it can achieve more efficient garbage collection and
 * high backup bandwidth.
 *
 * Segments are also a useful way of transferring RAMCloud objects over the
 * network. Simply add objects to a segment, then append the segment's
 * contents to an outgoing RPC buffer. The receiver can construct an iterator
 * to extract the individual objects.
 */
class Segment {
  public:
    // TODO(Steve): This doesn't belong here, but rather in SegmentManager.
    enum { INVALID_SEGMENT_ID = ~(0ull) };

#ifdef VALGRIND
    // can't use more than 1M, see http://bugs.kde.org/show_bug.cgi?id=203877
    enum { DEFAULT_SEGMENT_SIZE = 1 * 1024 * 1024 };
#else
    enum { DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024 };
#endif

  PRIVATE:
    /**
     * Every piece of data appended to a segment is described by one of these
     * structures. All this does is identify the type of data that follows an
     * instance of this header and how many bytes are needed to store the length
     * of the entry (not including the header itself).
     *
     * The Segment code allocates an entry by writing this single byte header,
     * and then appends the entry's length using between 1 and 4 bytes. The
     * actual entry content begins immediately after. This class does not
     * store or load the length. It only encapsulates the first byte header.
     * The rest is up to the Segment class.
     */
    struct EntryHeader {
        /**
         * Construct a new header, initializing it with the given log entry type
         * and length of the data this entry will contain.
         */
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

        // Number of bytes needed to describe the entry's length and the type of
        // the entry (downcasted from the LogEntryType enum).
        //
        // The upper two bits indicate whether the entry's length consists of
        // 1, 2, 3, or 4 bytes. The lower 6 bits allow for 64 different possible
        // types.
        uint8_t lengthBytesAndType;

        /**
         * Obtain the type of log entry this header describes. Useful for
         * deciding how to interface the data in an entry.
         */
        LogEntryType
        getType() const
        {
            return static_cast<LogEntryType>(lengthBytesAndType & 0x3f);
        }

        /**
         * Obtain the number of bytes this entry's length requires to be
         * stored. For example, small entries (< 256 bytes) can have the
         * length fit in a single byte.
         */
        uint8_t
        getLengthBytes() const
        {
            return downCast<uint8_t>((lengthBytesAndType >> 6) + 1);
        }
    } __attribute__((__packed__));
    static_assert(sizeof(EntryHeader) == 1,
                  "Unexpected padding in Segment::EntryHeader");

    /**
     * Each segment's very last entry is a Footer. The footer denotes the end
     * of the segment and contains a checksum for checking segment metadata
     * integrity.
     */
    class Footer {
      public:
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

  public:
    /**
     * Segment footer prefixed with the necessary metadata to make a full and
     * properly-formatted segment entry. This special case exists because such
     * entries are given by value to the ReplicaManager during getAppendedLength()
     * calls. This is so the ReplicaManager can send a correct footer along with
     * each write call, while allowing the master to overwrite the footer in its
     * copy of the segment during writes that are concurrent with the replication.
     *
     * This structure is opaque because no code outside of the Segment class should
     * need to understand the internals. The replication/backup modules need only
     * know that this blob should be placed immediately after any backed up segment
     * data.
     */
    class OpaqueFooterEntry {
      public:
        OpaqueFooterEntry()
            : entryHeader(LOG_ENTRY_TYPE_INVALID, 0),
              length(0),
              footer(false, Crc32C())
        {
        }
      PRIVATE:                      // Just to be very explicit. Do not touch.
        EntryHeader entryHeader;
        uint8_t length;
        Footer footer;
    } __attribute__((__packed__));
    static_assert(sizeof(OpaqueFooterEntry) ==
        (sizeof(EntryHeader) + 1 + sizeof(Footer)),
        "Unexpected padding in Segment::OpaqueFooterEntry");

    Segment();
    Segment(vector<Seglet*>& seglets, uint32_t segletSize);
    Segment(const void* buffer, uint32_t length);
    virtual ~Segment();
    bool append(LogEntryType type,
                Buffer& buffer,
                uint32_t offset,
                uint32_t length,
                uint32_t& outOffset);
    bool append(LogEntryType type, Buffer& buffer, uint32_t& outOffset);
    bool append(LogEntryType type, Buffer& buffer);
    bool append(LogEntryType type,
                const void* data,
                uint32_t length,
                uint32_t& outOffset);
    bool append(LogEntryType type, const void* data, uint32_t length);
    void close();
    void disableAppends();
    bool enableAppends();
    uint32_t appendToBuffer(Buffer& buffer,
                            uint32_t offset,
                            uint32_t length) const;
    uint32_t appendToBuffer(Buffer& buffer);
    LogEntryType getEntry(uint32_t offset, Buffer& buffer);
    uint32_t getAppendedLength(OpaqueFooterEntry& footerEntry) const;
    uint32_t getSegletsAllocated();
    uint32_t getSegletsInUse();
    bool freeUnusedSeglets(uint32_t count);
    bool checkMetadataIntegrity();

  PRIVATE:
    void appendFooter();
    const EntryHeader* getEntryHeader(uint32_t offset);
    void getEntryInfo(uint32_t offset,
                      LogEntryType& outType,
                      uint32_t &outDataOffset,
                      uint32_t &outDataLength);
    uint32_t peek(uint32_t offset, const void** outAddress) const;
    uint32_t bytesLeft();
    uint32_t bytesNeeded(uint32_t length);
    uint32_t copyOut(uint32_t offset, void* buffer, uint32_t length) const;
    uint32_t copyIn(uint32_t offset, const void* buffer, uint32_t length);
    uint32_t copyInFromBuffer(uint32_t segmentOffset,
                              Buffer& buffer,
                              uint32_t bufferOffset,
                              uint32_t length);

    /// Size of each seglet in bytes.
    uint32_t segletSize;

    /// Seglets that have been loaned to this segment to store data in. These
    /// will be freed to their owning allocator upon destruction or calls to
    /// freeUnusedSeglets().
    vector<Seglet*> seglets;

    /// Pointers to memory blocks this segment will append data to. Typically
    /// this is just a cache of the pointers contained in the seglets loaned to
    /// this object (avoiding another layer of indirection when looking up an
    /// address). However, if this class allocated its own space in the default
    /// constructor, or if it was given a static buffer, a block entry will
    /// exist here that is not associated with any seglet instance.
    //
    /// The order in the array represents the order in which blocks logically
    /// appear in the segment. That is, segletBlocks[0] will cover byte offset 0
    /// through segletSize - 1.
    vector<void*> segletBlocks;

    /// Indicates whether or not this segment can presently be appended to. This
    /// mutability may be flipped on and off so long as the segment is not
    /// closed.
    bool immutable;

    /// Indicates whether or not this segment is ever allowed to allocate any
    /// more space. Closing a segment is an undoable operation.
    bool closed;

    /// In the case that the default constructor was used and this class
    /// allocated heap space, this will be set to true indicating that the
    /// destructor must free that space.
    bool mustFreeBlocks;

    /// Offset to the next free byte in Segment.
    uint32_t head;

    /// Latest Segment checksum (crc32c). This is a checksum of all metadata
    /// in the Segment (that is, every Segment::Entry, ::Header, and ::Footer).
    /// Any user data that is stored in the Segment is unprotected. Integrity
    /// is their responsibility.
    Crc32C checksum;

    /// Temporary nonsense to make horrible unit tests that abuse segments in
    /// weird ways work.
    OpaqueFooterEntry currentFooter;

    friend class SegmentIterator;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
