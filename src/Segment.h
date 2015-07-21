/* Copyright (c) 2009-2015 Stanford University
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
#include "LogMetadata.h"
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
 * Segments are basically miniature logs that immutable data such as objects and
 * tombstones are appended to. Each piece of data appended is called an "entry"
 * and has a type and length metadata associated with it that this class
 * maintains. This information allows segments to be easily iterated and their
 * contents properly interpreted during, for example, failure recovery. To
 * ensure the integrity of segments, all metadata is checksummed. Verify a
 * segment requires checking the metadata contents against a "certificate",
 * which includes the checksum and the length of the segment. Entry contents
 * are not checksummed. If integrity checks are needed, the owner of the data
 * should implement and store their own entry-specific checksums.
 *
 * The log ties many segments together to form a larger logical log. By using
 * many smaller segments it can achieve more efficient garbage collection and
 * high backup bandwidth. This class defines both in in-memory and on-disk
 * layouts of the log. That is, this same class is used in both places.
 *
 * Each segment is composed of a collection of fixed-size chunks of memory
 * called "seglets". Seglets make it possible to support variable-sized segments
 * efficiently, which is important for log cleaning. The presence of seglets is
 * mostly hidden inside the segment class, except that it means that segment
 * users cannot assume that any particular entry in the segment is contiguous in
 * memory.
 *
 * Segments are also a useful way of transferring RAMCloud objects over the
 * network. Simply add objects to a segment, then append the segment's
 * contents to an outgoing RPC buffer. The receiver can construct an iterator
 * to extract the individual objects.
 *
 * Although similar, segments differ from buffers in several ways. First, any
 * append data is always copied. Second, they understand some of the data that
 * is stored within them. For example, they maintain internal metadata that
 * differentiates entries and allows for iteration. Third, they protect metadata
 * integrity with checksums. Finally, although segments may consist of many
 * discontiguous pieces of memory called "seglets", each seglet in a given
 * segment is always the same size.
 */
class Segment {
  public:
    // This doesn't belong here, but rather in SegmentManager.
    enum : uint64_t { INVALID_SEGMENT_ID = -1UL };

#ifdef VALGRIND
    // can't use more than 1M, see http://bugs.kde.org/show_bug.cgi?id=203877
    enum { DEFAULT_SEGMENT_SIZE = 1 * 1024 * 1024 };
#else
    enum { DEFAULT_SEGMENT_SIZE = 8 * 1024 * 1024 };
#endif

  PRIVATE:
    /**
     * Immediately before the contents of every segment entry are this header
     * and a varibly-lengthed field containing the length of the entry. These
     * serve only to record the type of the entry stored and its length. For
     * example:
     *
     *                                  Segment
     *   ______________________________________________________________________
     *  | EntryHeader | length (1-4 bytes) | entry contents | EntryHeader | ...
     *   ----------------------------------------------------------------------
     *   ^-- First entry start            First entry end --^
     *
     * The length field is variably-sized to save memory when entries are small.
     * The EntryHeader itself is only one byte. The upper two bits dictate how
     * long the subsequent length field is, while the lower six bits record the
     * entry's type.
     *
     * The Segment code allocates an entry by writing this single byte header,
     * and then appends the entry's length using between 1 and 4 bytes. This
     * class does not store or load the length. It only encapsulates the first
     * byte header. The rest is up to the Segment class.
     */
    struct EntryHeader {
        /**
         * Default constructor creating an EntryHeader for an invalid log entry
         * type with a single-byte length field. This is only used when creating
         * an uninitialized EntryHeader in memory before copying out from the
         * segment.
         */
        EntryHeader()
            : lengthBytesAndType(downCast<uint8_t>(LOG_ENTRY_TYPE_INVALID))
        {
        }

        /**
         * Construct a new header, initializing it with the given parameters.
         *
         * \param type
         *      Type of the log entry. See LogEntryTypes.h.
         * \param length
         *      Length of the entry this header describes in bytes.
         */
        EntryHeader(LogEntryType type, uint32_t length)
            : lengthBytesAndType(downCast<uint8_t>(type))
        {
            // The upper two bits encode the number of bytes needed to store
            // the length. Ensure that the type does not interfere.
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
            // This assertion may be a little fragile for any sort of forward
            // compatibility (assuming we only add types), but I'd rather make
            // sure.
            assert((lengthBytesAndType & 0x3f) < TOTAL_LOG_ENTRY_TYPES);
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

        /**
         * Returns true if other EntryHeader structures are equal, else false.
         * Exists primarily for unit tests.
         */
        bool
        operator==(const EntryHeader& other) const
        {
            return other.lengthBytesAndType == lengthBytesAndType;
        }
    } __attribute__((__packed__));
    static_assert(sizeof(EntryHeader) == 1,
                  "Unexpected padding in Segment::EntryHeader");

  public:
    /**
     * Segment References are handles used to access entries that have been
     * appended to a Segment. Since segments may be discontiguous, we cannot
     * simply return pointers to the start of an entry. However, since entries
     * are often contiguous, especially when they're small, we would like to
     * be able to access them directly when possible for efficiency's sake.
     *
     * This class simply wraps a direct pointer to the start of an entry in a
     * segment and allows callers to fill in a Buffer so that they can access
     * the appended data. In the common case where the entry is contiguous,
     * filling the Buffer in is very fast. If the entry spans multiple seglets,
     * a slower path is taken through the Segment code to look up all of the
     * constituent seglets.
     */
    class Reference {
      public:
        Reference()
            : reference(0)
        {
        }

        explicit Reference(uint64_t reference)
            : reference(reference)
        {
        }

        Reference(Segment* segment, uint32_t offset)
            : reference(0)
        {
            const void* p = NULL;
            uint32_t contigBytes = segment->peek(offset, &p);
            assert(contigBytes > 0);
            reference = reinterpret_cast<uint64_t>(p);
        }

        /**
         * Obtain the 64-bit integer value of the reference. The upper 16 bits
         * are guaranteed to be zero.
         */
        uint64_t
        toInteger() const
        {
            return reference;
        }

        LogEntryType getEntry(SegletAllocator* allocator,
                              Buffer* buffer,
                              uint32_t* lengthWithMetadata = NULL);

        /**
         * Compare references for equality. Returns true if equal, else false.
         */
        bool
        operator==(const Reference& other) const
        {
            return reference == other.reference;
        }

        /**
         * Returns the exact opposite of operator==.
         */
        bool
        operator!=(const Reference& other) const
        {
            return !operator==(other);
        }

      PRIVATE:
        uint64_t reference;
    };

    Segment();
    Segment(const vector<Seglet*>& seglets, uint32_t segletSize);
    Segment(const void* buffer, uint32_t length);
    virtual ~Segment();
    bool hasSpaceFor(uint32_t* entryLengths, uint32_t numEntries);
    bool hasSpaceFor(uint32_t length);
    bool append(LogEntryType type,
                const void* data,
                uint32_t length,
                Reference* outReference = NULL);
    bool append(LogEntryType type,
                Buffer& buffer,
                Reference* outReference = NULL);
    bool append(const void* buffer,
                uint32_t* entryDataLength = NULL,
                LogEntryType *type = NULL,
                Reference* outReference = NULL);
    static void appendLogHeader(LogEntryType type,
                                uint32_t objectSize,
                                Buffer *logBuffer);
    void close();
    void appendToBuffer(Buffer& buffer,
                        uint32_t offset,
                        uint32_t length) const;
    uint32_t appendToBuffer(Buffer& buffer);
    LogEntryType getEntry(uint32_t offset,
                          Buffer* buffer,
                          uint32_t* lengthWithMetadata = NULL);
    LogEntryType getEntry(Reference reference,
                          Buffer* buffer,
                          uint32_t* lengthWithMetadata = NULL);
    static LogEntryType getEntry(const void* buffer,
                                 uint32_t* entryDataLength = NULL,
                                 uint32_t* lengthWithMetadata = NULL);
    static LogEntryType getEntry(Buffer* buffer,
                                 uint32_t offset,
                                 uint32_t* entryDataLength = NULL,
                                 uint32_t* lengthWithMetadata = NULL);
    uint32_t getAppendedLength(SegmentCertificate* certificate = NULL) const;
    uint32_t getSegletsAllocated();
    uint32_t getSegletsInUse();
    bool freeUnusedSeglets(uint32_t count);
    bool checkMetadataIntegrity(const SegmentCertificate& certificate);
    uint32_t copyOut(uint32_t offset, void* buffer, uint32_t length) const;
    Reference getReference(uint32_t offset);

    /**
     * 'Peek' into the segment by specifying a logical byte offset and getting
     * back a pointer to some contiguous space underlying the start and the
     * number of contiguous bytes at that location. In other words, resolve the
     * offset to a pointer and learn how far from the end of the seglet that
     * offset is.
     *
     * \param offset
     *      Logical segment offset to being peeking into.
     * \param[out] outAddress
     *      Pointer to contiguous memory corresponding to the given offset.
     * \return
     *      The number of contiguous bytes accessible from the returned pointer
     *      (outAddress). 
     */
    uint32_t
    peek(uint32_t offset, const void** outAddress) const
    {
        // Possible issue: This method will not work properly if we compile with
        // -fstrict-aliasing (the default in -O2 and higher). It can lead to
        // especially surprising behavior in getEntryHeader. The issue is that
        // we return a void* in outAddress and end up casting it to some other
        // type. GCC's optimizer will happily speed up that invocation by
        // leaving the pointer's original value alone.
        // More details:
        // Steve Rumble came across this when he ported the log to memcached and
        // ended up using some different compiler flags (including
        // -fstrict-aliasing). It turned out that Segment::getEntryHeader()
        // would always try to deref NULL, because the compiler was making some
        // assumption he didn't fully understand. It was basically optimizing
        // away the entire call to peek ('header' couldn't possibly point to
        // the same thing as the second argument to peek() according to strict
        // aliasing, so it didn't return the address in that second out
        // argument).

        if (expect_false(offset >= (segletSize * segletBlocks.size())))
            return 0;

        uint32_t segletOffset = offset;
        uint32_t segletIndex = 0;

        // If we have more than one seglet, then they must all be the same size
        // and a power of two, so use bit ops rather than division and modulo to
        // save time. This method can be hot enough that this makes a big
        // difference.
        if (expect_true(segletSizeShift != 0)) {
            segletOffset = offset & (segletSize - 1);
            segletIndex = offset >> segletSizeShift;
        }

        uint8_t* segletPtr;
        segletPtr = reinterpret_cast<uint8_t*>(segletBlocks[segletIndex]);
        assert(segletPtr != NULL);
        *outAddress = static_cast<void*>(segletPtr + segletOffset);

        return segletSize - segletOffset;
    }

  PRIVATE:
    EntryHeader getEntryHeader(uint32_t offset);
    uint32_t copyIn(uint32_t offset, const void* buffer, uint32_t length);
    uint32_t copyInFromBuffer(uint32_t segmentOffset,
                              Buffer& buffer,
                              uint32_t bufferOffset,
                              uint32_t length);

    /// Size of each seglet in bytes.
    uint32_t segletSize;

    /// If the segment consists of multiple seglets, then this is simply equal
    /// to log2(segletSize). Otherwise, it is 0.
    ///
    /// This allows us to very quickly calculate the seglet index and offset
    /// in #peek() using bit ops, rather than division and modulo.
    int segletSizeShift;

    /// Seglets that have been loaned to this segment to store data in. These
    /// will be freed to their owning allocator upon destruction or calls to
    /// freeUnusedSeglets().
    ///
    /// This vector only exists to track seglet memory that must be freed in a
    /// special way. All seglet contents are accessed through pointers in the
    /// #segletBlocks vector.
    vector<Seglet*> seglets;

    /// Pointers to memory blocks this segment will append data to. Typically
    /// this is just a cache of the pointers contained in the seglets loaned to
    /// this object (avoiding another layer of indirection when looking up an
    /// address). However, if an instance of this class allocated its own space
    /// in the default constructor, or if it was given a static buffer, a block
    /// entry will exist here that is not associated with any seglet.
    //
    /// The order in the array represents the order in which blocks logically
    /// appear in the segment. That is, segletBlocks[0] will cover byte offset 0
    /// through segletSize - 1.
    vector<void*> segletBlocks;

    /// Indicates whether or not this segment may ever be appended to again.
    /// Closing a segment is a permanent operation. Once closed, all future
    /// appends will fail.
    bool closed;

    /// In the case that the default constructor was used and this class
    /// allocated heap space, this will be set to true indicating that the
    /// destructor must free that space.
    bool mustFreeBlocks;

    /// Offset to the next free byte in Segment.
    uint32_t head;

    /// Latest Segment checksum (crc32c). This is a checksum of all metadata
    /// in the Segment (that is, every Segment::Entry and ::Header).
    /// Any user data that is stored in the Segment is unprotected. Integrity
    /// is their responsibility. Used to generate SegmentCertificates.
    Crc32C checksum;

    friend class SegmentIterator;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
