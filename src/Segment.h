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

  public:
    /**
     * Indicates which porition of a segment contains valid data and
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
     * Absolutely no code outside of the Segment and SegmentIterator class
     * need to understand the internals and shouldn't attempt to use
     * certificates other than through the SegmentIterator or Segment code.
     */
    class Certificate {
      public:
        Certificate()
            : segmentLength()
            , checksum()
        {}
      PRIVATE:
        /// Bytes in the associated segment that #checksum covers. Determines
        /// how much of the segment should be checked for integrity and how
        /// much of a segment should be iterated over for SegmentIterator.
        uint32_t segmentLength;

        /// Checksum covering all metadata in the segment, including fields
        /// above in this struct.
        Crc32C::ResultType checksum;
        friend class Segment;
        friend class SegmentIterator;
    } __attribute__((__packed__));
    static_assert(sizeof(Certificate) == 8,
                  "Unexpected padding in Segment::Certificate");

    Segment();
    Segment(vector<Seglet*>& seglets, uint32_t segletSize);
    Segment(const void* buffer, uint32_t length);
    virtual ~Segment();
    bool append(LogEntryType type,
                const void* data,
                uint32_t length,
                uint32_t* outOffset = NULL);
    bool append(LogEntryType type,
                Buffer& buffer,
                uint32_t* outOffset = NULL);
    void close();
    void disableAppends();
    bool enableAppends();
    uint32_t appendToBuffer(Buffer& buffer,
                            uint32_t offset,
                            uint32_t length) const;
    uint32_t appendToBuffer(Buffer& buffer);
    LogEntryType getEntry(uint32_t offset, Buffer& buffer);
    uint32_t getAppendedLength() const;
    uint32_t getAppendedLength(Certificate& certificate) const;
    uint32_t getSegletsAllocated();
    uint32_t getSegletsInUse();
    bool freeUnusedSeglets(uint32_t count);
    bool checkMetadataIntegrity(const Certificate& certificate);
    uint32_t copyOut(uint32_t offset, void* buffer, uint32_t length) const;

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
        if (__builtin_expect(offset >= (segletSize * segletBlocks.size()), 0))
            return 0;

        uint32_t segletOffset = offset;
        uint32_t segletIndex = 0;

        // If we have more than one seglet, then they must all be the same size
        // and a power of two, so use bit ops rather than division and modulo to
        // save time. This method can be hot enough that this makes a big
        // difference.
        if (__builtin_expect(segletSizeShift != 0, 1)) {
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
    const EntryHeader* getEntryHeader(uint32_t offset);
    void getEntryInfo(uint32_t offset,
                      LogEntryType& outType,
                      uint32_t &outDataOffset,
                      uint32_t &outDataLength);
    uint32_t bytesLeft();
    uint32_t bytesNeeded(uint32_t length);
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
    /// more space. Closing a segment is a permanent operation.
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
    /// is their responsibility. Used to generate Segment::Certificates.
    Crc32C checksum;

    friend class SegmentIterator;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
