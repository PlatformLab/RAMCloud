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

#ifndef RAMCLOUD_SEGMENT_H
#define RAMCLOUD_SEGMENT_H

#include <vector>

#include "BackupManager.h"
#include "BitOps.h"
#include "BoostIntrusive.h"
#include "Common.h"
#include "Crc32C.h"
#include "LogTypes.h"
#include "Object.h"
#include "SpinLock.h"

namespace RAMCloud {

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
    uint64_t logId;
    uint64_t segmentId;
    uint32_t segmentCapacity;
} __attribute__((__packed__));
static_assert(sizeof(SegmentHeader) == 20,
              "SegmentHeader has unexpected padding");

struct SegmentFooter {
    SegmentChecksum::ResultType checksum;
} __attribute__((__packed__));
static_assert(sizeof(SegmentFooter) == sizeof(SegmentChecksum::ResultType),
              "SegmentFooter has unexpected padding");

typedef void (*SegmentEntryCallback)(LogEntryType, const void *,
                                     uint64_t, void *);

/**
 * An exception that is thrown when the Segment class is provided invalid
 * method arguments or mutating operations are attempted on a closed Segment.
 */
struct SegmentException : public Exception {
    explicit SegmentException(const CodeLocation& where)
        : Exception(where) {}
    SegmentException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    SegmentException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    SegmentException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

// forward decls
class Log;
class Segment;
class _SegmentEntryHandle;

/// SegmentEntryHandle structs should never exist. The class should only be
/// used as a const pointer to a SegmentEntry.
typedef const _SegmentEntryHandle* SegmentEntryHandle;

/// Vector of Segment pointers.
typedef std::vector<Segment*> SegmentVector;

class SegmentMultiAppendEntry {
  public:
    SegmentMultiAppendEntry(LogEntryType type,
                            const void* buffer,
                            uint32_t length,
                            Tub<SegmentChecksum::ResultType> expectedChecksum =
                                Tub<SegmentChecksum::ResultType>())
        : type(type),
          buffer(buffer),
          length(length),
          expectedChecksum(expectedChecksum)
    {
    }

    SegmentMultiAppendEntry(const SegmentMultiAppendEntry& other)
        : type(other.type),
          buffer(other.buffer),
          length(other.length),
          expectedChecksum(other.expectedChecksum)
    {
    }

    SegmentMultiAppendEntry&
    operator=(const RAMCloud::SegmentMultiAppendEntry& other)
    {
        type = other.type;
        buffer = other.buffer;
        length = other.length;
        expectedChecksum = other.expectedChecksum;
        return *this;
    }

    LogEntryType type;
    const void* buffer;
    uint32_t length;
    Tub<SegmentChecksum::ResultType> expectedChecksum;
};

/// XXX
/// These are actually used in the write fast path. We should probably
/// use a different allocator (or stack allocated memory) to avoid having
/// the vectors call new and delete each time we append.
typedef std::vector<SegmentMultiAppendEntry> SegmentMultiAppendVector;
typedef std::vector<SegmentEntryHandle> SegmentEntryHandleVector;

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
    /// The class used to calculate segment checksums.
    typedef SegmentChecksum Checksum;

    Segment(Log *log, uint64_t segmentId, void *baseAddress,
            uint32_t capacity, BackupManager* backup, LogEntryType type,
            const void *buffer, uint32_t length);
    Segment(uint64_t logId, uint64_t segmentId, void *baseAddress,
            uint32_t capacity, BackupManager* backup = NULL);
    ~Segment();

    SegmentEntryHandle append(LogEntryType type,
                             const void *buffer,
                             uint32_t length,
                             bool sync = true,
                             Tub<SegmentChecksum::ResultType> expectedChecksum =
                                Tub<SegmentChecksum::ResultType>());
    SegmentEntryHandle append(SegmentEntryHandle handle,
                              bool sync);
    SegmentEntryHandleVector
                       multiAppend(SegmentMultiAppendVector& appends,
                                   bool sync = true);
    void               rollBack(SegmentEntryHandle handle);
    void               free(SegmentEntryHandle entry);
    void               setImplicitlyFreedCounts(uint32_t freeByteSum,
                                                uint64_t freeSpaceTimeSum);
    void               close(bool sync = true);
    void               sync();
    const void        *getBaseAddress() const;
    uint64_t           getId() const;
    uint32_t           getCapacity() const;
    uint32_t           appendableBytes();
    int                getUtilisation();
    uint32_t           getLiveBytes();
    uint32_t           getFreeBytes();
    uint64_t           getAverageTimestamp();

    /**
     * Given a pointer anywhere into a Segment's backing memory, obtain the base
     * address (the first byte) of that Segment.
     */
    static const void*
    getSegmentBaseAddress(const void* p, uint32_t capacity)
    {
        assert(BitOps::isPowerOfTwo(capacity));
        // Segments are always capacity aligned, which must be a power of two.
        uintptr_t addr = reinterpret_cast<uintptr_t>(p);
        uintptr_t wideCapacity = capacity;
        return reinterpret_cast<const void*>(addr & ~(wideCapacity - 1));
    }

    /**
     * Given a number of entries, the total number of bytes they'd consume,
     * the largest possible entry, and the size of segments in the system,
     * compute the maximum number of segments needed to store all of the data.
     */
    static size_t
    maximumSegmentsNeededForEntries(uint64_t numberOfEntries,
                                    uint64_t totalBytesInEntries,
                                    uint32_t maxBytesPerEntry,
                                    uint32_t segmentCapacity)
    {
        uint32_t overHead = sizeof(SegmentEntry) * 2 +
                            sizeof(SegmentHeader) +
                            sizeof(SegmentFooter);

        assert(overHead < segmentCapacity);
        assert(maxBytesPerEntry < segmentCapacity);

        uint32_t usableCapacity = segmentCapacity - overHead;

        // The worst case is that we want to write the largest possible entry
        // to the end of each Segment and miss being able to fit it by just one
        // byte.
        //
        // Note that this does overestimate in some cases (e.g. the base case
        // where we could fit all data in a single Segment), but only by at
        // most one Segment.
        uint32_t largestPossibleEntry = downCast<uint32_t>(
            maxBytesPerEntry + sizeof(SegmentEntry));
        uint32_t worstCaseSegment = usableCapacity - largestPossibleEntry + 1;

        uint64_t bytesNeeded = numberOfEntries * sizeof(SegmentEntry) +
            totalBytesInEntries;

        size_t segmentsNeeded = (bytesNeeded + worstCaseSegment - 1) /
            worstCaseSegment;

        return segmentsNeeded;
    }

    /**
     * Given a Segment capacity, compute the largest append that would
     * ever be possible on it.
     */
    static uint32_t
    maximumAppendableBytes(uint32_t segmentCapacity)
    {
        uint32_t overHead = downCast<uint32_t>((sizeof(SegmentEntry) * 3) +
            sizeof(SegmentHeader) + sizeof(SegmentFooter));

        if (segmentCapacity > overHead)
            return segmentCapacity - overHead;
        else
            return 0;
    }

#ifdef VALGRIND
    // can't use more than 1M, see http://bugs.kde.org/show_bug.cgi?id=203877
    static const uint32_t  SEGMENT_SIZE = 1024 * 1024;
#else
    static const uint32_t  SEGMENT_SIZE = 8 * 1024 * 1024;
#endif
    static const uint64_t  INVALID_SEGMENT_ID = ~(0ull);

  PRIVATE:
    void               commonConstructor(LogEntryType type,
                                         const void *buffer, uint32_t length);
    SegmentEntryHandle locklessAppend(LogEntryType type,
                            const void *buffer, uint32_t length, bool sync,
                            Tub<SegmentChecksum::ResultType> expectedChecksum);
    uint32_t           locklessGetLiveBytes() const;
    uint32_t           locklessAppendableBytes() const;
    bool               locklessCanAppendEntries(size_t numberOfEntries,
                                           size_t numberOfBytesInEntries) const;
    void               locklessSync();
    void               incrementSpaceTimeSum(SegmentEntryHandle handle);
    void               decrementSpaceTimeSum(SegmentEntryHandle handle);
    void               adjustSpaceTimeSum(SegmentEntryHandle handle,
                                          bool subtract);
    const void        *forceAppendBlob(const void *buffer,
                                       uint32_t length);
    SegmentEntryHandle forceAppendWithEntry(LogEntryType type,
                             const void *buffer,
                             uint32_t length,
                             bool sync = true,
                             bool updateChecksum = true,
                             Tub<SegmentChecksum::ResultType> expectedChecksum =
                                 Tub<SegmentChecksum::ResultType>());

    /// BackupManager used to replicate this Segment. This is responsible for
    /// making operations on this Segment durable.
    BackupManager    *backup;

    /// Base address for the Segment. The base address must be aligned to at
    /// least the size of the Segment.
    void             *baseAddress;

    /// Optional pointer to Log. This is needed to obtain callback information
    /// on SegmentEntry types in order to update usage counters.
    Log              *const log;

    /// ID of the Log this Segment belongs to. This is written to the header of
    /// the Segment and passed down into the BackupManager.
    const uint64_t    logId;

    /// Segment identification number. This uniquely identifies this Segment in
    /// the Log. Segment IDs are never recycled.
    const uint64_t    id;

    /// Total byte length of segment when empty.
    const uint32_t    capacity;

    /// Offset to the next free byte in Segment.
    uint32_t          tail;

    /// Bytes freed in this Segment by invocation of the #free method on a
    /// particular entry handle.
    uint32_t          bytesExplicitlyFreed;

    /// Bytes freed in this Segment by discovery in the LogCleaner. When
    /// types are registered with the Log, entries of that type are designed
    /// either explicitly freed or not. For explicitly freed entries, the user
    /// of the Log must call #free when an entry is no longer needed.
    //
    /// Implicitly freed entries are used for things like tombstones, where the
    /// user doesn't want to keep track of their liveness state. Instead, the
    /// LogCleaner periodcally scans and discovers if these entries are free
    /// using the liveness callback. Since no state is kept describing which
    /// entries we know to be free or not from previous passes, the cleaner
    /// recomputes this value each time, rather than simply incrementing it.
    uint32_t          bytesImplicitlyFreed;

    /// Sum of live datas' space-time products. I.e., for each entry multiply
    /// its timestamp by its size in bytes and add to this counter. This is
    /// used to compute an average age per byte in the Segment, which in turn
    /// is used for the cleaner's cost-benefit analysis.
    uint64_t          spaceTimeSum;

    /// Sum of implicitly freed datas' space-time products. When the LogCleaner
    /// updates the byteImplicitlyFreed count, it also supplies the timestamp
    /// sums from the entries freed and this value is computed. Thus the actual
    /// space-time sum at any point is really:
    ///     spaceTimeSum - implicitlyFreedTimestampSum
    uint64_t          implicitlyFreedSpaceTimeSum;

    /// Latest Segment checksum (crc32c). This is a checksum of all individual
    /// entries' checksums.
    Checksum          checksum;

    /// Checksum as of the penultimate append. This is kept to enable the
    /// rollBack feature.
    Checksum          prevChecksum;

    /// We may only roll back the last entry written under special
    /// circumstances. This acts as an interlock to catch invalid rollbacks.
    bool              canRollBack;

    // When true, no appends are permitted (the Segment is immutable).
    bool              closed;

    /// Lock to protect against concurrent access. XXX- This should go away.
    SpinLock          mutex;

    /// The number of each type of entry present in the Segment. Note that this
    /// counts all entries, both dead and live.
    uint32_t          entryCountsByType[256];

    /*
     * The following fields are only used externally by the Log class;
     * the Segment code does not touch them.
     */

    /// List pointer for Log code to track the state of this Segment (e.g. is
    /// it live, free, cleaned but waiting to be freed, etc).
    IntrusiveListHook listEntries;

    /// The Epoch in which this Segment was cleaned. Used to determine when it
    /// is safe to return the memory backing this Segment to the free list.
    uint64_t          cleanedEpoch;

    /**
     * A handle to the open segment on backups,
     * or NULL if the segment is closed.
     */
    ReplicatedSegment* backupSegment;

    friend class Log;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

/**
 * SegmentEntryHandle is used to refer to an entry written into a Segment.
 * It has a few useful helper methods that access the SegmentEntry structure
 * to extract the type and length information.
 *
 * This is always used by the pointer typedef. There are never actually any
 * SegmentEntryHandles extant in the system. It's simply an accessor that
 * points to a SegmentEntry.
 */
class _SegmentEntryHandle {
  public:
    /**
     * Construct a SegmentEntryHandle that does not point to a
     * valid entry.
     */
    _SegmentEntryHandle()
    {
        throw Exception(HERE, "_SegmentEntryHandles don't really exist!");
    }

    /**
     * Return a pointer to the user data that this handle refers to.
     */
    const void*
    userData() const
    {
        const uint8_t* p = reinterpret_cast<const uint8_t*>(getSegmentEntry());
        return (p + sizeof(SegmentEntry));
    }

    template<typename T>
    const T*
    userData() const
    {
        return reinterpret_cast<const T*>(userData());
    }

    /**
     * Return the length of the user data referred to by this handle.
     * This does #not include any Segment overheads.
     */
    uint32_t
    length() const
    {
        return getSegmentEntry()->length;
    }

    /**
     * Return the total length, including overhead, of this entry
     * in the Segment.
     */
    uint32_t
    totalLength() const
    {
        return length() + downCast<uint32_t>(sizeof(SegmentEntry));
    }

    /**
     * Return the type of the data written. This is the value that was
     * passed to the Segment::append() method.
     */
    LogEntryType
    type() const
    {
        return static_cast<LogEntryType>(getSegmentEntry()->type);
    }

    /**
     * Return the checksum currently stored in memory for this SegmentEntry.
     * Note that if something is corrupt, then it may not be valid.
     */
    SegmentChecksum::ResultType
    checksum() const
    {
        return getSegmentEntry()->checksum ^ mutableFieldsChecksum();
    }

    /**
     * Calculate a checksum from the stored SegmentEntry. This is the checksum
     * of all non-mutable data in the entry. Note that it's not actually what's
     * stored in the entry. We store this checksum XORed with the mutableFields
     * checksum.
     *
     * When we check an entry's checksum, we do the following:
     *  1) Compute the mutableFields CRC
     *  2) XOR above with the stored checksum to get the immutable CRC
     *  3) Compute the CRC over the whole entry with checksum and
     *     mutableFields zeroed
     *  4) Compare #3 and #2
     */
    SegmentChecksum::ResultType
    generateChecksum() const
    {
        SegmentChecksum checksum;
        SegmentEntry temp = *getSegmentEntry();
        temp.checksum = 0;
        memset(&temp.mutableFields, 0, sizeof(temp.mutableFields));
        checksum.update(&temp, sizeof(SegmentEntry));
        checksum.update(userData(), length());
        return checksum.getResult();
    }

    /**
     * Calculate a checksum for this SegmentEntry and compare it against
     * the stored checksum. Returns true if they match, else false. 
     */
    bool
    isChecksumValid() const
    {
        return generateChecksum() == checksum();
    }

    /**
     * Return the LogTime assocated with this entry. To do so we need
     * to calculate the base address of the Segment. This lets us access
     * the SegmentHeader, which contains the SegmentId, as well as determine
     * our offset.
     */
    LogTime
    logTime() const
    {
        uintptr_t entryAddress = reinterpret_cast<uintptr_t>(this);
        uintptr_t segmentBase  = reinterpret_cast<uintptr_t>(
            Segment::getSegmentBaseAddress(reinterpret_cast<const void*>(this),
                                           segmentSize()));

        const _SegmentEntryHandle* headerHandle =
            reinterpret_cast<const _SegmentEntryHandle*>(segmentBase);

        if (headerHandle->type() != LOG_ENTRY_TYPE_SEGHEADER)
            throw Exception(HERE, "segment is corrupt or unaligned");

        const SegmentHeader* header = headerHandle->userData<SegmentHeader>();

        return LogTime(header->segmentId, entryAddress - segmentBase);
    }

    /**
     * Return the total length of the Segment this entry is a part of.
     * The length is always an even positive power of two less than or
     * equal to 2GB. Each SegmentEntry contains this value compressed
     * into 5 bits in order to make calculating the Segment's base address
     * possible (Segments are always aligned to their segment size).
     */
    uint32_t
    segmentSize() const
    {
        int exp = getSegmentEntry()->mutableFields.segmentCapacityExponent;
        return 1U << exp;
    }

    /**
     * Used by HashTable to get the first uint64_t key for supported
     * types.
     */
    uint64_t
    key1() const
    {
        if (type() == LOG_ENTRY_TYPE_OBJ) {
            return reinterpret_cast<const Object*>(
                userData())->id.tableId;
        } else if (type() == LOG_ENTRY_TYPE_OBJTOMB) {
            return reinterpret_cast<const ObjectTombstone*>(
                userData())->id.tableId;
        }
        throw Exception(HERE, "not of object or object tombstone types");
    }

    /**
     * Used by HashTable to get the second uint64_t key for supported
     * types.
     */
    uint64_t
    key2() const
    {
        if (type() == LOG_ENTRY_TYPE_OBJ) {
            return reinterpret_cast<const Object*>(
                userData())->id.objectId;
        } else if (type() == LOG_ENTRY_TYPE_OBJTOMB) {
            return reinterpret_cast<const ObjectTombstone*>(
                userData())->id.objectId;
        }
        throw Exception(HERE, "not of object or object tombstone types");
    }

  PRIVATE:
    /*/
     * ``this'' always points to a SegmentEntry structure, so return it.
     */
    const SegmentEntry*
    getSegmentEntry() const
    {
        if (this == NULL)
            throw Exception(HERE, "NULL SegmentEntryHandle dereference");
        return reinterpret_cast<const SegmentEntry*>(this);
    }

    /**
     * Return the checksum associated with this entry's mutable fields.
     *
     * We need to be able to remove the mutable fields portion of the checksum
     * so that we can modify it, if necessary, when moving an entry somewhere
     * else. The way we do this is by generating it independently and XORing
     * it into the checksum field with the full checksum (which covers
     * everything but the checksum and mutable fields).
     */
    uint32_t
    mutableFieldsChecksum() const
    {
        SegmentChecksum checksum;
        const SegmentEntry* entry = getSegmentEntry();
        checksum.update(&entry->mutableFields,
            sizeof(entry->mutableFields));
        return checksum.getResult();
    }
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
