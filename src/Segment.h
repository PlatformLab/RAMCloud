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

#include "BackupManager.h"
#include "Common.h"
#include "Crc32C.h"
#include "LogTypes.h"

namespace RAMCloud {

/// The class used to calculate segment checksums.
typedef Crc32C SegmentChecksum;

struct SegmentEntry {
    LogEntryType type;
    uint32_t     length;
} __attribute__((__packed__));
static_assert(sizeof(SegmentEntry) == 8,
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

// forward decl
class Log;

/**
 * SegmentEntryHandle is a simple wrapper around a const void*. This is
 * used to refer to an entry written into a Segment. It has a few useful
 * helper methods that access the preceding SegmentEntry structure to
 * extract the type and length information.
 */
class SegmentEntryHandle {
  public:
    /**
     * Construct a SegmentEntryHandle that does not point to a
     * valid entry.
     */
    SegmentEntryHandle() : p(NULL) {}

    /**
     * Construct a SegmentEntryHandle that points to data in the
     * Segment at ``p''.
     *
     * \param[in] p
     *      Pointer to the user-supplied data written into the
     *      Segment.
     */
    explicit SegmentEntryHandle(const void* p)
        : p(p)
    {
        static_assert(sizeof(*this) == sizeof(p),
            "SegmentEntryHandle != sizeof(void*)");
    }

    /**
     * Return a pointer to the user data that this handle refers to.
     */
    const void*
    pointer()
    {
        return p;
    }

    /**
     * Return the length of the user data referred to by this handle.
     * This does #not include any Segment overheads.
     */
    uint32_t
    length()
    {
        return getSegmentEntry()->length;
    }

    /**
     * Return the type of the data written. This is the value that was
     * passed to the Segment::append() method.
     */
    uint32_t
    type()
    {
        return getSegmentEntry()->type;
    }

    /**
     * Return true if the handle is valid, else false.
     */
    bool
    isValid()
    {
        return p != NULL;
    }

  private:
    /*
     * Since ``p'' points to the user data, we can subtract off it to
     * access the preceding SegmentEntry structure.
     */
    const SegmentEntry*
    getSegmentEntry()
    {
        return reinterpret_cast<const SegmentEntry*>(
            reinterpret_cast<const uint8_t*>(p) - sizeof(SegmentEntry));
    }

    const void* p;
};

class Segment {
  public:
    /// The class used to calculate segment checksums.
    typedef SegmentChecksum Checksum;

    Segment(Log *log, uint64_t segmentId, void *baseAddress,
            uint32_t capacity, BackupManager* backup = NULL);
    Segment(uint64_t logId, uint64_t segmentId, void *baseAddress,
            uint32_t capacity, BackupManager* backup = NULL);
    ~Segment();

    SegmentEntryHandle append(LogEntryType type, const void *buffer,
                              uint32_t length,
                              uint64_t *lengthInSegment = NULL,
                              uint64_t *offsetInSegment = NULL,
                              bool sync = true);
    void               free(SegmentEntryHandle entry);
    void               close(bool sync = true);
    void               sync();
    const void        *getBaseAddress() const;
    uint64_t           getId() const;
    uint64_t           getCapacity() const;
    uint64_t           appendableBytes() const;
    int                getUtilisation() const;

    static const uint32_t  SEGMENT_SIZE = 8 * 1024 * 1024;
    static const uint64_t  INVALID_SEGMENT_ID = ~(0ull);

  private:
    void             commonConstructor();
    const void      *forceAppendBlob(const void *buffer, uint32_t length,
                                     bool updateChecksum = true);
    const void      *forceAppendWithEntry(LogEntryType type,
                                          const void *buffer, uint32_t length,
                                          uint64_t *lengthOfAppend = NULL,
                                          bool sync = true);

    BackupManager   *backup;         // makes operations on this segment durable
    void            *baseAddress;    // base address for the Segment
    Log             *const log;      // optional pointer to Log (for stats)
    uint64_t         logId;          // log this belongs to, passed to backups
    uint64_t         id;             // segment identification number
    const uint32_t   capacity;       // total byte length of segment when empty
    uint64_t         tail;           // offset to the next free byte in Segment
    uint64_t         bytesFreed;     // bytes free()'d in this Segment
    Checksum         checksum;       // Latest Segment checksum (crc32c)
    bool             closed;         // when true, no appends permitted

    /**
     * A handle to the open segment on backups,
     * or NULL if the segment is closed.
     */
    BackupManager::OpenSegment* backupSegment;

    friend class SegmentTest;
    friend class SegmentIteratorTest;
    friend class LogTest;

    DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !RAMCLOUD_SEGMENT_H
