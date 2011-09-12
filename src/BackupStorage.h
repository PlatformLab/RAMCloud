/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_BACKUPSTORAGE_H
#define RAMCLOUD_BACKUPSTORAGE_H

#include <aio.h>

#include <boost/pool/pool.hpp>
#include <boost/dynamic_bitset.hpp>

#include "Segment.h"

namespace RAMCloud {

/**
 * Used for selecting various backup placement strategies at runtime.
 * These are primarily used for testing weird segment placement
 * and balancing ideas.
 */
enum BackupStrategy {
    RANDOM_REFINE_MIN,
    RANDOM_REFINE_AVG,
    EVEN_DISTRIBUTION,
    UNIFORM_RANDOM,
};

struct BackupStorageException : public Exception {
    explicit BackupStorageException(const CodeLocation& where)
        : Exception(where) {}
    BackupStorageException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    BackupStorageException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    BackupStorageException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

struct SegmentAllocator
{
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static char*
    malloc(const size_type bytes)
    {
        return reinterpret_cast<char *>(xmemalign(Segment::SEGMENT_SIZE,
                                                  bytes));
    }

    static void
    free(char* const block)
    {
        std::free(block);
    }
};

/**
 * The base class for all storage backends for backup.  This includes
 * SingleFileStorage for storing and recovering from disk and
 * InMemoryStorage for storing and recovering from RAM.
 */
class BackupStorage {
  public:
    virtual ~BackupStorage()
    {
    }

    virtual pair<uint32_t, uint32_t> benchmark(BackupStrategy backupStrategy);

    /**
     * An opaque handle used to access a stored segment.  All concrete
     * implementations of BackupStorage will subclass this to contain the
     * state needed to access storage for a particular segment.
     */
    class Handle
    {
      public:
        virtual ~Handle()
        {
            allocatedHandlesCount--;
        }

        static int32_t getAllocatedHandlesCount()
        {
            return allocatedHandlesCount;
        }

        /**
         * Return the current value of allocatedHandlesCount and zero it.
         */
        static int32_t resetAllocatedHandlesCount()
        {
            int32_t old = allocatedHandlesCount;
            allocatedHandlesCount = 0;
            return old;
        }

      protected:
        Handle()
        {
            allocatedHandlesCount++;
        }

      PRIVATE:
        static int32_t allocatedHandlesCount;

      DISALLOW_COPY_AND_ASSIGN(Handle);
    };

    /// See #storageType.
    enum class Type { UNKNOWN = 0, MEMORY = 1, DISK = 2 };

    /**
     * Set aside storage for a specific segment and give a handle back
     * for working with that storage.
     *
     * \param masterId
     *      The id of the master the segment to be stored belongs to.
     * \param segmentId
     *      The id of the segment to be stored.
     */
    virtual Handle* allocate(uint64_t masterId, uint64_t segmentId) = 0;

    /**
     * Release the storage for a segment.  The freed segment's data will
     * not appear during recovery.
     *
     * \param handle
     *      Handle for the segment storage to release for reuse.
     *      IMPORTANT: handle is no longer valid after this call.  The caller
     *      must take care not to reuse it.
     */
    virtual void free(Handle* handle) = 0;

    /**
     * Fetch a segment from its reserved storage (see allocate() and
     * putSegment()). Implementations of this method should be thread
     * safe since multiple simultaneous getSegment() calls are allowed.
     *
     * \param handle
     *      A Handle that was returned from this->allocate().
     * \param segment
     *      The start of a contiguous region of memory containing the
     *      segment to be fetched.
     */
    virtual void
    getSegment(const BackupStorage::Handle* handle, char* segment) const = 0;

    /// Return the segmentSize this storage backend operates on.
    uint32_t getSegmentSize() const { return segmentSize; }

    /**
     * Store an entire segment in its reserved storage (see allocate()).
     * Implementations of this method should be thread safe since multiple
     * simultaneous putSegment() calls are allowed.
     *
     * \param handle
     *      A Handle that was returned from this->allocate().
     * \param segment
     *      The start of a contiguous region of memory containing the
     *      segment to be stored.
     */
    virtual void
    putSegment(const Handle* handle, const char* segment) const = 0;

  protected:
    /**
     * Specify the segment size this BackupStorage will operate on.  Used
     * only by the implementers of the BackupStorage interface.
     *
     * \param segmentSize
     *      The segment size this BackupStorage operates on.
     * \param storageType
     *      The storage type corresponding with the concrete implementation of
     *      this class.
     */
    explicit BackupStorage(uint32_t segmentSize, Type storageType)
        : segmentSize(segmentSize)
        , storageType(storageType)
    {
    }

    /// The segment size this BackupStorage operates on.
    uint32_t const segmentSize;

  public:
    /// Used in Metrics to print out the backup storage type.
    const Type storageType;

    DISALLOW_COPY_AND_ASSIGN(BackupStorage);
};

/**
 * A BackupStorage backend which treats a file or disk device as a single
 * array of bytes, storing segments at each multiple of the size of a segment.
 */
class SingleFileStorage : public BackupStorage {
  public:
    /**
     * An opaque handle users of SingleFileStorage must use to access a
     * stored segment.
     *
     * For this implementation is just contains the segmentFrame.
     */
    class Handle : public BackupStorage::Handle {
      public:
        explicit Handle(uint32_t segmentFrame)
            : segmentFrame(segmentFrame)
        {
        }

        ~Handle()
        {
        }

        uint32_t getSegmentFrame() const
        {
            return segmentFrame;
        }

      PRIVATE:
        uint32_t segmentFrame;

      DISALLOW_COPY_AND_ASSIGN(Handle);
    };

    SingleFileStorage(uint32_t segmentSize,
                      uint32_t segmentFrames,
                      const char* filePath,
                      int openFlags = 0);
    virtual ~SingleFileStorage();
    virtual BackupStorage::Handle* allocate(uint64_t masterId,
                                            uint64_t segmentId);
    virtual pair<uint32_t, uint32_t> benchmark(BackupStrategy backupStrategy);
    virtual void free(BackupStorage::Handle* handle);
    virtual void
    getSegment(const BackupStorage::Handle* handle,
               char* segment) const;
    virtual void putSegment(const BackupStorage::Handle* handle,
                            const char* segment) const;

  PRIVATE:
    uint64_t offsetOfSegmentFrame(uint32_t segmentFrame) const;
    void reserveSpace();

    /// Type of the freeMap.  A bitmap.
    typedef boost::dynamic_bitset<> FreeMap;
    /// Keeps a bit set for each segmentFrame indicating if it is free.
    FreeMap freeMap;

    /// The file descriptor of the storage file.
    int fd;

    /**
     * A short segment aligned buffer used for mutilating segment frame
     * headers on disk.
     */
    void* killMessage;

    /// The length killMessage in bytes.
    uint32_t killMessageLen;

    /**
     * Track the last used segment frame so they can be used in FIFO.
     * This gives recovery dump tools a much better chance at recovering
     * data since old data is destroyed from disk first rather than new.
     */
    FreeMap::size_type lastAllocatedFrame;

    /// The number of segments this storage can store simultaneously.
    const uint32_t segmentFrames;

    friend class SingleFileStorageTest;
    DISALLOW_COPY_AND_ASSIGN(SingleFileStorage);
};

/**
 * A BackupStorage backend which uses an in-memory pool of chunks in the size
 * of segments.
 */
class InMemoryStorage : public BackupStorage {
  public:
    /**
     * An opaque handle users of InMemoryStorage must use to access a
     * stored segment.
     *
     * For this implementation is just contains the address the segment
     * is backed up at in RAM.
     */
    class Handle : public BackupStorage::Handle {
      PRIVATE:
        /// A pool of segmentSize chunks used to store segments.
        typedef boost::pool<SegmentAllocator> Pool;

      public:
        /// Create a Handle to segment storage at address.
        explicit Handle(char* address)
            : address(address)
        {
        }

        ~Handle()
        {
        }

        /// See Handle.
        char* getAddress() const
        {
            return address;
        }

      PRIVATE:
        /// See Handle.
        char* address;

      DISALLOW_COPY_AND_ASSIGN(Handle);
    };

    InMemoryStorage(uint32_t segmentSize,
                    uint32_t segmentFrames);
    virtual ~InMemoryStorage();
    virtual BackupStorage::Handle* allocate(uint64_t masterId,
                                            uint64_t segmentId);
    virtual void free(BackupStorage::Handle* handle);
    virtual void
    getSegment(const BackupStorage::Handle* handle,
               char* segment) const;
    virtual void putSegment(const BackupStorage::Handle* handle,
                            const char* segment) const;

  PRIVATE:
    typedef boost::pool<SegmentAllocator> Pool;
    /// A pool of segmentSize chunks used to store segments.
    Pool pool;

    /**
     * The number of free segmentFrames to accept storage to until
     * no more storage allocations are allowed.
     */
    uint32_t segmentFrames;

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorage);
};

} // namespace RAMCloud

#endif
