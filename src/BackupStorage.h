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

#include <unistd.h>

#include <boost/pool/pool.hpp>
#include <boost/dynamic_bitset.hpp>

#include "Memory.h"
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
        return reinterpret_cast<char *>(Memory::xmemalign(HERE,
                                                          Segment::SEGMENT_SIZE,
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
  PUBLIC:
    /**
     * Format for persisent metadata.  Includes the serverId of the backup that
     * uses this storage along with the name of the cluster to which this
     * backup belongs.
     */
    struct Superblock {
        /**
         * Distinguishes redundant copies of the superblock from one another
         * on storage so that only the most recent is used.
         */
        uint64_t version;

        /**
         * Server id of the backup which owns the replicas in the storage.
         * Before enlistment,  this contains the former server id that operated
         * using this storage.  After enlistment, this contains the server id
         * which is currently storing data in this storage.
         */
        uint64_t serverId;

        /**
         * Controls the reuse of replicas stored on this backup.  'Tags'
         * replicas created on this backup with this cluster name.  This has
         * two effects.  First, any replicas found in storage are discarded
         * unless they are tagged with an identical cluster name. Second, any
         * replicas created by the backup process will only be reused by future
         * backup processes if the cluster name on the stored replica matches
         * the cluster name of future process. The name '__unnamed__' is
         * special and never matches any cluster name (even itself), so it
         * guarantees all stored replicas are discarded on start and that all
         * replicas created by this process are discarded by future backups.
         * This is convenient for testing.
         */
        char clusterName[256];

        /**
         * Create a default superblock image; used in the case none is found
         * on storage.
         */
        Superblock()
            : version(0)
            , serverId(ServerId().getId())
            , clusterName()
        {
            const char* unnamed = "__unnamed__";
            assert(strlen(unnamed) < sizeof(clusterName) - 1);
            strncpy(clusterName, unnamed, sizeof(clusterName) - 1);
        }

        /**
         * Create a superblock image.
         */
        Superblock(uint64_t version, ServerId serverId, const char* name)
            : version(version)
            , serverId(serverId.getId())
        {
            assert(strlen(clusterName) < sizeof(clusterName) - 1);
            strncpy(clusterName, name, sizeof(clusterName) - 1);
        }

        /// Return '\0' terminated cluster name in this superblock.
        const char* getClusterName() { return clusterName; }

        /// Return the server id in this superblock.
        ServerId getServerId() { return ServerId(serverId); }
    } __attribute__((packed));

    virtual void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0) = 0;
    virtual Superblock loadSuperblock() = 0;

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
      PUBLIC:
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

      PROTECTED:
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
     */
    virtual Handle* allocate() = 0;
    virtual Handle* associate(uint32_t frame) = 0;

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
     * Fetch the starting and ending bytes from each segment frame.
     * Since segments align their header and footer entries to the
     * beginning and end this method can be used to fetch all the
     * locations where the entries would reside (note returned locations
     * don't necessarily contain valid segment data).
     *
     * Some storage implemenations may not support this operation in which
     * case they simply return NULL indicating they cannot or cannot reliably
     * retrieve the headers and footers.
     *
     * \param headerSize
     *      Bytes from the beginning of each segment frame to return; the
     *      starting \a headerSize bytes from each segment frame start at
     *      f * (headerSize + footerSize) for f = 0 to #segmentFrames - 1
     *      in the returned result.
     * \param footerSize
     *      Bytes from the end of each segment frame to return; the
     *      ending \a footerSize bytes from each segment frame start at
     *      f * (headerSize + footerSize) + headerSize for f = 0 to
     *      #segmentFrames - 1 in the returned result.
     * \return
     *      An array of bytes of back-to-back entries of alternating
     *      size (first \a headerSize, then \a footerSize, repeated
     *      #segmentFrames times).  NOTE: the caller is responsible
     *      for deleting the value with delete[].
     */
    virtual std::unique_ptr<char[]>
    getAllHeadersAndFooters(size_t headerSize, size_t footerSize) = 0;

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

  PROTECTED:
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
    uint32_t segmentSize;

  PUBLIC:
    /// Used in RawMetrics to print out the backup storage type.
    const Type storageType;

    DISALLOW_COPY_AND_ASSIGN(BackupStorage);
};

/**
 * A BackupStorage backend which treats a file or disk device as a single
 * array of bytes, storing segments at each multiple of the size of a segment.
 */
class SingleFileStorage : public BackupStorage {
  public:
    enum { BLOCK_SIZE = 512 };
    static_assert(sizeof(Superblock) < BLOCK_SIZE,
                  "Superblock doesn't fit in a single disk block");

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
    virtual BackupStorage::Handle* allocate();
    virtual BackupStorage::Handle* associate(uint32_t frame);
    virtual pair<uint32_t, uint32_t> benchmark(BackupStrategy backupStrategy);
    virtual void free(BackupStorage::Handle* handle);
    virtual std::unique_ptr<char[]>
    getAllHeadersAndFooters(size_t headerSize, size_t footerSize);
    virtual void
    getSegment(const BackupStorage::Handle* handle,
               char* segment) const;
    virtual void putSegment(const BackupStorage::Handle* handle,
                            const char* segment) const;
    virtual void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0);
    virtual Superblock loadSuperblock();

  PRIVATE:
    uint64_t offsetOfSegmentFrame(uint32_t segmentFrame) const;
    uint64_t offsetOfSuperblockFrame(uint32_t index) const;
    void reserveSpace();
    Tub<Superblock> tryLoadSuperblock(uint32_t superblockFrame);

    Superblock superblock;

    /// Tracks which of the superblock frames was most recently written.
    uint32_t lastSuperblockFrame;

    /// Type of the freeMap.  A bitmap.
    typedef boost::dynamic_bitset<> FreeMap;
    /// Keeps a bit set for each segmentFrame indicating if it is free.
    FreeMap freeMap;

    /// Extra flags for use while opening filePath (e.g. O_DIRECT | O_SYNC).
    int openFlags;

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
    virtual BackupStorage::Handle* allocate();
    virtual BackupStorage::Handle* associate(uint32_t frame);
    virtual void free(BackupStorage::Handle* handle);
    virtual std::unique_ptr<char[]>
    getAllHeadersAndFooters(size_t headerSize, size_t footerSize);
    virtual void
    getSegment(const BackupStorage::Handle* handle,
               char* segment) const;
    virtual void putSegment(const BackupStorage::Handle* handle,
                            const char* segment) const;
    virtual void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0);
    virtual Superblock loadSuperblock();

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
