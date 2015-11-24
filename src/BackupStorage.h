/* Copyright (c) 2010-2015 Stanford University
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

#include <boost/dynamic_bitset.hpp>

#include "Buffer.h"
#include "Exception.h"
#include "ServerId.h"
#include "Tub.h"

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

/**
 * Thrown if there is any kind of problem with a storage operation. Mostly
 * on startup or during allocation of frames.
 */
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

/**
 * Subclasses store replica data (along with some higher-level provided
 * metadata) and allow them to be retrieved later. Subclasses may have
 * different durability properties. This includes SingleFileStorage for
 * storing and recovering from disk and InMemoryStorage for storing and
 * recovering from RAM.
 */
class BackupStorage {
  PUBLIC:
    /**
     * Format for persisent metadata. Includes the serverId of the backup that
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
         * Before enlistment, this contains the former server id that operated
         * using this storage. After enlistment, this contains the server id
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
            , clusterName()
        {
            assert(strlen(clusterName) < sizeof(clusterName) - 1);
            strncpy(clusterName, name, sizeof(clusterName) - 1);
        }

        /// Return null terminated cluster name in this superblock.
        const char* getClusterName() { return clusterName; }

        /// Return the server id in this superblock.
        ServerId getServerId() { return ServerId(serverId); }
    } __attribute__((packed));

    /**
     * Represents a region of the storage which holds a single replica and its
     * metadata. Frames manage the details of moving replica data to and from
     * storage and manage buffers for doing so as well.
     *
     * Backups open frames, append() data, and then close() them.
     * See BackupStorage::open() to allocate and open a Frame.
     * Backups call open() on their storage instance to get a reference to a
     * frame. References to frames are counted; when the count drops to zero
     * the frame will be made available for reuse by another replica.
     *
     * If a master crashes backups use load() to access the replica data for
     * recovery.
     *
     * All concrete implementations of BackupStorage will subclass this to
     * contain the state and logic needed to access storage for replicas.
     */
    class Frame {
      PUBLIC:
        /**
         * Returns true if append has been called on this frame during the life
         * of this process; returns false otherwise. This includes across
         * free()/open() cycles. This is used by the backup replica garbage
         * collector to determine whether it might have missed a BackupFree rpc
         * from a master, in which case it has to query the master for the
         * replica status. This is "appended to" rather than "opened by"
         * because of benchmark(); benchmark "opens" replicas and loads them
         * but doesn't do any. Masters open and append data in a single rpc, so
         * this is a fine proxy.
         */
        virtual bool wasAppendedToByCurrentProcess() = 0;

        /**
         * Reloads metadata from storage into memory; only used when
         * restarting a backup from storage. After this call returns
         * getMetadata() will return the metadata as found on storage up until
         * the first call to BackupStorage::open() on this frame. This can only
         * be safely called immediately after frame is constructed (NOT
         * opened), before any methods are called on it.
         */
        virtual void loadMetadata() = 0;

        /**
         * Return a pointer to the most recently appended metadata for this
         * frame. The first getMetadataSize() bytes of the returned pointers
         * are valid, though it may or may not contain valid or meaningful (or
         * even consistent with the replica) metadata.  Warning: concurrent
         * calls to append modify the metadata that the return of this method
         * points to. In practice it should only be called when the frame
         * isn't accepting appends (either it was just constructed or one of
         * close, load, or free has already been called on it). Used only
         * during backup restart and master recovery to extract details about
         * the replica in this frame without loading the frame.
         */
        virtual const void* getMetadata() = 0;

        /**
         * Start loading the replica in this frame from storage without
         * blocking. Use load() to wait for the load to complete and to
         * retreive a pointer to the loaded replica data. isLoaded() can be
         * used to poll for the completion of the load without blocking.
         *
         * After this call the start of new appends to this frame are rejected
         * until this frame is recycled for use with another replica (via
         * BackupStorage::open()).
         */
        virtual void startLoading() = 0;

        /**
         * Returns true if calling load() would not block. Always returns
         * false if startLoading() or load() hasn't been called.
         */
        virtual bool isLoaded() = 0;

        virtual void unload() = 0;

        /**
         * Return a pointer to the replica data for recovery. If needed, the
         * replica is loaded from storage into memory. If the replica is
         * already in memory a load from storage is avoided. If the replica
         * buffer is dirty this call blocks until all data has been flushed to
         * storage to ensure that recoveries only use durable data.
         *
         * After this call the start of new appends to this frame are rejected
         * until this frame is recycled for use with another replica (via
         * open()).
         */
        virtual void* load() = 0;

        /**
         * Append data to frame and update metadata.
         * Data will be written to storage using the policy selected on
         * BackupStorage::open():
         * Writes data and metadata to storage asynchronously if #sync is false.
         * Writes data and metadata to storage synchronously if #sync is true.
         *
         * Idempotence: the caller must guarantee duplicated calls provide
         * identical arguments.
         *
         * append() after a load() or a close() throws an exception to the
         * master performing the append since it is either an error by the
         * master or the master has crashed.
         *
         * \param source
         *      Buffer contained the data to be copied into the frame.
         * \param sourceOffset
         *      Offset into \a source where data should be copied from.
         * \param length
         *      Bytes to copy to the frame starting at \a sourceOffset in \a
         *      source.
         * \param destinationOffset
         *      Offset into the frame where the source data should be copied.
         * \param metadata
         *      Metadata which should be written to storage immediately after
         *      the data appended is written. May be NULL if there is no
         *      updated metadata to commit to storage along with this data.
         * \param metadataLength
         *      Bytes of metadata pointed to by \a metadata. Ignored if \a
         *      metadata is NULL.
         */
        virtual void append(Buffer& source,
                            size_t sourceOffset,
                            size_t length,
                            size_t destinationOffset,
                            const void* metadata,
                            size_t metadataLength) = 0;

        /**
         * Mark this frame as closed. Once all data has been flushed to storage
         * in-memory buffers for this frame will be released. For synchronous
         * mode the caller must guarantee that there are no ongoing calls to
         * append() for this frame. Close is idempotent. Calls to close after a
         * call to load() throw BackupBadSegmentIdException which should kill
         * the calling master; in this case recovery has already started for
         * them so they are likely already dead.
         */
        virtual void close() = 0;

        /**
         * This method is invoked on a frame that is currently closed and
         * stored on disk; it brings the frame back into memory (if it wasn't\
         * there already) and opens it so that it can be appended to like a
         * new frame. This method is used when a backup has crashed and
         * restarted, and a master tries to write to an old frame from the
         * backup's previous incarnation (e.g., the master was trying to
         * rereplicate a segment because of the crash of the earlier
         * incarnation, and  just happened to choose the restarted backup
         * for that replica; with this method, that situation is handled
         * cleanly). This method is part of the solution to RAM-573.
         *
         * \param length
         *      Number of bytes in the reclaimed replica that are valid.
         */
        virtual void reopen(size_t length) = 0;

        /**
         * Do not call this; only called; called via BackupStorage::freeFrame()
         * which is called when the reference count associated with a frame
         * drops to zero.
         * Make this frame available for reuse; data previously stored in this
         * frame may or may not be part of future recoveries. It does not
         * modify storage, only in-memory bookkeeping structures, so a
         * previously freed frame will not be free on restart until
         * higher-level backup code explicitly free them after it determines it
         * is not needed. May block until any currently ongoing IO operation
         * for the completes.
         */
        virtual void free() = 0;

        virtual ~Frame() {}
        Frame() {}

      DISALLOW_COPY_AND_ASSIGN(Frame);
    };

    virtual ~BackupStorage() {}

    virtual uint32_t benchmark(BackupStrategy backupStrategy);
    void sleepToThrottleWrites(size_t count, uint64_t ticks) const;

    /**
     * Reference to a frame; when the reference drops to zero
     * BackupStorage::freeFrame() is called to release the frame for reuse with
     * another replica.
     */
    typedef std::shared_ptr<Frame> FrameRef;
    static void freeFrame(Frame* frame);

    /**
     * Allocate a frame on storage, resetting its state to accept appends for a
     * new replica. Open is not synchronous itself. Even after return from
     * open() if this backup crashes it may find the replica which was formerly
     * stored in this frame or metadata for the former replica and data for the
     * newly open replica. Recovery is expected to address these consistency
     * issues with the checksums.
     *
     * This call is NOT idempotent since it allocates and return resources to
     * the caller. The caller must take care not to lose frames. Any returned
     * frame which is not freed may be leaked until the backup (or the creating
     * master crashes). For example, the BackupService will need to guarantee
     * that any returned frame is associated with a particular replica and that
     * future RPCs requesting the creation of that replica reuse the returned
     * frame.
     *
     * \param sync
     *      Only return from append() calls when all enqueued data and the most
     *      recently enqueued metadata are durable on storage.
     * \return
     *      Reference to a frame through which handles all IO for a single
     *      replica. Maintains a reference count; when destroyed if the
     *      reference count drops to zero the frame will be freed for reuse with
     *      another replica.
     */
    virtual FrameRef open(bool sync) = 0;

    /**
     * Returns the maximum number of bytes of metadata that can be stored
     * which each append(). Also, how many bytes of getMetadata() are safe
     * for access after getMetadata() calls, though returned data may or may
     * not contain valid or meaningful (or even consistent with the
     * replica) metadata.
     */
    virtual size_t getMetadataSize() = 0;

    /**
     * Marks ALL storage frames as allocated and blows away any in-memory
     * copies of metadata. This should only be performed at backup startup. The
     * caller is reponsible for freeing the frames if the metadata indicates
     * the replica data stored there isn't useful.
     *
     * \return
     *      Reference to every frame which has various uses depending on the
     *      metadata that is found in that frame. BackupService code is
     *      must take care to hold references to frames which contain valid
     *      replicas which it may use for recoveries and to destroy all
     *      references for frames that aren't needed in order to free space
     *      for storage of new replicas.
     */
    virtual std::vector<FrameRef> loadAllMetadata() = 0;

    /**
     * Overwrite the on-storage superblock with new information that future
     * backups reusing this storage will need (in the case of this backup's
     * demise).
     * This is done safely so that a failure in the middle of the update
     * will leave either the old superblock or the new.
     *
     * \param serverId
     *      The server id of the process as assigned by the coordinator.
     *      It is persisted for the benefit of future processes reusing this
     *      storage.
     * \param clusterName
     *      Controls the reuse of replicas stored on this backup. 'Tags'
     *      replicas created on this backup with this cluster name. This has
     *      two effects. First, any replicas found in storage are discarded
     *      unless they are tagged with an identical cluster name. Second, any
     *      replicas created by the backup process will only be reused by future
     *      backup processes if the cluster name on the stored replica matches
     *      the cluster name of future process. The name '__unnamed__' is
     *      special and never matches any cluster name (even itself), so it
     *      guarantees all stored replicas are discarded on start and that all
     *      replicas created by this process are discarded by future backups.
     *      This is convenient for testing.
     * \param frameSkipMask
     *      Used for testing. This storage keeps two superblock images and
     *      overwrites the older first then the newer in the case a failure
     *      occurs in the middle of writing. Setting frameSkipMask to 0x1
     *      skips writing the first superblock image, 0x2 skips the second,
     *      and 0x3 skips both.
     */
    virtual void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0) = 0;

    /**
     * Read on-storage superblock locations and return the most up-to-date
     * and complete superblock since the last resetSuperblock().
     *
     * \return
     *      The most up-to-date complete superblock found on storage. If no
     *      superblock can be found a default superblock is returned which
     *      indicates no prior backup instance left behind intelligible
     *      traces of life on storage.
     */
    virtual Superblock loadSuperblock() = 0;

    /**
     * Return only after all data appended to all Frames prior to this call
     * has been flushed to storage.
     */
    virtual void quiesce() = 0;

    /**
     * Scribble on all the metadata blocks of all the storage frames to prevent
     * what is already on disk from being reused in future runs.
     * Only safe immedately after this class is instantiated, before it is used
     * to allocate or perform operations on frames.
     * Called whenever the cluster name changes from what is stored in
     * the superblock to prevent replicas already on storage from getting
     * confused for ones written by the starting up backup process.
     */
    virtual void fry() = 0;

    /// See #storageType.
    enum class Type { UNKNOWN = 0, MEMORY = 1, DISK = 2 };

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
     * \param writeRateLimit
     *      Throttle writes to this storage medium to at most the given number
     *      of megabytes per second. A value of 0 turns off throttling.
     */
    BackupStorage(size_t segmentSize, Type storageType, size_t writeRateLimit);

    /// Maximum length in bytes of a replica.
    size_t segmentSize;

    /// If non-0, writes bandwidth to this storage medium should be limited to
    /// this many megabytes to second. Subclasses of BackupStorage should invoke
    /// sleepToThrottleWrites() after they've written to their storage to ensure
    /// that the limit is maintained.
    size_t writeRateLimit;

  PUBLIC:
    /// Used in RawMetrics to print out the backup storage type.
    const Type storageType;

    DISALLOW_COPY_AND_ASSIGN(BackupStorage);
};

} // namespace RAMCloud

#endif
