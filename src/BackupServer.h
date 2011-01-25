/* Copyright (c) 2009-2010 Stanford University
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

/**
 * \file
 * Declarations for the backup server, currently all backup RPC
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the RPCs.
 */

#ifndef RAMCLOUD_BACKUPSERVER_H
#define RAMCLOUD_BACKUPSERVER_H

#include <cstdatomic>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/pool/pool.hpp>
#include <map>

#include "Common.h"
#include "BackupClient.h"
#include "BackupStorage.h"
#include "CoordinatorClient.h"
#include "LogTypes.h"
#include "Rpc.h"
#include "Server.h"

namespace RAMCloud {

#if TESTING
ObjectTub<uint64_t> whichPartition(const LogEntryType type,
                                   const void* data,
                                   const ProtoBuf::Tablets& partitions);
#endif

/**
 * Handles Rpc requests from Masters and the Coordinator to persistently store
 * Segments and to facilitate the recovery of object data when Masters crash.
 */
class BackupServer : public Server {
  PRIVATE:

    typedef std::atomic_int AtomicInt;

    /**
     * Decrement the value referred to on the constructor on
     * destruction.
     *
     * \tparam T
     *      Type of the value that will be decremented when an
     *      instance of a class that is a an instance of this
     *      template gets destructed.
     */
    template <typename T>
    class ReferenceDecrementer {
      public:
        /**
         * \param value
         *      Reference to a value that should be decremented when
         *      #this is destroyed.
         */
        explicit ReferenceDecrementer(T& value)
            : value(value)
        {
        }

        /// Decrement the value referred to by #value.
        ~ReferenceDecrementer()
        {
            --value;
        }

      private:
        /// Reference to the value to be decremented on destruction of this.
        T& value;
    };

    /**
     * Mediates access to a memory chunk pool to maintain thread safety.
     * Detailed documentation for each of the methods can be found
     * as part of boost::pool<>.
     */
    class ThreadSafePool {
      public:
        /// The type of the in-memory segment size chunk pool.
        typedef boost::pool<SegmentAllocator> Pool;

        /// The type of lock used to make access to #pool thread safe.
        typedef boost::unique_lock<boost::mutex> Lock;

        explicit ThreadSafePool(uint32_t chunkSize)
            : allocatedChunks()
            , mutex()
            , pool(chunkSize)
        {
        }

        // See boost::pool<>.
        ~ThreadSafePool()
        {
            Lock _(mutex);
            if (allocatedChunks)
                LOG(WARNING, "Backup segment pool destroyed with %u chunks "
                             "still allocated", allocatedChunks);
        }

        // See boost::pool<>.
        void
        free(void* chunk)
        {
            Lock _(mutex);
            pool.free(chunk);
            allocatedChunks--;
        }

#if TESTING
        // See boost::pool<>.
        bool
        is_from(void* chunk)
        {
            Lock _(mutex);
            return pool.is_from(chunk);
        }
#endif

        // See boost::pool<>.
        void*
        malloc()
        {
            Lock _(mutex);
            void* r = pool.malloc();
            allocatedChunks++;
            return r;
        }

      private:
        /// Track the number of allocated chunks for stat keeping.
        uint32_t allocatedChunks;

        /// Used to serialize access to #pool and #allocatedChunks.
        boost::mutex mutex;

        /// The backing pool that manages memory chunks.
        Pool pool;
    };

    class LoadOp;
    class StoreOp;

    /**
     * Tracks all state associated with a single segment and manages
     * resources and storage associated with it.  Public calls are
     * protected by #mutex to provide thread safety.
     */
    class SegmentInfo {
      public:
        /// The type of locks used to lock #mutex.
        typedef boost::unique_lock<boost::mutex> Lock;

        /**
         * Tracks current state of the segment which is sufficient to
         * determine which operations are legal.
         */
        enum State {
            UNINIT,     ///< open() and delete are the only valid ops.
            /**
             * Storage is reserved but segment is mutable.
             * This segment will be closed if this is deleted.
             */
            OPEN,
            CLOSED,     ///< Immutable and has moved to stable store.
            RECOVERING, ///< Rec segs building, all other ops must wait.
            FREED,      ///< delete is the only valid op.
        };

        SegmentInfo(BackupStorage& storage, ThreadSafePool& pool,
                    uint64_t masterId, uint64_t segmentId,
                    uint32_t segmentSize, bool primary);
        ~SegmentInfo();
        void appendRecoverySegment(uint64_t partitionId,
                                   Buffer& buffer);
        void buildRecoverySegments(const ProtoBuf::Tablets& partitions);
        void close();
        void free();

        /// See #rightmostWrittenOffset.
        uint32_t
        getRightmostWrittenOffset()
        {
            return rightmostWrittenOffset;
        }

        /// Return true if this segment is OPEN.
        bool
        isOpen()
        {
            Lock _(mutex);
            return state == OPEN;
        }

        void open();

        /**
         * Set the state to #RECOVERING from #OPEN or #CLOSED.
         * This can only be called on a primary segment.
         */
        void
        setRecovering()
        {
            Lock _(mutex);
            assert(primary);
            state = RECOVERING;
        }

        /**
         * Set the state to #RECOVERING from #OPEN or #CLOSED and store
         * a copy of the supplied tablet information in case construction
         * of recovery segments is needed later for this secondary
         * segment.
         */
        void
        setRecovering(const ProtoBuf::Tablets& partitions)
        {
            Lock _(mutex);
            assert(!primary);
            state = RECOVERING;
            // Make a copy of the partition list for deferred filtering.
            recoveryPartitions.construct(partitions);
        }

        void startLoading();
        void write(Buffer& src, uint32_t srcOffset,
                   uint32_t length, uint32_t destOffset);
        const void* getLogDigest(uint32_t* byteLength = NULL);

        /**
         * Return true if #this should be loaded from disk before
         * #info.  Locks both objects during comparision.
         *
         * \param info
         *      Another SegmentInfo to order against.
         */
        bool operator<(SegmentInfo& info)
        {
            if (&info == this)
                return false;
            Lock _(mutex);
            Lock __(info.mutex);
            return segmentId < info.segmentId;
        }

        /// The id of the master from which this segment came.
        const uint64_t masterId;

        /**
         * True if this is the primary copy of this segment for the master
         * who stored it.  Determines whether recovery segments are built
         * at recovery start or on demand.
         */
        const bool primary;

        /// The segment id given to this segment by the master who sent it.
        const uint64_t segmentId;

      PRIVATE:
        /// Return true if this segment is fully in memory.
        bool inMemory() { return segment; }

        /// Return true if this segment has storage allocated.
        bool inStorage() const { return storageHandle; }


        /// Return true if this segment's recovery segments have been built.
        bool isRecovered() const { return recoverySegments; }

        /**
         * Wait for any LoadOps or StoreOps to complete.
         * The caller must be holding a lock on #mutex.
         */
        void
        waitForOngoingOps(Lock& lock)
        {
            int lastThreadCount = 0;
            while (storageThreadCount > 0) {
                if (storageThreadCount != lastThreadCount) {
                    LOG(WARNING, "Waiting for storage threads to terminate "
                        "for a segment, %d threads still running",
                        static_cast<int>(storageThreadCount));
                    lastThreadCount = storageThreadCount;
                }
                condition.wait(lock);
            }
        }

        /**
         * Provides mutal exclusion between all public method calls and
         * storage operations that can be performed on SegmentInfos.
         */
        boost::mutex mutex;

        /**
         * Notified when a store or load for this segment completes, or
         * when this segment's recovery segments are constructed and valid.
         * Used in conjunction with #mutex.
         */
        boost::condition_variable condition;

        /// An array of recovery segments when non-null.
        /// The exception if one occurred while recovering a segment.
        boost::scoped_ptr<SegmentRecoveryFailedException> recoveryException;

        /**
         * Only used if this segment is recovering but the filtering is
         * deferred (i.e. this isn't the primary segment backup copy).
         */
        ObjectTub<ProtoBuf::Tablets> recoveryPartitions;

        /// An array of recovery segments when non-null.
#ifdef PERF_DEBUG_RECOVERY_CONTIGUOUS_RECOVERY_SEGMENTS
        // first is byte array, second is number of bytes used
        pair<char[Segment::SEGMENT_SIZE], uint32_t>* recoverySegments;
#else
        Buffer* recoverySegments;
#endif

        /// The number of Buffers in #recoverySegments.
        uint32_t recoverySegmentsLength;

        /**
         * Indicate to callers of startReadingData() that particular
         * segment's #rightmostWrittenOffset is not needed because it was
         * successfully closed.
         */
        enum { BYTES_WRITTEN_CLOSED = ~(0u) };

        /**
         * An approximation for written segment "length" for startReadingData
         * if this segment is still open, otherwise BYTES_WRITTEN_CLOSED.
         */
        uint32_t rightmostWrittenOffset;

        /**
         * The staging location for this segment in memory.
         *
         * Only valid when inMemory() (happens while OPEN or CLOSED).
         */
        char* segment;

        /// The size in bytes that make up this segment.
        const uint32_t segmentSize;

        /// The state of this segment.  See State.
        State state;

        /**
         * Handle to provide to the storage layer to access this segment.
         *
         * Allocated while OPEN, CLOSED.
         */
        BackupStorage::Handle* storageHandle;

        /// To allocate memory for segments to be staged/recovered in.
        ThreadSafePool& pool;

        /// For allocating, loading, storing segments to.
        BackupStorage& storage;

        /// Count of threads performing loads/stores on this segment.
        int storageThreadCount;

        friend class LoadOp;
        friend class StoreOp;
        friend class BackupServerTest;
        friend class SegmentInfoTest;
        DISALLOW_COPY_AND_ASSIGN(SegmentInfo);
    };

    /**
     * Load a segment from disk into a valid buffer in memory.  Locks
     * the #SegmentInfo::mutex to ensure other operations aren't performed
     * on the segment in the meantime.
     */
    class LoadOp {
      public:
        explicit LoadOp(SegmentInfo& info);
        void operator()();

      private:
        /// The SegmentInfo this LoadOp operates on.
        SegmentInfo& info;
    };

    /**
     * Store a segment to disk from a valid buffer in memory.  Locks
     * the #SegmentInfo::mutex to ensure other operations aren't performed
     * on the segment in the meantime.
     */
    class StoreOp {
      public:
        explicit StoreOp(SegmentInfo& info);
        void operator()();

      private:
        /// The SegmentInfo this StoreOp operates on.
        SegmentInfo& info;
    };

    /**
     * Asynchronously loads and splits stored segments into recovery segments,
     * trying to acheive efficiency by overlapping work where possible using
     * threading.  See #infos for important details on sharing constraints
     * for SegmentInfo structures while these threads are processing.
     */
    class RecoverySegmentBuilder
    {
      public:
        RecoverySegmentBuilder(const vector<SegmentInfo*>& infos,
                               const ProtoBuf::Tablets& partitions,
                               AtomicInt& recoveryThreadCount);
        void operator()();

      private:
        /**
         * The SegmentInfos that will be loaded from disk (asynchronously)
         * and (asynchronously) split into recovery segments.  These
         * SegmentInfos are owned by this RecoverySegmentBuilder instance
         * which will run in a separate thread by locking #mutex.
         * All public methods of SegmentInfo obey this mutex ensuring
         * thread safety.
         */
        const vector<SegmentInfo*> infos;

        /// Copy of the partitions use to split out the recovery segments.
        const ProtoBuf::Tablets partitions;

        AtomicInt& recoveryThreadCount;
    };

  public:
    struct Config {
        string coordinatorLocator;
        string localLocator;
        Config()
            : coordinatorLocator()
            , localLocator()
        {
        }
    };

    explicit BackupServer(const Config& config,
                          BackupStorage& storage);
    virtual ~BackupServer();
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder);
    uint64_t getServerId() const;
    void run();

  PRIVATE:
    void freeSegment(const BackupFreeRpc::Request& reqHdr,
                     BackupFreeRpc::Response& respHdr,
                     Transport::ServerRpc& rpc);
    SegmentInfo* findSegmentInfo(uint64_t masterId, uint64_t segmentId);
    void getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                         BackupGetRecoveryDataRpc::Response& respHdr,
                         Transport::ServerRpc& rpc);
    static bool segmentInfoLessThan(SegmentInfo* left,
                                    SegmentInfo* right);
    void startReadingData(const BackupStartReadingDataRpc::Request& reqHdr,
                          BackupStartReadingDataRpc::Response& respHdr,
                          Transport::ServerRpc& rpc,
                          Responder& responder);
    void writeSegment(const BackupWriteRpc::Request& req,
                      BackupWriteRpc::Response& resp,
                      Transport::ServerRpc& rpc);

    /// Settings passed to the constructor
    const Config& config;

    /// Handle to cluster coordinator
    CoordinatorClient coordinator;

    /// Coordinator-assigned ID for this backup server
    uint64_t serverId;

    /**
     * A pool of aligned segments (supporting O_DIRECT) to avoid
     * the memory allocator.
     */
    ThreadSafePool pool;

    /// Count of threads performing recoveries.
    AtomicInt recoveryThreadCount;

    /// Type of the key for the segments map.
    struct MasterSegmentIdPair {
        MasterSegmentIdPair(uint64_t masterId, uint64_t segmentId)
            : masterId(masterId)
            , segmentId(segmentId)
        {
        }

        /// Comparison is needed for the type to be a key in a map.
        bool
        operator<(const MasterSegmentIdPair& right) const
        {
            return std::make_pair(masterId, segmentId) <
                   std::make_pair(right.masterId, right.segmentId);
        }

        uint64_t masterId;
        uint64_t segmentId;
    };
    /// Type of the segments map.
    typedef std::map<MasterSegmentIdPair, SegmentInfo*> SegmentsMap;
    /**
     * Mapping from (MasterId, SegmentId) to a SegmentInfo for segments
     * that are currently open or in storage.
     */
    SegmentsMap segments;

    /// The uniform size of each segment this backup deals with.
    const uint32_t segmentSize;

    /// The storage backend where closed segments are to be placed.
    BackupStorage& storage;

    /// For unit testing.
    uint64_t bytesWritten;

    friend class BackupServerTest;
    friend class SegmentInfoTest;
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
};

} // namespace RAMCloud

#endif
