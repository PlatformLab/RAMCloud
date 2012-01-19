/* Copyright (c) 2009-2011 Stanford University
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
 * Declarations for the backup service, currently all backup RPC
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the RPCs.
 */

#ifndef RAMCLOUD_BACKUPSERVICE_H
#define RAMCLOUD_BACKUPSERVICE_H

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <thread>
#include <memory>
#include <boost/pool/pool.hpp>
#include <map>
#include <queue>

#include "Common.h"
#include "AtomicInt.h"
#include "BackupClient.h"
#include "BackupStorage.h"
#include "CoordinatorClient.h"
#include "CycleCounter.h"
#include "Fence.h"
#include "LogTypes.h"
#include "RawMetrics.h"
#include "Rpc.h"
#include "Service.h"
#include "ServerConfig.h"

namespace RAMCloud {

#if TESTING
Tub<uint64_t> whichPartition(const LogEntryType type,
                                   const void* data,
                                   const ProtoBuf::Tablets& partitions);
#endif

/**
 * Handles Rpc requests from Masters and the Coordinator to persistently store
 * Segments and to facilitate the recovery of object data when Masters crash.
 */
class BackupService : public Service {
  PRIVATE:

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
            // The following statement is really only needed when value
            // is an AtomicInt.
            Fence::leave();
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
        typedef std::unique_lock<std::mutex> Lock;

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
                RAMCLOUD_LOG(WARNING,
                             "Backup segment pool destroyed with "
                             "%u chunks still allocated",
                             allocatedChunks);
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
        std::mutex mutex;

        /// The backing pool that manages memory chunks.
        Pool pool;
    };

    class IoScheduler;
    class RecoverySegmentBuilder;

    /**
     * Tracks all state associated with a single segment and manages
     * resources and storage associated with it.  Public calls are
     * protected by #mutex to provide thread safety.
     */
    class SegmentInfo {
      public:
        /// The type of locks used to lock #mutex.
        typedef std::unique_lock<std::mutex> Lock;

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
                    IoScheduler& ioScheduler,
                    ServerId masterId, uint64_t segmentId,
                    uint32_t segmentSize, bool primary);
        ~SegmentInfo();
        Status appendRecoverySegment(uint64_t partitionId, Buffer& buffer)
            __attribute__((warn_unused_result));
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
            return segmentId < info.segmentId;
        }

        /// The id of the master from which this segment came.
        const ServerId masterId;

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
#ifndef SINGLE_THREADED_BACKUP
            int lastThreadCount = 0;
            while (storageOpCount > 0) {
                if (storageOpCount != lastThreadCount) {
                    RAMCLOUD_LOG(DEBUG,
                                 "Waiting for storage threads to terminate "
                                 "for a segment, %d threads still running",
                                 static_cast<int>(storageOpCount));
                    lastThreadCount = storageOpCount;
                }
                condition.wait(lock);
            }
#endif
        }

        /**
         * Provides mutal exclusion between all public method calls and
         * storage operations that can be performed on SegmentInfos.
         */
        std::mutex mutex;

        /**
         * Notified when a store or load for this segment completes, or
         * when this segment's recovery segments are constructed and valid.
         * Used in conjunction with #mutex.
         */
        std::condition_variable condition;

        /// Gatekeeper through which async IOs are scheduled.
        IoScheduler& ioScheduler;

        /// An array of recovery segments when non-null.
        /// The exception if one occurred while recovering a segment.
        std::unique_ptr<SegmentRecoveryFailedException> recoveryException;

        /**
         * Only used if this segment is recovering but the filtering is
         * deferred (i.e. this isn't the primary segment backup copy).
         */
        Tub<ProtoBuf::Tablets> recoveryPartitions;

        /// An array of recovery segments when non-null.
        Buffer* recoverySegments;

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

        /// Count of loads/stores pending for this segment.
        int storageOpCount;

        friend class IoScheduler;
        friend class RecoverySegmentBuilder;
        DISALLOW_COPY_AND_ASSIGN(SegmentInfo);
    };

    /**
     * Queues, prioritizes, and dispatches storage load/store operations.
     */
    class IoScheduler {
      public:
        IoScheduler();
        void operator()();
        void load(SegmentInfo& info);
        void quiesce();
        void store(SegmentInfo& info);
        void shutdown(std::thread& ioThread);

      private:
        void doLoad(SegmentInfo& info) const;
        void doStore(SegmentInfo& info) const;

        typedef std::unique_lock<std::mutex> Lock;

        /// Protects #loadQueue, #storeQueue, and #running.
        std::mutex queueMutex;

        /// Notified when new requests are added to either queue.
        std::condition_variable queueCond;

        /// Queue of SegmentInfos to be loaded from storage.
        std::queue<SegmentInfo*> loadQueue;

        /// Queue of SegmentInfos to be written to storage.
        std::queue<SegmentInfo*> storeQueue;

        /// When false scheduler will exit when no outstanding requests remain.
        bool running;

        /**
         * The number of store ops issued that have not yet completed.
         * More precisely, this is the size of #storeQueue plus the number of
         * threads currently executing #doStore. It is necessary for #quiesce.
         */
        mutable std::atomic<uint64_t> outstandingStores;

        DISALLOW_COPY_AND_ASSIGN(IoScheduler);
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
        RecoverySegmentBuilder(Context& context,
                               const vector<SegmentInfo*>& infos,
                               const ProtoBuf::Tablets& partitions,
                               AtomicInt& recoveryThreadCount);
        void operator()();

      private:
        /// The context in which the thread will execute.
        Context& context;

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
    explicit BackupService(const ServerConfig& config);
    virtual ~BackupService();
    void benchmark(uint32_t& readSpeed, uint32_t& writeSpeed);
    void dispatch(RpcOpcode opcode, Rpc& rpc);
    ServerId getServerId() const;
    void init(ServerId id);

  PRIVATE:
    void freeSegment(const BackupFreeRpc::Request& reqHdr,
                     BackupFreeRpc::Response& respHdr,
                     Rpc& rpc);
    SegmentInfo* findSegmentInfo(ServerId masterId, uint64_t segmentId);
    void getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                         BackupGetRecoveryDataRpc::Response& respHdr,
                         Rpc& rpc);
    void quiesce(const BackupQuiesceRpc::Request& reqHdr,
                 BackupQuiesceRpc::Response& respHdr,
                 Rpc& rpc);
    void recoveryComplete(const BackupRecoveryCompleteRpc::Request& reqHdr,
                         BackupRecoveryCompleteRpc::Response& respHdr,
                         Rpc& rpc);
    static bool segmentInfoLessThan(SegmentInfo* left,
                                    SegmentInfo* right);
    void startReadingData(const BackupStartReadingDataRpc::Request& reqHdr,
                          BackupStartReadingDataRpc::Response& respHdr,
                          Rpc& rpc);
    void writeSegment(const BackupWriteRpc::Request& req,
                      BackupWriteRpc::Response& resp,
                      Rpc& rpc);

    /// Settings passed to the constructor
    const ServerConfig& config;

    /// Handle to cluster coordinator
    CoordinatorClient coordinator;

    /// Coordinator-assigned ID for this backup service
    ServerId serverId;

    /**
     * Times each recovery. This is an Tub that is reset on every
     * recovery, since CycleCounters can't presently be restarted.
     */
    Tub<CycleCounter<RawMetric>> recoveryTicks;

    /**
     * A pool of aligned segments (supporting O_DIRECT) to avoid
     * the memory allocator.
     */
    ThreadSafePool pool;

    /// Count of threads performing recoveries.
    AtomicInt recoveryThreadCount;

    /// Type of the key for the segments map.
    struct MasterSegmentIdPair {
        MasterSegmentIdPair(ServerId masterId, uint64_t segmentId)
            : masterId(masterId)
            , segmentId(segmentId)
        {
        }

        /// Comparison is needed for the type to be a key in a map.
        bool
        operator<(const MasterSegmentIdPair& right) const
        {
            return std::make_pair(*masterId, segmentId) <
                   std::make_pair(*right.masterId, right.segmentId);
        }

        ServerId masterId;
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
    std::unique_ptr<BackupStorage> storage;

    /// The results of storage.benchmark().
    pair<uint32_t, uint32_t> storageBenchmarkResults;

    /// For unit testing.
    uint64_t bytesWritten;

    /// Gatekeeper through which async IOs are scheduled.
    IoScheduler ioScheduler;
    /// The thread driving #ioScheduler.
    std::thread ioThread;

    /// Used to ensure that init() is invoked before the dispatcher runs.
    bool initCalled;

    DISALLOW_COPY_AND_ASSIGN(BackupService);
};

} // namespace RAMCloud

#endif
