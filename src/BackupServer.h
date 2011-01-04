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

    /// The type of the in-memory segment size chunk pool.
    typedef boost::pool<SegmentAllocator> Pool;

    /**
     * Tracks all state associated with a single segment and manages
     * resources and storage associated with it.
     */
    class SegmentInfo {
      public:
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
            /**
             * A RecoverySegmentBuilder thread has ownership of this
             * SegmentInfo.  The only legal operation from the main thread
             * is to read the state field until the other thread changes
             * the state to CLOSED.  The builder thread may modify
             * any of the other fields while this object is so locked.
             */
            RECOVERING,
            CLOSED,     ///< Immutable and has moved to stable store.
            FREED,      ///< delete is the only valid op.
        };

        SegmentInfo(BackupStorage& storage, Pool& pool,
                    uint64_t masterId, uint64_t segmentId);
        ~SegmentInfo();
        void appendRecoverySegment(uint64_t partitionId,
                                   Buffer& buffer);
        void buildRecoverySegments(const ProtoBuf::Tablets& partitions,
                                   uint32_t segmentSize);
        void close();
        void free();
        char* getSegment();

        /// Return true if this segment is fully in memory.
        bool inMemory() const { return segment && !isLoading(); }

        /// Return true if this segment has storage allocated.
        bool inStorage() const { return storageHandle; }

        /// Return true if this segment is OPEN.
        bool isOpen() const { return state == OPEN; }

        /**
         * Return true if this segment is RECOVERING.
         * Forces read of the value from the cache hierarchy and does
         * not allow reordering around this call by the compiler or
         * the CPU.
         */
        bool isRecovering() const {
            __sync_synchronize();
            return state == RECOVERING;
        }

        void open();
        void startLoading();

        /**
         * Mark this SegmentInfo as RECOVERING and off limits to the
         * main thread other than status checks (isOpen(), isRecovering(),
         * isRecovered()).
         */
        void setRecovering() { state = RECOVERING; }

        /**
         * Mark this SegmentInfo as CLOSED and available to the
         * main thread.  Forces a write of the value to the cache
         * hierarchy and does not allow reordering around this call
         * by the compiler or the CPU.
         */
        void syncSetClosed() {
            state = CLOSED;
            __sync_synchronize();
        }

        bool operator<(const SegmentInfo& info) const {
            return segmentId < info.segmentId;
        }

      PRIVATE:
         /// Return true if an IO is fetching this stored segment.
        bool isLoading() const { return segmentSync; }

        /// Return true if this segment's recovery segments have been built.
        bool isRecovered() const { return !isRecovering() && recoverySegments; }

        /// The id of the master from which this segment came.
        const uint64_t masterId;

        /// An array of recovery segments when non-null.
        /// The exception if one occurred while recovering a segment.
        boost::scoped_ptr<SegmentRecoveryFailedException> recoveryException;

        /// An array of recovery segments when non-null.
        Buffer* recoverySegments;

        /// The number of Buffers in #recoverySegments.
        uint32_t recoverySegmentsLength;

        /// The segment id given to this segment by the master who sent it.
        const uint64_t segmentId;

        /**
         * The staging location for this segment in memory.
         *
         * Only valid when inMemory() (happens while OPEN or CLOSED).
         */
        char* segment;

        /**
         * Action which must be invoked for the data in #segment to be valid.
         *
         * Allocated when isLoading() (happens while CLOSED).
         */
        boost::scoped_ptr<BackupStorage::Syncable> segmentSync;

        /// The state of this segment.  See State.
        State state;

        /**
         * Handle to provide to the storage layer to access this segment.
         *
         * Allocated while OPEN, CLOSED.
         */
        BackupStorage::Handle* storageHandle;

        /// To allocate memory for segments to be staged/recovered in.
        Pool& pool;

        /// For allocating, loading, storing segments to.
        BackupStorage& storage;

        friend class BackupServerTest;
        friend class SegmentInfoTest;
        DISALLOW_COPY_AND_ASSIGN(SegmentInfo);
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
                               const uint32_t segmentSize);
        void operator()();

      private:
        /**
         * The SegmentInfos that will be loaded from disk (asynchronously)
         * and (asynchronously) split into recovery segments.  These
         * SegmentInfos are owned by this RecoverySegmentBuilder instance
         * which will run in a separate thread.  Other threads shall not
         * access any SegmentInfo in this list except to check that its
         * state (isOpen(), isRecovering(), isRecovered() methods).
         */
        const vector<SegmentInfo*> infos;

        /// Copy of the partitions use to split out the recovery segments.
        const ProtoBuf::Tablets partitions;

        /// Copy of the size of the segments each of the #infos describes.
        const uint32_t segmentSize;
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
    void closeSegment(uint64_t masterId, uint64_t segmentId);
    void freeSegment(const BackupFreeRpc::Request& reqHdr,
                     BackupFreeRpc::Response& respHdr,
                     Transport::ServerRpc& rpc);
    SegmentInfo* findSegmentInfo(uint64_t masterId, uint64_t segmentId);
    void getRecoveryData(const BackupGetRecoveryDataRpc::Request& reqHdr,
                         BackupGetRecoveryDataRpc::Response& respHdr,
                         Transport::ServerRpc& rpc);
    bool keepEntry(const LogEntryType type,
                   const void* data,
                   const ProtoBuf::Tablets& tablets) const;
    void openSegment(uint64_t masterId, uint64_t segmentId);
    void reserveSpace();
    static bool segmentInfoLessThan(const SegmentInfo* left,
                                    const SegmentInfo* right);
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
    Pool pool;

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
