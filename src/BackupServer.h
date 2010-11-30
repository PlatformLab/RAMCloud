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

/**
 * Handles Rpc requests from Masters and the Coordinator to persistently store
 * Segments and to facilitate the recovery of object data when Masters crash.
 */
class BackupServer : public Server {
    /**
     * Metadata associated with each segment describing where in memory
     * and storage it resides.
     */
    struct SegmentInfo {
        enum State {
            OPEN,       ///< Storage is reserved but segment is mutable.
            CLOSED,     ///< Immutable and has moved to stable store.
            RECOVERING, ///< Immutable but ready for recovery data requests.
        };

        SegmentInfo()
            : segment(NULL)
            , state(OPEN)
            , storageHandle(NULL)
        {
        }

        SegmentInfo(char* segment, BackupStorage::Handle* storageHandle)
            : segment(segment)
            , state(OPEN)
            , storageHandle(storageHandle)
        {
        }

        /**
         * The staging location for this segment in memory.  Only valid when
         * #state is OPEN.
         */
        char* segment;

        /// The state of this segment.  See State.
        State state;

        /// Handle to provide to the storage layer to access this segment.
        BackupStorage::Handle* storageHandle;
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

  private:
    void closeSegment(const BackupCloseRpc::Request& reqHdr,
                       BackupCloseRpc::Response& respHdr,
                       Transport::ServerRpc& rpc);
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
    void openSegment(const BackupOpenRpc::Request& reqHdr,
                     BackupOpenRpc::Response& respHdr,
                     Transport::ServerRpc& rpc);
    void openSegment(uint64_t masterId, uint64_t segmentId);
    void reserveSpace();
    void startReadingData(const BackupStartReadingDataRpc::Request& reqHdr,
                          BackupStartReadingDataRpc::Response& respHdr,
                          Transport::ServerRpc& rpc);
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
    boost::pool<> pool;

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
    typedef std::map<MasterSegmentIdPair, SegmentInfo> SegmentsMap;
    /**
     * Mapping from (MasterId, SegmentId) to a SegmentInfo for segments
     * that are currently open or in storage.
     */
    SegmentsMap segments;

    /// The uniform size of each segment this backup deals with.
    const uint32_t segmentSize;

    /// The storage backend where closed segments are to be placed.
    BackupStorage& storage;

    friend class BackupServerTest;
    DISALLOW_COPY_AND_ASSIGN(BackupServer);
};

} // namespace RAMCloud

#endif
