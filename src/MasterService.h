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

#ifndef RAMCLOUD_MASTERSERVICE_H
#define RAMCLOUD_MASTERSERVICE_H

#include <boost/unordered_map.hpp>

#include "Common.h"
#include "CoordinatorClient.h"
#include "Object.h"
#include "Log.h"
#include "LogCleaner.h"
#include "BackupManager.h"
#include "HashTable.h"
#include "RecoverySegmentIterator.h"
#include "Service.h"
#include "SegmentIterator.h"
#include "Table.h"

namespace RAMCloud {

#if TESTING
void
detectSegmentRecoveryFailure(const uint64_t masterId,
                             const uint64_t partitionId,
                             const ProtoBuf::ServerList& backups);
#endif

struct ServerConfig {
    string coordinatorLocator;
    string localLocator;

    /// Total number bytes to use for the Log.
    uint64_t logBytes;

    /// Total number of bytes to use for the HashTable.
    uint64_t hashTableBytes;

    ServerConfig()
        : coordinatorLocator()
        , localLocator()
        , logBytes(0)
        , hashTableBytes(0)
    {
    }
};

/**
 * An object of this class represents a RAMCloud server, which can
 * respond to client RPC requests to manipulate objects stored on the
 * server.
 */
class MasterService : public Service {
  public:
    static const uint64_t TOTAL_READ_REQUESTS_OBJID = 100000UL;

    MasterService(const ServerConfig config,
                  CoordinatorClient* coordinator,
                  uint32_t replicas);
    virtual ~MasterService();
    void init();
    void dispatch(RpcOpcode opcode,
                  Rpc& rpc);

    /**
     * Figure out the Master Server's memory requirements. This means computing
     * the number of bytes to use for the log and the hash table.
     *
     * The user may dictate these parameters by specifying the total memory
     * given to the server, as well as the amount of that to spend on the hash
     * table. The rest is given to the log.
     *
     * Both parameters are string options. If a "%" character is present, they
     * are interpreted as percentages, otherwise they are interpreted as
     * megabytes.
     *
     * \param[in] masterTotalMemory
     *      A string representing the total amount of memory allocated to the
     *      Server. E.g.: "10%" means 10 percent of total system memory, whereas
     *      "256" means 256 megabytes. Only integer quantities are acceptable.
     * \param[in] hashTableMemory
     *      The amount of masterTotalMemory to be used for the hash table. This
     *      may also be a percentage, as above. Only integer quantities are
     *      acceptable.
     * \param[out] config
     *      The ServerConfig object to update with the number of bytes the Log
     *      and hash tables should allocate, as determined by the input
     *      parameters.
     * \throw Exception
     *      An exception is thrown if the parameters given are invalid, or
     *      if the total system memory cannot be determined.
     */
    static void
    sizeLogAndHashTable(string masterTotalMemory, string hashTableMemory,
                        ServerConfig *config)
    {
        using namespace RAMCloud;

        uint64_t masterBytes, hashTableBytes;

        if (masterTotalMemory.find("%") != string::npos) {
            string str = masterTotalMemory.substr(
                0, masterTotalMemory.find("%"));
            uint64_t pct = strtoull(str.c_str(), NULL, 10);
            if (pct <= 0 || pct > 90)
                throw Exception(HERE,
                    "invalid `MasterTotalMemory' option specified: "
                    "not within range 1-90%");
            masterBytes = getTotalSystemMemory();
            if (masterBytes == 0) {
                throw Exception(HERE,
                    "Cannot determine total system memory - "
                    "`MasterTotalMemory' option must not be used");
            }
            masterBytes = (masterBytes * pct) / 100;
        } else {
            masterBytes = strtoull(masterTotalMemory.c_str(), NULL, 10);
            masterBytes *= (1024 * 1024);
        }

        if (hashTableMemory.find("%") != string::npos) {
            string str = hashTableMemory.substr(0, hashTableMemory.find("%"));
            uint64_t pct = strtoull(str.c_str(), NULL, 10);
            if (pct <= 0 || pct > 50) {
                throw Exception(HERE,
                    "invalid HashTableMemory option specified: "
                    "not within range 1-50%");
            }
            hashTableBytes = (masterBytes * pct) / 100;
        } else {
            hashTableBytes = strtoull(hashTableMemory.c_str(), NULL, 10);
            hashTableBytes *= (1024 * 1024);
        }

        if (hashTableBytes > masterBytes) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - HashTableMemory "
                            "cannot exceed MasterTotalMemory!");
        }

        uint64_t logBytes = masterBytes - hashTableBytes;
        uint64_t numSegments = logBytes / Segment::SEGMENT_SIZE;
        if (numSegments < 1) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - insufficient memory "
                            "left for the log!");
        }

        uint64_t numHashTableLines =
            hashTableBytes / HashTable<LogEntryHandle>::bytesPerCacheLine();
        if (numHashTableLines < 1) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - insufficient memory "
                            "left for the hash table!");
        }

        RAMCLOUD_LOG(NOTICE,
                     "Master to allocate %lu bytes total, %lu of which for the "
                     "hash table", masterBytes, hashTableBytes);
        RAMCLOUD_LOG(NOTICE, "Master will have %lu segments and %lu lines in "
                     "the hash table", numSegments, numHashTableLines);

        config->logBytes = logBytes;
        config->hashTableBytes = hashTableBytes;
    }

    /**
     * Used in detectSegmentRecoveryFailure() and MasterService::recover() to
     * mark and check getRecoveryData() requests statuses.
     */
    enum { REC_REQ_NOT_STARTED, REC_REQ_WAITING, REC_REQ_FAILED, REC_REQ_OK };

  PRIVATE:
    void create(const CreateRpc::Request& reqHdr,
                CreateRpc::Response& respHdr,
                Rpc& rpc);
    void fillWithTestData(const FillWithTestDataRpc::Request& reqHdr,
                          FillWithTestDataRpc::Response& respHdr,
                          Rpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Rpc& rpc);
    void multiRead(const MultiReadRpc::Request& reqHdr,
                   MultiReadRpc::Response& respHdr,
                   Rpc& rpc);
    void recover(const RecoverRpc::Request& reqHdr,
                 RecoverRpc::Response& respHdr,
                 Rpc& rpc);

    void recoverSegmentPrefetcher(RecoverySegmentIterator& i);
    void recoverSegment(uint64_t segmentId, const void *buffer,
                        uint32_t bufferLength);

    void recover(uint64_t masterId,
                 uint64_t partitionId,
                 ProtoBuf::ServerList& backups);

    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Rpc& rpc);
    void rereplicateSegments(const RereplicateSegmentsRpc::Request& reqHdr,
                             RereplicateSegmentsRpc::Response& respHdr,
                             Rpc& rpc);
    void setTablets(const ProtoBuf::Tablets& newTablets);
    void setTablets(const SetTabletsRpc::Request& reqHdr,
                    SetTabletsRpc::Response& respHdr,
                    Rpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Rpc& rpc);

    // The following variables are copies of constructor arguments;
    // see constructor documentation for details.
    const ServerConfig config;

  public:
    CoordinatorClient* coordinator;

    Tub<uint64_t> serverId;

  PRIVATE:
    /// Maximum number of bytes per partition. For Will calculation.
    static const uint64_t maxBytesPerPartition = 640UL * 1024 * 1024;

    /// Maximum number of referants (objs) per partition. For Will calculation.
    static const uint64_t maxReferantsPerPartition = 10UL * 1000 * 1000;

    BackupManager backup;

    /// Track total bytes of object data written (not including log overhead).
    uint64_t bytesWritten;

    /**
     * The main in-memory data structure holding all of the data stored
     * on this server.
     */
    Log log;

    /**
     * The (table ID, object ID) to #RAMCloud::Object pointer map for all
     * objects stored on this server. Before accessing objects via the hash
     * table, you usually need to check that the tablet still lives on this
     * server; objects from deleted tablets are not immediately purged from the
     * hash table.
     */
    HashTable<LogEntryHandle> objectMap;

    /**
     * Tablets this master owns.
     * The user_data field in each tablet points to a Table object.
     */
    ProtoBuf::Tablets tablets;

    /**
     * Used to identify the first write request, so that we can initialize
     * connections to all backups at that time (this is a temporary kludge
     * that needs to be replaced with a better solution).  False means this
     * service has not yet processed any write requests.
     */
    bool anyWrites;


    /* Temporary tombstone methods used during recovery. */
    LogEntryHandle allocRecoveryTombstone(const ObjectTombstone* srcTomb);
    void freeRecoveryTombstone(LogEntryHandle handle);
    void removeTombstones();

    friend void recoveryCleanup(LogEntryHandle maybeTomb, void *cookie);
    friend void objectEvictionCallback(LogEntryHandle handle, LogTime logTime,
        void* cookie);
    friend void tombstoneEvictionCallback(LogEntryHandle handle,
        LogTime logTime, void* cookie);
    friend void segmentReplayCallback(Segment* seg, void* cookie);
    Table& getTable(uint32_t tableId, uint64_t objectId);
    void rejectOperation(const RejectRules* rejectRules, uint64_t version);
    void storeData(uint64_t table, uint64_t id,
                   const RejectRules* rejectRules, Buffer* data,
                   uint32_t dataOffset, uint32_t dataLength,
                   uint64_t* newVersion, bool async);
    friend class MasterServiceTest;
    friend class MasterRecoverTest;
    friend class CoordinatorServiceTest;
    friend class RecoverSegmentBenchmark;
    DISALLOW_COPY_AND_ASSIGN(MasterService);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVICE_H
