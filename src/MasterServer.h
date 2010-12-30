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

#ifndef RAMCLOUD_MASTERSERVER_H
#define RAMCLOUD_MASTERSERVER_H

#include <boost/unordered_map.hpp>

#include "Common.h"
#include "CoordinatorClient.h"
#include "Object.h"
#include "Log.h"
#include "LogCleaner.h"
#include "BackupManager.h"
#include "HashTable.h"
#include "RecoverySegmentIterator.h"
#include "Server.h"
#include "SegmentIterator.h"
#include "Table.h"

namespace RAMCloud {

/**
 * Provides two major functions to recovery masters; a segment recovery
 * schedule and a mapping to available segment locations.
 */
class SegmentLocatorChooser {
  public:

    /// Schedule of segments to recover. Guaranteed to be a model of Container.
    typedef vector<uint64_t> SegmentIdList;

    explicit SegmentLocatorChooser(const ProtoBuf::ServerList& list);
    const string& get(uint64_t segmentId);
    const SegmentIdList& getSegmentIdList();
    void markAsDown(uint64_t segmentId, const string& locator);

  private:
    /// Type of the internal data structure that does the heavy lifting.
    typedef boost::unordered_multimap<uint64_t, string> LocatorMap;

    /// A pair of iterators used when finding elements for a key.
    typedef pair<LocatorMap::const_iterator, LocatorMap::const_iterator>
        ConstLocatorRange;
    /// A pair of iterators used when finding elements for a key.  Mutable.
    typedef pair<LocatorMap::iterator, LocatorMap::iterator> LocatorRange;

    /// Tracks segment ids to locator strings where they can be fetched from.
    LocatorMap map;

    /// A random ordering of segment ids each of which must be recovered.
    SegmentIdList ids;

    friend class SegmentLocatorChooserTest;
    DISALLOW_COPY_AND_ASSIGN(SegmentLocatorChooser);
};

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
class MasterServer : public Server {
  public:
    /// The max number of tables a Master will serve.
    static const int NUM_TABLES = 4;

    MasterServer(const ServerConfig config,
                 CoordinatorClient* coordinator,
                 uint32_t replicas);
    virtual ~MasterServer();
    void run();
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder);

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
            int pct = strtoull(str.c_str(), NULL, 10);
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
            int pct = strtoull(str.c_str(), NULL, 10);
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
            hashTableBytes / ObjectMap::bytesPerCacheLine();
        if (numHashTableLines < 1) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - insufficient memory "
                            "left for the hash table!");
        }

        LOG(NOTICE, "Master to allocate %lu bytes total, %lu of which for the "
            "hash table", masterBytes, hashTableBytes);
        LOG(NOTICE, "Master will have %lu segments and %lu lines in the hash "
            "table", numSegments, numHashTableLines);

        config->logBytes = logBytes;
        config->hashTableBytes = hashTableBytes;
    }


  private:
    void create(const CreateRpc::Request& reqHdr,
                CreateRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void ping(const PingRpc::Request& reqHdr,
              PingRpc::Response& respHdr,
              Transport::ServerRpc& rpc);
    void read(const ReadRpc::Request& reqHdr,
              ReadRpc::Response& respHdr,
              Transport::ServerRpc& rpc);
    void recover(const RecoverRpc::Request& reqHdr,
                 RecoverRpc::Response& respHdr,
                 Transport::ServerRpc& rpc,
                 Responder& responder);

    void recoverSegmentPrefetcher(RecoverySegmentIterator& i,
                                  ObjectTombstoneMap& tombstoneMap);
    void recoverSegment(uint64_t segmentId, const void *buffer,
                        uint64_t bufferLength,
                        ObjectTombstoneMap& tombstoneMap);

    void recover(uint64_t masterId,
                 uint64_t partitionId,
                 const ProtoBuf::ServerList& backups,
                 ObjectTombstoneMap& tombstoneMap);

    void remove(const RemoveRpc::Request& reqHdr,
                RemoveRpc::Response& respHdr,
                Transport::ServerRpc& rpc);
    void setTablets(const ProtoBuf::Tablets& newTablets);
    void setTablets(const SetTabletsRpc::Request& reqHdr,
                    SetTabletsRpc::Response& respHdr,
                    Transport::ServerRpc& rpc);
    void write(const WriteRpc::Request& reqHdr,
               WriteRpc::Response& respHdr,
               Transport::ServerRpc& rpc);

    // The following variables are copies of constructor arguments;
    // see constructor documentation for details.
    const ServerConfig config;

  public:
    CoordinatorClient* coordinator;

    uint64_t serverId;

  private:
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
    ObjectMap objectMap;

    /**
     * Tablets this master owns.
     * The user_data field in each tablet points to a Table object.
     */
    ProtoBuf::Tablets tablets;

    friend void objectEvictionCallback(LogEntryType type, const void* p,
        uint64_t entryLength, uint64_t lengthInLog, LogTime logTime,
        void* cookie);
    friend void tombstoneEvictionCallback(LogEntryType type, const void* p,
        uint64_t entryLength, uint64_t lengthInLog, LogTime logTime,
        void* cookie);
    friend void segmentReplayCallback(Segment* seg, void* cookie);
    Table& getTable(uint32_t tableId, uint64_t objectId);
    void rejectOperation(const RejectRules* rejectRules, uint64_t version);
    void storeData(uint64_t table, uint64_t id,
                   const RejectRules* rejectRules, Buffer* data,
                   uint32_t dataOffset, uint32_t dataLength,
                   uint64_t* newVersion);
    friend class MasterTest;
    friend class MasterRecoverTest;
    friend class CoordinatorTest;
    friend class RecoverSegmentBenchmark;
    DISALLOW_COPY_AND_ASSIGN(MasterServer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MASTERSERVER_H
