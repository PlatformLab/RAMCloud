/* Copyright (c) 2012-2014 Stanford University
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

#ifndef RAMCLOUD_TABLEMANAGER_H
#define RAMCLOUD_TABLEMANAGER_H

#include <mutex>

#include "Common.h"
#include "CoordinatorUpdateManager.h"
#include "ServerId.h"
#include "Table.pb.h"
#include "Tablet.h"
#include "TableConfig.pb.h"
#include "Indexlet.h"

namespace RAMCloud {

/**
 * Used by the coordinator to map each tablet to the master that serves
 * requests for that tablet. The TableMap is the definitive truth about
 * tablet ownership in a cluster.
 *
 * Instances are locked for thread-safety, and methods return tablets
 * by-value to avoid inconsistencies due to concurrency.
 */
class TableManager {
  PUBLIC:

    /// Thrown if the given table does not exist.
    struct NoSuchTable : public Exception {
        explicit NoSuchTable(const CodeLocation& where)
                : Exception(where) {}
    };

    /// Thrown in response to invalid splitTablet() arguments.
    struct BadSplit : public Exception {
        explicit BadSplit(const CodeLocation& where) : Exception(where) {}
    };

    /**
     * Thrown from methods when the arguments indicate a tablet that is
     * not present in the tablet map.
     */
    struct NoSuchTablet : public Exception {
        explicit NoSuchTablet(const CodeLocation& where) : Exception(where) {}
    };

    /**
     * Thrown from methods when the arguments indicate a indexlet that is
     * not present in the indexlet map.
     */
    struct NoSuchIndexlet : public Exception {
        explicit NoSuchIndexlet(const CodeLocation& where) : Exception(where) {}
    };

    explicit TableManager(Context* context,
            CoordinatorUpdateManager* updateManager);
    ~TableManager();

    bool createIndex(uint64_t tableId, uint8_t indexId, uint8_t indexType,
            uint8_t numIndexlets);
    uint64_t createTable(const char* name, uint32_t serverSpan);
    string debugString(bool shortForm = false);
    uint8_t dropIndex(uint64_t tableId, uint8_t indexId);
    vector<pair<uint8_t, uint8_t>>  dropTable(const char* name);
    uint64_t getTableId(const char* name);
    Tablet getTablet(uint64_t tableId, uint64_t keyHash);
    bool getIndexletInfoByIndexletTableId(uint64_t indexletTableId,
            ProtoBuf::Indexlets::Indexlet& indexletInfo);
    void indexletRecovered(uint64_t tableId, uint8_t indexId,
            void* firstKey, uint16_t firstKeyLength,
            void* firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
            ServerId serverId, uint64_t indexletTableId);
    bool isIndexletTable(uint64_t tableId);
    vector<Tablet> markAllTabletsRecovering(ServerId serverId);
    void reassignTabletOwnership(ServerId newOwner, uint64_t tableId,
            uint64_t startKeyHash, uint64_t endKeyHash,
            uint64_t ctimeSegmentId, uint64_t ctimeSegmentOffset);
    void recover(uint64_t lastCompletedUpdate);
    void serializeTableConfig(ProtoBuf::TableConfig* tableConfig,
            uint64_t tableId);
    void splitTablet(const char* name, uint64_t splitKeyHash);
    void splitRecoveringTablet(uint64_t tableId, uint64_t splitKeyHash);
    void tabletRecovered(uint64_t tableId, uint64_t startKeyHash,
            uint64_t endKeyHash, ServerId serverId, Log::Position ctime);

  PRIVATE:
    /**
     * The following structure holds information about a indexlet of an index.
     */
    struct Indexlet : public RAMCloud::Indexlet {
        public:
        Indexlet(const void *firstKey, uint16_t firstKeyLength,
                const void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
                ServerId serverId, uint64_t indexletTableId,
                uint64_t tableId, uint8_t indexId)
            : RAMCloud::Indexlet(firstKey, firstKeyLength, firstNotOwnedKey,
                       firstNotOwnedKeyLength)
            , serverId(serverId)
            , indexletTableId(indexletTableId)
            , tableId(tableId)
            , indexId(indexId)
        {}

        Indexlet(const Indexlet& indexlet)
            : RAMCloud::Indexlet(indexlet)
            , serverId(indexlet.serverId)
            , indexletTableId(indexlet.indexletTableId)
            , tableId(indexlet.tableId)
            , indexId(indexlet.indexId)
        {}

        /// The server id of the master owning this indexlet.
        ServerId serverId;

        /// The id of the table on serverId holding the index content
        uint64_t indexletTableId;

        /// The id of the owning table
        uint64_t tableId;

        /// The id of the owning index
        uint8_t indexId;
    };

    /**
     * The following class holds information about a single index of a table.
     */
    struct Index {
        Index(uint64_t tableId, uint8_t indexId, uint8_t indexType)
            : tableId(tableId)
            , indexId(indexId)
            , indexType(indexType)
            , indexlets()
        {}
        ~Index();

        /// The id of the containing table.
        uint64_t tableId;

        /// Identifier used to refer to index within a table.
        uint8_t indexId;

        /// Type of the index.
        uint8_t indexType;

        /// Information about each of the indexlets of index in the table. The
        /// entries are allocated and freed dynamically.
        vector<Indexlet*> indexlets;
    };

    typedef std::unordered_map<uint8_t, Index*> IndexMap;

    struct Table {
        Table(const char* name, uint64_t id)
            : name(name)
            , id(id)
            , tablets()
            , indexMap()
        {}
        ~Table();

        /// Human-readable name for the table (unique among all tables).
        string name;

        /// Identifier used to refer to the table in RPCs.
        uint64_t id;

        /// Information about each of the tablets in the table. The
        /// entries are allocated and freed dynamically.
        vector<Tablet*> tablets;

        /// Information about each of the indexes in the table. The
        /// entries are allocated and freed dynamically.
        //vector<Index*> indexMap;
        IndexMap indexMap;
    };

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// Shared information about the server.
    Context* context;

    /// Used for keeping track of updates on external storage.
    CoordinatorUpdateManager* updateManager;

    /// Id of the next table to be created.  Table ids are never reused.
    uint64_t nextTableId;

    /// Identifies the master to which we assigned the most recent tablet;
    /// used to rotate among the masters when assigning new tables.
    ServerId tabletMaster;

    /// Maps from a table name to information about that table. The table
    /// information is dynamically allocated (and shared with idMap).
    typedef std::unordered_map<string, Table*> Directory;
    Directory directory;

    /// Maps from a table id to information about that table. The table
    /// information is dynamically allocated (and shared with directory).
    typedef std::unordered_map<uint64_t, Table*> IdMap;
    IdMap idMap;

    /// Maps from indexletTable id to indexlet.
    /// This is a map since every indexletTable can have at most one
    /// containing indexlet
    typedef std::unordered_map<uint64_t, Indexlet*> IndexletTableMap;
    IndexletTableMap indexletTableMap;

    Tablet* findTablet(const Lock& lock, Table* table, uint64_t keyHash);
    void notifyCreate(const Lock& lock, Table* table);
    void notifyCreateIndex(const Lock& lock, Index* index);
    void notifyDropTable(const Lock& lock, ProtoBuf::Table* info);
    void notifyDropIndex(const Lock& lock, Index* index);
    void notifySplitTablet(const Lock& lock, ProtoBuf::Table* info);
    void notifyReassignTablet(const Lock& lock, ProtoBuf::Table* info);
    Table* recreateTable(const Lock& lock, ProtoBuf::Table* info);
    void serializeTable(const Lock& lock, Table* table,
            ProtoBuf::Table* externalInfo);
    void sync(const Lock& lock);
    void syncTable(const Lock& lock, Table* table,
            ProtoBuf::Table* externalInfo);
    void testAddTablet(const Tablet& tablet);
    void testCreateTable(const char* name, uint64_t id);
    Tablet* testFindTablet(uint64_t tableId, uint64_t keyHash);

    DISALLOW_COPY_AND_ASSIGN(TableManager);
};

} // namespace RAMCloud

#endif

