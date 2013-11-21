/* Copyright (c) 2012-2013 Stanford University
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
#include "Tablets.pb.h"

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

    explicit TableManager(Context* context,
            CoordinatorUpdateManager* updateManager);
    ~TableManager();

    uint64_t createTable(const char* name, uint32_t serverSpan);
    string debugString(bool shortForm = false);
    void dropTable(const char* name);
    uint64_t getTableId(const char* name);
    Tablet getTablet(uint64_t tableId, uint64_t keyHash);
    vector<Tablet> markAllTabletsRecovering(ServerId serverId);
    void reassignTabletOwnership(ServerId newOwner, uint64_t tableId,
                                 uint64_t startKeyHash, uint64_t endKeyHash,
                                 uint64_t ctimeSegmentId,
                                 uint64_t ctimeSegmentOffset);
    void recover(uint64_t lastCompletedUpdate);
    void serialize(ProtoBuf::Tablets* tablets) const;
    void splitTablet(const char* name,
                     uint64_t splitKeyHash);
    void splitRecoveringTablet(uint64_t tableId, uint64_t splitKeyHash);
    void tabletRecovered(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         ServerId serverId,
                         Log::Position ctime);

  PRIVATE:
    /**
     * The following class holds information about a single table.
     */
    struct Table {
        Table(const char* name, uint64_t id)
            : name(name)
            , id(id)
            , tablets()
        {}
        ~Table();

        /// Human-readable name for the table (unique among all tables).
        string name;

        /// Identifier used to refer to the table in RPCs.
        uint64_t id;

        /// Information about each of the tablets in the table. The
        /// entries are allocated and freed dynamically.
        vector<Tablet*> tablets;
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

    Tablet* findTablet(const Lock& lock, Table* table, uint64_t keyHash);
    void notifyCreate(const Lock& lock, Table* table);
    void notifyDropTable(const Lock& lock, ProtoBuf::Table* info);
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

    DISALLOW_COPY_AND_ASSIGN(TableManager);
};

} // namespace RAMCloud

#endif

