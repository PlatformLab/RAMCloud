/* Copyright (c) 2012 Stanford University
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

#include <Client/Client.h>
#include <mutex>

#include "LargestTableId.pb.h"
#include "SplitTablet.pb.h"
#include "TableDrop.pb.h"
#include "TableInformation.pb.h"
#include "TabletRecovered.pb.h"
#include "Tablets.pb.h"

#include "Common.h"
#include "Log.h"
#include "LogCabinHelper.h"
#include "LogEntryTypes.h"
#include "ServerId.h"
#include "Tablet.h"

namespace RAMCloud {

using LogCabin::Client::Entry;
using LogCabin::Client::EntryId;
using LogCabin::Client::NO_ID;

/**
 * Maps tablets to masters which serve requests for that tablet.
 * The tablet map is the definitive truth about tablet ownership in
 * a cluster.
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

    /// Thrown while trying to create a table that already exists.
    struct TableExists : public Exception {
        explicit TableExists(const CodeLocation& where) : Exception(where) {}
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

    explicit TableManager(Context* context);
    ~TableManager();

    uint64_t createTable(const char* name, uint32_t serverSpan);
    string debugString() const;
    void dropTable(const char* name);
    uint64_t getTableId(const char* name);
    vector<Tablet> markAllTabletsRecovering(ServerId serverId);
    void reassignTabletOwnership(ServerId newOwner, uint64_t tableId,
                                 uint64_t startKeyHash, uint64_t endKeyHash,
                                 uint64_t ctimeSegmentId,
                                 uint64_t ctimeSegmentOffset);
    void serialize(AbstractServerList* serverList,
                   ProtoBuf::Tablets* tablets) const;
    void splitTablet(const char* name,
                     uint64_t splitKeyHash);
    void tabletRecovered(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         ServerId serverId,
                         Log::Position ctime);

    void recoverAliveTable(ProtoBuf::TableInformation* state,
                           EntryId entryId);
    void recoverCreateTable(ProtoBuf::TableInformation* state,
                            EntryId entryId);
    void recoverDropTable(ProtoBuf::TableDrop* state,
                          EntryId entryId);
    void recoverLargestTableId(ProtoBuf::LargestTableId* state,
                               EntryId entryId);
    void recoverSplitTablet(ProtoBuf::SplitTablet* state,
                            EntryId entryId);
    void recoverTabletRecovered(ProtoBuf::TabletRecovered* state,
                                EntryId entryId);

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

  PRIVATE:

    /**
     * Defines methods and stores data to create a table.
     */
    class CreateTable {
      public:
        CreateTable(TableManager &tm,
                    const Lock& lock,
                    const char* name,
                    uint64_t tableId,
                    uint32_t serverSpan,
                    ProtoBuf::TableInformation state =
                                ProtoBuf::TableInformation())
            : tm(tm), lock(lock),
              name(name),
              tableId(tableId),
              serverSpan(serverSpan),
              state(state) {}
        uint64_t execute();
        uint64_t complete(EntryId entryId);

      private:
        /**
         * Reference to the instance of TableManager initializing this class.
         */
        TableManager &tm;
        /**
         * Explicitly needs a TableManager lock.
         */
        const Lock& lock;
        /**
         * Name for the table to be created.
         */
        const char* name;
        /**
         * tableId of the table created.
         */
        uint64_t tableId;
        /**
         * Number of servers across which this table should be split during
         * creation.
         */
        uint32_t serverSpan;
        /**
         * State information for this operation that was logged to LogCabin.
         * This is used to get the computed tablet to master mappings for
         * each tablet in this table.
         */
        ProtoBuf::TableInformation state;
        DISALLOW_COPY_AND_ASSIGN(CreateTable);
    };

    /**
     * Defines methods and stores data to create a table.
     */
    class DropTable {
      public:
        DropTable(TableManager &tm,
                  const Lock& lock,
                  const char* name)
            : tm(tm), lock(lock),
              name(name) {}
        void execute();
        void complete(EntryId entryId);

      private:
        /**
         * Reference to the instance of TableManager initializing this class.
         */
        TableManager &tm;
        /**
         * Explicitly needs a TableManager lock.
         */
        const Lock& lock;
        /**
         * Name for the table to be dropped.
         */
        const char* name;
        DISALLOW_COPY_AND_ASSIGN(DropTable);
    };

    /**
     * Defines methods and stores data to split a tablet in the tablet map.
     */
    class SplitTablet {
      public:
        SplitTablet(TableManager &tm,
                    const Lock& lock,
                    const char* name,
                    uint64_t splitKeyHash)
            : tm(tm), lock(lock),
              name(name),
              splitKeyHash(splitKeyHash) {}
        void execute();
        void complete(EntryId entryId);

      private:
        /**
         * Reference to the instance of TableManager initializing this class.
         */
        TableManager &tm;
        /**
         * Explicitly needs a TableManager lock.
         */
        const Lock& lock;
        /**
         * Name for the table containing the tablet.
         */
        const char* name;
        /**
         * Key hash to used to partition the tablet into two. Keys less than
         * \a splitKeyHash belong to one Tablet, keys greater than or equal to
         * \a splitKeyHash belong to the other.
         */
        uint64_t splitKeyHash;
        DISALLOW_COPY_AND_ASSIGN(SplitTablet);
    };

    /**
     * Defines methods and stores data to record the new master of a tablet
     * after it was recovered by MasterRecoveryManager.
     */
    class TabletRecovered {
      public:
        TabletRecovered(TableManager &tm,
                        const Lock& lock,
                        uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash,
                        ServerId serverId,
                        Log::Position ctime)
            : tm(tm), lock(lock),
              tableId(tableId),
              startKeyHash(startKeyHash),
              endKeyHash(endKeyHash),
              serverId(serverId),
              ctime(ctime) {}
        void execute();
        void complete(EntryId entryId);

      private:
        /**
         * Reference to the instance of TableManager initializing this class.
         */
        TableManager &tm;
        /**
         * Explicitly needs a TableManager lock.
         */
        const Lock& lock;
        /**
         * Table id of the tablet.
         */
        uint64_t tableId;
        /**
         * First key hash that is part of range of key hashes for the tablet.
         */
        uint64_t startKeyHash;
        /**
         * Last key hash that is part of range of key hashes for the tablet.
         */
        uint64_t endKeyHash;
        /**
         * Tablet is updated to indicate that it is owned by \a serverId.
         */
        ServerId serverId;
        /**
         * Tablet is updated with this ctime indicating any object earlier
         * than \a ctime in its log cannot contain objects belonging to it.
         */
        Log::Position ctime;
        DISALLOW_COPY_AND_ASSIGN(TabletRecovered);
    };

    void addTablet(const Lock& lock, const Tablet& tablet);
    Tablet& find(const Lock& lock,
                 uint64_t tableId,
                 uint64_t startKeyHash,
                 uint64_t endKeyHash);
    const Tablet& cfind(const Lock& lock,
                        uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash) const;
    bool splitExists(const Lock& lock,
                     uint64_t tableId,
                     uint64_t splitKeyHash);
    Tablet& getTabletSplit(const Lock& lock,
                            uint64_t tableId,
                            uint64_t splitKeyHash);
    EntryId getTableInfoLogId(const Lock& lock,
                              uint64_t tableId);
    Tablet getTablet(const Lock& lock,
                     uint64_t tableId,
                     uint64_t startKeyHash,
                     uint64_t endKeyHash) const;
    vector<Tablet> getTabletsForTable(const Lock& lock, uint64_t tableId) const;
    void modifyTablet(const Lock& lock,
                      uint64_t tableId,
                      uint64_t startKeyHash,
                      uint64_t endKeyHash,
                      ServerId serverId,
                      Tablet::Status status,
                      Log::Position ctime);
    vector<Tablet> removeTabletsForTable(const Lock& lock, uint64_t tableId);
    void setTableInfoLogId(const Lock& lock,
                           uint64_t tableId,
                           EntryId entryId);
    size_t size(const Lock& lock) const;

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * LogCabin EntryId corresponding to the LargestTableId entry.
     */
    EntryId logIdLargestTableId;

    /**
     * List of tablets that make up the current set of tables in a cluster.
     */
    vector<Tablet> map;

    /**
     * The id of the next table to be created.
     * These start at 0 and are never reused.
     */
    uint64_t nextTableId;

    /**
     * Used in #createTable() to assign new tables to masters.
     * If you take this modulo the number of entries in #masterList, you get
     * the index into #masterList of the master that should be assigned the
     * next table.
     */
    uint32_t nextTableMasterIdx;

    typedef std::map<string, uint64_t> Tables;
    /**
     * Map from table name to table id.
     */
    Tables tables;

    struct TableLogIds {
        EntryId tableInfoLogId;
        EntryId tableIncompleteOpLogId;
    };
    typedef std::map<uint64_t, TableLogIds> TablesLogIds;
    /**
     * Map from table id to LogCabin EntryId where the information corresponding
     * to this table was logged.
     */
    TablesLogIds tablesLogIds;

    DISALLOW_COPY_AND_ASSIGN(TableManager);
};

} // namespace RAMCloud

#endif

