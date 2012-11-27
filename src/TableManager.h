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
    void modifyTabletOnRecovery(uint64_t tableId,
                                uint64_t startKeyHash,
                                uint64_t endKeyHash,
                                ServerId serverId,
                                Tablet::Status status,
                                Log::Position ctime);
    void reassignTabletOwnership(ServerId newOwner, uint64_t tableId,
                                 uint64_t startKeyHash, uint64_t endKeyHash,
                                 uint64_t ctimeSegmentId,
                                 uint64_t ctimeSegmentOffset);
    void serialize(AbstractServerList& serverList,
                   ProtoBuf::Tablets& tablets) const;
    vector<Tablet> setStatusForServer(ServerId serverId,
                                      Tablet::Status status);
    void splitTablet(const char* name,
                     uint64_t startKeyHash, uint64_t endKeyHash,
                     uint64_t splitKeyHash);

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

  PRIVATE:
    void addTablet(const Lock& lock, const Tablet& tablet);
    Tablet& find(const Lock& lock,
                 uint64_t tableId,
                 uint64_t startKeyHash,
                 uint64_t endKeyHash);
    const Tablet& cfind(const Lock& lock,
                        uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash) const;
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
    size_t size(const Lock& lock) const;

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /// List of tablets that make up the current set of tables in a cluster.
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

    DISALLOW_COPY_AND_ASSIGN(TableManager);
};

} // namespace RAMCloud

#endif

