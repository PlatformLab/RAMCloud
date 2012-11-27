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

#include "CoordinatorServerList.h"
#include "Logger.h"
#include "MasterClient.h"
#include "ShortMacros.h"
#include "TableManager.h"

namespace RAMCloud {

/**
 * Construct a TableManager.
 */
TableManager::TableManager(Context* context)
    : mutex()
    , context(context)
    , map()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , tables()
{
    context->tableManager = this;
}

/**
 * Destructor for TableManager.
 */
TableManager::~TableManager(){}

/**
 * Create a table with the given name.
 * 
 * \param name
 *      Name for the table to be created.
 * \param serverSpan
 *      Number of servers across which this table should be split during
 *      creation.
 * 
 * \return
 *      tableId of the table created.
 * 
 * \throw TableExists
 *      If trying to create a table that already exists.
 */
uint64_t
TableManager::createTable(const char* name, uint32_t serverSpan)
{
    Lock lock(mutex);

    if (tables.find(name) != tables.end())
        throw TableExists(HERE);
    uint64_t tableId = nextTableId++;
    tables[name] = tableId;

    LOG(NOTICE, "Created table '%s' with id %lu", name, tableId);

    if (serverSpan == 0)
        serverSpan = 1;

    for (uint32_t i = 0; i < serverSpan; i++) {
        uint64_t firstKeyHash = i * (~0UL / serverSpan);
        if (i != 0)
            firstKeyHash++;
        uint64_t lastKeyHash = (i + 1) * (~0UL / serverSpan);
        if (i == serverSpan - 1)
            lastKeyHash = ~0UL;

        // Find the next master in the list.
        CoordinatorServerList::Entry master;
        while (true) {
            size_t masterIdx = nextTableMasterIdx++ %
                               context->coordinatorServerList->size();
            CoordinatorServerList::Entry entry;
            try {
                entry = (*context->coordinatorServerList)[masterIdx];
                if (entry.isMaster()) {
                    master = entry;
                    break;
                }
            } catch (ServerListException& e) {
                continue;
            }
        }
        // Get current log head. Only entries >= this can be part of the tablet.
        Log::Position headOfLog = MasterClient::getHeadOfLog(context,
                                                             master.serverId);

        // Create tablet map entry.
        addTablet(lock, {tableId, firstKeyHash, lastKeyHash,
                  master.serverId, Tablet::NORMAL, headOfLog});

        // Inform the master.
        MasterClient::takeTabletOwnership(context, master.serverId, tableId,
                                          firstKeyHash, lastKeyHash);

        LOG(NOTICE,
            "Assigned tablet [0x%lx,0x%lx] in table '%s' (id %lu) to %s",
            firstKeyHash, lastKeyHash, name, tableId,
            context->coordinatorServerList->toString(master.serverId).c_str());
    }

    return tableId;
}

/**
 * Returns a protocol-buffer-like debug string listing the details of each of
 * the tablets currently in the tablet map.
 */
string
TableManager::debugString() const
{
    Lock lock(mutex);
    std::stringstream result;
    for (auto it = map.begin(); it != map.end(); ++it)  {
        if (it != map.begin())
            result << " ";
        const auto& tablet = *it;
        const char* status = "NORMAL";
        if (tablet.status != Tablet::NORMAL)
            status = "RECOVERING";
        result << "Tablet { tableId: " << tablet.tableId
               << " startKeyHash: " << tablet.startKeyHash
               << " endKeyHash: " << tablet.endKeyHash
               << " serverId: " << tablet.serverId.toString().c_str()
               << " status: " << status
               << " ctime: " << tablet.ctime.getSegmentId()
               << ", " << tablet.ctime.getSegmentOffset() <<  " }";
    }
    return result.str();
}

/**
 * Drop the table with the given name.
 * 
 * \param name
 *      Name to identify the table that is to be dropped.
 * 
 * \throw NoSuchTable
 *      If name does not identify a table currently in the tables.
 */
void
TableManager::dropTable(const char* name)
{
    Lock lock(mutex);
    Tables::iterator it = tables.find(name);
    if (it == tables.end())
        return;
    uint64_t tableId = it->second;
    tables.erase(it);
    vector<Tablet> removed = removeTabletsForTable(lock, tableId);
    foreach (const auto& tablet, removed) {
            MasterClient::dropTabletOwnership(context,
                                              tablet.serverId,
                                              tableId,
                                              tablet.startKeyHash,
                                              tablet.endKeyHash);
    }

    LOG(NOTICE, "Dropped table '%s' (id %lu), %lu tablets left in map",
        name, tableId, size(lock));
}

/**
 * Get the tableId of the table with the given name.
 * 
 * \param name
 *      Name to identify the table.
 * 
 * \return
 *      tableId of the table whose name is given.
 * 
 * \throw NoSuchTable
 *      If name does not identify a table currently in the tables.
 */
uint64_t
TableManager::getTableId(const char* name)
{
    Lock lock(mutex);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        throw NoSuchTable(HERE);
    }
    return it->second;
}

/**
 * Public version of modifyTablet. To be called only by MasterRecoveryManager
 * after recovery.
 */
void
TableManager::modifyTabletOnRecovery(
        uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
        ServerId serverId, Tablet::Status status, Log::Position ctime)
{
    Lock lock(mutex);
    modifyTablet(lock, tableId, startKeyHash, endKeyHash,
                 serverId, status, ctime);
}

/**
 * Switch ownership of the tablet and alert the new owner that it may
 * begin servicing requests on that tablet.
 * 
 * \param newOwner
 *      ServerId of the server that will own this tablet at the end of
 *      the operation.
 * \param tableId
 *      Table id of the tablet whose ownership is being reassigned.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet.
 * \param ctimeSegmentId
 *      ServerId of the log head before migration.
 * \param ctimeSegmentOffset
 *      Offset in log head before migration.
 * 
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
void
TableManager::reassignTabletOwnership(
        ServerId newOwner, uint64_t tableId,
        uint64_t startKeyHash, uint64_t endKeyHash,
        uint64_t ctimeSegmentId, uint64_t ctimeSegmentOffset)
{
    Lock lock(mutex);
    // Could throw TableManager::NoSuchTablet exception
    Tablet tablet = getTablet(lock, tableId, startKeyHash, endKeyHash);
    LOG(NOTICE, "Reassigning tablet [0x%lx,0x%lx] in tableId %lu "
        "from %s to %s",
        startKeyHash, endKeyHash, tableId,
        context->coordinatorServerList->toString(tablet.serverId).c_str(),
        context->coordinatorServerList->toString(newOwner).c_str());

    // Get current head of log to preclude all previous data in the log
    // from being considered part of this tablet.
    Log::Position headOfLogAtCreation(ctimeSegmentId,
                                      ctimeSegmentOffset);

    // Could throw TableManager::NoSuchTablet exception
    modifyTablet(lock, tableId, startKeyHash, endKeyHash,
                 newOwner, Tablet::NORMAL, headOfLogAtCreation);

    // TODO(rumble/slaughter) If we fail to alert the new owner we could
    //      get stuck in limbo. What should we do? Retry? Fail the
    //      server and recover it? Can't return to the old master if we
    //      reply early...
    MasterClient::takeTabletOwnership(context, newOwner, tableId,
                                      startKeyHash, endKeyHash);
}

/**
 * Copy the contents of the tablet map into a protocol buffer, \a tablets,
 * suitable for sending across the wire to servers.
 *
 * \param serverList
 *      The single instance of the AbstractServerList. Used to fill in
 *      the service_locator field of entries in \a tablets;
 * \param tablets
 *      Protocol buffer to which entries are added representing each of
 *      the tablets in the tablet map.
 */
void
TableManager::serialize(AbstractServerList& serverList,
                        ProtoBuf::Tablets& tablets) const
{
    Lock lock(mutex);
    foreach (const auto& tablet, map) {
        ProtoBuf::Tablets::Tablet& entry(*tablets.add_tablet());
        tablet.serialize(entry);
        try {
            const char* locator = serverList.getLocator(tablet.serverId);
            entry.set_service_locator(locator);
        } catch (const Exception& e) {
            LOG(NOTICE, "Server id (%s) in tablet map no longer in server "
                "list; sending empty locator for entry",
                tablet.serverId.toString().c_str());
        }
    }
}

/**
 * Update the status of all the Tablets in the tablet map that are on a
 * specific server. Copies of the details about the affected Tablets are
 * returned.
 *
 * \param serverId
 *      Table id of the table whose tablets status should be changed to
 *      \a status.
 * \param status
 *      New status to change the Tablets in the tablet map residing on
 *      the server with id \a serverId.
 * \return
 *      List of copies of all the Tablets in the tablet map which are owned
 *      by the server indicated by \a serverId.
 */
vector<Tablet>
TableManager::setStatusForServer(ServerId serverId, Tablet::Status status)
{
    Lock lock(mutex);
    vector<Tablet> results;
    foreach (Tablet& tablet, map) {
        if (tablet.serverId == serverId) {
            tablet.status = status;
            results.push_back(tablet);
        }
    }
    return results;
}

/**
 * Split a Tablet in the tablet map into two disjoint Tablets at a specific
 * key hash. One Tablet will have all the key hashes
 * [ \a startKeyHash, \a splitKeyHash), the other will have
 * [ \a splitKeyHash, \a endKeyHash ].
 *
 * Also inform the master to split the tablet.
 * 
 * \param name
 *      Name of the table that contains the tablet to be split.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet
 *      to split.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet
 *      to split.
 * \param splitKeyHash
 *      Key hash to used to partition the tablet into two. Keys less than
 *      \a splitKeyHash belong to one Tablet, keys greater than or equal to
 *      \a splitKeyHash belong to the other.
 * 
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 * \throw BadSplit
 *      If \a splitKeyHash is not in the range
 *      ( \a startKeyHash, \a endKeyHash ].
 * \throw NoSuchTable
 *      If name does not identify a table currently in the tables.
 */
void
TableManager::splitTablet(const char* name,
                          uint64_t startKeyHash,
                          uint64_t endKeyHash,
                          uint64_t splitKeyHash)
{
    Lock lock(mutex);
    Tables::iterator it(tables.find(name));
    if (it == tables.end()) {
        throw NoSuchTable(HERE);
    }
    uint64_t tableId = it->second;

    if (startKeyHash >= (splitKeyHash - 1) || splitKeyHash >= endKeyHash) {
        throw BadSplit(HERE);
    }

    Tablet& tablet = find(lock, tableId, startKeyHash, endKeyHash);
    Tablet newTablet = tablet;
    newTablet.startKeyHash = splitKeyHash;
    tablet.endKeyHash = splitKeyHash - 1;
    map.push_back(newTablet);

    // Tell the master to split the tablet
    MasterClient::splitMasterTablet(context, tablet.serverId, tableId,
                                    startKeyHash, endKeyHash, splitKeyHash);
}

/////////////////////////////////////////////////////////////////////////////
//////////////////////////// Private Methods ////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/**
 * Add a Tablet to the tablet map.
 *
 *  \param lock
 *      Explicity needs caller to hold a lock.
 * \param tablet
 *      Tablet entry which is copied into the tablet map.
 */
void
TableManager::addTablet(const Lock& lock, const Tablet& tablet)
{
    map.push_back(tablet);
}

/**
 * Get the details of a Tablet in the tablet map by reference. For internal
 * use only since returning a reference to an internal value is not
 * thread-safe.
 *
 *  \param lock
 *      Explicity needs caller to hold a lock.
 * \param tableId
 *      Table id of the tablet to return the details of.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \return
 *      A copy of the Tablet entry in the tablet map corresponding to
 *      \a tableId, \a startKeyHash, \a endKeyHash.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
Tablet&
TableManager::find(const Lock& lock,
                   uint64_t tableId,
                   uint64_t startKeyHash,
                   uint64_t endKeyHash)
{
    foreach (auto& tablet, map) {
        if (tablet.tableId == tableId &&
            tablet.startKeyHash == startKeyHash &&
            tablet.endKeyHash == endKeyHash)
            return tablet;
    }
    throw NoSuchTablet(HERE);
}

/// Const version of find().
const Tablet&
TableManager::cfind(const Lock& lock,
                    uint64_t tableId,
                    uint64_t startKeyHash,
                    uint64_t endKeyHash) const
{
    return const_cast<TableManager*>(this)->find(lock, tableId,
                                                 startKeyHash, endKeyHash);
}

/**
 * Get the details of a Tablet in the tablet map.
 *
 * \param lock
 *      Explicity needs caller to hold a lock.
 * \param tableId
 *      Table id of the tablet to return the details of.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \return
 *      A copy of the Tablet entry in the tablet map corresponding to
 *      \a tableId, \a startKeyHash, \a endKeyHash.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
Tablet
TableManager::getTablet(const Lock& lock,
                        uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash) const
{
    return cfind(lock, tableId, startKeyHash, endKeyHash);
}

/**
 * Get the details of all the Tablets in the tablet map that are part of a
 * specific table.
 *
 * \param lock
 *      Explicity needs caller to hold a lock.
 * \param tableId
 *      Table id of the table whose tablets are to be returned.
 * \return
 *      List of copies of all the Tablets in the tablet map which are part
 *      of the table indicated by \a tableId.
 */
vector<Tablet>
TableManager::getTabletsForTable(const Lock& lock, uint64_t tableId) const
{
    vector<Tablet> results;
    foreach (const auto& tablet, map) {
        if (tablet.tableId == tableId)
            results.push_back(tablet);
    }
    return results;
}

/**
 * Change the server id, status, or ctime of a Tablet in the tablet map.
 *
 * \param lock
 *      Explicity needs caller to hold a lock.
 * \param tableId
 *      Table id of the tablet to return the details of.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet
 *      to return the details of.
 * \param serverId
 *      Tablet is updated to indicate that it is owned by \a serverId.
 * \param status
 *      Tablet is updated with this status (NORMAL or RECOVERING).
 * \param ctime
 *      Tablet is updated with this Log::Position indicating any object earlier
 *      than \a ctime in its log cannot contain objects belonging to it.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 */
void
TableManager::modifyTablet(const Lock& lock,
                           uint64_t tableId,
                           uint64_t startKeyHash,
                           uint64_t endKeyHash,
                           ServerId serverId,
                           Tablet::Status status,
                           Log::Position ctime)
{
    Tablet& tablet = find(lock, tableId, startKeyHash, endKeyHash);
    tablet.serverId = serverId;
    tablet.status = status;
    tablet.ctime = ctime;
}

/**
 * Remove all the Tablets in the tablet map that are part of a specific table.
 * Copies of the details about the removed Tablets are returned.
 *
 * \param lock
 *      Explicity needs caller to hold a lock.
 * \param tableId
 *      Table id of the table whose tablets are removed.
 * \return
 *      List of copies of all the Tablets in the tablet map which were part
 *      of the table indicated by \a tableId (which have now been removed from
 *      the tablet map).
 */
vector<Tablet>
TableManager::removeTabletsForTable(const Lock& lock, uint64_t tableId)
{
    vector<Tablet> removed;
    auto it = map.begin();
    while (it != map.end()) {
        if (it->tableId == tableId) {
            removed.push_back(*it);
            std::swap(*it, map.back());
            map.pop_back();
        } else {
            ++it;
        }
    }
    return removed;
}

/// Return the number of Tablets in the tablet map.
size_t
TableManager::size(const Lock& lock) const
{
    return map.size();
}

} // namespace RAMCloud
