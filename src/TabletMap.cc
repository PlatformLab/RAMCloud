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

#include "TabletMap.h"
#include "Logger.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a TabletMap.
 */
TabletMap::TabletMap()
    : mutex()
    , map()
{
}

/**
 * Add a Tablet to the tablet map.
 *
 * \param tablet
 *      Tablet entry which is copied into the tablet map.
 */
void
TabletMap::addTablet(const Tablet& tablet)
{
    Lock _(mutex);
    map.push_back(tablet);
}

/**
 * Returns a protocol-buffer-like debug string listing the details of each of
 * the tablets currently in the tablet map.
 */
string
TabletMap::debugString() const
{
    Lock _(mutex);
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
 * Get the details of a Tablet in the tablet map.
 *
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
TabletMap::getTablet(uint64_t tableId,
                     uint64_t startKeyHash,
                     uint64_t endKeyHash) const
{
    Lock _(mutex);
    return cfind(tableId, startKeyHash, endKeyHash);
}

/**
 * Get the details of all the Tablets in the tablet map that are part of a
 * specific table.
 *
 * \param tableId
 *      Table id of the table whose tablets are to be returned.
 * \return
 *      List of copies of all the Tablets in the tablet map which are part
 *      of the table indicated by \a tableId.
 */
vector<Tablet>
TabletMap::getTabletsForTable(uint64_t tableId) const
{
    Lock _(mutex);
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
TabletMap::modifyTablet(uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash,
                        ServerId serverId,
                        Tablet::Status status,
                        Log::Position ctime)
{
    Lock _(mutex);
    Tablet& tablet = find(tableId, startKeyHash, endKeyHash);
    tablet.serverId = serverId;
    tablet.status = status;
    tablet.ctime = ctime;
}

/**
 * Remove all the Tablets in the tablet map that are part of a specific table.
 * Copies of the details about the removed Tablets are returned.
 *
 * \param tableId
 *      Table id of the table whose tablets are removed.
 * \return
 *      List of copies of all the Tablets in the tablet map which were part
 *      of the table indicated by \a tableId (which have now been removed from
 *      the tablet map).
 */
vector<Tablet>
TabletMap::removeTabletsForTable(uint64_t tableId)
{
    Lock _(mutex);
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
TabletMap::serialize(AbstractServerList& serverList,
                     ProtoBuf::Tablets& tablets) const
{
    Lock _(mutex);
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
TabletMap::setStatusForServer(ServerId serverId, Tablet::Status status)
{
    Lock _(mutex);
    vector<Tablet> results;
    foreach (Tablet& tablet, map) {
        if (tablet.serverId == serverId) {
            tablet.status = status;
            results.push_back(tablet);
        }
    }
    return results;
}

/// Return the number of Tablets in the tablet map.
size_t
TabletMap::size() const
{
    Lock _(mutex);
    return map.size();
}

/**
 * Split a Tablet in the tablet map into two disjoint Tablets at a specific
 * key hash. One Tablet will have all the key hashes
 * [ \a startKeyHash, \a splitKeyHash), the other will have
 * [ \a splitKeyHash, \a endKeyHash ].
 *
 * \param tableId
 *      Table id of the tablet to split.
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
 * \return
 *      Returns a pair containing the two Tablets affected by the split. The
 *      first Tablet contains the lower part of the key hash range, the second
 *      contains the upper part.
 * \throw NoSuchTablet
 *      If the arguments do not identify a tablet currently in the tablet map.
 * \throw BadSplit
 *      If \a splitKeyHash is not in the range
 *      ( \a startKeyHash, \a endKeyHash ].
 */
pair<Tablet, Tablet>
TabletMap::splitTablet(uint64_t tableId,
                       uint64_t startKeyHash,
                       uint64_t endKeyHash,
                       uint64_t splitKeyHash)
{
    if (startKeyHash >= (splitKeyHash - 1) || splitKeyHash >= endKeyHash)
        throw BadSplit(HERE);

    Lock _(mutex);
    Tablet& tablet = find(tableId, startKeyHash, endKeyHash);
    Tablet newTablet = tablet;
    newTablet.startKeyHash = splitKeyHash;
    tablet.endKeyHash = splitKeyHash - 1;
    map.push_back(newTablet);
    return {tablet, newTablet};
}

// - private -

/**
 * Get the details of a Tablet in the tablet map by reference. For internal
 * use only since returning a reference to an internal value is not
 * thread-safe.
 *
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
TabletMap::find(uint64_t tableId,
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
TabletMap::cfind(uint64_t tableId,
                 uint64_t startKeyHash,
                 uint64_t endKeyHash) const
{
    return const_cast<TabletMap*>(this)->find(tableId,
                                              startKeyHash, endKeyHash);
}

} // namespace RAMCloud
