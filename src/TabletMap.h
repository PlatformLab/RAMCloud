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

#ifndef RAMCLOUD_TABLETMAP_H
#define RAMCLOUD_TABLETMAP_H

#include "Common.h"
#include "CoordinatorServerList.h"
#include "LogTypes.h"
#include "ServerId.h"

namespace RAMCloud {

/**
 * Describes a contiguous subrange of key hashes from a single table.
 * Used as the unit of mapping of key to owning master.
 */
struct Tablet {
    /// The id of the containing table.
    uint64_t tableId;

    /// The smallest hash value for a key that is in this tablet.
    uint64_t startKeyHash;

    /// The largest hash value for a key that is in this tablet.
    uint64_t endKeyHash;

    /// The server id of the master owning this tablet.
    ServerId serverId;

    enum Status : uint8_t {
        /// The tablet is available.
        NORMAL = 0 ,
        /// The tablet is being recovered, it is not available.
        RECOVERING = 1,
    };

    /// The status of the tablet, see Status.
    Status status;

    /**
     * The LogPosition of the log belonging to the master that owns this
     * tablet when it was assigned to the server. Any earlier position
     * cannot contain data belonging to this tablet.
     */
    LogPosition ctime;
};

/**
 * Maps tablets to masters which serve requests for that tablet.
 * The tablet map is the definitive truth about tablet ownership in
 * a cluster.
 *
 * Instances are locked for thread-safety, and methods return tablets
 * by-value to avoid inconsistencies due to concurrency.
 */
class TabletMap {
  PUBLIC:
    /**
     * Thrown from methods when the arguments indicate a tablet that is
     * not present in the tablet map.
     */
    struct NoSuchTablet : public Exception {
        explicit NoSuchTablet(const CodeLocation& where) : Exception(where) {}
    };

    /// Thrown in response to invalid splitTablet() arguments.
    struct BadSplit : public Exception {
        explicit BadSplit(const CodeLocation& where) : Exception(where) {}
    };

    TabletMap();
    void addTablet(const Tablet& tablet);
    string debugString() const;
    Tablet getTablet(uint64_t tableId,
                     uint64_t startKeyHash,
                     uint64_t endKeyHash) const;
    vector<Tablet> getTabletsForTable(uint64_t tableId) const;
    void modifyTablet(uint64_t tableId,
                      uint64_t startKeyHash,
                      uint64_t endKeyHash,
                      ServerId serverId,
                      Tablet::Status status,
                      LogPosition ctime);
    vector<Tablet> removeTabletsForTable(uint64_t tableId);
    void serialize(const CoordinatorServerList& serverList,
                   ProtoBuf::Tablets& tablets) const;
    vector<Tablet> setStatusForServer(ServerId serverId,
                                      Tablet::Status status);
    size_t size() const;
    pair<Tablet, Tablet> splitTablet(uint64_t tableId,
                                     uint64_t startKeyHash,
                                     uint64_t endKeyHash,
                                     uint64_t splitKeyHash);

  PRIVATE:
    Tablet& find(uint64_t tableId,
                 uint64_t startKeyHash,
                 uint64_t endKeyHash);
    const Tablet& cfind(uint64_t tableId,
                        uint64_t startKeyHash,
                        uint64_t endKeyHash) const;

    /**
     * Provides monitor-style protection for all operations on the tablet map.
     * A Lock for this mutex must be held to read or modify any state in
     * the tablet map.
     */
    mutable std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    /// List of tablets that make up the current set of tables in a cluster.
    vector<Tablet> map;
};

} // namespace RAMCloud

#endif

