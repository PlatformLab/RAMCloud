/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_TABLET_H
#define RAMCLOUD_TABLET_H

#include "Tablets.pb.h"

#include "Common.h"
#include "Log.h"
#include "LogEntryTypes.h"
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

    Tablet(uint64_t tableId, uint64_t startKeyHash, uint64_t endKeyHash,
            ServerId serverId, Status status, LogPosition ctime)
        : tableId(tableId)
        , startKeyHash(startKeyHash)
        , endKeyHash(endKeyHash)
        , serverId(serverId)
        , status(status)
        , ctime(ctime)
    {}

    Tablet(const Tablet& tablet)
        : tableId(tablet.tableId)
        , startKeyHash(tablet.startKeyHash)
        , endKeyHash(tablet.endKeyHash)
        , serverId(tablet.serverId)
        , status(tablet.status)
        , ctime(tablet.ctime)
    {}

    void serialize(ProtoBuf::Tablets::Tablet& entry) const;
    string debugString(int verbose = 0) const;
};

} // namespace RAMCloud

#endif

