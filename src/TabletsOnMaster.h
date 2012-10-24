/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_TABLETSONMASTER_H
#define RAMCLOUD_TABLETSONMASTER_H

#include "Common.h"
#include "Object.h"
#include "HashTable.h"
#include "ServerStatistics.pb.h"

namespace RAMCloud {

/**
 * This class keeps information for the subset of a table stored on a
 * particular master. Multiple tablets of the same table that happen to be
 * co-located on a single master will all refer to a single
 * TabletsOnMaster object.
 *
 * This class is pretty thin right now. Eventually, it's likely that some
 * access control stuff will go in here.
 */
class TabletsOnMaster {
  public:

    explicit TabletsOnMaster(uint64_t tableId, uint64_t start_key_hash,
                             uint64_t end_key_hash)
        : objectCount(0),
          objectBytes(0),
          tombstoneCount(0),
          tombstoneBytes(0),
          statEntry(),
          tableId(tableId)
    {
        statEntry.set_table_id(tableId);
        statEntry.set_start_key_hash(start_key_hash);
        statEntry.set_end_key_hash(end_key_hash);
    }

    /**
     * Get the Table's identifier.
     */
    uint64_t getId() {
        return tableId;
    }

    uint64_t objectCount;
    uint64_t objectBytes;
    uint64_t tombstoneCount;
    uint64_t tombstoneBytes;
    ProtoBuf::ServerStatistics_TabletEntry statEntry;

  private:

    /**
     * The unique numerical identifier for this table.
     */
    uint64_t tableId;

    DISALLOW_COPY_AND_ASSIGN(TabletsOnMaster);
};

} // namespace RAMCloud

#endif // RAMCLOUD_TABLETSONMASTER_H
