/* Copyright (c) 2010-2014 Stanford University
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

#ifndef RAMCLOUD_INDEXLETMANAGER_H
#define RAMCLOUD_INDEXLETMANAGER_H

#include "Common.h"

namespace RAMCloud {

/**
 * This class maps from an data table (uniquely identified by a tableId)
 * and an index on that table (identified by indexId) and key range of that
 * index (firstKey - lastKey) to a unique table Id for that Index Partition.
 * An index partition is called an Indexlet.
 * 
 * This IndexletTableId can then be used to get get a session using
 * objectFinder (in the same way as for data tables).
 *
 * It retrieves configuration information from the coordinator and caches it.
 */
class IndexletManager {
  public:
    explicit IndexletManager(Context* context)
        : context(context) {};

    void lookup(uint64_t tableId, uint8_t indexId,
                const void* firstKey, uint16_t firstKeyLength,
                const void* lastKey, uint16_t lastKeyLength,
                uint32_t* numIndexlets, Buffer* indexletTableIds)
    {}

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
