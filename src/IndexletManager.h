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

// TODO(ashgup)

namespace RAMCloud {

/**
 * This class is on every index server.
 * It manages and stores the metadata regarding indexlets (index partitions)
 * stored on this server.
 *
 * The coordinator invokes most of these functions to manage the metadata.
 * 
 * This class has no information about the actual entries in the index
 * (and does not interface with ObjectManager or index tree code).
 */
class IndexletManager {
  public:
    explicit IndexletManager(Context* context)
        : context(context) {};

    // Modify function signature as required. This is just an approximation.
    void takeIndexletOwnership(uint64_t tableId, uint8_t indexId,
                               const void* firstKey, uint16_t firstKeyLength,
                               const void* lastKey, uint16_t lastKeyLength);
    void dropIndexletOwnership(uint64_t tableId, uint8_t indexId,
                               const void* firstKey, uint16_t firstKeyLength,
                               const void* lastKey, uint16_t lastKeyLength);

  PRIVATE:
    /**
     * Shared RAMCloud information.
     */
    Context* context;

    // Keep information about indexlets owned by this server here.

    DISALLOW_COPY_AND_ASSIGN(IndexletManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_INDEXLETMANAGER_H
