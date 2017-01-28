/* Copyright (c) 2017 Stanford University
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

#ifndef RAMCLOUD_WITNESSTRACKER_H
#define RAMCLOUD_WITNESSTRACKER_H

#include <stack>
#include <unordered_map>
#include "Common.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Used in RAMCloud client. Trackes all witness entries this client has recorded
 * and provides garbage collectable entries.
 */
class WitnessTracker {
  PUBLIC:
    explicit WitnessTracker();
    ~WitnessTracker();

    void free(uint64_t witnessServerId,
              uint64_t targetMasterId,
              int16_t hashIndex);
    void getDeletable(uint64_t witnessServerId,
                      uint64_t targetMasterId,
                      int16_t deletableIndices[]);

  PRIVATE:

    struct WitnessTableId {
        uint64_t witnessServerId;
        uint64_t targetMasterId;


        bool operator==(const WitnessTableId& other) const
        {
            return witnessServerId == other.witnessServerId &&
                    targetMasterId == other.targetMasterId;
        }
    };

    /**
     * This class computes a hash of an WitnessTableId, so that WitnessTableId
     * can be used as keys in unordered_maps.
     */
    struct Hasher {
        std::size_t operator()(const WitnessTableId& id) const {
            std::size_t h1 = std::hash<uint64_t>()(id.witnessServerId);
            std::size_t h2 = std::hash<uint64_t>()(id.targetMasterId);
            return h1 ^ (h2 << 1);
        }
    };

    std::unordered_map<WitnessTableId, std::stack<int16_t>, Hasher> deletable;

    /**
     * Monitor-style lock. Any operation on internal data structure should
     * hold this lock.
     */
    std::mutex mutex;
    typedef std::lock_guard<std::mutex> Lock;

    DISALLOW_COPY_AND_ASSIGN(WitnessTracker);
};

}  // namespace RAMCloud

#endif // RAMCLOUD_WITNESSTRACKER_H
