/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_WILL_H
#define RAMCLOUD_WILL_H

#include <vector>
#include "Common.h"
#include "Tablets.pb.h"
#include "TabletProfiler.h"

namespace RAMCloud {

class Will {
  public:
    Will(ProtoBuf::Tablets& tablets, uint64_t maxBytesPerPartition,
         uint64_t maxReferantsPerPartition);
    void debugDump();

  private:
    struct WillEntry {
        uint64_t partitionId;
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        uint64_t minBytes;
        uint64_t maxBytes;
        uint64_t minReferants;
        uint64_t maxReferants;
    };
    typedef std::vector<WillEntry> WillList;

    // current partition state
    uint64_t currentId;
    uint64_t currentMaxBytes;
    uint64_t currentMaxReferants;

    // current number of TabletProfiler partitions added since the
    // last currentId increment
    uint64_t currentCount;

    // parameters dictating partition sizes
    uint64_t maxBytesPerPartition;
    uint64_t maxReferantsPerPartition;

    // list tablets, ordered by partition
    WillList entries;

    void     addTablet(const ProtoBuf::Tablets::Tablet& tablet);
    void     addPartition(Partition& partition,
                          const ProtoBuf::Tablets::Tablet& tablet);


    friend class WillBenchmark;
    friend class WillTest;

    DISALLOW_COPY_AND_ASSIGN(Will);
};

} // namespace

#endif // !RAMCLOUD_WILL_H
