/* Copyright (c) 2010-2012 Stanford University
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

#include "BackupStorage.h"
#include "CycleCounter.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- BackupStorage ---

/**
 * Report the read speed of this storage in MB/s.
 *
 * \return
 *      Storage read speed in MB/s.
 */
uint32_t
BackupStorage::benchmark(BackupStrategy backupStrategy)
{
    const uint32_t count = 16;
    uint32_t readSpeeds[count];
    BackupStorage::Frame* frames[count];

    try {
        for (uint32_t i = 0; i < count; ++i)
            frames[i] = NULL;

        for (uint32_t i = 0; i < count; ++i) {
            frames[i] = open(true);
            frames[i]->close();
        }

        for (uint32_t i = 0; i < count; ++i) {
            CycleCounter<> counter;
            frames[i]->load();
            uint64_t ns = Cycles::toNanoseconds(counter.stop());
            readSpeeds[i] = downCast<uint32_t>(
                                segmentSize * 1000UL * 1000 * 1000 /
                                (1 << 20) / ns);
        }
    } catch (...) {
        for (uint32_t i = 0; i < count; ++i)
            if (frames[i])
                frames[i]->free();
        throw;
    }

    for (uint32_t i = 0; i < count; ++i)
        frames[i]->free();

    uint32_t minRead = *std::min_element(readSpeeds,
                                         readSpeeds + count);
    uint32_t avgRead = ({
        uint32_t sum = 0;
        foreach (uint32_t speed, readSpeeds)
            sum += speed;
        sum / count;
    });

    LOG(NOTICE, "Backup storage speeds (min): %u MB/s read", minRead);
    LOG(NOTICE, "Backup storage speeds (avg): %u MB/s read,", avgRead);

    if (backupStrategy == RANDOM_REFINE_MIN) {
        LOG(NOTICE, "RANDOM_REFINE_MIN BackupStrategy selected");
        return minRead;
    } else if (backupStrategy == RANDOM_REFINE_AVG) {
        LOG(NOTICE, "RANDOM_REFINE_AVG BackupStrategy selected");
        return avgRead;
    } else if (backupStrategy == EVEN_DISTRIBUTION) {
        LOG(NOTICE, "EVEN_SELECTION BackupStrategy selected");
        return 100;
    } else {
        DIE("Bad BackupStrategy selected");
    }
}

} // namespace RAMCloud
