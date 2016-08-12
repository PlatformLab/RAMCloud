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

BackupStorage::BackupStorage(size_t segmentSize,
                             Type storageType,
                             size_t writeRateLimit)
    : segmentSize(segmentSize)
    , writeRateLimit(writeRateLimit)
    , storageType(storageType)
{
}

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
    BackupStorage::FrameRef frames[count];

    for (uint32_t i = 0; i < count; ++i) {
        frames[i] = open(true, ServerId(), 0);
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

/**
 * Release the frame for reuse with another replica.
 * Called implicitly when the reference count associated with a FrameRef
 * drops to zero.
 */
void
BackupStorage::freeFrame(Frame* frame)
{
    frame->free();
}

/**
 * Called by subclass instances to throttle the client-perceived write
 * throughput after each write operation.
 *
 * \param count
 *      The number of bytes written to storage in the last operation.
 * \param ticks
 *      The number of actual ticks it took to write 'count' bytes to storage.
 */
void
BackupStorage::sleepToThrottleWrites(size_t count, uint64_t ticks) const
{
    if (writeRateLimit == 0)
        return;

    double rateLimit = static_cast<double>(writeRateLimit);
    double minAllowedTime = static_cast<double>(count) / 1048576 / rateLimit;
    double actualTime = Cycles::toSeconds(ticks);
    if (actualTime < minAllowedTime) {
        double delaySec = minAllowedTime - actualTime;
        useconds_t delayUsec = (useconds_t) (delaySec * 1.0e6 + 0.5);
        usleep(delayUsec);
        TEST_LOG("delayed %u usec", delayUsec);
    }
}

} // namespace RAMCloud
