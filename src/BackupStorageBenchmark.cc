/* Copyright (c) 2011 Stanford University
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

#include <thread>

#include "BackupStorage.h"
#include "Cycles.h"
#include "Logging.h"
#include "Segment.h"

using namespace RAMCloud;

struct Store {
    Store(MultiFileStorage& storage,
          const char* segment,
          BackupStorage::Handle* handle)
        : storage(storage)
        , segment(segment)
        , handle(handle)
    {
    }

    void
    operator()()
    {
        uint64_t startTime = Cycles::rdtsc();
        storage.putSegment(handle, segment);
        LOG(DEBUG, "StoreOp took %lu us",
            Cycles::toNanoseconds(Cycles::rdtsc() - startTime) / 1000);
    }

    MultiFileStorage& storage;
    const char* segment;
    BackupStorage::Handle* handle;
};

struct Bench {
    explicit Bench(const char* backupFile)
        : backupFile(backupFile)
        , segmentCount(80 * 2)
        , storage(Segment::SEGMENT_SIZE,
                  segmentCount,
                  backupFile,
                  O_DIRECT | O_SYNC | O_NOATIME)
        , allocated()
        , scratch(SegmentAllocator::malloc(Segment::SEGMENT_SIZE))
        , mb(static_cast<double>(Segment::SEGMENT_SIZE) / (1 << 20))
    {
    }

    ~Bench()
    {
        SegmentAllocator::free(scratch);
    }

    void
    fill(const uint32_t count)
    {
        double sum = 0., min = 100000.0, max = 0.;
        for (uint32_t i = 0; i < count; i++) {
            auto* handle = storage.allocate(0, i);
            allocated.push_back(handle);
            uint64_t start = Cycles::rdtsc();
            storage.putSegment(handle, scratch);
            uint64_t putTime = Cycles::rdtsc() - start;
            double mbSec = mb / Cycles::toSeconds(putTime);
            LOG(NOTICE, "Stored segment at %.1f MB/s", mbSec);
            sum += mbSec;
            max = std::max(max, mbSec);
            min = std::min(min, mbSec);
        }
        LOG(WARNING, "=== Min/Avg/Max Put Bandwidth: %.1f %.1f %.1f ===",
            min, sum / count, max);
    }

    void
    testRead()
    {
        const uint32_t count = 80;

        fill(count);
        usleep(1000000);

        double sum = 0., min = 100000.0, max = 0.;
        foreach (auto* handle, allocated) {
            uint64_t start = Cycles::rdtsc();
            storage.getSegment(handle, scratch);
            uint64_t getTime = Cycles::rdtsc() - start;
            double mbSec = mb / Cycles::toSeconds(getTime);
            LOG(NOTICE, "Fetched segment at %.1f MB/s", mbSec);
            sum += mbSec;
            max = std::max(max, mbSec);
            min = std::min(min, mbSec);
        }
        LOG(WARNING, "=== Min/Avg/Max Get Bandwidth: %.1f %.1f %.1f ===",
            min, sum / count, max);
    }

    void
    testReadWithWriteInterference()
    {
        const uint32_t count = 80;

        fill(count);
        usleep(1000000);

        auto writeList = allocated;
        std::random_shuffle(writeList.begin(), writeList.end());

        auto writeHandle = writeList.begin();

        double sum = 0., min = 100000.0, max = 0.;
        int i = 0;
        foreach (auto* handle, allocated) {
            if (!(i++ % 3))
                std::thread(Store(storage, scratch, *(writeHandle++)));
            uint64_t start = Cycles::rdtsc();
            storage.getSegment(handle, scratch);
            uint64_t getTime = Cycles::rdtsc() - start;
            double mbSec = mb / Cycles::toSeconds(getTime);
            LOG(NOTICE, "Fetched segment at %.1f MB/s", mbSec);
            sum += mbSec;
            max = std::max(max, mbSec);
            min = std::min(min, mbSec);
        }
        LOG(WARNING, "=== Min/Avg/Max Get Bandwidth: %.1f %.1f %.1f ===",
            min, sum / count, max);
    }

    const string backupFile;
    const uint32_t segmentCount;
    MultiFileStorage storage;
    vector<BackupStorage::Handle*> allocated;
    char* const scratch;
    const double mb;

    DISALLOW_COPY_AND_ASSIGN(Bench);
};

int
main(int ac, char* av[])
{
    const char* backupFile = "/var/tmp/backup.log";
    if (ac > 1)
        backupFile = av[1];
    LOG(WARNING, "Writing to %s", backupFile);

    Bench(backupFile).testRead();
    //Bench(backupFile).testReadWithWriteInterference();

    return 0;
}
