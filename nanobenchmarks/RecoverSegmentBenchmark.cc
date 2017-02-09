/* Copyright (c) 2010-2015 Stanford University
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

#include "ClientException.h"
#include "Cycles.h"
#include "Logger.h"
#include "MasterService.h"
#include "Memory.h"
#include "SegmentIterator.h"
#include "Seglet.h"
#include "Tablets.pb.h"
#include "Util.h"

namespace RAMCloud {

class RecoverSegmentBenchmark {

  public:
    Context context;
    ServerConfig config;
    ServerList serverList;
    MasterService* service;
    size_t numSegments;
    bool hwThreadsBeforeCores;

    std::atomic<size_t> next;
    std::vector<Segment*> segments;
    std::atomic<size_t> nReady;
    std::atomic<bool> go;
    std::atomic<size_t> nDone;

    RecoverSegmentBenchmark(
        string logSize,
        string hashTableSize,
        size_t numSegments,
        bool hwThreadsBeforeCores)
        : context()
        , config(ServerConfig::forTesting())
        , serverList(&context)
        , service(NULL)
        , numSegments{numSegments}
        , hwThreadsBeforeCores{hwThreadsBeforeCores}
        , next{}
        , segments{}
        , nReady{}
        , go{}
        , nDone{}
    {
        Logger::get().setLogLevels(WARNING);
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        config.setLogAndHashTableSize(logSize, hashTableSize);
        config.services = {WireFormat::MASTER_SERVICE};
        config.master.numReplicas = 0;
        config.segmentSize = Segment::DEFAULT_SEGMENT_SIZE;
        config.segletSize = Seglet::DEFAULT_SEGLET_SIZE;
        service = new MasterService(&context, &config);
        service->setServerId({1, 0});
    }

    ~RecoverSegmentBenchmark()
    {
        for (Segment* segment : segments)
            delete segment;
        delete service;
    }

    Segment*
    getNextSegment()
    {
        size_t i = next++;
        if (i < numSegments)
            return segments[i];
        return nullptr;
    }

    void
    doReplay(int threadId)
    {
        if (!hwThreadsBeforeCores) {
            // Prefer Linux's core enumeration order: core-to-core, then across
            // sockets, then loop back over hyperthreads.
            Util::pinThreadToCore(threadId);
        } else {
            // Can also hack to prefer colocated hyperthreads to more cores.
            Util::pinThreadToCore(((threadId % 2) * 8) + (threadId / 2));
        }

        SideLog sideLog(service->objectManager.getLog());

        nReady++;
        while (!go);

        Segment* s = nullptr;
        while ((s = getNextSegment()) != nullptr) {
            Buffer buffer;
            s->appendToBuffer(buffer);
            SegmentCertificate certificate;
            s->getAppendedLength(&certificate);
            const void* contigSeg = buffer.getRange(0, buffer.size());
            SegmentIterator it(contigSeg, buffer.size(), certificate);
            service->objectManager.replaySegment(&sideLog, it);
        }

        nDone++;

        sideLog.commit();
    }

    void
    run(uint32_t dataLen, size_t nThreads)
    {
        /*
         * Allocate numSegments Segments and fill them up with objects of
         * size dataLen. These will be the Segments that we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextKeyVal = 0;
        for (size_t i = 0; i < numSegments; i++) {
            segments.push_back(new Segment());
            while (1) {
                Key key(0, &nextKeyVal, sizeof(nextKeyVal));

                char objectData[dataLen];

                Buffer dataBuffer;
                Object object(key, objectData, dataLen, 0, 0, dataBuffer);

                Buffer buffer;
                object.assembleForLog(buffer);
                if (!segments[i]->append(LOG_ENTRY_TYPE_OBJ, buffer))
                    break;
                nextKeyVal++;
                numObjects++;
            }
            segments[i]->close();
        }

        /* Update the list of Tablets */
        service->tabletManager.addTablet(0, 0, ~0UL, TabletManager::NORMAL);

        metrics->temp.ticks0 =
        metrics->temp.ticks1 =
        metrics->temp.ticks2 =
        metrics->temp.ticks3 =
        metrics->temp.ticks4 =
        metrics->temp.ticks5 =
        metrics->temp.ticks6 =
        metrics->temp.ticks7 =
        metrics->temp.ticks8 =
        metrics->temp.ticks9 = 0;

        metrics->temp.count0 =
        metrics->temp.count1 =
        metrics->temp.count2 =
        metrics->temp.count3 =
        metrics->temp.count4 =
        metrics->temp.count5 =
        metrics->temp.count6 =
        metrics->temp.count7 =
        metrics->temp.count8 =
        metrics->temp.count9 = 0;

        ObjectManager::TombstoneProtector _{&service->objectManager};

        /*
         * Now run a fake recovery.
         */
        std::deque<std::thread> threads{};
        for (size_t i = 0; i < nThreads; ++i)
            threads.emplace_back(&RecoverSegmentBenchmark::doReplay, this, i);

        while (nReady < nThreads)
            usleep(100);
        go = true;

        uint64_t before = Cycles::rdtsc();

        while (nDone < nThreads)
            usleep(10000);

        uint64_t ticks = Cycles::rdtsc() - before;

        for (auto& thread : threads)
            thread.join();

        uint64_t totalObjectBytes = numObjects * dataLen;
        uint64_t totalSegmentBytes = uint64_t(numSegments) *
                                     Segment::DEFAULT_SEGMENT_SIZE;

        printf("%lu threads\n", nThreads);
        printf("Recovery of %lu %uKB Segments with %u byte Objects took %lu "
            "ms\n", numSegments, Segment::DEFAULT_SEGMENT_SIZE / 1024,
            dataLen, RAMCloud::Cycles::toNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 *
            static_cast<double>(totalSegmentBytes - totalObjectBytes) /
            static_cast<double>(totalSegmentBytes));

        double seconds = Cycles::toSeconds(ticks);
        printf("Recovery object throughput: %.2f MB/s\n",
               static_cast<double>(totalObjectBytes) / seconds / 1024. / 1024.);
        printf("Recovery log throughput: %.2f MB/s\n",
              static_cast<double>(totalSegmentBytes) / seconds / 1024. / 1024.);

        printf("\n");
        printf("Verify object checksums: %.2f ms\n",
               Cycles::toSeconds(metrics->master.verifyChecksumTicks.load()) *
               1000.);
        metrics->master.verifyChecksumTicks = 0;

#define DUMP_TEMP_TICKS(i)  \
if (metrics->temp.ticks##i.load()) { \
    printf("temp.ticks%d: %.2f ms\n", i, \
           Cycles::toSeconds(metrics->temp.ticks##i.load()) * \
           1000.); \
    metrics->temp.ticks##i = 0; \
}

#define DUMP_TEMP_COUNT(i)  \
if (metrics->temp.count##i.load()) { \
    printf("temp.count%d: %lu\n", i, \
           metrics->temp.count##i.load()); \
    metrics->temp.count##i = 0; \
}

        DUMP_TEMP_TICKS(0);
        DUMP_TEMP_TICKS(1);
        DUMP_TEMP_TICKS(2);
        DUMP_TEMP_TICKS(3);
        DUMP_TEMP_TICKS(4);
        DUMP_TEMP_TICKS(5);
        DUMP_TEMP_TICKS(6);
        DUMP_TEMP_TICKS(7);
        DUMP_TEMP_TICKS(8);
        DUMP_TEMP_TICKS(9);

        DUMP_TEMP_COUNT(0);
        DUMP_TEMP_COUNT(1);
        DUMP_TEMP_COUNT(2);
        DUMP_TEMP_COUNT(3);
        DUMP_TEMP_COUNT(4);
        DUMP_TEMP_COUNT(5);
        DUMP_TEMP_COUNT(6);
        DUMP_TEMP_COUNT(7);
        DUMP_TEMP_COUNT(8);
        DUMP_TEMP_COUNT(9);
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverSegmentBenchmark);
};

}  // namespace RAMCloud

int
main(int argc, char* argv[])
{
    size_t numSegments = 4096 / 8;
    std::vector<uint32_t> dataLen{ 64, 128, 256, 512, 1024, 2048, 8192 };
    std::vector<size_t> nThreads{ 1, 2, 4, 8, 16 };
    bool hwThreadsBeforeCores = false;

    int c;
    while ((c = getopt(argc, argv, "t:s:h")) != -1) {
      switch (c) {
        case 't':
          nThreads.clear();
          nThreads.emplace_back(atol(optarg));
          break;
        case 's':
          dataLen.clear();
          dataLen.emplace_back(atol(optarg));
          break;
        case 'h':
          hwThreadsBeforeCores = true;
          break;
      }
    }

    for (size_t threads : nThreads) {
        for (uint32_t len : dataLen) {
            printf("==========================\n");
            RAMCloud::RecoverSegmentBenchmark rsb{"8192", "10%",
                                                  numSegments,
                                                  hwThreadsBeforeCores};;
            rsb.run(len, threads);
        }
    }

    return 0;
}
