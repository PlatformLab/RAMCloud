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

#include "ClientException.h"
#include "Cycles.h"
#include "Logger.h"
#include "MasterService.h"
#include "Memory.h"
#include "SegmentIterator.h"
#include "Seglet.h"
#include "Tablets.pb.h"

namespace RAMCloud {

class RecoverSegmentBenchmark {

  public:
    Context context;
    ServerConfig config;
    ServerList serverList;
    MasterService* service;

    RecoverSegmentBenchmark(string logSize, string hashTableSize,
        int numSegments)
        : context()
        , config(ServerConfig::forTesting())
        , serverList(&context)
        , service(NULL)
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
        delete service;
    }

    void
    run(int numSegments, int dataLen)
    {
        /*
         * Allocate numSegments Segments and fill them up with objects of
         * size dataLen. These will be the Segments that we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextKeyVal = 0;
        Segment *segments[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = new Segment();
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

        /*
         * Now run a fake recovery.
         */
        SideLog sideLog(service->objectManager.getLog());
        uint64_t before = Cycles::rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment* s = segments[i];
            Buffer buffer;
            s->appendToBuffer(buffer);
            Segment::Certificate certificate;
            s->getAppendedLength(&certificate);
            const void* contigSeg = buffer.getRange(0, buffer.size());
            SegmentIterator it(contigSeg, buffer.size(), certificate);
            service->objectManager.replaySegment(&sideLog, it);
        }
        uint64_t ticks = Cycles::rdtsc() - before;

        uint64_t totalObjectBytes = numObjects * dataLen;
        uint64_t totalSegmentBytes = numSegments *
                                     Segment::DEFAULT_SEGMENT_SIZE;
        printf("Recovery of %d %dKB Segments with %d byte Objects took %lu "
            "ms\n", numSegments, Segment::DEFAULT_SEGMENT_SIZE / 1024,
            dataLen, RAMCloud::Cycles::toNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 *
            static_cast<double>(totalSegmentBytes - totalObjectBytes) /
            static_cast<double>(totalSegmentBytes));

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

        // clean up
        for (int i = 0; i < numSegments; i++) {
            delete segments[i];
        }
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverSegmentBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    int numSegments = 600 / 8; // = 72.
    int dataLen[] = { 64, 128, 256, 512, 1024, 2048, 8192, 0 };

    for (int i = 0; dataLen[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::RecoverSegmentBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, dataLen[i]);
    }

    return 0;
}
