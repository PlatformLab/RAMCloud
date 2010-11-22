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

#include "BenchUtil.h"
#include "ClientException.h"
#include "Logging.h"
#include "MasterServer.h"

namespace RAMCloud {

class RecoverSegmentBenchmark {

  public:
    ServerConfig config;
    MasterServer* server;

    RecoverSegmentBenchmark(string logSize, string hashTableSize,
        int numSegments)
        : config()
        , server(NULL)
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        MasterServer::sizeLogAndHashTable(logSize, hashTableSize, &config);
        server = new MasterServer(config, NULL, NULL);
    }

    ~RecoverSegmentBenchmark()
    {
        delete server;
    }

    void
    run(int numSegments, int objectBytes, int tombstoneMapBytes)
    {
        /*
         * Allocate numSegments Segments and fill them up objects of
         * objectBytes bytes. These will be the Segment we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextObjId = 0;
        Segment *segments[numSegments];
        for (int i = 0; i < numSegments; i++) {
            void *p = xmalloc(Segment::SEGMENT_SIZE);
            segments[i] = new Segment(0, 0, p, Segment::SEGMENT_SIZE, NULL);
            while (1) {
                DECLARE_OBJECT(o, objectBytes);
                o->id = nextObjId++;
                o->table = 0;
                o->version = 0;
                o->checksum = 0;
                o->data_len = objectBytes;
                const void *so = segments[i]->append(LOG_ENTRY_TYPE_OBJ,
                    o, o->size());
                if (so == NULL)
                    break;
                numObjects++;
            }
            segments[i]->close();
        }

        /*
         * Caution: This is a little fragile. We don't want to have to call
         * MasterServer::Recover(), which sets up the tombstone map, so do
         * it manually.
         */
        server->tombstoneMap = new ObjectTombstoneMap(tombstoneMapBytes /
            ObjectTombstoneMap::bytesPerCacheLine());

        /*
         * Now run a fake recovery.
         */
        uint64_t before = rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment *s = segments[i];
            server->recoverSegment(s->getId(), s->getBaseAddress(),
                s->getCapacity());
        }
        uint64_t ticks = rdtsc() - before;

        uint64_t totalObjectBytes = numObjects * objectBytes;
        uint64_t totalSegmentBytes = numSegments * Segment::SEGMENT_SIZE;
        printf("Recovery of %d %dKB Segments with %d byte Objects took %lu "
            "milliseconds\n", numSegments, Segment::SEGMENT_SIZE / 1024,
            objectBytes, RAMCloud::cyclesToNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 * (totalSegmentBytes - totalObjectBytes) / totalSegmentBytes);

        // clean up
        for (int i = 0; i < numSegments; i++) {
            free(const_cast<void *>(segments[i]->getBaseAddress()));
            delete segments[i];
        }
    }

    DISALLOW_COPY_AND_ASSIGN(RecoverSegmentBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    int numSegments = 20;
    int objectBytes[] = { 64, 128, 256, 512, 1024, 2048, 8192, 0 };

    for (int i = 0; objectBytes[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::RecoverSegmentBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, objectBytes[i], 64 * 1024 * 1024);
    }

    return 0;
}
