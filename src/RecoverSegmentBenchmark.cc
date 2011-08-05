/* Copyright (c) 2010-2011 Stanford University
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
#include "Logging.h"
#include "MasterService.h"
#include "Tablets.pb.h"

namespace RAMCloud {

class RecoverSegmentBenchmark {

  public:
    ServerConfig config;
    MasterService* service;

    RecoverSegmentBenchmark(string logSize, string hashTableSize,
        int numSegments)
        : config()
        , service(NULL)
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        MasterService::sizeLogAndHashTable(logSize, hashTableSize, &config);
        service = new MasterService(config, NULL, 0);
        service->serverId.construct(1);
    }

    ~RecoverSegmentBenchmark()
    {
        delete service;
    }

    void
    run(int numSegments, int objectBytes)
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
            segments[i] = new Segment((uint64_t)0, i, p,
                Segment::SEGMENT_SIZE, NULL);
            while (1) {
                DECLARE_OBJECT(o, objectBytes);
                o->id.objectId = nextObjId++;
                o->id.tableId = 0;
                o->version = 0;
                SegmentEntryHandle seh = segments[i]->append(LOG_ENTRY_TYPE_OBJ,
                    o, o->objectLength(objectBytes));
                if (seh == NULL)
                    break;
                numObjects++;
            }
            segments[i]->close();
        }

        /* Update the list of Tablets */
        ProtoBuf::Tablets_Tablet tablet;
        tablet.set_table_id(0);
        tablet.set_start_object_id(0);
        tablet.set_end_object_id(nextObjId - 1);
        tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet.set_server_id(service->serverId);
        ProtoBuf::Tablets tablets;
        *tablets.add_tablet() = tablet;
        service->setTablets(tablets);

        /*
         * Now run a fake recovery.
         */
        uint64_t before = Cycles::rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment *s = segments[i];
            service->recoverSegment(s->getId(), s->getBaseAddress(),
                s->getCapacity());
        }
        uint64_t ticks = Cycles::rdtsc() - before;

        uint64_t totalObjectBytes = numObjects * objectBytes;
        uint64_t totalSegmentBytes = numSegments * Segment::SEGMENT_SIZE;
        printf("Recovery of %d %dKB Segments with %d byte Objects took %lu "
            "milliseconds\n", numSegments, Segment::SEGMENT_SIZE / 1024,
            objectBytes, RAMCloud::Cycles::toNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 *
            static_cast<double>(totalSegmentBytes - totalObjectBytes) /
            static_cast<double>(totalSegmentBytes));

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
    int numSegments = 80;
    int objectBytes[] = { 64, 128, 256, 512, 1024, 2048, 8192, 0 };

    for (int i = 0; objectBytes[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::RecoverSegmentBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, objectBytes[i]);
    }

    return 0;
}
