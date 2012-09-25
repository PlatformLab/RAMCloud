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

#include "ClientException.h"
#include "Cycles.h"
#include "Logger.h"
#include "MasterService.h"
#include "Memory.h"
#include "SegmentIterator.h"
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
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        config.setLogAndHashTableSize(logSize, hashTableSize);
        config.services = {WireFormat::MASTER_SERVICE};
        config.master.numReplicas = 0;
        service = new MasterService(&context, config);
        service->init({1, 0});
    }

    ~RecoverSegmentBenchmark()
    {
        delete service;
    }

    void
    run(int numSegments, int dataBytes)
    {
        /*
         * Allocate numSegments Segments and fill them up with objects of
         * size dataBytes. These will be the Segments that we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextKeyVal = 0;
        Segment *segments[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = new Segment();
            while (1) {
                Key key(0, &nextKeyVal, sizeof(nextKeyVal));

                char objectData[dataBytes];
                Object object(key, objectData, dataBytes, 0, 0);
                Buffer buffer;
                object.serializeToBuffer(buffer);
                if (!segments[i]->append(LOG_ENTRY_TYPE_OBJ, buffer))
                    break;
                nextKeyVal++;
                numObjects++;
            }
            segments[i]->close();
        }

        /* Update the list of Tablets */
        ProtoBuf::Tablets_Tablet tablet;
        tablet.set_table_id(0);
        tablet.set_start_key_hash(0);
        tablet.set_end_key_hash(~0UL);
        tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet.set_server_id(service->serverId.getId());
        *service->tablets.add_tablet() = tablet;

        /*
         * Now run a fake recovery.
         */
        uint64_t before = Cycles::rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment* s = segments[i];
            Buffer buffer;
            s->appendToBuffer(buffer);
            Segment::Certificate certificate;
            s->getAppendedLength(certificate);
            const void* contigSeg = buffer.getRange(0, buffer.getTotalLength());
            SegmentIterator it(contigSeg, buffer.getTotalLength(), certificate);
            service->recoverSegment(it);
        }
        uint64_t ticks = Cycles::rdtsc() - before;

        uint64_t totalObjectBytes = numObjects * dataBytes;
        uint64_t totalSegmentBytes = numSegments *
                                     Segment::DEFAULT_SEGMENT_SIZE;
        printf("Recovery of %d %dKB Segments with %d byte Objects took %lu "
            "milliseconds\n", numSegments, Segment::DEFAULT_SEGMENT_SIZE / 1024,
            dataBytes, RAMCloud::Cycles::toNanoseconds(ticks) / 1000 / 1000);
        printf("Actual total object count: %lu (%lu bytes in Objects, %.2f%% "
            "overhead)\n", numObjects, totalObjectBytes,
            100.0 *
            static_cast<double>(totalSegmentBytes - totalObjectBytes) /
            static_cast<double>(totalSegmentBytes));

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
    int numSegments = 80;
    int dataBytes[] = { 64, 128, 256, 512, 1024, 2048, 8192, 0 };

    for (int i = 0; dataBytes[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::RecoverSegmentBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, dataBytes[i]);
    }

    return 0;
}
