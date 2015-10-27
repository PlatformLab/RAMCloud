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
#include "LogCleaner.h"
#include "Memory.h"
#include "ObjectManager.h"
#include "SegmentIterator.h"
#include "Seglet.h"
#include "TabletManager.h"
#include "Tablets.pb.h"
#include "MasterTableMetadata.h"

namespace RAMCloud {

class CleanerCompactionBenchmark {

  public:
    Context context;
    ClusterClock clusterClock;
    ClientLeaseValidator clientLeaseValidator;
    ServerConfig config;
    ServerList serverList;
    TabletManager tabletManager;
    MasterTableMetadata masterTableMetadata;
    UnackedRpcResults unackedRpcResults;
    PreparedOps preparedOps;
    TxRecoveryManager txRecoveryManager;
    ServerId serverId;
    ObjectManager* objectManager;

    CleanerCompactionBenchmark(string logSize, string hashTableSize,
        int numSegments)
        : context()
        , clusterClock()
        , clientLeaseValidator(&context, &clusterClock)
        , config(ServerConfig::forTesting())
        , serverList(&context)
        , tabletManager()
        , masterTableMetadata()
        , unackedRpcResults(&context, NULL, &clientLeaseValidator)
        , preparedOps(&context)
        , txRecoveryManager(&context)
        , serverId(1, 1)
        , objectManager(NULL)
    {
        Logger::get().setLogLevels(WARNING);
        config.localLocator = "bogus";
        config.coordinatorLocator = "bogus";
        config.setLogAndHashTableSize(logSize, hashTableSize);
        config.services = {};
        config.master.numReplicas = 0;
        config.master.disableLogCleaner = true;
        config.segmentSize = Segment::DEFAULT_SEGMENT_SIZE;
        config.segletSize = Seglet::DEFAULT_SEGLET_SIZE;
        objectManager = new ObjectManager(&context,
                                          &serverId,
                                          &config,
                                          &tabletManager,
                                          &masterTableMetadata,
                                          &unackedRpcResults,
                                          &preparedOps,
                                          &txRecoveryManager);
        unackedRpcResults.resetFreer(objectManager);
    }

    ~CleanerCompactionBenchmark()
    {
        delete objectManager;
    }

    void
    run(uint32_t numSegments, uint32_t dataLen)
    {
        tabletManager.addTablet(0, 0, ~0UL, TabletManager::NORMAL);

        /*
         * Fill up 'numSegments' worth of segments in the log with objects of
         * size 'dataLen'. These will be the Segments that we will clean.
         */
        uint64_t numObjects = 0;
        uint64_t nextKeyVal = 0;
        do {
            Key key(0, &nextKeyVal, sizeof(nextKeyVal));

            char objectData[dataLen];

            Buffer dataBuffer;
            Object object(key, objectData, dataLen, 0, 0, dataBuffer);

            Status status = objectManager->writeObject(object, NULL, NULL);
            if (status != STATUS_OK) {
                fprintf(stderr, "Failed to write object! Out of memory?\n");
                exit(1);
            }
            nextKeyVal++;
            numObjects++;
        } while (objectManager->log.head->id <= numSegments);

        /*
         * Delete 10% of the objects we just added at random.
         */
        for (uint64_t i = 0; i < nextKeyVal / 10; i++) {
            uint64_t r = generateRandom() % nextKeyVal;
            Key key(0, &r, sizeof(r));
            Status status = objectManager->removeObject(key, NULL, NULL);
            if (status != STATUS_OK) {
                // already deleted; try again
                i--;
            }
        }

        /*
         * Now compact each segment.
         */
        uint64_t before = Cycles::rdtsc();
        for (uint32_t i = 0; i < numSegments; i++)
            objectManager->log.cleaner->doMemoryCleaning();
        uint64_t ticks = Cycles::rdtsc() - before;

        LogCleanerMetrics::InMemory<>* metrics =
            &objectManager->log.cleaner->inMemoryMetrics;
        printf("Compaction took %lu ms (%.2f%% in callbacks)\n",
            Cycles::toNanoseconds(ticks) / 1000 / 1000,
            100.0 * Cycles::toSeconds(metrics->relocationCallbackTicks) /
                    Cycles::toSeconds(ticks));

        uint64_t totalEntriesScanned = 0;
        for (size_t i = 0; i < arrayLength(metrics->totalEntriesScanned); i++)
            totalEntriesScanned += metrics->totalEntriesScanned[i];
        printf("  Avg Time / Entry Scanned:     %.0f ns\n",
            Cycles::toSeconds(ticks / totalEntriesScanned) * 1.0e9);

        printf("  Avg Relocation Callback Time: %.0f ns "
            "(minus survivor append: %.0f)\n",
            Cycles::toSeconds(metrics->relocationCallbackTicks /
                              metrics->totalRelocationCallbacks) * 1.0e9,
            Cycles::toSeconds((metrics->relocationCallbackTicks -
                               metrics->relocationAppendTicks) /
                              metrics->totalRelocationCallbacks) * 1.0e9);
    }

    DISALLOW_COPY_AND_ASSIGN(CleanerCompactionBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    uint32_t numSegments = 600 / 8; // = 72.
    uint32_t dataBytes[] = { 100, 0 };

    for (int i = 0; dataBytes[i] != 0; i++) {
        printf("==========================\n");
        RAMCloud::CleanerCompactionBenchmark rsb("2048", "10%", numSegments);
        rsb.run(numSegments, dataBytes[i]);
    }

    return 0;
}
