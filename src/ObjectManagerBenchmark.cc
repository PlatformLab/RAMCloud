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

class ObjectManagerBenchmark {
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

    ObjectManagerBenchmark(string logSize, string hashTableSize)
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

    ~ObjectManagerBenchmark()
    {
        delete objectManager;
    }

    static void
    readerThreadEntry(ObjectManager* objectManager,
                      uint32_t numReads,
                      uint64_t numKeys,
                      std::atomic<uint32_t>* startFlag,
                      std::atomic<uint32_t>* stopCount)
    {
        while (*startFlag == 0) {
            // wait until master thread releases us
        }

        for (uint32_t i = 0; i < numReads; i++) {
            uint64_t keyInt = generateRandom() % numKeys;
            Key key(0, &keyInt, sizeof(keyInt));
            Buffer buffer;
            objectManager->readObject(key, &buffer, NULL, NULL);
        }

        (*stopCount)++;
    }

    double
    run(uint32_t numSegments, uint32_t dataBytes, uint32_t numThreads)
    {
        tabletManager.addTablet(0, 0, ~0UL, TabletManager::NORMAL);

        /*
         * Fill up 'numSegments' worth of segments in the log with objects of
         * size 'dataBytes'. These will be the objects that we will read.
         */
        uint64_t nextKeyVal = 0;
        do {
            Key key(0, &nextKeyVal, sizeof(nextKeyVal));

            char objectData[dataBytes];
            Buffer dataBuffer;
            Object object(key, objectData, dataBytes, 0, 0, dataBuffer);
            Status status = objectManager->writeObject(object, NULL, NULL);
            if (status != STATUS_OK) {
                fprintf(stderr, "Failed to write object! Out of memory?\n");
                exit(1);
            }
            nextKeyVal++;
        } while (objectManager->log.head->id <= numSegments);

        /*
         * Now "read" a bunch of random objects.
         */
        const uint32_t numReads = 1000000;
        std::atomic<uint32_t> startFlag(0);
        std::atomic<uint32_t> stopCount(0);
        std::thread* threads[numThreads];
        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i] = new std::thread(readerThreadEntry,
                                         objectManager,
                                         numReads,
                                         nextKeyVal,
                                         &startFlag,
                                         &stopCount);
        }

        usleep(1000);

        uint64_t start = Cycles::rdtsc();
        startFlag = 1;
        while (stopCount != numThreads) {
            // sleep just a wink.
            usleep(10000);
        }
        uint64_t stop = Cycles::rdtsc();

        for (uint32_t i = 0; i < numThreads; i++) {
            threads[i]->join();
            delete threads[i];
        }

        return static_cast<double>(numReads * numThreads /
                                   Cycles::toSeconds(stop - start));
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectManagerBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    uint32_t numSegments = 600 / 8; // = 72.
    uint32_t threads[] = { 1, 2, 3, 4, 6, 8, 12, 16, 20, 24, 28, 32, 0 };

    printf("============ 100-byte Objects ==============\n");
    double oneThreadRate = 0;
    for (int i = 0; threads[i] != 0; i++) {
        RAMCloud::ObjectManagerBenchmark omb("2048", "10%");
        double readsPerSec = omb.run(numSegments, 100, threads[i]);
        if (i == 0)
            oneThreadRate = readsPerSec;
        printf(" %u thread(s): %.2f reads/s, %.3f us/read, "
            "ratio: %.2fx (%.2f%% of optimal)\n",
            threads[i],
            readsPerSec,
            1.0e6 / readsPerSec * threads[i],
            readsPerSec / oneThreadRate,
            (readsPerSec / oneThreadRate) / threads[i] * 100);
    }

    return 0;
}
