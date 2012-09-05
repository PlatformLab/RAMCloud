/* Copyright (c) 2011-2012 Stanford University
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

/**
 * \file
 * This implements a series of benchmarks for the log cleaner. Many of the
 * tests are cribbed from descriptions of the LFS simulator. We run this as
 * a client for end-to-end evaluation.
 */

#include <sys/stat.h>

#include "Common.h"

#include "Context.h"
#include "Histogram.h"
#include "MasterService.h"
#include "MasterClient.h"
#include "OptionParser.h"
#include "Object.h"
#include "RawMetrics.h"
#include "RamCloud.h"
#include "Segment.h"
#include "Tub.h"

#include "LogMetrics.pb.h"
#include "ServerConfig.pb.h"

namespace RAMCloud {

/**
 * Interface definition for Distribution objects.
 *
 * Distributions dictate which objects are written during a benchmark run. This
 * includes specifying the keys that are used, the object data associated with
 * each key, how many times each object is (over-)written, and in what sequence
 * they're written.
 *
 * Many distributions are simple. For instance, a uniform random distribution
 * with fixed-sized objects would simply choose a random key within a given
 * range (dictated by the log's size and desired memory utilization) and would
 * specify the same object contents for each key.
 */
class Distribution {
  public:
    virtual ~Distribution() { };
    virtual bool isPrefillDone() = 0;
    virtual bool isDone() = 0;
    virtual void advance() = 0;
    virtual void getKey(void* outKey) = 0;
    virtual uint16_t getKeyLength() = 0;
    virtual uint16_t getMaximumKeyLength() = 0;
    virtual void getObject(void* outObject) = 0;
    virtual uint32_t getObjectLength() = 0;
    virtual uint32_t getMaximumObjectLength() = 0;

  PROTECTED:
    /**
     * Compute the number of distinct objects one would have to store to fill
     * the log to a specific utilization, assuming all objects are of the same
     * given length.
     */
    uint64_t
    objectsNeeded(uint64_t logSize,
                  int utilization,
                  uint16_t keyLength,
                  uint32_t dataLength)
    {
        return logSize * utilization / 100 /
            objectLengthInLog(keyLength, dataLength);
    }

    /**
     * Compute the total length of an object when stored in the log. This simply
     * adds the amount of metadata to the object key and data lengths.
     */
    uint32_t
    objectLengthInLog(uint16_t keyLength, uint32_t dataLength)
    {
        uint32_t metaDataLength = 0;

        // XXX- Seriously? How lazy is this?
        if (dataLength < 256)
            metaDataLength = 26 + 1 + 1;
        else if (dataLength < 65536)
            metaDataLength = 26 + 1 + 2;
        else if (dataLength < 16777216)
            metaDataLength = 26 + 1 + 3;
        else
            metaDataLength = 26 + 1 + 4;

        return dataLength + keyLength + metaDataLength;
    }

    /**
     * Return a random integer within the given range.
     */
    uint64_t
    randomInteger(uint64_t min, uint64_t max)
    {
        assert(max >= min);
        return min + (generateRandom() % (max - min + 1));
    }
};


/**
 * The uniform distribution allocates enough keys to fill the log to the desired
 * utilization and then chooses a key at random at each next step (after first
 * pre-filling the log to the desired utilization with unique keys).
 */
class UniformDistribution : public Distribution {
  public:
    /**
     * \param logSize
     *      Size of the target server's log in bytes.
     * \param utilization
     *      Desired utilization of live data in the server's log.
     * \param objectLength
     *      Size of each object to write.
     * \param fillCount
     *      Number of times to fill the log with logSize bytes worth of data.
     *      This determines the total amount of data to be written.
     */
    UniformDistribution(uint64_t logSize,
                        int utilization,
                        uint32_t objectLength,
                        int fillCount)
        : objectLength(objectLength),
          maxObjectId(objectsNeeded(logSize, utilization, 8, objectLength)),
          maximumObjects(maxObjectId * fillCount),
          objectCount(0),
          key(0)
    {
    }

    bool
    isPrefillDone()
    {
        return (objectCount >= maxObjectId);
    }

    bool
    isDone()
    {
        return (objectCount >= maximumObjects);
    }

    void 
    advance()
    {
        if (isPrefillDone())
            key = randomInteger(0, maxObjectId);
        else
            key++;
        objectCount++;
    }

    void 
    getKey(void* outKey)
    {
        *reinterpret_cast<uint64_t*>(outKey) = key;
    }

    uint16_t
    getKeyLength()
    {
        return sizeof(key);
    }

    uint16_t
    getMaximumKeyLength()
    {
        return sizeof(key);
    }

    void
    getObject(void* outObject)
    {
        // Do nothing. Content doesn't matter.
    }

    uint32_t
    getObjectLength()
    {
        return objectLength;
    }

    uint32_t
    getMaximumObjectLength()
    {
        return objectLength;
    }

  PRIVATE:
    uint32_t objectLength;
    uint64_t maxObjectId;
    uint64_t maximumObjects;
    uint64_t objectCount;
    uint64_t key;

    DISALLOW_COPY_AND_ASSIGN(UniformDistribution);
};

/**
 * The hot-and-cold distribution allocates enough keys to fill the log to the
 * desired utilization and then chooses a key randomly from one or two pools
 * (after pre-filling the log with unique keys first).
 *
 * The first pool is the "hot" pool, which has a higher probability of being
 * chosen. The "cold" pool has a lower probability of being chosen.
 *
 * The two pools may be of different size. For instance, LFS often used the
 * "hot-and-cold 90->10" distribution, which means 90% of writes were to objects
 * in the hot pool that corresponded to only 10% of the keys. In other words,
 * 10% of the objects got 90% of the writes. The other 90% of the data was cold,
 * receiving only 10% of the writes.
 *
 * Both the 90% and 10% parameters above are configurable.
 */
class HotAndColdDistribution : public Distribution {
  public:
    HotAndColdDistribution(uint64_t logSize,
                           int utilization,
                           uint32_t objectLength,
                           int fillCount,
                           int hotDataAccessPercentage,
                           int hotDataSpacePercentage)
        : hotDataAccessPercentage(hotDataAccessPercentage),
          hotDataSpacePercentage(hotDataSpacePercentage),
          objectLength(objectLength),
          maxObjectId(objectsNeeded(logSize, utilization, 8, objectLength)),
          maximumObjects(maxObjectId * fillCount),
          objectCount(0),
          key(0)
    {
    }

    bool
    isPrefillDone()
    {
        return (objectCount >= maxObjectId);
    }

    bool
    isDone()
    {
        return (objectCount >= maximumObjects);
    }

    void
    advance()
    {
        if (isPrefillDone()) {
            double hotFraction = hotDataSpacePercentage / 100.0;
            uint64_t maxHotObjectId = static_cast<uint64_t>(hotFraction *
                                        static_cast<double>(maxObjectId));

            if (randomInteger(0, 99) < hotDataAccessPercentage) {
                key = randomInteger(0, maxHotObjectId - 1);
            } else {
                key = randomInteger(maxHotObjectId, maxObjectId);
            }
        } else {
            key++;
        }

        objectCount++;
    }

    void 
    getKey(void* outKey)
    {
        *reinterpret_cast<uint64_t*>(outKey) = key;
    }

    uint16_t
    getKeyLength()
    {
        return sizeof(key);
    }

    uint16_t
    getMaximumKeyLength()
    {
        return sizeof(key);
    }

    void
    getObject(void* outObject)
    {
        // Do nothing. Content doesn't matter.
    }

    uint32_t
    getObjectLength()
    {
        return objectLength;
    }

    uint32_t
    getMaximumObjectLength()
    {
        return objectLength;
    }

  PRIVATE:
    uint32_t hotDataAccessPercentage;
    uint32_t hotDataSpacePercentage;
    uint32_t objectLength;
    uint64_t maxObjectId;
    uint64_t maximumObjects;
    uint64_t objectCount;
    uint64_t key;

    DISALLOW_COPY_AND_ASSIGN(HotAndColdDistribution);
};

/**
 * Benchmark objects carry out the bulk of the benchmark work. This includes
 * pre-filling the log to the desired utilization, over-writing objects
 * provided by the given distribution, and all the while maintaining various
 * statistics and periodically dumping some of them to the terminal.
 */
class Benchmark {
  public:
    Benchmark(RamCloud& ramcloud,
              uint64_t tableId,
              string serverLocator,
              Distribution& distribution)
        : ramcloud(ramcloud),
          tableId(tableId),
          serverLocator(serverLocator),
          distribution(distribution),
          latencyHistogram(20 * 1000 * 1000, 1000),     // 20s of 1us buckets
          totalObjectsWritten(0),
          totalBytesWritten(0),
          start(0),
          stop(0),
          lastOutputUpdateTsc(0),
          serverConfig(),
          pipelineMax(1)
    {
    }

    void
    run()
    {
        if (start != 0)
            return;

        // Pre-fill up to the desired utilization before measuring.
        writeNextObjects(pipelineMax);

        start = Cycles::rdtsc();

        // Now issue writes until we're done.
        writeNextObjects(pipelineMax);

        stop = Cycles::rdtsc();
    }

  PRIVATE:
    void
    updateOutput()
    {
        if (Cycles::toSeconds(Cycles::rdtsc() - lastOutputUpdateTsc) >= 2) {
            double totalMegabytesWritten =
                static_cast<double>(totalBytesWritten) / 1024 / 1024;
            double averageWriteRate = totalMegabytesWritten /
                Cycles::toSeconds(Cycles::rdtsc() - start);
            uint64_t averageObjectRate = totalObjectsWritten /
                static_cast<uint64_t>(Cycles::toSeconds(Cycles::rdtsc() - start));

            fprintf(stderr, "\r %lu objects written (%.2f MB) at average of %.2f MB/s (%lu objs/s)",
                totalObjectsWritten, totalMegabytesWritten, averageWriteRate, averageObjectRate);

            lastOutputUpdateTsc = Cycles::rdtsc();

ProtoBuf::LogMetrics logMetrics;
ramcloud.getLogMetrics(serverLocator.c_str(), logMetrics);
fprintf(stderr, "%s\n", logMetrics.DebugString().c_str());
        }
    }

    /**
     * This class simply encapulsates an asynchronous RPC sent to the server.
     * It records the TSC when the RPC was initiated, as well as the key and
     * object that were transmitted.
     */
    class OutstandingWrite {
      public:         
        OutstandingWrite(RamCloud& ramcloud, uint64_t tableId,
                         const void* key, uint16_t keyLength,
                         const void* object, uint32_t objectLength)
            : ticks(),
              rpc(ramcloud, tableId, key, keyLength, object, objectLength),
              key(key),
              keyLength(keyLength),
              object(object),
              objectLength(objectLength)
        {
        }

        CycleCounter<uint64_t> ticks;
        WriteRpc rpc;
        const void* key;
        uint16_t keyLength;
        const void* object;
        uint32_t objectLength;

        DISALLOW_COPY_AND_ASSIGN(OutstandingWrite);
    };

    void
    writeNextObjects(const int pipelined)
    {
        Tub<OutstandingWrite> rpcs[pipelined];
        uint8_t keys[pipelined][distribution.getMaximumKeyLength()];
        uint8_t objects[pipelined][distribution.getMaximumObjectLength()];
        bool prefilling = !distribution.isPrefillDone();

        bool isDone = false;
        while (!isDone) {
            // While any RPCs can still be sent, send them.
            bool allRpcsSent = false;
            for (int i = 0; i < pipelined; i++) { 
                allRpcsSent = prefilling ? distribution.isPrefillDone() :
                                           distribution.isDone();
                if (allRpcsSent)
                    break;

                if (rpcs[i])
                    continue;

                uint16_t keyLength = distribution.getKeyLength();
                uint32_t objectLength = distribution.getObjectLength();
                distribution.getKey(&keys[i][0]);
                distribution.getObject(&objects[i][0]);

                rpcs[i].construct(ramcloud, tableId, &keys[i][0], keyLength, &objects[i][0], objectLength);

                distribution.advance();
            }

            // As long as there are RPCs left outstanding, loop until one has
            // completed.
            bool anyRpcsDone = false;
            int numRpcsLeft = -1;
            while (!anyRpcsDone && numRpcsLeft != 0) {
                numRpcsLeft = 0;

                // As a client we need to let the dispatcher run. Calling
                // isReady() on an RPC doesn't do it (perhaps it should?),
                // so do so here.
                ramcloud.clientContext.dispatch->poll();

                for (int i = 0; i < pipelined; i++) {
                    if (!rpcs[i])
                        continue;

                    if (!rpcs[i]->rpc.isReady()) {
                        numRpcsLeft++;
                        continue;
                    }

                    if (!prefilling) {
                        latencyHistogram.storeSample(Cycles::toNanoseconds(rpcs[i]->ticks.stop()));
                        totalObjectsWritten++;
                        totalBytesWritten += rpcs[i]->objectLength;
                    }

                    rpcs[i].destroy();
                    anyRpcsDone = true;
                }
            }

            if (numRpcsLeft == 0 && allRpcsSent)
                isDone = true;

            updateOutput();
        }
    }

    /// RamCloud object used to access the storage being benchmarked.
    RamCloud& ramcloud;

    /// Identifier of the table objects are written to.
    uint64_t tableId;

    /// ServiceLocator of the server we're benchmarking.
    string serverLocator;

    /// The distribution object provides us with the next object to write,
    /// as well as dictates the object's size and how many total objects to
    /// store.
    Distribution& distribution;
 
    /// Histogram of write latencies. One sample is stored for each write.
    Histogram latencyHistogram;

    /// Total objects written during the benchmark (not including pre-filling).
    uint64_t totalObjectsWritten;

    /// Total object bytes written during the benchmark (not including
    /// pre-filling).
    uint64_t totalBytesWritten;

    /// Cycle counter at the start of the benchmark.
    uint64_t start;

    /// Cycle counter at the end of the benchmark.
    uint64_t stop;

    /// Cycle counter of last statistics update dumped to screen.
    uint64_t lastOutputUpdateTsc;

    /// Configuration information for the server we're benchmarking.
    ProtoBuf::ServerConfig serverConfig;

    /// Number of RPCs we'll pipeline to the server before waiting for
    /// acknowledgements. Setting to 1 essentially does a synchronous RPC for
    /// each write.
    const int pipelineMax;

    DISALLOW_COPY_AND_ASSIGN(Benchmark);
};

} // namespace RAMCloud

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    Context context(true);

    int objectSize;
    int utilization;
    int timesToFillLog;
    string distributionName;
    string tableName;
    string latencyOutputFile;
    string metricsOutputFile;

    OptionsDescription benchOptions("Bench");
    benchOptions.add_options()
        ("table,t",
         ProgramOptions::value<string>(&tableName)->
            default_value("cleanerBench"),
         "name of the table to use for testing.")
        ("size,s",
         ProgramOptions::value<int>(&objectSize)->
           default_value(1000),
         "size of each object in bytes.")
        ("utilization,u",
         ProgramOptions::value<int>(&utilization)->
           default_value(50),
         "Percentage of the log space to utilize.")
        ("distribution,d",
         ProgramOptions::value<string>(&distributionName)->
           default_value("uniform"),
         "Object distribution; choose one of \"uniform\" or "
         "\"hotAndCold\"")
        ("latencyOutputFile,O",
         ProgramOptions::value<string>(&latencyOutputFile)->
           default_value("/dev/null"),
         "File to dump a request latency histogram into after the benchmark.")
        ("metricsOutputFile,M",
         ProgramOptions::value<string>(&metricsOutputFile)->
           default_value("/dev/null"),
         "File to dump a final print out of metrics into after the benchmark.")
        ("timesToFillLog,T",
         ProgramOptions::value<int>(&timesToFillLog)->
           default_value(200),
         "Determines the amount of data to fill the log with. The timeToFillLog "
         "value given is multipled by the log size to determine how much total "
         "data to write");

    OptionParser optionParser(benchOptions, argc, argv);

    if (utilization < 1 || utilization > 100) {
        fprintf(stderr, "ERROR: Utilization must be between 1 and 100, "
            "inclusive\n");
        exit(1);
    }
    if (distributionName != "uniform" && distributionName != "hotAndCold") {
        fprintf(stderr, "ERROR: Distribution must be one of \"uniform\" or "
            "\"hotAndCold\"\n");
        exit(1);
    }
    if (objectSize < 1 || objectSize > MAX_OBJECT_SIZE) {
        fprintf(stderr, "ERROR: objectSize must be between 1 and %u\n",
            MAX_OBJECT_SIZE);
        exit(1);
    }
    if (timesToFillLog < 2) {
        fprintf(stderr, "ERROR: timesToFillLog must be >= 2\n");
        exit(1);
    }

#if 0
    if ((fileExists(latencyOutputFile) && latencyOutputFile != "/dev/null") ||
      (fileExists(metricsOutputFile) && metricsOutputFile != "/dev/null")) {
        fprintf(stderr, "ERROR: Latency and/or metrics output file already exists!\n");
        exit(1);
    }

    if (latencyOutputFile == metricsOutputFile && latencyOutputFile != "/dev/null") {
        fprintf(stderr, " ERROR: Latency and Metrics output files must be different!\n");
        exit(1);
    }
#endif

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    printf("Connecting to %s\n", coordinatorLocator.c_str());
    RamCloud ramcloud(coordinatorLocator.c_str());

    // Get server parameters...
    // Perhaps this (and creating the distribution?) should be pushed into Benchmark.
    ramcloud.createTable(tableName.c_str());
    uint64_t tableId = ramcloud.getTableId(tableName.c_str());

    string locator =
        ramcloud.objectFinder.lookupTablet(tableId, 0).service_locator();

    ProtoBuf::ServerConfig serverConfig;
    ramcloud.getServerConfig(locator.c_str(), serverConfig);

    ProtoBuf::LogMetrics logMetrics;
    ramcloud.getLogMetrics(locator.c_str(), logMetrics);
    uint64_t logSize = logMetrics.seglet_metrics().total_usable_seglets() *
                       serverConfig.seglet_size();
fprintf(stderr, "Usable log size: %lu MB\n", logSize / 1024 / 1024);
    Distribution* distribution = NULL;
    if (distributionName == "uniform") {
        distribution = new UniformDistribution(logSize,
                                               utilization,
                                               objectSize,
                                               timesToFillLog);
    } else {
        distribution = new HotAndColdDistribution(logSize,
                                                  utilization,
                                                  objectSize,
                                                  timesToFillLog,
                                                  90, 10);
    }

    Benchmark benchmark(ramcloud, tableId, locator, *distribution);

    printf("========== Log Cleaner Benchmark ==========\n");
    benchmark.run();

    return 0;
} catch (ClientException& e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
