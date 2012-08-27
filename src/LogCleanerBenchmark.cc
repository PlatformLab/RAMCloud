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
 * Generic histogram class. XXX document me.
 */
class Histogram {
  public:
    Histogram(uint32_t numBuckets, uint32_t bucketWidth)
        : numBuckets(numBuckets),
          bucketWidth(bucketWidth),
          samples(),
          totalSamples(0),
          outliers(0),
          highestOutlier(0)
    {
        samples.resize(numBuckets, 0);
    }

    void
    addSample(uint64_t sample)
    {
        // round to nearest bucket
        sample = (sample + (bucketWidth - 1)) / bucketWidth;
        if (sample < numBuckets) {
            samples[sample]++;
        } else {
            outliers++;
            if (sample > highestOutlier)
                highestOutlier = sample;
        }

        totalSamples++;
    }

    string
    toString(bool omitZeros = true)
    {
        string s;
        for (uint32_t i = 0; i < numBuckets; i++) {
            uint64_t count = samples[i];
            if (count > 0 || !omitZeros)
                s += format("  %9u  %12lu\n", i, count);
        }
        if (outliers > 0)
            s += format("  %9d  %12lu\n", -1, outliers);
        return s;
    }

    uint64_t
    getOutliers(uint64_t* outHighestOutlier = NULL)
    {
        if (outHighestOutlier != NULL)
            *outHighestOutlier = highestOutlier;
        return outliers;
    }

    uint64_t
    getTotalSamples()
    {
        return totalSamples;
    }

  PRIVATE:
    /// The number of buckets in our histogram. Each bucket stores counts for
    /// samples falling into a particular range, as determined by the buckets'
    /// width.
    const uint32_t numBuckets;

    /// The width of each bucket determines which bucket a particular sample
    /// falls in. The sample is divided by this value and rounded to the nearest
    /// integer to choose the bucket index.
    const uint32_t bucketWidth;

    /// The histogram itself as a vector of sample counters, one counter for
    /// each bucket.
    vector<uint64_t> samples;

    /// Total number of samples reported to the histogram, including outliers.
    uint64_t totalSamples;

    /// The number of samples added to the histogram that exceeded the maximum
    /// bucket index.
    uint64_t outliers;
    
    /// The highest-valued sample that was an outlier.
    uint64_t highestOutlier;
};

class ServerConfiguration {
  public:
    ServerConfiguration()
        : logBytes(0),
          hashTableBuckets(0),
          replicationFactor(0),
          writeCostThreshold(0),
          memoryCleanerEnabled(true),
          memoryLowWatermark(0),
          diskLowWatermark(0),
          diskExpansionFactor(0)
    {
    }
    
    /// Size of the log in bytes. This does not include space reserved for any
    /// special purposes, including cleaning.
    uint64_t logBytes;

    /// Size of the hash table in buckets. Each bucket holds 8 entries. This
    /// does not include any chaining.
    uint64_t hashTableBuckets;

    /// The log's replication factor determines the number of time each segment
    /// is replicated for durability.
    uint32_t replicationFactor;

    /// The server's configured write cost threshold determines the balance
    /// between disk and memory cleaning.
    uint32_t writeCostThreshold;

    /// By default the memory cleaner is enabled, but it may be disabled on the
    /// commandline to exercize just the disk cleaner itself.
    bool memoryCleanerEnabled;

    /// The memory utilization percentage at which cleaning will begin. Before
    /// memory hits this utilization, no cleaning will take place as long as
    /// backup disk space is free. If the in-memory cleaner is enabled, it
    /// will run at this utilization until it becomes too costly (as determined
    /// by the writeCostThreshold). After that, disk cleaning may be used. If
    /// the memory cleaner is disabled, the disk cleaner will run at this level
    /// regardless.
    uint32_t memoryLowWatermark;

    /// The backup disk space utilization percentage at which disk cleaning will
    /// take place. Disk cleaning may also occur earlier due to memory pressure.
    uint32_t diskLowWatermark;

    /// The expansion factor determines how many segments may be allocated on
    /// backup disks. It is at least 1.0. Higher values mean more space may be
    /// allocated on backups than in main memory.
    double diskExpansionFactor;
};

/**
 * Interface definition for Distribution objects.
 *
 * Distributions dictate which objects are written during a benchmark run. This
 * includes specifying the keys that are used, the object data associate with
 * each key, how many times each object is (over-)written, and in what sequence
 * they're written.
 *
 * Many distributions are simple. For instance, a uniform random distribution
 * with fixed-sized objects would simply choose a random key within a given
 * range (dictated by the log's size and desired memory utilization) and write
 * specify the same object contents for each key.
 */
class Distribution {
  public:
    virtual ~Distribution() { };
    virtual bool isPrefillDone() = 0;
    virtual bool isDone() = 0;
    virtual void advance() = 0;
    virtual const void* getKey() = 0;
    virtual uint16_t getKeyLength() = 0;
    virtual const void* getObject() = 0;
    virtual uint32_t getObjectLength() = 0;

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
          key(0),
          data(NULL)
    {
        data = new uint8_t[objectLength];
    }

    ~UniformDistribution()
    {
        delete[] data;
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

    const void*
    getKey()
    {
        return &key;
    }

    uint16_t
    getKeyLength()
    {
        return sizeof(key);
    }

    const void*
    getObject()
    {
        return data;
    }

    uint32_t
    getObjectLength()
    {
        return objectLength;
    }

  PRIVATE:
    uint32_t objectLength;
    uint64_t maxObjectId;
    uint64_t maximumObjects;
    uint64_t objectCount;
    uint64_t key;
    uint8_t* data;

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
          key(0),
          data(NULL)
    {
        data = new uint8_t[objectLength];
    }

    ~HotAndColdDistribution()
    {
        delete[] data;
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

    const void*
    getKey()
    {
        return &key;
    }

    uint16_t
    getKeyLength()
    {
        return sizeof(key);
    }

    const void*
    getObject()
    {
        return data;
    }

    uint32_t
    getObjectLength()
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
    uint8_t* data;

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
              string& table,
              Distribution& distribution)
        : ramcloud(ramcloud),
          table(table),
          tableId(-1),
          distribution(distribution),
          latencyHistogram(20 * 1000 * 1000, 1000),     // 20s of 1us buckets
          totalObjectsWritten(0),
          totalBytesWritten(0),
          start(0),
          lastOutputUpdateTsc(0),
          serverConfig()
    {
        ramcloud.createTable(table.c_str());
        tableId = ramcloud.getTableId(table.c_str());

        string locator =
            ramcloud.objectFinder.lookupTablet(tableId, 0).service_locator();
        ramcloud.getServerConfig(locator.c_str(), serverConfig);
        fprintf(stderr, serverConfig.DebugString().c_str());

        ProtoBuf::LogMetrics logMetrics;
        ramcloud.getLogMetrics(locator.c_str(), logMetrics);
        fprintf(stderr, logMetrics.DebugString().c_str());
    }

    void
    run()
    {
        // Pre-fill up to the desired utilization before measuring.
        while (!distribution.isPrefillDone()) {
            writeNextObject();
            distribution.advance();
        }

        start = Cycles::rdtsc();

        // Now issue writes until we're done.
        while (!distribution.isDone()) {
            uint64_t ticks = writeNextObject();
            latencyHistogram.addSample(Cycles::toNanoseconds(ticks));
            distribution.advance();
            updateOutput();
        }

        updateOutput();
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

            fprintf(stderr, "\r %lu objects written (%.2f MB) at average of %.2f MB/s",
                totalObjectsWritten, totalMegabytesWritten, averageWriteRate);

            lastOutputUpdateTsc = Cycles::rdtsc();
        }
    }

    uint64_t
    writeNextObject()
    {
        if (distribution.isDone())
            return 0;

        CycleCounter<uint64_t> ticks;
        ramcloud.write(tableId,
                       distribution.getKey(),
                       distribution.getKeyLength(),
                       distribution.getObject(),
                       distribution.getObjectLength());
        totalObjectsWritten++;
        totalBytesWritten += distribution.getObjectLength();
        return ticks.stop();
    }

    /// RamCloud object used to access the storage being benchmarked.
    RamCloud& ramcloud;

    /// Name of the table to create/open for storing objects.
    string table;

    /// Identifier of the table objects are written to.
    uint64_t tableId;

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

    /// Cycle counter of last statistics update dumped to screen.
    uint64_t lastOutputUpdateTsc;

    /// Configuration information for the server we're benchmarking.
    ProtoBuf::ServerConfig serverConfig;

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

    uint64_t logSize = 4898;
    Distribution* distribution = NULL;
    if (distributionName == "uniform") {
        distribution = new UniformDistribution(logSize * 1024 * 1024,
                                               utilization,
                                               objectSize,
                                               timesToFillLog);
    } else {
        distribution = new HotAndColdDistribution(logSize * 1024 * 1024,
                                                  utilization,
                                                  objectSize,
                                                  timesToFillLog,
                                                  90, 10);
    }

    Benchmark benchmark(ramcloud, tableName, *distribution);

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
