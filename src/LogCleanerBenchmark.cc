/* Copyright (c) 2011-2015 Stanford University
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
#include <signal.h>

#include "Common.h"

#include "Context.h"
#include "ClusterMetrics.h"
#include "Histogram.h"
#include "LogMetricsStringer.h"
#include "MasterService.h"
#include "MasterClient.h"
#include "MultiWrite.h"
#include "OptionParser.h"
#include "Object.h"
#include "ObjectFinder.h"
#include "RawMetrics.h"
#include "RamCloud.h"
#include "Segment.h"
#include "Tub.h"

#include "LogMetrics.pb.h"
#include "ServerConfig.pb.h"

namespace RAMCloud {

// Set to true if SIGINT is caught, terminating the benchmark prematurely.
static bool interrupted = false;

/**
 * This class simply wraps up options that are given to this program, making
 * it easier to pass them around.
 */
class Options {
  public:
    Options(int argc, char** argv)
        : commandLineArgs(),
          objectSize(0),
          utilization(0),
          pipelinedRpcs(0),
          objectsPerRpc(0),
          writeCostConvergence(0),
          abortTimeout(0),
          minimumBenchmarkSeconds(0),
          distributionName(),
          tableName(),
          outputFilesPrefix(),
          doneWhenCleanerRuns(false)
    {
        for (int i = 0; i < argc; i++)
            commandLineArgs += format("%s ", argv[i]);
    }

    string commandLineArgs;
    int objectSize;
    int utilization;
    int pipelinedRpcs;
    int objectsPerRpc;
    int writeCostConvergence;
    unsigned abortTimeout;
    unsigned minimumBenchmarkSeconds;
    string distributionName;
    string tableName;
    string outputFilesPrefix;
    bool doneWhenCleanerRuns;
};

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

    /**
     * Return a uniform random number between 0 and 1 inclusive.
     */
    double
    randomFloat()
    {
        return static_cast<double>(generateRandom()) /
            static_cast<double>(std::numeric_limits<uint64_t>::max());
    }

    /**
     * Our distributions often have similar locality for keys that are close
     * together. For example, HotAndCold separates the key space into two
     * adjacent ranges, and Zipfian chops it up into a thousand pieces of
     * exponentially-increasing size. This means that simply iterating each
     * key during the prefill stage would segregate objects into different
     * locality classes from the get go since they'd be written sequentially
     * to the log in that order.
     *
     * This class provides a way of iterating a sequence of numbers in [0, N]
     * out of order, so we mix up the objects during the prefill stage. The
     * technique is really simple: treat the 0...N space as a 2D array with
     * N / 1000 columns, then enumerate by walking each column, rather than
     * each row.
     */
    class OutOfOrderSequence {
        public:
            explicit OutOfOrderSequence(uint64_t N)
                : N(N)
                , columns(N / 1000 + 1)
                , i(0)
                , j(0)
                , count(0)
            {
            }

            uint64_t
            next()
            {
                if (count > N)
                    return -1;

                while (true) {
                    uint64_t n = j++ * columns + i;
                    if (n > N) {
                        j = 0;
                        i++;
                    } else {
                        count++;
                        return n;
                    }
                }
            }

        private:
            const uint64_t N;
            const uint64_t columns;
            uint64_t i;
            uint64_t j;
            uint64_t count;
    };
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
     */
    UniformDistribution(uint64_t logSize,
                        int utilization,
                        uint32_t objectLength)
        : objectLength(objectLength),
          maxObjectId(objectsNeeded(logSize, utilization, 8, objectLength)),
          objectCount(0),
          key(0)
    {
    }

    bool
    isPrefillDone()
    {
        return (objectCount >= maxObjectId);
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
                           int hotDataAccessPercentage,
                           int hotDataSpacePercentage)
        : hotDataAccessPercentage(hotDataAccessPercentage),
          hotDataSpacePercentage(hotDataSpacePercentage),
          objectLength(objectLength),
          maxObjectId(objectsNeeded(logSize, utilization, 8, objectLength)),
          objectCount(0),
          key(0),
          prefiller(maxObjectId)
    {
    }

    bool
    isPrefillDone()
    {
        return (objectCount >= maxObjectId);
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
            key = prefiller.next();
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
    uint64_t objectCount;
    uint64_t key;
    OutOfOrderSequence prefiller;

    DISALLOW_COPY_AND_ASSIGN(HotAndColdDistribution);
};

/**
 * The Zipfian distribution generates an access pattern of high locality where
 * X% of accesses go to Y% of the data. Berk found that approximately 90% of
 * accesses go to 15% of data in Facebook's workloads, but this class may take
 * whatever numbers are desirable.
 */
class ZipfianDistribution : public Distribution {
  private:
    double
    generalizedHarmonic(uint64_t N, double s)
    {
        static uint64_t lastN = -1;
        static double lastS = 0;
        static double cachedResult = 0;

        if (N != lastN || s != lastS) {
            lastN = N;
            lastS = s;
            cachedResult = 0;
            for (uint64_t n = 1; n <= N; n++)
                cachedResult += 1.0 / pow(static_cast<double>(n), s);
        }

        return cachedResult;
    }

    /*
     * Zipfian frequency formula from wikipedia.
     */
    double
    f(uint64_t k, double s, uint64_t N)
    {
        return 1.0 / pow(static_cast<double>(k), s) / generalizedHarmonic(N, s);
    }

    double
    getHotDataPct(uint64_t N, double s, int hotAccessPct)
    {
        double sum = 0;
        for (uint64_t i = 1; i <= N; i++) {
            double p = f(i, s, N);
            sum += 100.0 * p;
            if (sum >= hotAccessPct)
                return 100.0 * static_cast<double>(i) / static_cast<double>(N);
        }
        return 100;
    }

    /**
     * Try to compute the proper skew value to get a Zipfian distribution
     * over N keys where hotAccessPct of the requests go to hotDataPct of
     * the data.
     *
     * This method simply does a binary search across a range of skew
     * values until it finds something close. For large values of N this
     * can take a while (a few minutes when N = 100e6). If there's a fast
     * approximation of the generalizedHarmonic method above (which supposedly
     * converges to the Riemann Zeta Function for large N), this could be
     * significantly improved.
     *
     * But, it's good enough for current purposes.
     */
    double
    calculateSkew(uint64_t N, int hotAccessPct, int hotDataPct)
    {
        double minS = 0.01;
        double maxS = 10;

        for (int i = 0; i < 100; i++) {
            double s = (minS + maxS) / 2;
            double r = getHotDataPct(N, s, hotAccessPct);
            if (fabs(r - hotDataPct) < 0.5)
                return s;
            if (r > hotDataPct)
                minS = s;
            else
                maxS = s;
        }

        // Bummer. We didn't find a good value. Just bail.
        fprintf(stderr, "%s: Failed to calculate reasonable skew for "
            "N = %lu, %d%% -> %d%%\n", __func__, N, hotAccessPct, hotDataPct);
        exit(1);
    }

    /**
     * Each of these entries represents a contiguous portion of the CDF curve
     * of keys vs. frequency. We use these to sample keys out to fit an
     * approximation of the Zipfian distribution. See generateTable for more
     * details on what's going on here.
     */
    struct Group {
        Group(uint64_t firstKey, uint64_t lastKey, double firstCdf)
            : firstKey(firstKey)
            , lastKey(lastKey)
            , firstCdf(firstCdf)
        {
        }
        uint64_t firstKey;
        uint64_t lastKey;
        double firstCdf;
    };

    std::vector<Group> groupsTable;

    /**
     * It'd require too much space and be too slow to generate a full histogram
     * representing the frequency of each individual key when there are a large
     * number of them. Instead, we divide the keys into a number of groups and
     * choose a group based on the cumulative frequency of keys it contains. We
     * then uniform-randomly select a key within the chosen group (see the
     * chooseNextKey method).
     *
     * The number of groups is fixed (1000) and each contains 0.1% of the key
     * frequency space. This means that groups tend to increase exponentially in
     * the size of the range of keys they contain (unless the Zipfian distribution
     * was skewed all the way to uniform). The result is that we are very accurate
     * for the most popular keys (the most popular keys are singleton groups) and
     * only lose accuracy for ones further out in the curve where it's much flatter
     * anyway.
     */
    void
    generateTable(const uint64_t N, const double s)
    {
        const double groupFrequencySpan = 0.001;

        double cdf = 0;
        double startCdf = 0;
        uint64_t startI = 1;
        for (uint64_t i = startI; i <= N; i++) {
            double p = f(i, s, N);
            cdf += p;
            if ((cdf - startCdf) >= groupFrequencySpan || i == N) {
                groupsTable.push_back({startI, i, startCdf});
                startCdf = cdf;
                startI = i + 1;
            }
        }
    }

    /**
     * Obtain the next key fitting the Zipfian distribution described by the
     * 'groupsTable' table.
     *
     * This method binary searches (logn complexity), but the groups table
     * is small enough to fit in L2 cache, so it's quite fast (~6M keys
     * per second -- more than sufficient for now).
     */
    uint64_t
    chooseNextKey()
    {
        double p = randomFloat();
        size_t min =  0;
        size_t max = groupsTable.size() - 1;
        while (true) {
            size_t i = (min + max) / 2;
            double start = groupsTable[i].firstCdf;
            double end = 1.1;
            if (i != groupsTable.size() - 1)
                end = groupsTable[i + 1].firstCdf;
            if (p < start) {
                max = i - 1;
            } else if (p >= end) {
                min = i + 1;
            } else {
                return randomInteger(groupsTable[i].firstKey,
                                     groupsTable[i].lastKey);
            }
        }
    }

  public:
    ZipfianDistribution(uint64_t logSize,
                        int utilization,
                        uint32_t objectLength,
                        int hotDataAccessPercentage,
                        int hotDataSpacePercentage)
        : groupsTable(),
          objectLength(objectLength),
          maxObjectId(objectsNeeded(logSize, utilization, 8, objectLength)),
          objectCount(0),
          key(0),
          prefiller(maxObjectId)
    {
        fprintf(stderr, "%s: Calculating skew value (may take a while)...\n",
            __func__);
        double s = calculateSkew(maxObjectId,
                                 hotDataAccessPercentage,
                                 hotDataSpacePercentage);
        fprintf(stderr, "%s: done.\n", __func__);
        generateTable(maxObjectId, s);
    }

    bool
    isPrefillDone()
    {
        return (objectCount >= maxObjectId);
    }

    void
    advance()
    {
        if (isPrefillDone()) {
            key = chooseNextKey();
        } else {
            key = prefiller.next();
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
    uint32_t objectLength;
    uint64_t maxObjectId;
    uint64_t objectCount;
    uint64_t key;
    OutOfOrderSequence prefiller;

    DISALLOW_COPY_AND_ASSIGN(ZipfianDistribution);
};

class Benchmark;

/**
 * This class handles all pretty-printed user output. There is a single static
 * method that updates the same line of console on stderr for giving a little
 * view into what's going on for those inclined to stare at a run in progress,
 * as well as a bunch of other methods for printing out interesting stats from
 * a completed run.
 *
 * This class can be fed any number of FILE pointers and it will dump contents
 * to all of them, making it easy to output to both stdout and text files for
 * archival.
 */
class Output {
  public:
    Output(RamCloud& ramcloud,
           string& masterLocator,
           ProtoBuf::ServerConfig& serverConfig,
           Benchmark& benchmark);

    void addFile(FILE* fp);
    void removeFiles();
    void dumpBeginning();
    void dumpEnd();
    void dumpParameters(Options& options,
                        ProtoBuf::LogMetrics& logMetrics);
    void dump();

    static bool updateLiveLine(RamCloud& ramcloud,
                               string& masterLocator,
                               uint64_t objects,
                               uint64_t bytes,
                               uint64_t ticks);

  PRIVATE:
    template<typename T>
    static double
    d(T value)
    {
        return static_cast<double>(value);
    }

    void dumpParameters(FILE* fp,
                        Options& options,
                        ProtoBuf::LogMetrics& logMetrics);
    void dumpSummary(FILE* fp);
    void dumpPrefillMetrics(FILE* fp);
    void dumpCleanerMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics);
    template<typename T> void dumpSegmentEntriesScanned(FILE* fp,
                                                        T& metrics,
                                                        double elapsed,
                                                        double cleanerTime);
    void dumpDiskMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics);
    void dumpMemoryMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics);
    void dumpLogMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics);
    void dumpSpinLockMetrics(FILE* fp, ProtoBuf::ServerStatistics& serverStats);

    RamCloud& ramcloud;
    string masterLocator;
    ProtoBuf::ServerConfig& serverConfig;
    Benchmark& benchmark;
    vector<FILE*> outputFiles;
};

/**
 * Benchmark objects carry out the bulk of the benchmark work. This includes
 * pre-filling the log to the desired utilization, over-writing objects
 * provided by the given distribution, and all the while maintaining various
 * statistics and periodically dumping some of them to the terminal.
 *
 * \param pipelineRpcs
 *      Number of write RPCs that will be pipelined (sent before previous ones
 *      are acknowleged). By permitting multiple outstanding RPCs we can
 *      increase the write rate, since RPCs will queue up on the server and be
 *      serviced while replies get sent back and processed in this client.
 * \param writeCostConvergence
 *      After this many decimal digits of the write cost remain unchanged for
 *      60 seconds, end the benchmark.
 */
class Benchmark {
  public:
    Benchmark(RamCloud& ramcloud,
              uint64_t tableId,
              string serverLocator,
              Distribution& distribution,
              Options& options)
        : ramcloud(ramcloud),
          tableId(tableId),
          serverLocator(serverLocator),
          distribution(distribution),
          options(options),
          latencyHistogram(20 * 1000 * 1000, 100),        // 2s of 100ns buckets
          prefillLatencyHistogram(20 * 1000 * 1000, 100),
          prefillStart(0),
          prefillStop(0),
          totalPrefillObjectsWritten(0),
          totalPrefillBytesWritten(0),
          totalPrefillOperations(0),
          totalObjectsWritten(0),
          totalBytesWritten(0),
          totalOperations(0),
          start(0),
          stop(0),
          lastOutputUpdateTsc(0),
          serverConfig(),
          lastWriteCostCheck(0),
          lastDiskWriteCost(0),
          lastWriteCostDiskCleanerTicks(0),
          lastWriteCostStart(0),
          prefillLogMetrics(),
          finalLogMetrics(),
          prefillClusterMetrics(),
          benchmarkClusterMetrics()
    {
    }

    /**
     * Run the benchmark by first pre-filling to the desired memory utilization
     * and then overwriting objects according to the given distribution until
     * the disk write cost stabilizes sufficiently.
     *
     * This method may only be called once on each instance of this class.
     */
    void
    run(unsigned timeoutSeconds)
    {
        if (start != 0)
            return;

        ClusterMetrics metricsBefore(&ramcloud);

        // Pre-fill up to the desired utilization before measuring.
        fprintf(stderr, "Prefilling...\n");
        prefillStart = Cycles::rdtsc();
        writeNextObjects(timeoutSeconds);
        prefillStop = Cycles::rdtsc();

        updateOutput(true);
        fprintf(stderr, "\n");

        ramcloud.getLogMetrics(serverLocator.c_str(), prefillLogMetrics);
        ClusterMetrics metricsAfterPrefill(&ramcloud);

        // Now issue writes until we're done.
        fprintf(stderr, "Prefill complete. Running benchmark...\n");
        start = Cycles::rdtsc();
        writeNextObjects(timeoutSeconds);
        stop = Cycles::rdtsc();

        updateOutput(true);
        fprintf(stderr, "\n");

        ramcloud.getLogMetrics(serverLocator.c_str(), finalLogMetrics);
        ClusterMetrics metricsAfterBenchmark(&ramcloud);

        prefillClusterMetrics =
            metricsAfterPrefill.difference(metricsBefore);
        benchmarkClusterMetrics =
            metricsAfterBenchmark.difference(metricsAfterPrefill);
    }

  PRIVATE:
    bool
    updateOutput(bool force = false)
    {
        bool cleanerRan = false;
        double delta = Cycles::toSeconds(Cycles::rdtsc() - lastOutputUpdateTsc);
        if (force || delta >= 2) {
            if (start == 0) {
                cleanerRan = Output::updateLiveLine(ramcloud,
                                                serverLocator,
                                                totalPrefillObjectsWritten,
                                                totalPrefillBytesWritten,
                                                Cycles::rdtsc() - prefillStart);
            } else {
                cleanerRan = Output::updateLiveLine(ramcloud,
                                             serverLocator,
                                             totalObjectsWritten,
                                             totalBytesWritten,
                                             Cycles::rdtsc() - start);
            }

            lastOutputUpdateTsc = Cycles::rdtsc();
        }

        return cleanerRan;
    }

    /**
     * This class simply encapulsates an asynchronous RPC sent to the server,
     * the TSC when the RPC was initiated, as well as the key(s) and object(s)
     * that were transmitted.
     *
     * If the write consists of only one RPC, it will be sent in a normal
     * WriteRpc request. Otherwise MultiWrite will be used to send multiple
     * writes at once.
     *
     * Note: This class does too much dynamic memory allocation.
     */
    class OutstandingWrite {
      private:
        struct WriteData {
            WriteData(uint64_t tableId, const void* key, uint16_t keyLength,
                      const void* object, uint32_t objectLength)
                : tableId(tableId), key(key), keyLength(keyLength),
                  object(object), objectLength(objectLength)
            {
            }
            uint64_t tableId;
            const void* key;
            uint16_t keyLength;
            const void* object;
            uint32_t objectLength;
        };

      public:
        explicit OutstandingWrite(RamCloud* ramcloud)
            : ramcloud(ramcloud)
            , ticks()
            , rpc()
            , multiRpc()
            , writes()
            , multiWriteObjs()
            , keys()
            , objects()
        {
        }

        ~OutstandingWrite()
        {
            if (rpc)
                rpc.destroy();

            if (multiRpc)
                multiRpc.destroy();

            for (size_t i = 0; i < keys.size(); i++)
                delete[] keys[i];

            for (size_t i = 0; i < objects.size(); i++)
                delete[] objects[i];

            for (size_t i = 0; i < multiWriteObjs.size(); i++)
                delete multiWriteObjs[i];
        }

        void
        addObject(uint64_t tableId, Distribution* distribution)
        {
            uint16_t keyLength = distribution->getKeyLength();
            uint32_t objectLength = distribution->getObjectLength();

            keys.push_back(new uint8_t[keyLength]);
            objects.push_back(new uint8_t[objectLength]);

            distribution->getKey(keys.back());
            distribution->getObject(objects.back());

            writes.push_back({ tableId,
                               keys.back(), keyLength,
                               objects.back(), objectLength });

            distribution->advance();
        }

        void
        start()
        {
            assert(!rpc && !multiRpc);
            assert(!writes.empty());

            // single WriteRpc case
            if (writes.size() == 1) {
                ticks.construct();
                rpc.construct(ramcloud,
                              writes[0].tableId,
                              writes[0].key,
                              writes[0].keyLength,
                              writes[0].object,
                              writes[0].objectLength);
                return;
            }

            // MultiWrite case
            for (size_t i = 0; i < writes.size(); i++) {
                multiWriteObjs.push_back(new MultiWriteObject(
                                           writes[i].tableId,
                                           writes[i].key,
                                           writes[i].keyLength,
                                           writes[i].object,
                                           writes[i].objectLength));
            }
            ticks.construct();
            multiRpc.construct(ramcloud,
                               &multiWriteObjs[0],
                               downCast<uint32_t>(multiWriteObjs.size()));
        }

        bool
        isReady()
        {
            if (rpc) {
                assert(!multiRpc);
                return rpc->isReady();
            }
            if (multiRpc) {
                assert(!rpc);
                return multiRpc->isReady();
            }
            return false;
        }

        uint64_t
        getTicks()
        {
            if (ticks)
                return ticks->stop();
            return 0;
        }

        uint64_t
        getObjectCount()
        {
            return writes.size();
        }

        uint64_t
        getObjectLengths()
        {
            uint64_t sum = 0;
            for (size_t i = 0; i < writes.size(); i++)
                sum += writes[i].objectLength;
            return sum;
        }

      private:
        RamCloud* ramcloud;
        Tub<CycleCounter<uint64_t>> ticks;
        Tub<WriteRpc> rpc;
        Tub<MultiWrite> multiRpc;
        vector<WriteData> writes;
        vector<MultiWriteObject*> multiWriteObjs;
        vector<uint8_t*> keys;
        vector<uint8_t*> objects;

        DISALLOW_COPY_AND_ASSIGN(OutstandingWrite);
    };

    /**
     * Write objects to the master. If the distribution we're running has not
     * prefilled yet, this will prefill to the desired memory utilization and
     * then return. If we have prefilled, it will continue to write until the
     * disk write cost has converged to a sufficiently stable value.
     *
     * \param timeoutSeconds
     *      If no progress is made within the given number of seconds, throw
     *      an Exception to terminate the benchmark. This avoids wedging the
     *      test if a server has stopped responding due to crash, deadlock, etc.
     */
    void
    writeNextObjects(const unsigned timeoutSeconds)
    {
        Tub<OutstandingWrite> rpcs[options.pipelinedRpcs];
        bool prefilling = !distribution.isPrefillDone();

        bool isDone = false;
        while (!isDone && !interrupted) {
            // While any RPCs can still be sent, send them.
            bool allRpcsSent = false;
            for (int i = 0; i < options.pipelinedRpcs; i++) {
                allRpcsSent = prefilling ? distribution.isPrefillDone() : false;
                if (allRpcsSent)
                    break;

                if (rpcs[i])
                    continue;

                rpcs[i].construct(&ramcloud);
                bool outOfObjects = false;
                for (int cnt = 0;
                  cnt < options.objectsPerRpc && !outOfObjects; cnt++) {
                    rpcs[i]->addObject(tableId, &distribution);
                    outOfObjects = prefilling ?
                        distribution.isPrefillDone() : false;
                }

                rpcs[i]->start();
            }

            // As long as there are RPCs left outstanding, loop until one has
            // completed.
            bool anyRpcsDone = false;
            int numRpcsLeft = -1;
            uint64_t start = Cycles::rdtsc();
            while (!anyRpcsDone && numRpcsLeft != 0 && !interrupted) {
                double delta = Cycles::toSeconds(Cycles::rdtsc() - start);
                if (delta >= timeoutSeconds)
                    throw Exception(HERE, "benchmark hasn't made progress");

                numRpcsLeft = 0;

                // As a client we need to let the dispatcher run. Calling
                // isReady() on an RPC doesn't do it (perhaps it should?),
                // so do so here.
                ramcloud.clientContext->dispatch->poll();

                for (int i = 0; i < options.pipelinedRpcs; i++) {
                    if (!rpcs[i])
                        continue;

                    if (!rpcs[i]->isReady()) {
                        numRpcsLeft++;
                        continue;
                    }

                    if (prefilling) {
                        prefillLatencyHistogram.storeSample(
                            Cycles::toNanoseconds(rpcs[i]->getTicks()));
                        totalPrefillObjectsWritten += rpcs[i]->getObjectCount();
                        totalPrefillBytesWritten += rpcs[i]->getObjectLengths();
                        totalPrefillOperations++;
                    } else {
                        latencyHistogram.storeSample(
                            Cycles::toNanoseconds(rpcs[i]->getTicks()));
                        totalObjectsWritten += rpcs[i]->getObjectCount();
                        totalBytesWritten += rpcs[i]->getObjectLengths();
                        totalOperations++;
                    }

                    rpcs[i].destroy();
                    anyRpcsDone = true;
                }
            }

            bool cleanerRan = updateOutput();
            if (cleanerRan && options.doneWhenCleanerRuns)
                isDone = true;

            // If we're prefilling, determine when we're done.
            if (numRpcsLeft == 0 && allRpcsSent)
                isDone = true;

            // If we're not prefilling, we're done once the write cost has
            // stabilized.
            if (!prefilling && writeCostHasConverged())
                isDone = true;
        }
    }

    bool
    writeCostHasConverged()
    {
        // If we haven't started the real benchmark, it can't have converged.
        if (start == 0 || !distribution.isPrefillDone())
            return false;

        // Only check every handful of seconds to reduce overhead.
        if (Cycles::toSeconds(Cycles::rdtsc() - lastWriteCostCheck) < 3)
            return false;

        // Never allow an experiment to run too quickly so we have time for
        // sampled values to average out.
        double benchTime = Cycles::toSeconds(Cycles::rdtsc() - start);
        if (benchTime < options.minimumBenchmarkSeconds)
            return false;

        lastWriteCostCheck = Cycles::rdtsc();

        ProtoBuf::LogMetrics logMetrics;
        ramcloud.getLogMetrics(serverLocator.c_str(), logMetrics);

        const ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& onDiskMetrics =
            logMetrics.cleaner_metrics().on_disk_metrics();

        uint64_t diskFreed = onDiskMetrics.total_disk_bytes_freed();
        uint64_t diskWrote = onDiskMetrics.total_bytes_appended_to_survivors();
        uint64_t diskCleanerTicks = onDiskMetrics.total_ticks();

        // Nothing counts until we've cleaned on disk at least once.
        if (diskFreed == 0 && diskWrote == 0)
            return false;

        // Compute the write costs. Shift digits over and convert to integer for
        // the needed comparison.
        double diskWriteCost = static_cast<double>(diskFreed + diskWrote) /
                               static_cast<double>(diskFreed);
        uint64_t intDiskWriteCost = static_cast<uint64_t>(
                    diskWriteCost * pow(10, options.writeCostConvergence));
        uint64_t intLastDiskWriteCost = static_cast<uint64_t>(
                    lastDiskWriteCost * pow(10, options.writeCostConvergence));

        bool areEqual = (intDiskWriteCost == intLastDiskWriteCost);

        if (lastWriteCostStart == 0 || !areEqual) {
            lastDiskWriteCost = diskWriteCost;
            lastWriteCostDiskCleanerTicks = diskCleanerTicks;
            lastWriteCostStart = Cycles::rdtsc();
            return false;
        }

        double diskCleanerSec = Cycles::toSeconds(diskCleanerTicks -
             lastWriteCostDiskCleanerTicks, logMetrics.ticks_per_second());

        return (diskCleanerSec >= 30);
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

    /// Various options passed to this program affect the execution of the
    /// Benchmark class.
    Options& options;

  public:
    /// Histogram of write latencies. One sample is stored for each write
    /// (not including prefill writes).
    Histogram latencyHistogram;

    /// Histogram of write latencies. One sample is stored for each write
    /// (not including prefill writes).
    Histogram prefillLatencyHistogram;

    /// Cycle counter at the start of the prefill phase.
    uint64_t prefillStart;

    /// Cycle counter at the end of the prefill phase.
    uint64_t prefillStop;

    /// Total objects written during prefilling.
    uint64_t totalPrefillObjectsWritten;

    /// Total object bytes written during prefilling.
    uint64_t totalPrefillBytesWritten;

    /// Total number of RPCs sent during prefilling. This is also the number
    /// of times Log::sync() is called. If multiWrites are being used, each
    /// operation may encompass multiple individual object writes.
    uint64_t totalPrefillOperations;

    /// Total objects written during the benchmark (not including pre-filling).
    uint64_t totalObjectsWritten;

    /// Total object bytes written during the benchmark (not including
    /// pre-filling).
    uint64_t totalBytesWritten;

    /// Total number of RPCs sent during the benchmark. This is also the number
    /// of times Log::sync() is called. If multiWrites are being used, each
    /// operation may encompass multiple individual object writes.
    uint64_t totalOperations;

    /// Cycle counter at the start of the benchmark.
    uint64_t start;

    /// Cycle counter at the end of the benchmark.
    uint64_t stop;

  PRIVATE:

    /// Cycle counter of last statistics update dumped to screen.
    uint64_t lastOutputUpdateTsc;

    /// Configuration information for the server we're benchmarking.
    ProtoBuf::ServerConfig serverConfig;

    /// Local ticks at which we last checked the server's disk write cost. Used
    /// to avoid querying the server too frequently.
    uint64_t lastWriteCostCheck;

    /// The last disk write cost computed. Used to determine when the benchmark
    /// should end (see options.writeCostConvergence).
    double lastDiskWriteCost;

    /// Number of ticks the server's disk cleaner has spent in cleaning as of
    /// #lastWriteCostStart. This is used to track how long the cleaner has been
    /// running at a particular stable write cost. Since it doesn't run all the
    /// time, using a strict wallclock value could be problematic (what if it
    /// doesn't run at all in the desired interval?) and is slightly easier to
    /// reason about than some other value like the amount of data cleaned.
    uint64_t lastWriteCostDiskCleanerTicks;

    /// Cycle counter when lastWriteCost was updated.
    uint64_t lastWriteCostStart;

  public:

    /// Log metrics from the benchmarked server immediately after pre-filling
    /// before the benchmark proper begins.
    ProtoBuf::LogMetrics prefillLogMetrics;

    /// Log metrics from the benchmarked server immediately after the benchmark
    /// has completed.
    ProtoBuf::LogMetrics finalLogMetrics;

    /// ClusterMetrics sampled after the prefill stage has completed.
    /// This contains generic metrics data on all servers in the system.
    ClusterMetrics prefillClusterMetrics;

    /// ClusterMetrics sampled after the benchmark itself has completed.
    /// This contains generic metrics data on all servers in the system.
    ClusterMetrics benchmarkClusterMetrics;

  PRIVATE:

    /// Let the output class poke around inside to extract what it needs.
    friend class Output;

    DISALLOW_COPY_AND_ASSIGN(Benchmark);
};


Output::Output(RamCloud& ramcloud,
               string& masterLocator,
               ProtoBuf::ServerConfig& serverConfig,
               Benchmark& benchmark)
    : ramcloud(ramcloud),
      masterLocator(masterLocator),
      serverConfig(serverConfig),
      benchmark(benchmark),
      outputFiles()
{
}

void
Output::addFile(FILE* fp)
{
    outputFiles.push_back(fp);
}

void
Output::removeFiles()
{
    outputFiles.clear();
}

void
Output::dumpParameters(Options& options,
                       ProtoBuf::LogMetrics& logMetrics)
{
    foreach (FILE* fp, outputFiles)
        dumpParameters(fp, options, logMetrics);
}

void
Output::dumpParameters(FILE* fp,
                       Options& options,
                       ProtoBuf::LogMetrics& logMetrics)
{
    fprintf(fp, "===> EXPERIMENT PARAMETERS\n");

    fprintf(fp, "  Commandline Args:              %s\n",
        options.commandLineArgs.c_str());

    fprintf(fp, "  Object Size:                   %d\n",
        options.objectSize);

    fprintf(fp, "  Distribution:                  %s\n",
        options.distributionName.c_str());

    fprintf(fp, "  Utilization:                   %d\n",
        options.utilization);

    fprintf(fp, "  WC Convergence:                %d decimal places\n",
        options.writeCostConvergence);

    fprintf(fp, "  Pipelined RPCs:                %d\n",
        options.pipelinedRpcs);

    fprintf(fp, "  Objects Per RPC:               %d   %s\n",
        options.objectsPerRpc,
        (options.objectsPerRpc > 1) ? "(MultiWrite used)" : "");

    fprintf(fp, "  Abort Timeout:                 %u sec\n",
        options.abortTimeout);

    fprintf(fp, "  Minimum Benchmark Time:        %u sec\n",
        options.minimumBenchmarkSeconds);

    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start);
    string s = LogMetricsStringer(&logMetrics,
        &serverConfig, elapsed).getServerParameters();
    fprintf(fp, "%s", s.c_str());

    fprintf(fp, "  Quit Once Cleaner Runs:        %s\n",
        options.doneWhenCleanerRuns ? "true" : "false");
}

void
Output::dumpSummary(FILE* fp)
{
    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start);

    fprintf(fp, "===> BENCHMARK SUMMARY\n");

    fprintf(fp, "  Benchmark Elapsed Time:        %.2f sec\n", elapsed);

    fprintf(fp, "  Objects Written:               %lu  (%.2f objs/sec)\n",
        benchmark.totalObjectsWritten,
        d(benchmark.totalObjectsWritten) / elapsed);

    fprintf(fp, "  Object Value Bytes Written:    %lu  (%.2f MB/sec)\n",
        benchmark.totalBytesWritten,
        d(benchmark.totalBytesWritten) / elapsed / 1024 / 1024);

    uint64_t bytesAppended = benchmark.finalLogMetrics.total_bytes_appended() -
                             benchmark.prefillLogMetrics.total_bytes_appended();
    fprintf(fp, "  Total Log Bytes Written:       %lu  (%.2f MB/sec)\n",
        bytesAppended, d(bytesAppended) / elapsed / 1024 / 1024);

    fprintf(fp, "  Average Latency:               %lu us / RPC (end-to-end, "
        "including queueing delays)\n",
        benchmark.latencyHistogram.getAverage() / 1000);

    double serverHz = benchmark.finalLogMetrics.ticks_per_second();

    double appendTime = Cycles::toSeconds(
        benchmark.finalLogMetrics.total_append_ticks() -
        benchmark.prefillLogMetrics.total_append_ticks(), serverHz);
    fprintf(fp, "  Average Log Append Time:       %.1f us / RPC (%.1f / obj; "
        "including tombstone append)\n",
        1.0e6 * appendTime / d(benchmark.totalOperations),
        1.0e6 * appendTime / d(benchmark.totalObjectsWritten));

    double syncTime = Cycles::toSeconds(
        benchmark.finalLogMetrics.total_sync_ticks() -
        benchmark.prefillLogMetrics.total_sync_ticks(), serverHz);
    fprintf(fp, "  Average Log Sync Time:         %.1f us / RPC\n",
        1.0e6 * syncTime / d(benchmark.totalOperations));
}

void
Output::dumpPrefillMetrics(FILE* fp)
{
    double elapsed = Cycles::toSeconds(benchmark.prefillStop -
                                       benchmark.prefillStart);

    fprintf(fp, "===> PREFILL SUMMARY\n");

    fprintf(fp, "  Prefill Elapsed Time:          %.2f sec\n", elapsed);

    fprintf(fp, "  Objects Written:               %lu  (%.2f objs/sec)\n",
        benchmark.totalPrefillObjectsWritten,
        d(benchmark.totalPrefillObjectsWritten) / elapsed);

    fprintf(fp, "  Object Value Bytes Written:    %lu  (%.2f MB/sec)\n",
        benchmark.totalPrefillBytesWritten,
        d(benchmark.totalPrefillBytesWritten) / elapsed / 1024 / 1024);

    uint64_t bytesAppended = benchmark.prefillLogMetrics.total_bytes_appended();
    fprintf(fp, "  Total Log Bytes Written:       %lu  (%.2f MB/sec)\n",
        bytesAppended, d(bytesAppended) / elapsed / 1024 / 1024);

    fprintf(fp, "  Average Latency:               %lu us / RPC (end-to-end "
        "including queueing delays)\n",
        benchmark.prefillLatencyHistogram.getAverage() / 1000);

    double serverHz = benchmark.prefillLogMetrics.ticks_per_second();

    double appendTime = Cycles::toSeconds(
        benchmark.prefillLogMetrics.total_append_ticks(), serverHz);
    fprintf(fp, "  Average Log Append Time:       %.1f us / RPC (%.1f / obj)\n",
        1.0e6 * appendTime / d(benchmark.totalPrefillOperations),
        1.0e6 * appendTime / d(benchmark.totalPrefillObjectsWritten));

    double syncTime = Cycles::toSeconds(
        benchmark.prefillLogMetrics.total_sync_ticks(), serverHz);
    fprintf(fp, "  Average Log Sync Time:         %.1f us / RPC\n",
        1.0e6 * syncTime / d(benchmark.totalPrefillOperations));
}

void
Output::dumpCleanerMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics)
{
    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start);
    string s = LogMetricsStringer(&metrics,
        &serverConfig, elapsed).getGenericCleanerMetrics();
    fprintf(fp, "%s", s.c_str());
}

void
Output::dumpDiskMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics)
{
    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start);
    string s = LogMetricsStringer(&metrics,
        &serverConfig, elapsed).getDiskCleanerMetrics();
    fprintf(fp, "%s", s.c_str());
}

void
Output::dumpMemoryMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics)
{
    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start);
    string s = LogMetricsStringer(&metrics,
        &serverConfig, elapsed).getMemoryCompactorMetrics();
    fprintf(fp, "%s", s.c_str());
}

void
Output::dumpLogMetrics(FILE* fp, ProtoBuf::LogMetrics& metrics)
{
    double elapsed = Cycles::toSeconds(benchmark.stop - benchmark.start +
                            benchmark.prefillStop - benchmark.prefillStart);
    string s = LogMetricsStringer(&metrics,
        &serverConfig, elapsed).getLogMetrics();
    fprintf(fp, "%s", s.c_str());
}

struct SpinLockStats {
    SpinLockStats(string name, double contentionPct, uint64_t contendedNsec)
        : name(name), contentionPct(contentionPct), contendedNsec(contendedNsec)
    {
    }
    string name;
    double contentionPct;
    uint64_t contendedNsec;
};

static bool
lockSorter(const SpinLockStats& a, const SpinLockStats& b)
{
    return a.contentionPct > b.contentionPct;
}

void
Output::dumpSpinLockMetrics(FILE* fp, ProtoBuf::ServerStatistics& serverStats)
{
    const ProtoBuf::SpinLockStatistics& spinLockStats =
        serverStats.spin_lock_stats();

    vector<SpinLockStats> locks;

    foreach (const ProtoBuf::SpinLockStatistics_Lock& lock,
      spinLockStats.locks()) {
        locks.push_back({ lock.name(),
                          static_cast<double>(lock.contended_acquisitions()) /
                            static_cast<double>(lock.acquisitions()) * 100,
                          lock.contended_nsec() });
    }

    std::sort(locks.begin(), locks.end(), lockSorter);

    // we don't want to report them all, since the hashTable has a ton
    // protecting different buckets.
    const int maxLocks = 10;
    fprintf(fp, "===> %d MOST CONTENDED SPINLOCKS\n", maxLocks);
    for (int i = 0; i < maxLocks; i++) {
        fprintf(fp, "  %-30s %.3f%% contended (%lu ms waited for)\n",
            (locks[i].name + ":").c_str(),
            locks[i].contentionPct,
            locks[i].contendedNsec / 1000000);
    }
}

void
Output::dump()
{
    ProtoBuf::LogMetrics metrics;
    ProtoBuf::ServerStatistics serverStats;
    ramcloud.getLogMetrics(masterLocator.c_str(), metrics);
    ramcloud.getServerStatistics(masterLocator.c_str(), serverStats);

    foreach (FILE* fp, outputFiles) {
        dumpSummary(fp);
        dumpPrefillMetrics(fp);
        dumpCleanerMetrics(fp, metrics);
        dumpDiskMetrics(fp, metrics);
        dumpMemoryMetrics(fp, metrics);
        dumpLogMetrics(fp, metrics);
        dumpSpinLockMetrics(fp, serverStats);
    }
}

void
Output::dumpBeginning()
{
    time_t now = time(NULL);
    foreach (FILE* fp, outputFiles)
        fprintf(fp, "===> START TIME:    %s", ctime(&now));
}

void
Output::dumpEnd()
{
    time_t now = time(NULL);
    foreach (FILE* fp, outputFiles)
        fprintf(fp, "===> END TIME:      %s", ctime(&now));
}

bool
Output::updateLiveLine(RamCloud& ramcloud,
                       string& masterLocator,
                       uint64_t objects,
                       uint64_t bytes,
                       uint64_t ticks)
{
    ProtoBuf::LogMetrics logMetrics;
    ramcloud.getLogMetrics(masterLocator.c_str(), logMetrics);
    const ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& onDiskMetrics =
        logMetrics.cleaner_metrics().on_disk_metrics();
    uint64_t freed = onDiskMetrics.total_disk_bytes_freed();
    uint64_t wrote = onDiskMetrics.total_bytes_appended_to_survivors();
    double diskWriteCost = static_cast<double>(freed + wrote) /
                           static_cast<double>(freed);
    double elapsed = Cycles::toSeconds(ticks);

    fprintf(stderr, "\r %.0f objects written (%.2f MB) at average of "
        "%.2f MB/s (%.0f objs/s). Disk WC: %.3f",
        d(objects),
        d(bytes) / 1024 / 1024,
        d(bytes) / elapsed / 1024 / 1024,
        d(objects) / elapsed,
        diskWriteCost);

    const ProtoBuf::LogMetrics_CleanerMetrics_InMemoryMetrics& inMemoryMetrics =
        logMetrics.cleaner_metrics().in_memory_metrics();
    uint64_t memWrote = inMemoryMetrics.total_bytes_appended_to_survivors();

    // Return true if either cleaner has ever run. If not, false.
    return memWrote != 0 || wrote != 0;
}

} // namespace RAMCloud

using namespace RAMCloud;

void
timedOut(int dummy)
{
    fprintf(stderr, "TIMED OUT SETTING UP BENCHMARK!\n");
    fprintf(stderr, "  Is the server or coordinator not up?\n");
    exit(1);
}

bool
fileExists(string& file)
{
    struct stat sb;
    return (stat(file.c_str(), &sb) == 0);
}

void
sigIntHandler(int sig)
{
    fprintf(stderr, "Caught ctrl+c! Exiting...\n");
    interrupted = true;
}

void
dumpClusterMetrics(ClusterMetrics* metrics, FILE* fp)
{
    for (ClusterMetrics::iterator serverIt = metrics->begin();
            serverIt != metrics->end(); serverIt++) {
        fprintf(fp, "Metrics: begin server %s\n", serverIt->first.c_str());
        ServerMetrics &server = serverIt->second;
        for (ServerMetrics::iterator metricIt = server.begin();
            metricIt != server.end(); metricIt++) {
            fprintf(fp, "Metrics: %s %lu\n", metricIt->first.c_str(),
                metricIt->second);
        }
    }
}

int
main(int argc, char *argv[])
try
{
    Context context(true);

    Options options(argc, argv);
    bool verifyObjects = false;

    OptionsDescription benchOptions("Bench");
    benchOptions.add_options()
        ("abortTimeout,a",
         ProgramOptions::value<unsigned>(&options.abortTimeout)->
            default_value(60),
         "If the benchmark makes no progress after this many seconds, assume "
         "that something is wedged and abort.")
        ("table,t",
         ProgramOptions::value<string>(&options.tableName)->
            default_value("cleanerBench"),
         "name of the table to use for testing.")
        ("size,s",
         ProgramOptions::value<int>(&options.objectSize)->
           default_value(1000),
         "size of each object in bytes.")
        ("utilization,u",
         ProgramOptions::value<int>(&options.utilization)->
           default_value(50),
         "Percentage of the log space to utilize.")
        ("distribution,d",
         ProgramOptions::value<string>(&options.distributionName)->
           default_value("uniform"),
         "Object distribution; choose one of \"uniform\", "
         "\"hotAndCold\", or \"zipfian\"")
        ("minimumBenchmarkSeconds,m",
         ProgramOptions::value<unsigned>(&options.minimumBenchmarkSeconds)->
            default_value(600),
         "Even if the write cost has converged, don't terminate the benchmark "
         "before it has run for this many seconds. Basically a kludge to avoid "
         "rare cases in which the benchmark terminates early and the average "
         "throughput is skewed too high. A real fix would be to reset counters "
         "once we've converged, and then measure for another X seconds to take "
         "whatever ``steady-state'' measurements we want.")
        ("outputFilesPrefix,O",
         ProgramOptions::value<string>(&options.outputFilesPrefix)->
           default_value(""),
         "File prefix used to generate filenames metrics, write latency "
         "distributions, and raw protocol buffer data will be dumped to "
         "after the benchmark completes. This program will append \"-m.txt\" "
         ", \"-l.txt\", and \"-rp.txt/-rb.txt\" prefixes for metrics, latency, "
         "and raw prefill/benchmark files.")
        ("objectsPerRpc,o",
         ProgramOptions::value<int>(&options.objectsPerRpc)->default_value(75),
         "Number of objects to write for each RPC sent to the server. If 1, "
         "normal write RPCs are used. If greater than 1, MultiWrite RPCs will "
         "be used to batch up writes. This parameter greatly increases small"
         "object throughput. This can also be used with the pipelinedRpcs "
         "parameter to both batch and cause the server to process writes in "
         "parallel across multiple MasterService threads.")
        ("pipelinedRpcs,p",
         ProgramOptions::value<int>(&options.pipelinedRpcs)->default_value(10),
         "Number of write RPCs that will be sent to the server without first "
         "getting any acknowledgement.")
        ("writeCostConvergence,w",
         ProgramOptions::value<int>(&options.writeCostConvergence)->
            default_value(2),
         "Stop the benchmark after the disk write cost converges to a value "
         "that is stable (unchanging) to this many decimal places for 30 "
         "seconds' worth of disk cleaner run time. Higher values will "
         "significantly increase benchmark time, but lead to somewhat "
         "more accurate results.")
        ("doneWhenCleanerRuns",
         ProgramOptions::bool_switch(&options.doneWhenCleanerRuns)->
            default_value(false),
         "Stop the benchmark once the cleaner has done any work whatsoever. "
         "This is useful when we want to measure performance/latency of "
         "object overwrite operations without the cleaner on. This lets us "
         "compare apples to apples (since there's a little overhead in writing "
         "and object and a tombstone, rather than just an object alone).")
        ("verifyObjects",
         ProgramOptions::bool_switch(&verifyObjects)->default_value(false),
         "Verify that all objects stored are accessible after the experiment "
         "completes. This is simple sanity check is disabled by default since "
         "the naive implementation can take a while to complete.");

    OptionParser optionParser(benchOptions, argc, argv);

    // We should probably figure out some way of having this be implicitly set
    // if the argument is provided to OptionParser. Right now every main()
    // method has to do this separately (and if they don't, the argument just
    // silently has no effect). It gets even more confusing if there are
    // multiple contexts per process.
    context.transportManager->setSessionTimeout(
        optionParser.options.getSessionTimeout());

    if (options.utilization < 1 || options.utilization > 100) {
        fprintf(stderr, "ERROR: Utilization must be between 1 and 100, "
            "inclusive\n");
        exit(1);
    }
    if (options.distributionName != "uniform" &&
      options.distributionName != "hotAndCold" &&
      options.distributionName != "zipfian") {
        fprintf(stderr, "ERROR: Distribution must be one of \"uniform\", "
            "\"hotAndCold\", or \"zipfian\"\n");
        exit(1);
    }
    if (options.objectSize < 1 || options.objectSize > MAX_OBJECT_SIZE) {
        fprintf(stderr, "ERROR: objectSize must be between 1 and %u\n",
            MAX_OBJECT_SIZE);
        exit(1);
    }
    if (options.objectsPerRpc < 1) {
        fprintf(stderr, "ERROR: objectPerRpc must be >= 1\n");
        exit(1);
    }
    if (options.pipelinedRpcs < 1) {
        fprintf(stderr, "ERROR: pipelinedRpcs must be >= 1\n");
        exit(1);
    }

    FILE* metricsFile = NULL;
    FILE* latencyFile = NULL;
    FILE* rawPrefillFile = NULL;
    FILE* rawBenchFile = NULL;
    FILE* diskHistCleanedFile = NULL;
    FILE* diskHistAllFile = NULL;
    FILE* clusterMetricsPrefillFile = NULL;
    FILE* clusterMetricsBenchFile = NULL;

    if (options.outputFilesPrefix != "") {
        string& outPrefix = options.outputFilesPrefix;
        string metricsFilename = outPrefix + "-m.txt";
        string latencyFilename = outPrefix + "-l.txt";
        string rawPrefillFilename = outPrefix + "-rp.txt";
        string rawBenchFilename = outPrefix + "-rb.txt";
        string diskHistCleanedFilename = outPrefix + "-dhc.txt";
        string diskHistAllFilename = outPrefix + "-dha.txt";
        string clusterMetricsPrefillFilename = outPrefix + "-cmp.txt";
        string clusterMetricsBenchFilename = outPrefix + "-cmb.txt";

        if (fileExists(metricsFilename) ||
          fileExists(latencyFilename) ||
          fileExists(rawPrefillFilename) ||
          fileExists(rawBenchFilename) ||
          fileExists(diskHistCleanedFilename) ||
          fileExists(diskHistAllFilename) ||
          fileExists(clusterMetricsPrefillFilename) ||
          fileExists(clusterMetricsBenchFilename)) {
            fprintf(stderr,
                "One or more output files (%s, %s, %s, %s, %s, %s, %s or %s) "
                "already exist!\n",
                metricsFilename.c_str(),
                latencyFilename.c_str(),
                rawPrefillFilename.c_str(),
                rawBenchFilename.c_str(),
                diskHistCleanedFilename.c_str(),
                diskHistAllFilename.c_str(),
                clusterMetricsPrefillFilename.c_str(),
                clusterMetricsBenchFilename.c_str());
            exit(1);
        }

        metricsFile = fopen(metricsFilename.c_str(), "w");
        latencyFile = fopen(latencyFilename.c_str(), "w");
        rawPrefillFile = fopen(rawPrefillFilename.c_str(), "w");
        rawBenchFile = fopen(rawBenchFilename.c_str(), "w");
        diskHistCleanedFile = fopen(diskHistCleanedFilename.c_str(), "w");
        diskHistAllFile = fopen(diskHistAllFilename.c_str(), "w");
        clusterMetricsPrefillFile =
            fopen(clusterMetricsPrefillFilename.c_str(), "w");
        clusterMetricsBenchFile =
            fopen(clusterMetricsBenchFilename.c_str(), "w");
    }

    // Set an alarm to abort this in case we can't connect.
    signal(SIGALRM, timedOut);
    alarm(options.abortTimeout);

    string coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
    if (coordinatorLocator.size() == 0) {
        coordinatorLocator = optionParser.options.getCoordinatorLocator();
    }
    fprintf(stderr, "Connecting to %s\n", coordinatorLocator.c_str());
    RamCloud ramcloud(&context, coordinatorLocator.c_str(),
            optionParser.options.getClusterName().c_str());

    // Get server parameters...
    // Perhaps this (and creating the distribution?) should be pushed into
    // Benchmark.
    ramcloud.createTable(options.tableName.c_str());
    uint64_t tableId = ramcloud.getTableId(options.tableName.c_str());

    string locator =
        context.objectFinder->lookupTablet(tableId, 0)->serviceLocator;

    ProtoBuf::ServerConfig serverConfig;
    ramcloud.getServerConfig(locator.c_str(), serverConfig);

    ProtoBuf::LogMetrics logMetrics;
    ramcloud.getLogMetrics(locator.c_str(), logMetrics);
    uint64_t logSize = logMetrics.seglet_metrics().total_usable_seglets() *
                       serverConfig.seglet_size();

    Distribution* distribution = NULL;
    if (options.distributionName == "uniform") {
        distribution = new UniformDistribution(logSize,
                                               options.utilization,
                                               options.objectSize);
    } else if (options.distributionName == "hotAndCold") {
        distribution = new HotAndColdDistribution(logSize,
                                                  options.utilization,
                                                  options.objectSize,
                                                  90, 10);
    } else if (options.distributionName == "zipfian") {
        // Since Zipfian can take a little while to compute the right
        // distribution, disable the alarm temporarily.
        alarm(0);
        distribution = new ZipfianDistribution(logSize,
                                               options.utilization,
                                               options.objectSize,
                                               90, 15);
        alarm(options.abortTimeout);
    } else {
        assert(0);
    }

    Benchmark benchmark(ramcloud,
                        tableId,
                        locator,
                        *distribution,
                        options);
    setvbuf(stdout, NULL, _IONBF, 0);
    Output output(ramcloud, locator, serverConfig, benchmark);
    output.addFile(stdout);
    if (metricsFile != NULL)
        output.addFile(metricsFile);
    output.dumpBeginning();
    output.dumpParameters(options, logMetrics);

    // Reset the alarm. Benchmark::run() will throw an exception if it can't
    // make progress.
    alarm(0);

    signal(SIGINT, sigIntHandler);
    benchmark.run(options.abortTimeout);
    if (interrupted) {
        output.removeFiles();
        output.addFile(stdout);
        output.dump();
        output.dumpEnd();
        exit(1);
    }

    output.dump();
    output.dumpEnd();

    if (latencyFile != NULL) {
        fprintf(latencyFile, "=== PREFILL LATENCIES ===\n");
        fprintf(latencyFile, "%s\n\n",
            benchmark.prefillLatencyHistogram.toString().c_str());
        fprintf(latencyFile, "=== BENCHMARK LATENCIES ===\n");
        fprintf(latencyFile, "%s\n",
            benchmark.latencyHistogram.toString().c_str());
    }

    if (rawPrefillFile != NULL) {
        fprintf(rawPrefillFile, "%s", serverConfig.DebugString().c_str());
        fprintf(rawPrefillFile, "%s",
            benchmark.prefillLogMetrics.DebugString().c_str());
    }

    if (rawBenchFile != NULL) {
        fprintf(rawBenchFile, "%s", serverConfig.DebugString().c_str());
        fprintf(rawBenchFile, "%s",
            benchmark.finalLogMetrics.DebugString().c_str());
    }

    if (diskHistCleanedFile != NULL) {
        const ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& onDiskMetrics =
            benchmark.finalLogMetrics.cleaner_metrics().on_disk_metrics();
        Histogram h(onDiskMetrics.cleaned_segment_disk_histogram());
        fprintf(diskHistCleanedFile, "%s", h.toString().c_str());
    }

    if (diskHistAllFile != NULL) {
        const ProtoBuf::LogMetrics_CleanerMetrics_OnDiskMetrics& onDiskMetrics =
            benchmark.finalLogMetrics.cleaner_metrics().on_disk_metrics();
        Histogram h(onDiskMetrics.all_segments_disk_histogram());
        fprintf(diskHistAllFile, "%s", h.toString().c_str());
    }

    if (clusterMetricsPrefillFile != NULL) {
        dumpClusterMetrics(&benchmark.prefillClusterMetrics,
                           clusterMetricsPrefillFile);
    }

    if (clusterMetricsBenchFile != NULL) {
        dumpClusterMetrics(&benchmark.benchmarkClusterMetrics,
                           clusterMetricsBenchFile);
    }

    if (verifyObjects) {
        uint64_t key = 0;
        uint64_t totalBytes = 0;
        while (1) {
            Buffer buffer;
            try {
                ramcloud.read(tableId, &key, sizeof(key), &buffer);
            } catch (...) {
                break;
            }
            totalBytes += buffer.size();
            key++;
        }
        fprintf(stderr, "%lu keys with %lu object bytes\n", key, totalBytes);
    }

    return 0;
} catch (ClientException& e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
