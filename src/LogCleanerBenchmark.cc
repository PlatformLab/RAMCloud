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

#include "Common.h"

#include "Context.h"
#include "RawMetrics.h"
#include "RamCloud.h"
#include "MasterService.h"
#include "OptionParser.h"
#include "MasterClient.h"
#include "Tub.h"

namespace RAMCloud {

static uint64_t
randInt(uint64_t floor, uint64_t ceiling)
{
    assert(ceiling >= floor);
    return floor + (generateRandom() % (ceiling - floor + 1));
}

static uint64_t
uniform(uint64_t maxKeyVal)
{
    return randInt(0, maxKeyVal);
}

static uint64_t
hotAndCold(uint64_t maxKeyVal)
{
    // HOT% of objs written 100 - HOT% of the time.
    unsigned int HOT = 10;

    double hotFraction = static_cast<double>(HOT) / 100.0;
    if (randInt(0, 100) < (100 - HOT)) {
        return randInt(0, static_cast<uint64_t>(hotFraction *
            static_cast<double>(maxKeyVal)));
    } else {
        return randInt(static_cast<uint64_t>(hotFraction *
            static_cast<double>(maxKeyVal)), maxKeyVal);
    }
}

static void
runIt(RamCloud* client,
      uint64_t tableId,
      uint64_t maxId,
      int objectSize,
      uint64_t (*nextId)(uint64_t))
{
    char objBuf[objectSize];

    const int loops = 100;

    string* keys = new string[maxId * loops];
    for (uint64_t i = 0; i < maxId * loops; i++)
        keys[i] = format("%08lu", nextId(maxId));

    uint64_t bytesWritten = 0;
    uint64_t objectsWritten = 0;
    uint64_t startTime = Cycles::rdtsc();
    double lastUpdateTime = 0;
    uint64_t lastUpdateBytes = 0;
    uint64_t lastUpdateObjects = 0;

    for (uint64_t i = 0; i < maxId * loops; i++) {
        client->write(tableId, keys[i].c_str(),
                downCast<uint16_t>(keys[i].length()), objBuf, objectSize);

        bytesWritten += objectSize;
        objectsWritten++;

        double totalTime = Cycles::toSeconds(Cycles::rdtsc() - startTime);
        if (totalTime >= 1 + lastUpdateTime) {
            double timeDelta = totalTime - lastUpdateTime;

            printf("\r%60s", "");
            printf("\r%lu objects, %.2f MB  (%.1f objs/sec, %.1f MB/sec)",
                objectsWritten,
                static_cast<double>(bytesWritten) / 1024 / 1024,
                static_cast<double>(objectsWritten - lastUpdateObjects) /
                timeDelta,
                static_cast<double>(bytesWritten - lastUpdateBytes) /
                timeDelta / 1024 / 1024);
            fflush(stdout);

            lastUpdateTime = totalTime;
            lastUpdateBytes = bytesWritten;
            lastUpdateObjects = objectsWritten;
        }
    }

    printf("\n");
}

} // namespace RAMCloud

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    Context context(true);

    int objectSize;
    int logSize;
    int utilisation;
    string distribution;
    string tableName;

    OptionsDescription benchOptions("Bench");
    benchOptions.add_options()
        ("table,t",
         ProgramOptions::value<string>(&tableName)->
            default_value("cleanerBench"),
         "name of the table to use for testing.")
        ("size,s",
         ProgramOptions::value<int>(&objectSize)->
           default_value(4096),
         "size of each object in bytes.")
         ("logMegs,m",
          ProgramOptions::value<int>(&logSize)->
            default_value(0),
          "total size of the master's log in megabytes.")
        ("utilisation,u",
         ProgramOptions::value<int>(&utilisation)->
           default_value(50),
         "Percentage of the log space to utilise.")
        ("distribution,d",
         ProgramOptions::value<string>(&distribution)->
           default_value("uniform"),
         "Object distribution; choose one of \"uniform\" or "
         "\"hotAndCold\"");

    OptionParser optionParser(benchOptions, argc, argv);

    if (logSize <= 0) {
        fprintf(stderr, "ERROR: You must specify a log size in megabytes\n");
        exit(1);
    }
    if (utilisation < 1 || utilisation > 100) {
        fprintf(stderr, "ERROR: Utilisation must be between 1 and 100, "
            "inclusive\n");
        exit(1);
    }
    if (distribution != "uniform" && distribution != "hotAndCold") {
        fprintf(stderr, "ERROR: Distribution must be one of \"uniform\" or "
            "\"hotAndCold\"\n");
        exit(1);
    }
    if (objectSize < 1 || objectSize > MAX_OBJECT_SIZE) {
        fprintf(stderr, "ERROR: objectSize must be between 1 and %u\n",
            MAX_OBJECT_SIZE);
    }

    uint64_t maxKeyVal = ((uint64_t)logSize * 1024 * 1024) / objectSize;
    maxKeyVal = static_cast<uint64_t>(static_cast<double>(maxKeyVal) *
         static_cast<double>(utilisation) / 100.0);

    printf("========== Log Cleaner Benchmark ==========\n");
    printf(" %dMB Log, %d-byte objects, %d%% utilisation, max key value %lu\n",
        logSize, objectSize, utilisation, maxKeyVal);
    printf(" running the %s distribution\n", distribution.c_str());

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    printf("client: Connecting to %s\n", coordinatorLocator.c_str());

    RamCloud* client = new RamCloud(coordinatorLocator.c_str());
    client->createTable(tableName.c_str());
    uint64_t table = client->getTableId(tableName.c_str());

    if (distribution == "uniform")
        runIt(client, table, maxKeyVal, objectSize, uniform);
    else
        runIt(client, table, maxKeyVal, objectSize, hotAndCold);

    return 0;
} catch (ClientException& e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
