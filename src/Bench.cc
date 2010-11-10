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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <getopt.h>
#include <errno.h>

#include <string>

#include "RamCloud.h"
#include "BenchUtil.h"
#include "OptionParser.h"

namespace RC = RAMCloud;

std::string coordinatorLocator;
bool multirow;
bool randomReads;
bool pmcInsteadOfTSC;
uint64_t count;
uint64_t size;
int cpu;

RC::RamCloud *client;
uint32_t table;

void
cleanup()
{
    client->dropTable("test");
    delete client;
    client = NULL;
}

void
setup()
{
    if (cpu != -1) {
        if (!RC::pinToCpu(cpu))
            DIE("bench: Couldn't pin to core %d", cpu);
        LOG(RC::DEBUG, "bench: Pinned to core %d", cpu);
    }

    client = new RC::RamCloud(coordinatorLocator.c_str());

    assert(!atexit(cleanup));

    RC::PerfCounterType type;
    type = pmcInsteadOfTSC ? RC::PERF_COUNTER_PMC : RC::PERF_COUNTER_TSC;
    client->selectPerfCounter(type,
                              RC::MARK_RPC_PROCESSING_BEGIN,
                              RC::MARK_RPC_PROCESSING_END);

    client->createTable("test");
    table = client->openTable("test");
}

void
bench(const char *name, uint64_t (f)(void))
{
    uint64_t start, end, cycles;

    start = rdtsc();
    uint64_t serverCounter = f();
    end = rdtsc();

    cycles = end - start;
    printf("%s ns     %12lu\n", name,
           RC::cyclesToNanoseconds(cycles));
    printf("%s avgns  %12.2f\n", name,
           static_cast<double>(RC::cyclesToNanoseconds(cycles)) /
           static_cast<double>(count));
    printf("%s ctr    %12.0f\n", name,
           static_cast<double>(serverCounter));
    printf("%s avgctr %12.2f\n", name,
           static_cast<double>(serverCounter) /
           static_cast<double>(count));
}

#define BENCH(fname) bench(#fname, fname)

uint64_t
writeOne()
{
    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    client->write(table, 0, &buf[0], size);

    return client->counterValue;
}

uint64_t
writeMany(void)
{
    uint64_t serverCounter;

    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    serverCounter = 0;
    for (uint64_t i = 0; i < count; i++) {
        client->write(table, i, &buf[0], size);
        serverCounter += client->counterValue;
    }

    return serverCounter;
}

uint64_t
readMany()
{
    uint64_t serverCounter;
    uint64_t key;

    serverCounter = 0;
    for (uint64_t i = 0; i < count; i++) {
        RC::Buffer value;
        key = randomReads ? generateRandom() % count : i;
        client->read(table, multirow ? key : 0, &value);
        serverCounter += client->counterValue;
    }

    return serverCounter;
}

int
main(int argc, char *argv[])
try
{
    RC::OptionsDescription benchOptions("Bench");
    benchOptions.add_options()
        ("cpu,p",
         RC::ProgramOptions::value<int>(&cpu)->
           default_value(-1),
         "CPU mask to pin to")
        ("multirow,m",
         RC::ProgramOptions::bool_switch(&multirow),
         "Write number of objects equal to number parameter.")
        ("number,n",
         RC::ProgramOptions::value<uint64_t>(&count)->
           default_value(10000),
         "Number of iterations to write/read.")
        ("random,R",
         RC::ProgramOptions::bool_switch(&randomReads),
         "Randomize key order instead of incremental.")
        ("performance,P",
         RC::ProgramOptions::bool_switch(&pmcInsteadOfTSC),
         "Measure using rdpmc instead of rdtsc")
        ("size,S",
         RC::ProgramOptions::value<uint64_t>(&size)->
           default_value(100),
         "Size in bytes of objects to write/read.");

    RC::OptionParser optionParser(benchOptions, argc, argv);

    coordinatorLocator = optionParser.options.getCoordinatorLocator();
    printf("client: Connecting to %s\n", coordinatorLocator.c_str());

    printf("Reads: %lu, Size: %lu, Multirow: %lu, RandomReads: %lu\n",
           count, size, multirow, randomReads);

    setup();

    if (multirow) {
        BENCH(writeMany);
    } else {
        BENCH(writeOne);
    }

    BENCH(readMany);

    return 0;
} catch (RC::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
