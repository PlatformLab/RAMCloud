/* Copyright (c) 2009-2012 Stanford University
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
#include <getopt.h>
#include <assert.h>

// #include "OptionParser.h"
#include "Context.h"
#include "ClusterMetrics.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"

// #define USE_VALGRIND

#ifdef USE_VALGRIND
#define REPEAT_TO_LEAK 50
#else
// #define REPEAT_TO_LEAK 800000
#define REPEAT_TO_LEAK 600000
#endif // USE_VALGRIND

using namespace RAMCloud;

/*
 * Speed up recovery insertion with the single-shot FillWithTestData RPC.
 */
bool fillWithTestData = false;

int
main(int argc, char *argv[])
try
{
    int count, removeCount;
    uint32_t objectDataSize;
    uint32_t tableCount;
    uint32_t skipCount;
    uint64_t b;
    int clientIndex;
    int numClients;

    // need external context to set log levels with OptionParser
    Context context(true);

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()

        // These first two options are currently ignored. They're here so that
        // this script can be run with cluster.py.
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
            default_value(0),
         "Index of this client (first client is 0; currently ignored)")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
            default_value(1),
         "Total number of clients running (currently ignored)")

        ("fast,f",
         ProgramOptions::bool_switch(&fillWithTestData),
         "Use a single fillWithTestData rpc to insert recovery objects.")
        ("tables,t",
         ProgramOptions::value<uint32_t>(&tableCount)->
            default_value(1),
         "The number of tables to create with number objects on the master.")
        ("skip,k",
         ProgramOptions::value<uint32_t>(&skipCount)->
            default_value(1),
         "The number of empty tables to create per real table."
         "An enormous hack to create partitions on the crashed master.")
        ("number,n",
         ProgramOptions::value<int>(&count)->
            default_value(1024),
         "The number of values to insert.")
        ("removals,r",
         ProgramOptions::value<int>(&removeCount)->default_value(0),
         "The number of values inserted to remove (creating tombstones).")
        ("size,s",
         ProgramOptions::value<uint32_t>(&objectDataSize)->
            default_value(1024),
         "Number of bytes to insert per object during insert phase.");

    OptionParser optionParser(clientOptions, argc, argv);
    context.transportManager->setTimeout(
            optionParser.options.getTransportTimeout());

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    b = Cycles::rdtsc();
    int rep = REPEAT_TO_LEAK;
    LOG(NOTICE, "Now repeat RamCloud call %d times.",
        rep);
    const char* coord
            = optionParser.options.getCoordinatorLocator().c_str();

    for (int i=0; i < rep; i++) {
        RamCloud client0(&context,coord);
        client0.createTable("test");
        client0.getTableId("test");
        if (i % 10000L == 0) {
            LOG(NOTICE, "%d times.. ", i);
        }
    }

    // table = client.getTableId("test");
    LOG(NOTICE, "create+open table took %lu ticks", Cycles::rdtsc() - b);

    RamCloud client(&context,
                    optionParser.options.getCoordinatorLocator().c_str());
    
    // just test run.
    client.createTable("test");
    uint64_t table;
    table = client.getTableId("test");

    b = Cycles::rdtsc();
    client.write(table, "42", 2, "Hello, World!", 14);
    LOG(NOTICE, "write took %lu ticks", Cycles::rdtsc() - b);

    b = Cycles::rdtsc();
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    client.write(table, "43", 2, value, downCast<uint32_t>(strlen(value) + 1));
    LOG(NOTICE, "write took %lu ticks", Cycles::rdtsc() - b);

    Buffer buffer;
    b = Cycles::rdtsc();
    uint32_t length;

    client.read(table, "43", 2, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);

    length = buffer.getTotalLength();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    client.read(table, "42", 2, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);
    length = buffer.getTotalLength();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    char val[objectDataSize];
    memset(val, 0xcc, objectDataSize);

    LOG(NOTICE, "Performing %u writes of %u byte objects",
        count, objectDataSize);
    string keys[count];
    for (int j = 0; j < count; j++)
        keys[j] = format("%d", j);
    b = Cycles::rdtsc();
    for (int j = 0; j < count; j++)
        client.write(table, keys[j].c_str(),
                     downCast<uint16_t>(keys[j].length()),
                     val, downCast<uint32_t>(strlen(val) + 1));
    LOG(NOTICE, "%d writes took %lu ticks", count, Cycles::rdtsc() - b);
    LOG(NOTICE, "avg write took %lu ticks", (Cycles::rdtsc() - b) / count);

    LOG(NOTICE, "Reading one of the objects just inserted");
    client.read(table, "0", 1, &buffer);

    LOG(NOTICE, "Performing %u removals of objects just inserted", removeCount);
    for (int j = 0; j < count && j < removeCount; j++) {
        string key = format("%d", j);
        client.remove(table, key.c_str(), downCast<uint16_t>(key.length()));
    }

    client.dropTable("test");

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
