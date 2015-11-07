/* Copyright (c) 2009-2015 Stanford University
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

#include "ClusterMetrics.h"
#include "Context.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"
#include "IndexLookup.h"

using namespace RAMCloud;

/*
 * Speed up recovery insertion with the single-shot FillWithTestData RPC.
 */
bool fillWithTestData = false;

/**
 * This method is used for testing coordinator crash recovery. It is
 * normally invoked repeatedly. Each invocation runs a set of representative
 * cluster operations, with some consistency checks mixed in.
 *
 * \param client
 *      Connection to the RAMCloud cluster.
 */
void
exerciseCluster(RamCloud* client)
{
    // This method maintains a collection of tables with names of the
    // form "tableX" where X is a number. At any given time a contiguous
    // range of tables should exist, such as table2, table3, and table4.
    // Over time, tables get created and deleted such that the existing
    // range gradually moves up. Each table contains a single object
    // named "tableName" whose value should be the same as the name of
    // the table.

    // Index of the last table that we believe should exist (0 means "none").
    static int expectedLast = 0;

    // Step 1: find the beginning of the range of existing tables.
    char tableName[100];
    int first, last;
    uint64_t tableId = 0;
    for (first = 1; first < 1000; first++) {
        snprintf(tableName, sizeof(tableName), "table%d", first);
        try {
            tableId = client->getTableId(tableName);
            break;
        } catch (TableDoesntExistException& e) {
            // This table doesn't exist; just go on to the next one.
        }
    }
    if (tableId == 0) {
        first = 1;
        printf("Couldn't find existing tables; starting at table1\n");
    }

    // Step 2: scan all existing tables to make sure they have the expected
    // objects.
    for (last = first; ; last++) {
        snprintf(tableName, sizeof(tableName), "table%d", last);
        try {
            tableId = client->getTableId(tableName);
            Buffer value;
            try {
                client->read(tableId, "tableName", 9, &value);
                const char* valueString = static_cast<const char*>(
                        value.getRange(0, value.size()));
                if (strcmp(valueString, tableName) != 0) {
                    printf("Bad value for tableName object in %s; "
                            "expected \"%s\", got \"%s\"\n",
                            tableName, tableName, valueString);
                }
            } catch (ClientException& e) {
                printf("Error reading tableName object in %s: %s\n",
                        tableName, e.toString());
            }
        } catch (TableDoesntExistException& e) {
            // End this step when we reach a table that does not exist.
            break;
        }
    }

    // Step 3: verify that what we have is what we expected.
    int numTables = last - first;
    last--;
    printf("-------------------------------------------------\n");
    if (numTables > 0) {
        printf("Found existing tables: table%d..table%d\n", first, last);
    }
    if (expectedLast > 0) {
        int expectedFirst = expectedLast - 4;
        if (expectedFirst < 1) {
            expectedFirst = 1;
        }
        if ((last != expectedLast) || (first != expectedFirst)) {
            printf("*** Error: expected table%d..table%d\n", expectedFirst,
                    expectedLast);
        }
    }
    printf("-------------------------------------------------\n");

    // Step 4: if we already have a bunch of tables, delete the oldest
    // table.
    if (numTables >= 5) {
        snprintf(tableName, sizeof(tableName), "table%d", first);
        try {
            client->dropTable(tableName);
            printf("Dropped %s\n", tableName);
        } catch (ClientException& e) {
            printf("Error dropping %s: %s\n",
                    tableName, e.toString());
        }
    }

    // Step 4: unless we already have a lot of tables, make a new table.
    last++;
    if (numTables <= 5) {
        snprintf(tableName, sizeof(tableName), "table%d", last);
        try {
            tableId = client->createTable(tableName, 1);
            try {
                client->write(tableId, "tableName", 9, &tableName,
                        downCast<uint32_t>(strlen(tableName) + 1));
            } catch (ClientException& e) {
                printf("Error write tableName object in %s: %s\n",
                        tableName, e.toString());
            }
            printf("Created new table %s\n", tableName);
        } catch (ClientException& e) {
            printf("Error creating %s: %s\n",
                    tableName, e.toString());
        }
    }
    expectedLast = last;
}

// Utility method used by indexCrash to print indexed entry.
bool
lookupAndLog(RamCloud* client, uint64_t tableId, const char* message)
{
    IndexKey::IndexKeyRange range(1, "Aaa", 3, "Bbb", 3);
    IndexLookup indexLookup(client, tableId, range);
    bool foundAny = false;
    while (indexLookup.getNext()) {
        Object* object = indexLookup.currentObject();
        uint32_t valueLength;
        const char* value = static_cast<const char*>(
                object->getValue(&valueLength));
        uint16_t  key1Length;
        const char* key1 = static_cast<const char*>(
                object->getKey(1, &key1Length));
        uint16_t key2Length;
        const char* key2 = static_cast<const char*>(
                object->getKey(2, &key2Length));
        LOG(NOTICE, "%s: value = %.*s, key1 = %.*s, key2 = %.*s", message,
                valueLength, value, key1Length, key1, key2Length, key2);
        foundAny = true;
    }
    return foundAny;
}

// Simple crash recovery test for a table with two indexes.
void indexCrash(RamCloud* client)
{
    uint64_t tableId = client->createTable("test", 3);
    LOG(NOTICE, "Created table");
    client->createIndex(tableId, 1, 0, 1);
    LOG(NOTICE, "Created index 1");
    client->createIndex(tableId, 2, 0, 1);
    LOG(NOTICE, "Created index 2");

    KeyInfo keys[3];
    keys[0].key = "InfoForAlice";
    keys[0].keyLength = 12;
    keys[1].key = "Alice";
    keys[1].keyLength = 5;
    keys[2].key = "California";
    keys[2].keyLength = 10;
    uint64_t newVersion;
    client->write(tableId, 3, keys, "This is a test value", 20, NULL,
            &newVersion, false);
    LOG(NOTICE, "Wrote value into table");

    if (!lookupAndLog(client, tableId, "Object value after writing")) {
        LOG(NOTICE, "Couldn't find value just written!");
    }

    KillRpc deathMessage(client, 2, "abc", 3);
    LOG(NOTICE, "Killed server containing index");
    // client->objectFinder.waitForTabletDown(tableId);
    uint64_t stopSleeping = Cycles::rdtsc() + Cycles::fromSeconds(0.5);
    while (Cycles::rdtsc() < stopSleeping) {
        client->clientContext->dispatch->poll();
    }
    if (!lookupAndLog(client, tableId, "Object value after recovery")) {
        LOG(NOTICE, "Found no objects after crash recovery");
    }
}

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
    bool exercise, indexCrashArg;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

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
         "Number of bytes to insert per object during insert phase.")
        ("exercise",
         ProgramOptions::bool_switch(&exercise),
         "Call exerciseCluster repeatedly (intended for coordinator "
         "crash testing).")
        ("indexCrash",
         ProgramOptions::bool_switch(&indexCrashArg),
         "Create a table with two indexes and one object, crash server,"
         "make sure index is recovered properly");

    OptionParser optionParser(clientOptions, argc, argv);
    context.transportManager->setSessionTimeout(
            optionParser.options.getSessionTimeout());

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    string locator = optionParser.options.getExternalStorageLocator();
    if (locator.size() == 0) {
        locator = optionParser.options.getCoordinatorLocator();
    }
    RamCloud client(&context, locator.c_str(),
            optionParser.options.getClusterName().c_str());

    if (exercise) {
        while (1) {
            exerciseCluster(&client);
            usleep(2000000);
        }
    }

    if (indexCrashArg) {
        indexCrash(&client);
        exit(0);
    }

    b = Cycles::rdtsc();
    client.createTable("test");
    uint64_t table;
    table = client.getTableId("test");
    LOG(NOTICE, "create+open table took %lu ticks", Cycles::rdtsc() - b);

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

    length = buffer.size();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    client.read(table, "42", 2, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);
    length = buffer.size();
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
    uint64_t bb =  Cycles::rdtsc();
    LOG(NOTICE, "%d writes took %lu ticks", count, bb - b);
    LOG(NOTICE, "avg write took %lu ticks, %lu nano seconds"
        , (bb - b) / count
        , Cycles::toNanoseconds((bb - b) / count));
    LOG(NOTICE, "Reading one of the objects just inserted");
    b = Cycles::rdtsc();
    client.read(table, "0", 1, &buffer);
    bb =  Cycles::rdtsc();
    LOG(NOTICE, "read took %lu ticks, %lu nano seconds"
        , bb - b
        , Cycles::toNanoseconds(bb - b));
    LOG(NOTICE, "Reading all of the objects just inserted.");
    b = Cycles::rdtsc();
    for (int j = 0; j < count; j++)
         client.read(table, keys[j].c_str(),
                     downCast<uint16_t>(keys[j].length()),
                     &buffer);
    bb =  Cycles::rdtsc();
    LOG(NOTICE, "avg read took %lu ticks, %lu nano seconds"
        , (bb - b) / count
        , Cycles::toNanoseconds((bb - b) / count));

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
