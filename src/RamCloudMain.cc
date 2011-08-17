/* Copyright (c) 2009 Stanford University
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

#include "Cycles.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "RamCloud.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "Tub.h"

using namespace RAMCloud;

/*
 * If true, add the table and object ids to every object, calculate and
 * append a checksum, and verify the whole package when recovery is done.
 * The crc is the first 4 bytes of the object. The tableId and objectId
 * are the last 16 bytes.
 */
bool verify = false;

/*
 * Speed up recovery insertion with the single-shot FillWithTestData RPC.
 */
bool fillWithTestData = false;

void
runRecovery(RamCloud& client,
            int count,
            int removeCount,
            uint32_t objectDataSize,
            int tableCount,
            int tableSkip)
{
    if (removeCount > count)
        DIE("cannot remove more objects than I create!");
    if (verify && objectDataSize < 20)
        DIE("need >= 20 byte objects to do verification!");
    if (verify && fillWithTestData)
        DIE("verify not supported with fillWithTestData");

    char tableName[20];
    int tables[tableCount];

    for (int t = 0; t < tableCount; t++) {
        snprintf(tableName, sizeof(tableName), "%d", t);
        client.createTable(tableName);
        tables[t] = client.openTable(tableName);

        // Create tables on the other masters so we skip back around to the
        // first in round-robin order to create multiple tables in the same
        // will
        for (int tt = 0; tt < tableSkip; tt++) {
            snprintf(tableName, sizeof(tableName), "junk%d.%d", t, tt);
            client.createTable(tableName);

            // For the first table on each other master, create an object in
            // the table. The sole purpose of this is to trigger code in the
            // master that will open connections with all the backups so that
            // connection setup doesn't happen during recovery and slow it down.
            if (t == 0) {
                int table = client.openTable(tableName);
                client.write(table, 1, "abcd", 4);
            }
        }
    }

    LOG(NOTICE, "Performing %u inserts of %u byte objects",
        count * tableCount, objectDataSize);

    if (fillWithTestData) {
        LOG(NOTICE, "Using the fillWithTestData rpc on a single master");
        uint64_t b = Cycles::rdtsc();
        MasterClient master(client.objectFinder.lookup(0, 0));
        master.fillWithTestData(count * tableCount, objectDataSize);
        LOG(NOTICE, "%d inserts took %lu ticks",
            count * tableCount, Cycles::rdtsc() - b);
        LOG(NOTICE, "avg insert took %lu ticks",
                    (Cycles::rdtsc() - b) / count / tableCount);
    } else {
        char val[objectDataSize];
        memset(val, 0xcc, objectDataSize);
        Crc32C checksumBasis;
        checksumBasis.update(&val[4], objectDataSize - 20);
        Tub<RamCloud::Create> createRpcs[8];
        uint64_t b = Cycles::rdtsc();
        for (int j = 0; j < count - 1; j++) {
            for (int t = 0; t < tableCount; t++) {
                auto& createRpc = createRpcs[(j * tableCount + t) %
                                             arrayLength(createRpcs)];
                if (createRpc)
                    (*createRpc)();

                if (verify) {
                    uint32_t *crcPtr = reinterpret_cast<uint32_t*>(&val[0]);
                    uint64_t *tableIdPtr =
                        reinterpret_cast<uint64_t*>(&val[objectDataSize-16]);
                    uint64_t *objectIdPtr =
                        reinterpret_cast<uint64_t*>(&val[objectDataSize-8]);
                    *tableIdPtr = tables[t];
                    *objectIdPtr = j;
                    Crc32C checksum = checksumBasis;
                    checksum.update(&val[objectDataSize-16], 16);
                    *crcPtr = checksum.getResult();
                }

                createRpc.construct(client,
                                    tables[t],
                                    static_cast<void*>(val), objectDataSize,
                                    /* version = */static_cast<uint64_t*>(NULL),
                                    /* async = */ true);
            }
        }
        foreach (auto& createRpc, createRpcs) {
            if (createRpc)
                (*createRpc)();
        }
        client.create(tables[0], val, objectDataSize,
                      /* version = */ NULL,
                      /* async = */ false);
        LOG(NOTICE, "%d inserts took %lu ticks",
            count * tableCount, Cycles::rdtsc() - b);
        LOG(NOTICE, "avg insert took %lu ticks",
            (Cycles::rdtsc() - b) / count / tableCount);
    }

    // remove objects if we've been instructed. just start from table 0, obj 0.
    LOG(NOTICE, "Performing %u removals of objects just created", removeCount);
    for (int t = 0; t < tableCount; t++) {
        for (int j = 0; removeCount > 0; j++, removeCount--)
            client.remove(tables[t], j);

    }

    // dump the tablet map
    for (int t = 0; t < tableCount; t++) {
        Transport::SessionRef session =
            client.objectFinder.lookup(tables[t], 0);
        LOG(NOTICE, "%s has table %u",
            session->getServiceLocator().c_str(), tables[t]);
    }

    // dump out coordinator rpc info
    // As of 6/2011 this needs to be reworked: ping no longer contains the
    // hack to print statistics.
    // client.ping();

    LOG(NOTICE, "- quiescing writes");
    client.coordinator.quiesce();

    Transport::SessionRef session = client.objectFinder.lookup(tables[0], 0);
    LOG(NOTICE, "--- hinting that the server is down: %s ---",
        session->getServiceLocator().c_str());

    uint64_t b = Cycles::rdtsc();
    client.coordinator.hintServerDown(
        session->getServiceLocator().c_str());

    client.objectFinder.waitForAllTabletsNormal();
    LOG(NOTICE, "all tablets now normal");

    Buffer nb;
    uint64_t stopTime = Cycles::rdtsc();
    // Check a value in each table to make sure we're good
    for (int t = 0; t < tableCount; t++) {
        int table = tables[t];
        try {
            client.read(table, 0, &nb);
            if (t == 0)
                stopTime = Cycles::rdtsc();
        } catch (...) {
        }
        session = client.objectFinder.lookup(tables[t], 0);
        LOG(NOTICE, "recovered value read from %s has length %u",
            session->getServiceLocator().c_str(), nb.getTotalLength());
    }
    LOG(NOTICE, "Recovery completed in %lu ns",
        Cycles::toNanoseconds(stopTime - b));

    b = Cycles::rdtsc();
    if (verify) {
        LOG(NOTICE, "Verifying all data.");

        int total = count * tableCount;
        int tenPercent = total / 10;
        int logCount = 0;
        for (int j = 0; j < count - 1; j++) {
            for (int t = 0; t < tableCount; t++) {
                try {
                    client.read(tables[t], j, &nb);
                } catch (...) {
                    LOG(ERROR, "Failed to access object (tbl %d, obj %d)!",
                        tables[t], j);
                    continue;
                }
                const char* objData = nb.getStart<char>();
                uint32_t objBytes = nb.getTotalLength();

                if (objBytes != objectDataSize) {
                    LOG(ERROR, "Bad object size (tbl %d, obj %d)",
                        tables[t], j);
                } else {
                    Crc32C checksum;
                    checksum.update(&objData[4], objBytes - 4);
                    if (checksum.getResult() != *nb.getStart<uint32_t>()) {
                        LOG(ERROR, "Bad object checksum (tbl %d, obj %d)",
                            tables[t], j);
                    }
                }
            }

            logCount += tableCount;
            if (logCount >= tenPercent) {
                LOG(DEBUG, " -- %.2f%% done",
                    100.0 * (j * tableCount) / total);
                logCount = 0;
            }
        }

        LOG(NOTICE, "Verification took %lu ns",
            Cycles::toNanoseconds(Cycles::rdtsc() - b));
    }

    // dump out coordinator rpc info
    // As of 6/2011 this needs to be reworked: ping no longer contains the
    // hack to print statistics.
    // client.ping();
}

int
main(int argc, char *argv[])
try
{
    bool hintServerDown;
    int count, removeCount;
    uint32_t objectDataSize;
    uint32_t tableCount;
    uint32_t skipCount;

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("down,d",
         ProgramOptions::bool_switch(&hintServerDown),
         "Report the master we're talking to as down just before exit.")
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
        ("verify,v",
         ProgramOptions::bool_switch(&verify),
         "Verify the contents of all objects after recovery completes.");

    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());

    if (hintServerDown) {
        runRecovery(client, count, removeCount,
            objectDataSize, tableCount, skipCount);
        return 0;
    }

    uint64_t b;

    b = Cycles::rdtsc();
    client.createTable("test");
    uint32_t table;
    table = client.openTable("test");
    LOG(NOTICE, "create+open table took %lu ticks", Cycles::rdtsc() - b);

    b = Cycles::rdtsc();
    client.ping(optionParser.options.getCoordinatorLocator().c_str(),
                12345, 100000000);
    LOG(NOTICE, "coordinator ping took %lu ticks", Cycles::rdtsc() - b);

    b = Cycles::rdtsc();
    client.ping(table, 42, 12345, 100000000);
    LOG(NOTICE, "master ping took %lu ticks", Cycles::rdtsc() - b);

    b = Cycles::rdtsc();
    client.write(table, 42, "Hello, World!", 14);
    LOG(NOTICE, "write took %lu ticks", Cycles::rdtsc() - b);

    b = Cycles::rdtsc();
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    client.write(table, 43, value, downCast<uint32_t>(strlen(value) + 1));
    LOG(NOTICE, "write took %lu ticks", Cycles::rdtsc() - b);

    Buffer buffer;
    b = Cycles::rdtsc();
    uint32_t length;

    client.read(table, 43, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);

    length = buffer.getTotalLength();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    client.read(table, 42, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);
    length = buffer.getTotalLength();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    b = Cycles::rdtsc();
    uint64_t id = 0xfffffff;
    id = client.create(table, "Hello, World?", 14);
    LOG(NOTICE, "insert took %lu ticks", Cycles::rdtsc() - b);
    LOG(NOTICE, "Got back [%lu] id", id);

    b = Cycles::rdtsc();
    client.read(table, id, &buffer);
    LOG(NOTICE, "read took %lu ticks", Cycles::rdtsc() - b);
    length = buffer.getTotalLength();
    LOG(NOTICE, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    char val[objectDataSize];
    memset(val, 0xcc, objectDataSize);
    id = 0xfffffff;

    LOG(NOTICE, "Performing %u inserts of %u byte objects",
        count, objectDataSize);
    uint64_t* ids = static_cast<uint64_t*>(malloc(sizeof(ids[0]) * count));
    b = Cycles::rdtsc();
    for (int j = 0; j < count; j++)
        ids[j] = client.create(table, val, downCast<uint32_t>(strlen(val) + 1));
    LOG(NOTICE, "%d inserts took %lu ticks", count, Cycles::rdtsc() - b);
    LOG(NOTICE, "avg insert took %lu ticks", (Cycles::rdtsc() - b) / count);

    LOG(NOTICE, "Performing %u removals of objects just inserted", removeCount);
    for (int j = 0; j < count && j < removeCount; j++)
            client.remove(table, ids[j]);

    client.dropTable("test");

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
