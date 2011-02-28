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

#include "BenchUtil.h"
#include "RamCloud.h"
#include "OptionParser.h"
#include "Tub.h"

using namespace RAMCloud;

void
runRecovery(RamCloud& client,
            int count, uint32_t objectDataSize,
            int tableCount,
            int tableSkip)
{
    uint64_t b;

    b = rdtsc();
    char tableName[20];
    int tables[tableCount];

    for (int t = 0; t < tableCount; t++) {
        snprintf(tableName, sizeof(tableName), "%d", t);
        client.createTable(tableName);
        tables[t] = client.openTable(tableName);
        int table = tables[t];

        char val[objectDataSize];
        memset(val, 0xcc, objectDataSize);
        uint64_t id = 0xfffffff;

        LOG(NOTICE, "Performing %u inserts of %u byte objects",
            count, objectDataSize);
        b = rdtsc();
        Tub<RamCloud::Create> createRpcs[8];
        for (int j = 0; j < count - 1; j++) {
            auto& createRpc = createRpcs[j % arrayLength(createRpcs)];
            if (createRpc)
                (*createRpc)();
            createRpc.construct(client,
                                table, static_cast<void*>(val), objectDataSize,
                                /* version = */ static_cast<uint64_t*>(NULL),
                                /* async = */ true);
        }
        foreach (auto& createRpc, createRpcs) {
            if (createRpc)
                (*createRpc)();
        }
        id = client.create(table, val, objectDataSize,
                           /* version = */ NULL,
                           /* async = */ false);
        LOG(DEBUG, "%d inserts took %lu ticks", count, rdtsc() - b);
        LOG(DEBUG, "avg insert took %lu ticks", (rdtsc() - b) / count);

        // Create tables on the other masters so we skip back around to the
        // first in round-robin order to create multiple tables in the same
        // will
        for (int tt = 0; tt < tableSkip; tt++) {
            snprintf(tableName, sizeof(tableName), "junk%d.%d", t, tt);
            client.createTable(tableName);
        }
    }

    // dump the tablet map
    for (int t = 0; t < tableCount; t++) {
        Transport::SessionRef session =
            client.objectFinder.lookup(tables[t], 0);
        LOG(NOTICE, "%s has table %u",
            session->getServiceLocator().c_str(), tables[t]);
    }

    // dump out coordinator rpc info
    client.ping();

    LOG(NOTICE, "- quiescing writes");
    client.coordinator.quiesce();

    Transport::SessionRef session = client.objectFinder.lookup(tables[0], 0);
    LOG(NOTICE, "--- hinting that the server is down: %s ---",
        session->getServiceLocator().c_str());

    b = rdtsc();
    client.coordinator.hintServerDown(
        session->getServiceLocator().c_str());

    LOG(NOTICE, "- flushing map\n");
    client.objectFinder.flush();

    Buffer nb;
    session = client.objectFinder.lookup(tables[0], 0);
    LOG(NOTICE, "- attempting read from recovery master: %s",
        session->getServiceLocator().c_str());

    // Check a value in each table to make sure we're good
    for (int t = 0; t < tableCount; t++) {
        LOG(NOTICE, "reading recovered data  on %s",
            session->getServiceLocator().c_str());
        int table = tables[t];
        try {
            client.read(table, 0, &nb);
        } catch (...) {
        }

        session = client.objectFinder.lookup(tables[t], 0);
        LOG(NOTICE, "read value has length %u", nb.getTotalLength());
    }
    LOG(NOTICE, "Recovery completed in %lu ns",
        cyclesToNanoseconds(rdtsc() - b));

    // dump out coordinator rpc info
    client.ping();
}

int
main(int argc, char *argv[])
try
{
    bool hintServerDown;
    int count;
    uint32_t objectDataSize;
    uint32_t tableCount;
    uint32_t skipCount;

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("down,d",
         ProgramOptions::bool_switch(&hintServerDown),
         "Report the master we're talking to as down just before exit.")
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
        ("size,s",
         ProgramOptions::value<uint32_t>(&objectDataSize)->
            default_value(1024),
         "Number of bytes to insert per object during insert phase.");

    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());

    if (hintServerDown) {
        runRecovery(client, count, objectDataSize, tableCount, skipCount);
        return 0;
    }

    uint64_t b;

    b = rdtsc();
    client.createTable("test");
    uint32_t table;
    table = client.openTable("test");
    LOG(DEBUG, "create+open table took %lu ticks", rdtsc() - b);

    b = rdtsc();
    client.ping();
    LOG(DEBUG, "ping took %lu ticks on the client", rdtsc() - b);

    b = rdtsc();
    client.write(table, 42, "Hello, World!", 14);
    LOG(DEBUG, "write took %lu ticks", rdtsc() - b);

    b = rdtsc();
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    client.write(table, 43, value, strlen(value) + 1);
    LOG(DEBUG, "write took %lu ticks", rdtsc() - b);

    Buffer buffer;
    b = rdtsc();
    uint32_t length;

    client.read(table, 43, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);

    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    client.read(table, 42, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);
    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    b = rdtsc();
    uint64_t id = 0xfffffff;
    id = client.create(table, "Hello, World?", 14);
    LOG(DEBUG, "insert took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "Got back [%lu] id", id);

    b = rdtsc();
    client.read(table, id, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);
    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    char val[objectDataSize];
    memset(val, 0xcc, objectDataSize);
    id = 0xfffffff;

    LOG(NOTICE, "Performing %u inserts of %u byte objects",
        count, objectDataSize);
    b = rdtsc();
    for (int j = 0; j < count; j++)
        id = client.create(table, val, strlen(val) + 1);
    LOG(DEBUG, "%d inserts took %lu ticks", count, rdtsc() - b);
    LOG(DEBUG, "avg insert took %lu ticks", (rdtsc() - b) / count);

    client.dropTable("test");

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
