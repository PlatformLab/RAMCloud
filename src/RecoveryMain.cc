/* Copyright (c) 2009-2011 Stanford University
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
#include "Cycles.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"
#include "PingClient.h"

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

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    int count, removeCount;
    uint32_t objectDataSize;
    uint32_t tableCount;
    uint32_t tableSkip;

    // need external context to set log levels with OptionParser
    Context context(true);
    Context::Guard _(context);

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
            default_value(0),
         "Index of this client (first client is 0)")
        ("fast,f",
         ProgramOptions::bool_switch(&fillWithTestData),
         "Use a single fillWithTestData rpc to insert recovery objects.")
        ("tables,t",
         ProgramOptions::value<uint32_t>(&tableCount)->
            default_value(1),
         "The number of tables to create with number objects on the master.")
        ("skip,k",
         ProgramOptions::value<uint32_t>(&tableSkip)->
            default_value(1),
         "The number of empty tables to create per real table."
         "An enormous hack to create partitions on the crashed master.")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
            default_value(1),
         "Total number of clients running")
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
    Context::get().transportManager->setTimeout(
            optionParser.options.getTransportTimeout());

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    RamCloud client(context,
                    optionParser.options.getCoordinatorLocator().c_str());

    if (removeCount > count)
        DIE("cannot remove more objects than I create!");
    if (verify && objectDataSize < 20)
        DIE("need >= 20 byte objects to do verification!");
    if (verify && fillWithTestData)
        DIE("verify not supported with fillWithTestData");

    char tableName[20];
    int tables[tableCount];

    for (uint32_t t = 0; t < tableCount; t++) {
        snprintf(tableName, sizeof(tableName), "%d", t);
        client.createTable(tableName);
        tables[t] = client.openTable(tableName);

        // Create tables on the other masters so we skip back around to the
        // first in round-robin order to create multiple tables in the same
        // will
        for (uint32_t tt = 0; tt < tableSkip; tt++) {
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
            for (uint32_t t = 0; t < tableCount; t++) {
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
    for (uint32_t t = 0; t < tableCount; t++) {
        for (int j = 0; removeCount > 0; j++, removeCount--)
            client.remove(tables[t], j);

    }

    // dump the tablet map
    for (uint32_t t = 0; t < tableCount; t++) {
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
    LOG(NOTICE, "--- Terminating master %s ---",
        session->getServiceLocator().c_str());

    // Take an initial snapshot of performance metrics.
    ClusterMetrics metricsBefore(&client);

    MasterClient oldMaster(session);
    PingClient pingClient;
    uint64_t startTime = Cycles::rdtsc();
    PingClient::Kill killOp(pingClient, session->getServiceLocator().c_str());

    // Wait for failure to be detected
    client.objectFinder.waitForTabletDown();
    uint64_t downTime = Cycles::rdtsc();
    LOG(NOTICE, "tablet down");

    // Cancel the kill RPC
    killOp.cancel();

    // Wait for recovery to complete
    client.objectFinder.waitForAllTabletsNormal();
    LOG(NOTICE, "all tablets now normal");

    Buffer nb;
    uint64_t stopTime = Cycles::rdtsc();
    // Check a value in each table to make sure we're good
    for (uint32_t t = 0; t < tableCount; t++) {
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
    LOG(NOTICE, "Recovery completed in %lu ns, failure detected in %lu ns",
        Cycles::toNanoseconds(stopTime - startTime),
        Cycles::toNanoseconds(downTime - startTime));

    // Take another snapshot of performance metrics.
    ClusterMetrics metricsAfter(&client);

    uint64_t verificationStart = Cycles::rdtsc();
    if (verify) {
        LOG(NOTICE, "Verifying all data.");

        int total = count * tableCount;
        int tenPercent = total / 10;
        int logCount = 0;
        for (int j = 0; j < count - 1; j++) {
            for (uint32_t t = 0; t < tableCount; t++) {
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
            Cycles::toNanoseconds(Cycles::rdtsc() - verificationStart));
    }

    // Log the delta of the recovery time statistics
    ClusterMetrics diff = metricsAfter.difference(metricsBefore);
    if (metricsAfter.size() != diff.size()) {
        LOG(ERROR, "Metrics mismatches: %lu",
                metricsAfter.size() - diff.size());
    }
    for (ClusterMetrics::iterator serverIt = diff.begin();
            serverIt != diff.end(); serverIt++) {
        LOG(NOTICE, "Metrics: begin server %s", serverIt->first.c_str());
        ServerMetrics &server = serverIt->second;
        for (ServerMetrics::iterator metricIt = server.begin();
                metricIt != server.end(); metricIt++) {
            LOG(NOTICE, "Metrics: %s %lu", metricIt->first.c_str(),
                    metricIt->second);
        }
    }

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
