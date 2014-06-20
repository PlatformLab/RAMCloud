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
 * If true, add the table, keyLength and key to every object, calculate and
 * append a checksum, and verify the whole package when recovery is done.
 * The crc is the first 4 bytes of the object. The tableId, keyLength and
 * key immediately follow the crc. There may be data bytes following this.
 */
bool verify = false;

/*
 * Speed up recovery insertion with the single-shot FillWithTestData RPC.
 */
bool fillWithTestData = false;

/**
 * TODO(Anyone): This method is duplicated wholesale in ClusterPerf.cc.
 *
 * Fill a buffer with an ASCII value that can be checked later to ensure
 * that no data has been lost or corrupted.  A particular tableId, key and
 * keyLength are incorporated into the value (under the assumption that
 * the value will be stored in that object), so that values stored in
 * different objects will be detectably different.
 *
 * \param buffer
 *      Buffer to fill; any existing contents will be discarded.
 * \param size
 *      Number of bytes of data to place in the buffer.
 * \param tableId
 *      This table identifier will be reflected in the value placed in the
 *      buffer.
 * \param key
 *      This key will be reflected in the value placed in the buffer.
 * \param keyLength
 *      This key Length will be reflected in the value placed in the buffer.
 */
void
fillBuffer(Buffer& buffer, uint32_t size, uint64_t tableId,
           const void* key, uint16_t keyLength)
{
    char chunk[51];
    buffer.reset();
    uint32_t bytesLeft = size;
    int position = 0;
    while (bytesLeft > 0) {
        // Write enough data to completely fill the chunk buffer, then
        // ignore the terminating NULL character that snprintf puts at
        // the end.
        snprintf(chunk, sizeof(chunk),
            "| %d: tableId 0x%lx, key %.*s, keyLength 0x%x %s",
            position, tableId, keyLength, reinterpret_cast<const char*>(key),
            keyLength, "0123456789");
        uint32_t chunkLength = static_cast<uint32_t>(sizeof(chunk) - 1);
        if (chunkLength > bytesLeft) {
            chunkLength = bytesLeft;
        }
        buffer.appendCopy(chunk, chunkLength);
        bytesLeft -= chunkLength;
        position += chunkLength;
    }
}

/**
 * TODO(Anyone): This method is duplicated wholesale in ClusterPerf.cc. 
 *
 * Check the contents of a buffer to ensure that it contains the same data
 * generated previously by fillBuffer.  Generate a log message if a
 * problem is found.
 *
 * \param buffer
 *      Buffer whose contents are to be checked.
 * \param expectedLength
 *      The buffer should contain this many bytes.
 * \param tableId
 *      This table identifier should be reflected in the buffer's data.
 * \param key
 *      This key should be reflected in the buffer's data.
 * \param keyLength
 *      This key length should be reflected in the buffer's data.
 *
 * \return
 *      True means the buffer has the "expected" contents; false means
 *      there was an error.
 */
bool
checkBuffer(Buffer& buffer, uint32_t expectedLength, uint64_t tableId,
            const void* key, uint16_t keyLength)
{
    uint32_t length = buffer.getTotalLength();
    if (length != expectedLength) {
        RAMCLOUD_LOG(ERROR, "corrupted data: expected %u bytes, "
                "found %u bytes", expectedLength, length);
        return false;
    }
    Buffer comparison;
    fillBuffer(comparison, expectedLength, tableId, key, keyLength);
    for (uint32_t i = 0; i < expectedLength; i++) {
        char c1 = *buffer.getOffset<char>(i);
        char c2 = *comparison.getOffset<char>(i);
        if (c1 != c2) {
            int start = i - 10;
            const char* prefix = "...";
            const char* suffix = "...";
            if (start <= 0) {
                start = 0;
                prefix = "";
            }
            uint32_t length = 20;
            if (start+length >= expectedLength) {
                length = expectedLength - start;
                suffix = "";
            }
            RAMCLOUD_LOG(ERROR, "corrupted data: expected '%c', got '%c' "
                    "(\"%s%.*s%s\" vs \"%s%.*s%s\")", c2, c1, prefix, length,
                    static_cast<const char*>(comparison.getRange(start,
                    length)), suffix, prefix, length,
                    static_cast<const char*>(buffer.getRange(start,
                    length)), suffix);
            return false;
        }
    }
    return true;
}

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
    context.transportManager->setSessionTimeout(
        optionParser.options.getSessionTimeout());

    string coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
    if (coordinatorLocator.size() == 0) {
        coordinatorLocator = optionParser.options.getCoordinatorLocator();
    }
    RamCloud client(&context, coordinatorLocator.c_str(),
            optionParser.options.getClusterName().c_str());

    if (removeCount > count)
        DIE("cannot remove more objects than I create!");
    if (verify && objectDataSize < 20)
        DIE("need >= 20 byte objects to do verification!");
    if (verify && fillWithTestData)
        DIE("verify not supported with fillWithTestData");

    char tableName[20];
    uint64_t tables[tableCount];

    for (uint32_t t = 0; t < tableCount; t++) {
        snprintf(tableName, sizeof(tableName), "%d", t);
        client.createTable(tableName);
        tables[t] = client.getTableId(tableName);

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
                uint64_t table = client.getTableId(tableName);
                client.write(table, "1", 1, "abcd", 4);
            }
        }
    }

    LOG(NOTICE, "Performing %u inserts of %u byte objects",
        count * tableCount, objectDataSize);

    if (fillWithTestData) {
        LOG(NOTICE, "Using the fillWithTestData rpc on the master "
            "with key <1,'0'>");
        uint64_t b = Cycles::rdtsc();
        client.testingFill(1, "0", 1, count * tableCount, objectDataSize);
        LOG(NOTICE, "%d inserts took %lu ticks",
            count * tableCount, Cycles::rdtsc() - b);
        LOG(NOTICE, "avg insert took %lu ticks",
                    (Cycles::rdtsc() - b) / count / tableCount);
    } else {
        Buffer writeVal;

        Tub<WriteRpc> writeRpcs[8];
        uint64_t b = Cycles::rdtsc();
        int j;
        for (j = 0; j < count - 1; j++) {
            string key = format("%d", j);
            uint16_t keyLength = downCast<uint16_t>(key.length());
            for (uint32_t t = 0; t < tableCount; t++) {
                auto& writeRpc = writeRpcs[(j * tableCount + t) %
                                           arrayLength(writeRpcs)];
                if (writeRpc) {
                    writeRpc->wait();
                }

                if (verify) {
                    fillBuffer(writeVal, objectDataSize, tables[t],
                               key.c_str(), keyLength);
                } else {
                    char chunk[objectDataSize];
                    memset(&chunk, 0xcc, objectDataSize);
                    writeVal.reset();
                    writeVal.appendCopy(chunk, objectDataSize);
                }
                writeRpc.construct(&client,
                                   static_cast<uint32_t>(tables[t]),
                                   key.c_str(),
                                   keyLength,
                                   writeVal.getRange(0, objectDataSize),
                                   objectDataSize,
                                   static_cast<RejectRules*>(NULL),
                                   /* async = */ true);
            }
        }
        foreach (auto& writeRpc, writeRpcs) {
            if (writeRpc)
                writeRpc->wait();
        }

        string key = format("%d", j);
        char chunk[objectDataSize];
        memset(&chunk, 0xcc, objectDataSize);
        writeVal.reset();
        writeVal.appendCopy(chunk, objectDataSize);
        client.write(tables[0], key.c_str(), downCast<uint16_t>(key.length()),
                     writeVal.getRange(0, objectDataSize), objectDataSize,
                     static_cast<RejectRules*>(NULL),
                     static_cast<uint64_t*>(NULL),
                     /* async = */ false);
        LOG(NOTICE, "%d inserts took %lu ticks",
            count * tableCount, Cycles::rdtsc() - b);
        LOG(NOTICE, "avg insert took %lu ticks",
            (Cycles::rdtsc() - b) / count / tableCount);
    }

    // remove objects if we've been instructed. just start from table 0, obj 0.
    LOG(NOTICE, "Performing %u removals of objects just created", removeCount);
    for (uint32_t t = 0; t < tableCount; t++) {
        for (int j = 0; removeCount > 0; j++, removeCount--) {
            string key = format("%d", j);
            client.remove(tables[t], key.c_str(),
                    downCast<uint16_t>(key.length()));
        }
    }

    // dump the tablet map
    for (uint32_t t = 0; t < tableCount; t++) {
        Transport::SessionRef session =
            client.objectFinder.lookup(tables[t], "0", 1);
        LOG(NOTICE, "%s has table %lu",
            session->getServiceLocator().c_str(), tables[t]);
    }

    // dump out coordinator rpc info
    // As of 6/2011 this needs to be reworked: ping no longer contains the
    // hack to print statistics.
    // client.ping();

    LOG(NOTICE, "- quiescing writes");
    client.quiesce();

    // Take an initial snapshot of performance metrics.
    ClusterMetrics metricsBefore(&client);

    uint64_t startTime = Cycles::rdtsc();
    client.testingKill(tables[0], "0", 1);
    uint64_t downTime = Cycles::rdtsc();
    LOG(NOTICE, "tablet down");

    // Wait for recovery to complete
    for (uint32_t t = 0; t < tableCount; t++) {
        uint64_t tableId = tables[t];
        client.objectFinder.waitForAllTabletsNormal(tableId);
    }
    LOG(NOTICE, "all tablets now normal");

    Buffer nb;
    uint64_t stopTime = Cycles::rdtsc();
    // Check a value in each table to make sure we're good
    bool somethingWentWrong = false;
    for (uint32_t t = 0; t < tableCount; t++) {
        uint64_t table = tables[t];
        try {
            client.read(table, "0", 1, &nb);
            if (t == 0)
                stopTime = Cycles::rdtsc();
        } catch (...) {
        }
        auto session = client.objectFinder.lookup(tables[t], "0", 1);
        if (nb.getTotalLength() == objectDataSize) {
            LOG(NOTICE, "recovered value read from %s has length %u",
                session->getServiceLocator().c_str(), nb.getTotalLength());
        } else {
            LOG(ERROR, "recovered value read from %s has length %u "
                "(expected %u)", session->getServiceLocator().c_str(),
                nb.getTotalLength(), objectDataSize);
            somethingWentWrong = true;
        }
    }
    if (somethingWentWrong)
        DIE("Recovery failed; some objects seem to be missing");
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
            string key = format("%d", j);
            uint16_t keyLength = downCast<uint16_t>(key.length());
            for (uint32_t t = 0; t < tableCount; t++) {
                nb.reset();
                try {
                    client.read(tables[t], key.c_str(), keyLength, &nb);
                } catch (...) {
                    LOG(ERROR, "Failed to access object (tbl %lu, obj %.*s)!",
                        tables[t], keyLength, key.c_str());
                    continue;
                }
                uint32_t objBytes = nb.getTotalLength();

                if (objBytes != objectDataSize) {
                    LOG(ERROR, "Bad object size (tbl %lu, obj %.*s)",
                        tables[t], keyLength, key.c_str());
                } else {
                    checkBuffer(nb, objectDataSize, tables[t],
                                key.c_str(), keyLength);
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
    Logger::get().disableCollapsing();
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
    Logger::get().enableCollapsing();

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
