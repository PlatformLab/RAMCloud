/* Copyright (c) 2011-2014 Stanford University
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
 * This utility simply initiates tablet migration on a single tablet to a
 * specified recipient master.
 */

#include "Common.h"

#include "Context.h"
#include "CycleCounter.h"
#include "RamCloud.h"
#include "OptionParser.h"
#include "MasterClient.h"
#include "ShortMacros.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    Context context(true);

    string tableName;
    uint64_t firstKey;
    uint64_t lastKey;
    uint64_t newOwnerMasterId;

    uint32_t objectCount = 0;
    uint32_t objectSize = 0;
    uint32_t otherObjectCount = 0;

    OptionsDescription migrateOptions("Migrate");
    migrateOptions.add_options()
        ("table,t",
         ProgramOptions::value<string>(&tableName)->
            default_value(""),
         "name of the table to migrate.")
        ("firstKey,f",
         ProgramOptions::value<uint64_t>(&firstKey)->
           default_value(0),
         "First key of the tablet range to migrate")
        ("lastKey,z",
         ProgramOptions::value<uint64_t>(&lastKey)->
           default_value(-1),
         "Last key of the tablet range to migrate")
        ("recipient,r",
         ProgramOptions::value<uint64_t>(&newOwnerMasterId)->
           default_value(0),
         "ServerId of the master to migrate to")
        ("objectCount",
         ProgramOptions::value<uint32_t>(&objectCount)->
           default_value(1000000),
         "Number of objects to pre-populate in the table to be migrated")
        ("objectSize",
         ProgramOptions::value<uint32_t>(&objectSize)->
           default_value(1000),
         "Size of objects to pre-populate in tables")
        ("otherObjectCount",
         ProgramOptions::value<uint32_t>(&otherObjectCount)->
           default_value(0),
         "Number of objects to pre-populate in the table to be "
         "NOT TO BE migrated")
        ("numClients",
         "Ignored by this program")
        ("clientIndex",
         "Ignored by this program");

    OptionParser optionParser(migrateOptions, argc, argv);

    if (tableName == "") {
        DIE("error: please specify the table name");
        exit(1);
    }
    if (newOwnerMasterId == 0) {
        DIE("error: please specify the recipient's ServerId");
        exit(1);
    }

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    LOG(NOTICE, "client: Connecting to coordinator %s",
        coordinatorLocator.c_str());

    RamCloud client(&context, coordinatorLocator.c_str());

#if 1
    // The following crud shouldn't be part of this utility
    uint64_t tableId = client.createTable(tableName.c_str());
    client.testingFill(tableId, "", 0, objectCount, objectSize);
    const uint64_t totalBytes = objectCount * objectSize;

    uint64_t otherTableId = client.createTable("junk");
    client.testingFill(otherTableId, "", 0, otherObjectCount, objectSize);
#endif

    LOG(ERROR, "Issuing migration request:");
    LOG(ERROR, "  table \"%s\" (%lu)", tableName.c_str(), tableId);
    LOG(ERROR, "  first key %lu", firstKey);
    LOG(ERROR, "  last key  %lu", lastKey);
    LOG(ERROR, "  recipient master id %lu", newOwnerMasterId);

    {
        CycleCounter<> counter{};
        client.migrateTablet(tableId,
                             firstKey,
                             lastKey,
                             ServerId(newOwnerMasterId));
        double seconds = Cycles::toSeconds(counter.stop());
        LOG(ERROR, "Migration took %0.2f seconds", seconds);
        LOG(ERROR, "Migration took %0.2f MB/s",
                double(totalBytes) / seconds / double(1 << 20));
    }

#if 0
    // The following crud shouldn't be part of this utility
    uint64_t key = 23471324234;
    client.write(tableId, reinterpret_cast<char*>(&key), sizeof(key), "YO HO!");

    Buffer buf;
    client.read(tableId, reinterpret_cast<char*>(&firstKey),
                sizeof(firstKey), &buf);
    LOG(ERROR, "read obj 0: \"%10s\"",
        (const char*)buf.getRange(0, buf.size()));

    LOG(ERROR, "Migrating back again!");
    client.migrateTablet(tableId, firstKey, lastKey, ServerId(1, 0));
    client.read(tableId, reinterpret_cast<char*>(&firstKey),
                sizeof(firstKey), &buf);
    LOG(ERROR, "read obj 0: \"%10s\"",
        (const char*)buf.getRange(0, buf.size()));
    client.read(tableId, reinterpret_cast<char*>(&key), sizeof(key), &buf);
    LOG(ERROR, "read obj %lu: \"%10s\"", key,
        (const char*)buf.getRange(0, buf.size()));
#endif

    return 0;
} catch (ClientException& e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
