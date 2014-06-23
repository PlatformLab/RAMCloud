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
#include "RamCloud.h"
#include "OptionParser.h"
#include "MasterClient.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    Context context(true);

    string tableName;
    uint64_t tableId;
    uint64_t firstKey;
    uint64_t lastKey;
    uint64_t newOwnerMasterId;

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
         "ServerId of the master to migrate to");

    OptionParser optionParser(migrateOptions, argc, argv);

    if (tableName == "") {
        fprintf(stderr, "error: please specify the table name\n");
        exit(1);
    }
    if (newOwnerMasterId == 0) {
        fprintf(stderr, "error: please specify the recipient's ServerId\n");
        exit(1);
    }

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    printf("client: Connecting to coordinator %s\n",
        coordinatorLocator.c_str());

    RamCloud client(&context, coordinatorLocator.c_str());

#if 1
    // The following crud shouldn't be part of this utility
    client.createTable(tableName.c_str());
    for (uint64_t i = 0; i < 838860 / 4 + 1; i++) {
        client.write(0, reinterpret_cast<char*>(&i), sizeof(i),
            "0123456789", 10);
    }
#endif

    tableId = client.getTableId(tableName.c_str());

    printf("Issuing migration request:\n");
    printf("  table \"%s\" (%lu)\n", tableName.c_str(), tableId);
    printf("  first key %lu\n", firstKey);
    printf("  last key  %lu\n", lastKey);
    printf("  recipient master id %lu\n", newOwnerMasterId);

    client.migrateTablet(tableId,
                         firstKey,
                         lastKey,
                         ServerId(newOwnerMasterId));

#if 1
    // The following crud shouldn't be part of this utility
    uint64_t key = 23471324234;
    client.write(0, reinterpret_cast<char*>(&key), sizeof(key), "YO HO!");

    Buffer buf;
    client.read(0, reinterpret_cast<char*>(&firstKey), sizeof(firstKey), &buf);
    printf("read obj 0: \"%10s\"\n",
        (const char*)buf.getRange(0, buf.size()));

    printf("Migrating back again!");
    client.migrateTablet(tableId, firstKey, lastKey, ServerId(1, 0));
    client.read(0, reinterpret_cast<char*>(&firstKey), sizeof(firstKey), &buf);
    printf("read obj 0: \"%10s\"\n",
        (const char*)buf.getRange(0, buf.size()));
    client.read(0, reinterpret_cast<char*>(&key), sizeof(key), &buf);
    printf("read obj %lu: \"%10s\"\n", key,
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
