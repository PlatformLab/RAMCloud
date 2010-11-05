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

#include "RamCloud.h"
#include "OptionParser.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    bool hintServerDown;

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("down,d",
         ProgramOptions::bool_switch(&hintServerDown),
         "Report the master we're talking to as down just before exit.");

    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "client: Connecting to %s",
        optionParser.options.getCoordinatorLocator().c_str());

    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());
    client.selectPerfCounter(PERF_COUNTER_TSC,
                             MARK_RPC_PROCESSING_BEGIN,
                             MARK_RPC_PROCESSING_END);

    uint64_t b;

    b = rdtsc();
    client.createTable("test");
    uint32_t table;
    table = client.openTable("test");
    LOG(DEBUG, "create+open table took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "open took %u ticks on the server",
           client.counterValue);

    b = rdtsc();
    client.ping();
    LOG(DEBUG, "ping took %lu ticks on the client", rdtsc() - b);
    LOG(DEBUG, "ping took %u ticks on the server",
           client.counterValue);

    b = rdtsc();
    client.write(table, 42, "Hello, World!", 14);
    LOG(DEBUG, "write took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "write took %u ticks on the server",
           client.counterValue);

    b = rdtsc();
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    client.write(table, 43, value, strlen(value) + 1);
    LOG(DEBUG, "write took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "write took %u ticks on the server",
           client.counterValue);

    Buffer buffer;
    b = rdtsc();
    uint32_t length;

    client.read(table, 43, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "read took %u ticks on the server",
           client.counterValue);

    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    client.read(table, 42, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "read took %u ticks on the server",
           client.counterValue);
    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    b = rdtsc();
    uint64_t id = 0xfffffff;
    id = client.create(table, "Hello, World?", 14);
    LOG(DEBUG, "insert took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "insert took %u ticks on the server",
           client.counterValue);
    LOG(DEBUG, "Got back [%lu] id", id);

    b = rdtsc();
    client.read(table, id, &buffer);
    LOG(DEBUG, "read took %lu ticks", rdtsc() - b);
    LOG(DEBUG, "read took %u ticks on the server",
           client.counterValue);
    length = buffer.getTotalLength();
    LOG(DEBUG, "Got back [%s] len %u",
        static_cast<const char*>(buffer.getRange(0, length)),
        length);

    b = rdtsc();
    int count = 100;
    id = 0xfffffff;
    const char *val = "0123456789ABCDEF";
    uint64_t sum = 0;
    for (int j = 0; j < count; j++) {
        id = client.create(table, val, strlen(val) + 1);
        sum += client.counterValue;
    }
    LOG(DEBUG, "%d inserts took %lu ticks", count, rdtsc() - b);
    LOG(DEBUG, "avg insert took %lu ticks", (rdtsc() - b) / count);
    LOG(DEBUG, "%d inserts took %lu ticks on the server", count, sum);
    LOG(DEBUG, "%d avg insert took %lu ticks on the server", count,
           sum / count);

    if (hintServerDown) {
        LOG(DEBUG, "--- hinting that the server is down ---");
        client.coordinator.hintServerDown("tcp:host=127.0.0.1,port=12242");
        printf("- flushing map\n");
        client.objectFinder.flush();

        printf("- attempting read from recovery master\n");
        client.read(table, 43, &buffer);
        printf("read took %lu ticks\n", rdtsc() - b);
        printf("read took %u ticks on the server\n",
               client.counterValue);
        printf("read value: %s\n", buffer.getRange(0, buffer.getTotalLength()));
        printf("- recovery worked!\n");
    } else {
        client.dropTable("test");
    }

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
