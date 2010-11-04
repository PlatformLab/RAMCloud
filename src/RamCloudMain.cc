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

namespace RC = RAMCloud;

struct ClientConfig {
    string coordinatorLocator;
    string serverLocator;

    ClientConfig()
        : coordinatorLocator()
        , serverLocator()
    {
    }
};


int
main(int argc, char *argv[])
try
{
    ClientConfig config;

    RC::OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("server,s",
         RC::ProgramOptions::value<string>(&config.serverLocator)->
            default_value("fast+udp:host=127.0.0.1,port=12242"),
         "RAMCloud server to connect to");

    RC::OptionParser optionParser(clientOptions, argc, argv);

    config.coordinatorLocator = optionParser.options.getCoordinatorLocator();

    printf("client: Connecting to %s\n", config.serverLocator.c_str());

    RC::RamCloud client(config.serverLocator.c_str());
    client.selectPerfCounter(RC::PERF_COUNTER_TSC,
                             RC::MARK_RPC_PROCESSING_BEGIN,
                             RC::MARK_RPC_PROCESSING_END);

    uint64_t b;

    b = rdtsc();
    client.createTable("test");
    uint32_t table;
    table = client.openTable("test");
    printf("create+open table took %lu ticks\n", rdtsc() - b);
    printf("open took %u ticks on the server\n",
           client.counterValue);

    b = rdtsc();
    client.ping();
    printf("ping took %lu ticks on the client\n", rdtsc() - b);
    printf("ping took %u ticks on the server\n",
           client.counterValue);

    b = rdtsc();
    client.write(table, 42, "Hello, World!", 14);
    printf("write took %lu ticks\n", rdtsc() - b);
    printf("write took %u ticks on the server\n",
           client.counterValue);

    b = rdtsc();
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    client.write(table, 43, value, strlen(value) + 1);
    printf("write took %lu ticks\n", rdtsc() - b);
    printf("write took %u ticks on the server\n",
           client.counterValue);

    RC::Buffer buffer;
    b = rdtsc();
    uint32_t length;

    client.read(table, 43, &buffer);
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("read took %u ticks on the server\n",
           client.counterValue);

    length = buffer.getTotalLength();
    printf("Got back [%s] len %lu\n", buffer.getRange(0, length),
            length);

    client.read(table, 42, &buffer);
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("read took %u ticks on the server\n",
           client.counterValue);
    length = buffer.getTotalLength();
    printf("Got back [%s] len %lu\n", buffer.getRange(0, length),
            length);

    b = rdtsc();
    uint64_t id = 0xfffffff;
    id = client.create(table, "Hello, World?", 14);
    printf("insert took %lu ticks\n", rdtsc() - b);
    printf("insert took %u ticks on the server\n",
           client.counterValue);
    printf("Got back [%lu] id\n", id);

    b = rdtsc();
    client.read(table, id, &buffer);
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("read took %u ticks on the server\n",
           client.counterValue);
    length = buffer.getTotalLength();
    printf("Got back [%s] len %lu\n", buffer.getRange(0, length),
            length);

    b = rdtsc();
    int count = 16384;
    id = 0xfffffff;
    const char *val = "0123456789ABCDEF";
    uint64_t sum = 0;
    for (int j = 0; j < count; j++) {
        id = client.create(table, val, strlen(val) + 1);
        sum += client.counterValue;
    }
    printf("%d inserts took %lu ticks\n", count, rdtsc() - b);
    printf("avg insert took %lu ticks\n", (rdtsc() - b) / count);
    printf("%d inserts took %lu ticks on the server\n", count, sum);
    printf("%d avg insert took %lu ticks on the server\n", count,
           sum / count);

    client.dropTable("test");
    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
