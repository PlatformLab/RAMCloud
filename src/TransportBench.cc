/* Copyright (c) 2011 Stanford University
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

#include <iostream>

#include "RamCloud.h"
#include "OptionParser.h"
#include "ClientException.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "RawMetrics.h"

using std::cerr;
using std::endl;
using namespace RAMCloud;

void
bench(RamCloud& client,
      const uint32_t table,
      const bool mcp,
      const uint64_t count,
      const uint64_t size,
      const bool uncached)
{
    char buf[size];
    const uint64_t targetSize = 1 * 1024 * 1024 * 1024;
    const uint64_t insCount = uncached ? ((targetSize + size - 1) / size) : 1;

    if (mcp) {
        cerr << "Master Control Program writing test value "
                "and syncing up workers" << endl;
        cerr << "Inserting " << insCount
             << " objects to store of " << size << " bytes"
             << endl;
        if (uncached) {
            master.fillWithTestData(0, "abc", 3, downCast<uint32_t>(insCount),
                                    downCast<uint32_t>(size));
        }
        // make sure to write 0 last to trigger master metrics
        client.write(table, 0,
                     &buf[0], downCast<uint32_t>(size));
    }

    cerr << "Reading " << count
         <<" objects of " << size << " bytes each" << endl;
    uint64_t readCount = 0;

    Buffer response;
    for (;;) {
        try {
            // warm up caches and sync metrics on drones
            client.read(table, 0, &response);
            break;
        } catch (ObjectDoesntExistException& e) {
        }
    }

    CycleCounter<> counter;
    for (uint64_t i = 0; !mcp || i < count; ++i) {
        try {
            uint64_t k = generateRandom() % insCount;
            client.read(table, k, &response);
            ++readCount;
        } catch (ObjectDoesntExistException& e) {
            if (mcp)
                throw;
            break;
        }
    }
    uint64_t recoveryTicks = counter.stop();

    // stop metrics for all other clients
    if (mcp) {
        RejectRules rr;
        rr.exists = false;
        uint64_t version = 0;
        client.remove(table, 0, &rr, &version);
    }

    uint64_t ns = Cycles::toNanoseconds(recoveryTicks);
    cerr << "Took " << (ns / 1000000) << " ms"  << endl;
    cerr << "Throughput: "
         << double(readCount * size * 1000000000l) / double(ns * (1 << 20))
         << " MB/s" << endl;
    cerr << "Latency: "
         << double(ns / 1000) / double(readCount)
         << " us/read"  << endl;

    cerr << "METRICS: "
          << "{'ns': " << ns << ", 'count': " << count << ","
          << " 'size': " << size << "}"
          << endl;
}

int
main(int argc, char* argv[])
try
{
    bool uncached;
    bool mcp;
    uint64_t count;
    uint64_t size;

    OptionsDescription options("TransportBench");
    options.add_options()
        ("number,n",
         ProgramOptions::value<uint64_t>(&count)->
           default_value(10000),
         "Number of iterations to read.")
        ("size,S",
         ProgramOptions::value<uint64_t>(&size)->
           default_value(100),
         "Size in bytes of objects to read.")
        ("mcp,m",
         ProgramOptions::bool_switch(&mcp),
         "This is the master control program.")
        ("uncached,u",
         ProgramOptions::bool_switch(&uncached),
         "Pollute the master with many objects and read randomly");

    OptionParser optionParser(options, argc, argv);

    auto coordinatorLocator = optionParser.options.getCoordinatorLocator();
    cerr << "TransportBench: Connecting to " << coordinatorLocator << endl;

    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());

    client.createTable("TransportBench");
    auto table = client.getTableId("TransportBench");
    assert(table == 0);

    bench(client, table, mcp, count, size, uncached);
} catch (ClientException& e) {
    cerr << "RAMCloud Client exception: " << e.what() << endl;
    return -1;
} catch (Exception& e) {
    cerr << "RAMCloud exception: " << e.what() << endl;
    return -1;
}
