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

/**
 * \file
 * A little example/benchmark and sanity check for the StringKeyAdapter.
 */

#include <unordered_set>
#include <iostream>

#include "Cycles.h"
#include "RamCloud.h"
#include "OptionParser.h"
#include "ClientException.h"
#include "CycleCounter.h"
#include "StringKeyAdapter.h"

using std::cerr;
using std::endl;
using namespace RAMCloud;

/// Generates garbage strings quickly for test key values.
struct Enumerator {
    explicit Enumerator(uint32_t iterations)
        : i(iterations)
    {
        memset(key, ' ', sizeof(key));
        key[sizeof(key) - 1] = '\0';
    }

    operator bool()
    {
        return i > 0;
    }

    Enumerator& operator++()
    {
        ++key[0];
        for (uint32_t j = 0; j < sizeof(key) - 2; ++j) {
            if (key[j] == '~') {
                key[j] = ' ';
                ++key[j + 1];
            } else {
                break;
            }
        }
        if (key[sizeof(key) - 2] == '~')
            throw Exception(HERE, "Overflow");
        --i;
        return *this;
    }

    uint32_t
    keyLength()
    {
        return downCast<uint32_t>(sizeof(key) - 1);
    }

    uint32_t i;
    char key[5];
};

int
main(int argc, char* argv[])
try
{
    uint32_t count;
    uint32_t size;

    OptionsDescription options("StringKeys");
    options.add_options()
        ("number,n",
         ProgramOptions::value<uint32_t>(&count)->
           default_value(10000),
         "Number of iterations to read.")
        ("size,S",
         ProgramOptions::value<uint32_t>(&size)->
           default_value(100),
         "Size in bytes of objects to read.");

    OptionParser optionParser(options, argc, argv);

    auto coordinatorLocator = optionParser.options.getCoordinatorLocator();
    cerr << "StringKeys: Connecting to " << coordinatorLocator << endl;

    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());

    client.createTable("StringKeys");
    auto table = client.openTable("StringKeys");
    assert(table == 0);

    StringKeyAdapter sk(client);

    // write values to master watching for collisions
    char value[size];
    std::unordered_set<uint64_t> set;
    uint64_t collisions = 0;
    for (Enumerator e(count) ; e; ++e) {
        const uint64_t hashedKey = StringKeyAdapter::hash(e.key, e.keyLength());
        if (set.find(hashedKey) != set.end()) {
            cerr << "Collision: '" << e.key << "' " << hashedKey << endl;
            ++collisions;
        }
        set.insert(hashedKey);
        sk.write(table, e.key, e.keyLength(), value, size);
    }
    cerr << "Total collisions: " << collisions << endl;

    // read values back and time responses
    CycleCounter<uint64_t> counter;
    Buffer response;
    for (Enumerator e(count) ; e; ++e) {
        sk.read(table, e.key, e.keyLength(), response);
    }
    double ns = Cycles::toSeconds(counter.stop()) * 1e09;
    printf("Took %.0f ns\n", ns);
    printf("Read RTT %.1f ns\n", ns / count);
} catch (ClientException& e) {
    cerr << "RAMCloud Client exception: " << e.what() << endl;
    return -1;
} catch (Exception& e) {
    cerr << "RAMCloud exception: " << e.what() << endl;
    return -1;
}
