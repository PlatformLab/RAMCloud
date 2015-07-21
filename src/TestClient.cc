/* Copyright (c) 2015 Stanford University
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

// This file contains a trivial RAMCloud application. It is used by
// "make testInstall" target to test the "make install" target: if RAMCloud
// has been properly installed, then this application will compile using
// only the installed files.

#include <stdio.h>
#include "ramcloud/RamCloud.h"
using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s coordinatorLocator", argv[0]);
    }
    RamCloud cluster(argv[1], "__unnamed__");
    uint64_t table = cluster.createTable("test");
    cluster.write(table, "42", 2, "Hello, World!", 14);
    const char *value = "0123456789012345678901234567890"
        "123456789012345678901234567890123456789";
    cluster.write(table, "43", 2, value, downCast<uint32_t>(strlen(value) + 1));
    Buffer buffer;
    cluster.read(table, "43", 2, &buffer);
    cluster.read(table, "42", 2, &buffer);
    cluster.dropTable("test");
    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
