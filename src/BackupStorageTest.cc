/* Copyright (c) 2012 Stanford University
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

#include "TestUtil.h"

#include "BackupStorage.h"
#include "InMemoryStorage.h"
#include "StringUtil.h"

namespace RAMCloud {

class BackupStorageTest : public ::testing::Test {
  PUBLIC:
    InMemoryStorage storage;

    BackupStorageTest()
        : storage(1024, 16, 0)
    {}
};

TEST_F(BackupStorageTest, benchmark) {
    // Basic smoke test. Not much to check.
    EXPECT_NE(0u, storage.benchmark(RANDOM_REFINE_MIN));
    EXPECT_NE(0u, storage.benchmark(RANDOM_REFINE_AVG));
    EXPECT_EQ(100u, storage.benchmark(EVEN_DISTRIBUTION));
}

TEST_F(BackupStorageTest, sleepToThrottleWrites) {
    TestLog::Enable _;

    storage.writeRateLimit = 0;
    storage.sleepToThrottleWrites(1000, 1000);
    EXPECT_EQ("", TestLog::get());

    storage.writeRateLimit = 1;
    storage.sleepToThrottleWrites(1024 * 1024, Cycles::fromSeconds(0.001));
    EXPECT_EQ("sleepToThrottleWrites: delayed 999000 usec", TestLog::get());

    TestLog::reset();

    storage.writeRateLimit = 1000;
    storage.sleepToThrottleWrites(100 * 1024 * 1024, Cycles::fromSeconds(0.05));
    EXPECT_EQ("sleepToThrottleWrites: delayed 50000 usec", TestLog::get());
}

} // namespace RAMCloud
