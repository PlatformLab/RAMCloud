/* Copyright (c) 2010-2012 Stanford University
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

#include <string>
#include "TestUtil.h"
#include "RuntimeOptions.h"

namespace RAMCloud {

struct RuntimeOptionsTest : public ::testing::Test {
    RuntimeOptions options;

    RuntimeOptionsTest()
        : options()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    DISALLOW_COPY_AND_ASSIGN(RuntimeOptionsTest);
};

TEST_F(RuntimeOptionsTest, set) {
    // Check std::queue<T> parser.
    options.set("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, options.failRecoveryMasters.size());
    EXPECT_EQ(1u, options.popFailRecoveryMasters());
    EXPECT_EQ(2u, options.popFailRecoveryMasters());
    EXPECT_EQ(3u, options.popFailRecoveryMasters());
    EXPECT_EQ(0u, options.popFailRecoveryMasters());
    options.set("failRecoveryMasters", "");
    ASSERT_EQ(0u, options.failRecoveryMasters.size());
    options.set("failRecoveryMasters", "foo");
    ASSERT_EQ(0u, options.failRecoveryMasters.size());
    options.set("failRecoveryMasters", "1 foo 2 other 3");
    ASSERT_EQ(1u, options.failRecoveryMasters.size());
}

TEST_F(RuntimeOptionsTest, get) {
    options.set("failRecoveryMasters", "1 2 3");
    ASSERT_EQ(3u, options.failRecoveryMasters.size());
    ASSERT_STREQ("1 2 3", options.get("failRecoveryMasters").c_str());
    options.popFailRecoveryMasters();
    ASSERT_STREQ("1 2 3", options.get("failRecoveryMasters").c_str());
    options.set("failRecoveryMasters", "black 1 white 2");
    ASSERT_STREQ("black 1 white 2", options.get("failRecoveryMasters").c_str());
}

TEST_F(RuntimeOptionsTest, popFailRecoveryMasters) {
    EXPECT_EQ(0u, options.popFailRecoveryMasters());
    options.set("failRecoveryMasters", "1 2 3");
    EXPECT_EQ(1u, options.popFailRecoveryMasters());
    EXPECT_EQ(2u, options.popFailRecoveryMasters());
    EXPECT_EQ(3u, options.popFailRecoveryMasters());
    EXPECT_EQ(0u, options.popFailRecoveryMasters());
    options.set("failRecoveryMasters", "1 2 3");
    options.set("failRecoveryMasters", "4");
    EXPECT_EQ(4u, options.popFailRecoveryMasters());
    EXPECT_EQ(0u, options.popFailRecoveryMasters());
}


}  // namespace RAMCloud
