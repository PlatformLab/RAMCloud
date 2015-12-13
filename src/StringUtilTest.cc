/* Copyright (c) 2011 Facebook
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
#include "StringUtil.h"

namespace RAMCloud {

using namespace StringUtil; // NOLINT

TEST(StringUtilTest, startsWith) {
    EXPECT_TRUE(startsWith("foo", "foo"));
    EXPECT_TRUE(startsWith("foo", "fo"));
    EXPECT_TRUE(startsWith("foo", ""));
    EXPECT_TRUE(startsWith("", ""));
    EXPECT_FALSE(startsWith("f", "foo"));
}

TEST(StringUtilTest, endsWith) {
    EXPECT_TRUE(endsWith("foo", "foo"));
    EXPECT_TRUE(endsWith("foo", "oo"));
    EXPECT_TRUE(endsWith("foo", ""));
    EXPECT_TRUE(endsWith("", ""));
    EXPECT_FALSE(endsWith("o", "foo"));
}

TEST(StringUtilTest, contains) {
    EXPECT_TRUE(contains("foo", "foo"));
    EXPECT_TRUE(contains("foo", "oo"));
    EXPECT_TRUE(contains("foo", ""));
    EXPECT_TRUE(contains("", ""));
    EXPECT_FALSE(contains("o", "foo"));
}

TEST(StringUtilTest, regsub) {
    EXPECT_EQ("0 yyy zzz 0 0 0 qqq",
            regsub("xxx yyy zzz xxx xxx xxx qqq", "[x]+", "0"));
    EXPECT_EQ("Unmatched [ or [^",
            regsub("xxx yyy zzz xxx xxx xxx qqq", "[xyz", "0"));
    EXPECT_EQ("no match here",
            regsub("no match here", "xyzzy", "0"));
}

TEST(StringUtilTest, binaryToString) {
    EXPECT_EQ("\\x00\\x01\\xfe\\xff",
        binaryToString("\x00\x01\xfe\xff", 4));

    EXPECT_EQ("there's binary\\x13\\x09crap in here",
        binaryToString("there's binary\x13\tcrap in here", 28));
}

TEST(StringUtilTest, split) {
    EXPECT_EQ("foo", split("foo bar", ' ')[0]);
    EXPECT_EQ("bar", split("foo bar", ' ')[1]);
}

TEST(StringUtilTest, stringToInt) {
    bool error;
    EXPECT_EQ(12345, stringToInt("12345", &error));
    EXPECT_FALSE(error);
    EXPECT_EQ(-80, stringToInt(" -80", &error));
    EXPECT_FALSE(error);
    EXPECT_EQ(0, stringToInt("", &error));
    EXPECT_TRUE(error);
    EXPECT_EQ(0, stringToInt("xyz", &error));
    EXPECT_TRUE(error);
    EXPECT_EQ(0, stringToInt("   99xy", &error));
    EXPECT_TRUE(error);
    EXPECT_EQ(0, stringToInt("99999999999999999999", &error));
    EXPECT_TRUE(error);
    EXPECT_EQ(0, stringToInt("-9999999999999999999", &error));
    EXPECT_TRUE(error);
}

}  // namespace RAMCloud
