/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "CoordinatorServerList.h"

namespace RAMCloud {

class CoordinatorServerListTest : public ::testing::Test {
  public:
    CoordinatorServerListTest()
        : sl()
    {
    }

    CoordinatorServerList sl;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

TEST_F(CoordinatorServerListTest, add) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(ServerId(1, 0), sl.add("hi", true));
    EXPECT_TRUE(sl.serverList[1].entry);
    EXPECT_FALSE(sl.serverList[0].entry);
    EXPECT_EQ(ServerId(1, 0), sl.serverList[1].entry->serverId);
    EXPECT_EQ("hi", sl.serverList[1].entry->serviceLocator);
    EXPECT_TRUE(sl.serverList[1].entry->isMaster);
    EXPECT_FALSE(sl.serverList[1].entry->isBackup);
    EXPECT_EQ(1U, sl.serverList[1].nextGenerationNumber);

    EXPECT_EQ(ServerId(2, 0), sl.add("hi again", false));
    EXPECT_TRUE(sl.serverList[2].entry);
    EXPECT_EQ(ServerId(2, 0), sl.serverList[2].entry->serverId);
    EXPECT_EQ("hi again", sl.serverList[2].entry->serviceLocator);
    EXPECT_FALSE(sl.serverList[2].entry->isMaster);
    EXPECT_TRUE(sl.serverList[2].entry->isBackup);
    EXPECT_EQ(1U, sl.serverList[2].nextGenerationNumber);
}

TEST_F(CoordinatorServerListTest, remove) {
    EXPECT_THROW(sl.remove(ServerId(0, 0)), Exception);

    sl.add("hi!", true);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 0)));
    EXPECT_FALSE(sl.serverList[1].entry);
    EXPECT_THROW(sl.remove(ServerId(1, 0)), Exception);

    sl.add("hi, again", true);
    EXPECT_TRUE(sl.serverList[1].entry);
    EXPECT_THROW(sl.remove(ServerId(1, 2)), Exception);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 1)));
}

TEST_F(CoordinatorServerListTest, indexOperator) {
    EXPECT_THROW(sl[ServerId(0, 0)], Exception);
    sl.add("yo!", true);
    EXPECT_EQ(ServerId(1, 0), sl[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", sl[ServerId(1, 0)].serviceLocator);
    sl.remove(ServerId(1, 0));
    EXPECT_THROW(sl[ServerId(1, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(1U, sl.firstFreeIndex());
    EXPECT_EQ(2U, sl.serverList.size());
    sl.add("hi", true);
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.add("hi again", true);
    EXPECT_EQ(3U, sl.firstFreeIndex());
    sl.remove(ServerId(2, 0));
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.remove(ServerId(1, 0));
    EXPECT_EQ(1U, sl.firstFreeIndex());
}

}  // namespace RAMCloud
