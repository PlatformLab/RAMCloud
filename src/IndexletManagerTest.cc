/* Copyright (c) 2014 Stanford University
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
#include "IndexletManager.h"
#include "StringUtil.h"

namespace RAMCloud {

class IndexletManagerTest : public ::testing::Test {
  public:
    Context context;
    IndexletManager im;

    IndexletManagerTest()
        : context()
        , im(&context)
    {
    }
    DISALLOW_COPY_AND_ASSIGN(IndexletManagerTest);
};

TEST_F(IndexletManagerTest, constructor) {
    EXPECT_EQ(0U, im.indexletMap.size());
}

TEST_F(IndexletManagerTest, addIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    EXPECT_TRUE(im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));
    EXPECT_FALSE(im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_TRUE(im.addIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key2.c_str(), (uint16_t)key2.length()));

    EXPECT_TRUE(im.addIndexlet(0, 0, key4.c_str(),
        (uint16_t)key4.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.addIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key3.c_str(), (uint16_t)key3.length()));

    SpinLock lock;
    IndexletManager::Lock fakeGuard(lock);
    IndexletManager::Indexlet* indexlet = &im.lookup(0, 0, key2.c_str(),
        (uint16_t)key2.length(), fakeGuard)->second;
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, getIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(1, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key3.c_str(), (uint16_t)key3.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key3.c_str(),
        (uint16_t)key3.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.getIndexlet(0, 1, key1.c_str(),
        (uint16_t)key1.length(), key5.c_str(), (uint16_t)key5.length()));

    IndexletManager::Indexlet* indexlet =
                    im.getIndexlet(0, 0, key2.c_str(), (uint16_t)key2.length(),
                                        key4.c_str(), (uint16_t)key4.length());
    EXPECT_TRUE(indexlet);
    string firstKey = StringUtil::binaryToString(
                        indexlet->firstKey, indexlet->firstKeyLength);
    string firstNotOwnedKey = StringUtil::binaryToString(
                indexlet->firstNotOwnedKey, indexlet->firstNotOwnedKeyLength);

    EXPECT_EQ(0, firstKey.compare("c"));
    EXPECT_EQ(0, firstNotOwnedKey.compare("k"));
}

TEST_F(IndexletManagerTest, deleteIndexlet) {
    string key1 = "a";
    string key2 = "c";
    string key3 = "f";
    string key4 = "k";
    string key5 = "u";

    //TODO(ashgup): add comments on each individual test
    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_TRUE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_TRUE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.getIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length()));

    im.addIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key4.c_str(), (uint16_t)key4.length());

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key2.c_str(),
        (uint16_t)key2.length(), key5.c_str(), (uint16_t)key5.length()));

    EXPECT_FALSE(im.deleteIndexlet(0, 0, key1.c_str(),
        (uint16_t)key1.length(), key4.c_str(), (uint16_t)key4.length()));
}

}  // namespace RAMCloud
