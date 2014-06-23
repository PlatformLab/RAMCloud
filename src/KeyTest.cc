/* Copyright (c) 2012-2014 Stanford University
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

#include "Key.h"
#include "Object.h"

namespace RAMCloud {

/**
 * Unit tests for Key.
 */
class KeyTest : public ::testing::Test {
  public:
    KeyTest()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(KeyTest);
};

TEST_F(KeyTest, constructor_fromLog)
{
    Buffer buffer;
    Key key(12, "blah", 5);

    Buffer dataBuffer;
    Object object(key, NULL, 0, 0, 0, dataBuffer);

    object.assembleForLog(buffer);
    Key key2(LOG_ENTRY_TYPE_OBJ, buffer);
    EXPECT_EQ(12U, key2.getTableId());
    EXPECT_STREQ("blah", reinterpret_cast<const char*>(key2.getStringKey()));
    EXPECT_EQ(5U, key2.getStringKeyLength());

    ObjectTombstone tombstone(object, 5, 0);
    buffer.reset();
    tombstone.assembleForLog(buffer);
    Key key3(LOG_ENTRY_TYPE_OBJTOMB, buffer);
    EXPECT_EQ(12U, key3.getTableId());
    EXPECT_STREQ("blah", reinterpret_cast<const char*>(key3.getStringKey()));
    EXPECT_EQ(5U, key3.getStringKeyLength());

    EXPECT_FALSE(key.hash);
    EXPECT_FALSE(key2.hash);
    EXPECT_FALSE(key3.hash);

    EXPECT_THROW(Key(LOG_ENTRY_TYPE_SEGHEADER, buffer), FatalError);
}

TEST_F(KeyTest, constructor_fromBuffer)
{
    Buffer buffer;
    buffer.appendExternal("woops", 6);

    Key key(48, buffer, 0, 6);
    EXPECT_EQ(48U, key.getTableId());
    EXPECT_EQ("woops", string(reinterpret_cast<const char*>(
                       key.getStringKey())));
    EXPECT_EQ(6U, key.getStringKeyLength());

    Key key2(59, buffer, 1, 3);
    EXPECT_EQ(59U, key2.getTableId());
    EXPECT_EQ("oop", string(reinterpret_cast<const char*>(
                            key2.getStringKey()), 3));
    EXPECT_EQ(3U, key2.getStringKeyLength());

    EXPECT_FALSE(key.hash);
    EXPECT_FALSE(key2.hash);
}

TEST_F(KeyTest, constructor_fromVoidPointer) {
    Key key(74, "na-na-na-na", 13);
    EXPECT_EQ(74U, key.getTableId());
    EXPECT_EQ("na-na-na-na", string(reinterpret_cast<const char*>(
                             key.getStringKey())));
    EXPECT_EQ(13U, key.getStringKeyLength());
    EXPECT_FALSE(key.hash);
}

TEST_F(KeyTest, getHash) {
    Key key(82, "hey-hey-hey", 13);
    EXPECT_FALSE(key.hash);
    EXPECT_EQ(0xed7cc41c7081ba0UL, key.getHash());
    EXPECT_TRUE(key.hash);
    EXPECT_EQ(0xed7cc41c7081ba0UL, key.getHash());
}

TEST_F(KeyTest, getHash_static) {
    Key key(82, "goodbye", 8);
    Key key1(83, "goodbye", 8);
    Key key2(83, "goodbye", 7);
    EXPECT_NE(key, key1);
    EXPECT_NE(key, key2);
    EXPECT_NE(key1, key2);
}

/*
 * Ensure that #RAMCloud::HashTable::hash() generates hashes using the full
 * range of bits.
 */
TEST_F(KeyTest, getHash_UsesAllBits) {
    uint64_t observedBits = 0UL;
    srand(1);
    for (uint32_t i = 0; i < 50; i++) {
        uint64_t input1 = generateRandom();
        uint64_t input2 = generateRandom();
        observedBits |= Key::getHash(input1, &input2, sizeof(input2));
    }
    EXPECT_EQ(~0UL, observedBits);
}

TEST_F(KeyTest, operatorEquals_and_operatorNotEquals) {
    Key key(82, "fun", 4);
    Key key2(82, "fun", 4);
    EXPECT_EQ(key, key);
    EXPECT_EQ(key, key2);

    Key key3(83, "fun", 4);
    EXPECT_NE(key, key3);

    Key key4(83, "fun", 3);
    EXPECT_NE(key, key4);
    EXPECT_NE(key2, key4);
    EXPECT_NE(key3, key4);
}

TEST_F(KeyTest, getTableId) {
    EXPECT_EQ(8274U, Key(8274, "hi", 3).getTableId());
}

TEST_F(KeyTest, getStringKey) {
    const char *keyString = reinterpret_cast<const char*>(
                            Key(8274, "hi", 3).getStringKey());
    EXPECT_EQ("hi", string(keyString));
}

TEST_F(KeyTest, getStringKeyLength) {
    EXPECT_EQ(3, Key(8274, "hi", 3).getStringKeyLength());
}

TEST_F(KeyTest, toString) {
    EXPECT_EQ("<tableId: 27, key: \"ascii key\", "
              "keyLength: 9, hash: 0xe415add6e960d438>",
              Key(27, "ascii key", 9).toString());

    EXPECT_EQ("<tableId: 814, key: \"binary key\\xaa\\x0a\", "
              "keyLength: 12, hash: 0xe972fd5603c67b0e>",
              Key(814, "binary key\xaa\x0a", 12).toString());
}

} // namespace RAMCloud
