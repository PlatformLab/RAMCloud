/* Copyright (c) 2014-2015 Stanford University
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
#include "ObjectBuffer.h"

namespace RAMCloud {

/**
 * Unit tests for ObjectBuffer.
 */
class ObjectBufferTest : public ::testing::Test {
  public:
    ObjectBufferTest()
        : stringKeys(),
          dataBlob(),
          numKeys(3),
          cumulativeKeyLengths(),
          buffer()
    {
        snprintf(stringKeys[0], sizeof(stringKeys[0]), "ha");
        snprintf(stringKeys[1], sizeof(stringKeys[1]), "hi");
        snprintf(stringKeys[2], sizeof(stringKeys[2]), "ho");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");

        buffer.appendExternal(&numKeys, sizeof(numKeys));

        cumulativeKeyLengths[0] = 3;
        cumulativeKeyLengths[1] = 6;
        cumulativeKeyLengths[2] = 9;
        buffer.appendExternal(cumulativeKeyLengths, 3 *sizeof(KeyLength));

        // append keys here.
        buffer.appendExternal(&stringKeys[0], sizeof(stringKeys[0]));
        buffer.appendExternal(&stringKeys[1], sizeof(stringKeys[1]));
        buffer.appendExternal(&stringKeys[2], sizeof(stringKeys[2]));

        // append data
        buffer.appendExternal(&dataBlob[0], 2);
        buffer.appendExternal(&dataBlob[2], 2);
    }

    ~ObjectBufferTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKeys[3][3];
    char dataBlob[4];
    KeyCount numKeys;
    CumulativeKeyLength cumulativeKeyLengths[3];
    ObjectBuffer buffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectBufferTest);
};

TEST_F(ObjectBufferTest, constructor)
{
    EXPECT_FALSE((buffer.object));
}

TEST_F(ObjectBufferTest, getNumKeys)
{
    EXPECT_EQ(3U, buffer.getNumKeys());
}

TEST_F(ObjectBufferTest, getKey)
{
    EXPECT_EQ("ha", string(reinterpret_cast<const char*>(
                    buffer.getKey(0))));
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(
                    buffer.getKey(1))));
    EXPECT_EQ("ho", string(reinterpret_cast<const char*>(
                    buffer.getKey(2))));
    EXPECT_STREQ(NULL, (const char *)buffer.getKey(3));
}

TEST_F(ObjectBufferTest, getKeyLength)
{
    EXPECT_EQ(3U, buffer.getKeyLength(0));
    EXPECT_EQ(3U, buffer.getKeyLength(1));
    EXPECT_EQ(3U, buffer.getKeyLength(2));
    EXPECT_EQ(0U, buffer.getKeyLength(3));
}

TEST_F(ObjectBufferTest, getValue)
{
    uint32_t dataLen;
    EXPECT_EQ("YO!", string(reinterpret_cast<const char*>(
                    buffer.getValue(&dataLen))));
    EXPECT_EQ(4U, dataLen);
}

TEST_F(ObjectBufferTest, getValueOffset)
{
    uint32_t valueOffset;
    EXPECT_TRUE(buffer.getValueOffset(&valueOffset));
    EXPECT_EQ(16U, valueOffset);
}

TEST_F(ObjectBufferTest, getVersion) {
    // These two implicitly construct the object
    EXPECT_EQ(0U, buffer.getVersion());
    EXPECT_EQ(0U, buffer.object->getVersion());


    buffer.object->setVersion(999);
    EXPECT_EQ(999U, buffer.getVersion());
    EXPECT_EQ(999U, buffer.object->getVersion());
}

TEST_F(ObjectBufferTest, getObject) {
    EXPECT_FALSE(buffer.object);
    EXPECT_NE(reinterpret_cast<Object*>(NULL), buffer.getObject());
    EXPECT_TRUE(buffer.object);
    EXPECT_EQ(buffer.object.get(), buffer.getObject());
}

TEST_F(ObjectBufferTest, reset)
{
    buffer.reset();
    EXPECT_FALSE((buffer.object));
}
} // namespace
