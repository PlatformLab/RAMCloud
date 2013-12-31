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
          endKeyOffsets(),
          buffer()
    {
        snprintf(stringKeys[0], sizeof(stringKeys[0]), "ha");
        snprintf(stringKeys[1], sizeof(stringKeys[1]), "hi");
        snprintf(stringKeys[2], sizeof(stringKeys[2]), "ho");
        snprintf(dataBlob, sizeof(dataBlob), "YO!");

        buffer.append(&numKeys, sizeof(numKeys));

        endKeyOffsets[0] = 2;
        endKeyOffsets[1] = 5;
        endKeyOffsets[2] = 8;
        buffer.append(endKeyOffsets, 3 *sizeof(uint16_t));

        // append keys here.
        buffer.append(&stringKeys[0], sizeof(stringKeys[0]));
        buffer.append(&stringKeys[1], sizeof(stringKeys[1]));
        buffer.append(&stringKeys[2], sizeof(stringKeys[2]));

        // append data
        buffer.append(&dataBlob[0], 2);
        buffer.append(&dataBlob[2], 2);
    }

    ~ObjectBufferTest()
    {
    }

    // Don't use static strings, since they'll be loaded into read-only
    // memory and we can't mutate to test checksumming.
    char stringKeys[3][3];
    char dataBlob[4];
    uint8_t numKeys;
    uint16_t endKeyOffsets[3];
    ObjectBuffer buffer;

    DISALLOW_COPY_AND_ASSIGN(ObjectBufferTest);
};

TEST_F(ObjectBufferTest, getNumKeys)
{
    EXPECT_EQ(3U, buffer.getNumKeys());
}

TEST_F(ObjectBufferTest, getKey)
{
    EXPECT_EQ(0, memcmp("ha", buffer.getKey(0), 3));
    EXPECT_EQ(0, memcmp("hi", buffer.getKey(1), 3));
    EXPECT_EQ(0, memcmp("ho", buffer.getKey(2), 3));
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
    EXPECT_EQ(0, memcmp("YO!", buffer.getValue(&dataLen), 4));
    EXPECT_EQ(4U, dataLen);
}

TEST_F(ObjectBufferTest, getValueOffset)
{
    EXPECT_EQ(16U, buffer.getValueOffset());
}

} // namespace
