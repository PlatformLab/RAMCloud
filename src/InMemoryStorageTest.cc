/* Copyright (c) 2010-2014 Stanford University
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
#include "InMemoryStorage.h"

namespace RAMCloud {

class InMemoryStorageTest : public ::testing::Test {
  public:
    typedef char* bytes;
    enum { METADATA_SIZE = InMemoryStorage::METADATA_SIZE };
    typedef InMemoryStorage::Frame Frame;

    const char* test;
    uint32_t testLength;
    Buffer testSource;
    uint32_t segmentFrames;
    uint32_t segmentSize;
    Tub<InMemoryStorage> storage;

    InMemoryStorageTest()
        : test("test")
        , testLength(downCast<uint32_t>(strlen(test)))
        , testSource()
        , segmentFrames(4)
        , segmentSize(4 * 1024)
        , storage()
    {
        testSource.appendExternal(test, testLength + 1);
        storage.construct(segmentSize, segmentFrames, 0);
    }

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorageTest);
};

TEST_F(InMemoryStorageTest, Frame_append) {
    BackupStorage::FrameRef frame = storage->open(false);
    frame->append(testSource, 0, 5, 0, test, testLength + 1);

    char* replica = bytes(frame->load());
    EXPECT_STREQ(test, replica);
    char* metadata = bytes(frame->getMetadata());
    EXPECT_STREQ(test, metadata);
}

TEST_F(InMemoryStorageTest, open) {
    BackupStorage::FrameRef frame = storage->open(false);
    EXPECT_EQ(0, storage->freeMap[0]);
    EXPECT_EQ(0u,
              static_cast<InMemoryStorage::Frame*>(frame.get())->frameIndex);
}

} // namespace RAMCloud
