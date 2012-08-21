/* Copyright (c) 2010 Stanford University
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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <memory>

#include "TestUtil.h"

#include "BackupStorage.h"
#include "StringUtil.h"

namespace RAMCloud {

#if 0
class InMemoryStorageTest : public ::testing::Test {
  public:
    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    InMemoryStorage* storage;

    InMemoryStorageTest()
        : segmentFrames(2)
        , segmentSize(8)
        , storage(NULL)
    {
        storage = new InMemoryStorage(segmentSize, segmentFrames);
    }

    ~InMemoryStorageTest()
    {
        delete storage;
        EXPECT_EQ(0,
            BackupStorage::Frame::resetAllocatedFramesCount());
    }

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorageTest);
};

TEST_F(InMemoryStorageTest, allocate) {
    std::unique_ptr<BackupStorage::Frame>
        handle(storage->allocate());
    EXPECT_TRUE(0 !=
        static_cast<InMemoryStorage::Frame*>(handle.get())->
            getAddress());
}

TEST_F(InMemoryStorageTest, allocate_noFreeFrames) {
    delete storage->allocate();
    delete storage->allocate();
    EXPECT_THROW(
        std::unique_ptr<BackupStorage::Frame>(storage->allocate()),
        BackupStorageException);
}

TEST_F(InMemoryStorageTest, free) {
    BackupStorage::Frame* handle = storage->allocate();
    storage->free(handle);
}

TEST_F(InMemoryStorageTest, getSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Frame>
        handle(storage->allocate());

    const char* src = "1234567";
    char dst[segmentSize];

    storage->putSegment(handle.get(), src);
    storage->getSegment(handle.get(), dst);

    EXPECT_STREQ("1234567", dst);
}

TEST_F(InMemoryStorageTest, putSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Frame>
        handle(storage->allocate());

    const char* src = "1234567";
    storage->putSegment(handle.get(), src);
    EXPECT_STREQ("1234567",
        static_cast<InMemoryStorage::Frame*>(handle.get())->getAddress());
}
#endif

} // namespace RAMCloud
