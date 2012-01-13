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

namespace RAMCloud {

namespace {
const char* path = "/tmp/ramcloud-backup-storage-test-delete-this";
};

class SingleFileStorageTest : public ::testing::Test {
  public:
    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    SingleFileStorage* storage;

    SingleFileStorageTest()
        : segmentFrames(2)
        , segmentSize(8)
        , storage(NULL)
    {
        storage = new SingleFileStorage(segmentSize, segmentFrames, path, 0);
    }

    ~SingleFileStorageTest()
    {
        delete storage;
        unlink(path);
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    DISALLOW_COPY_AND_ASSIGN(SingleFileStorageTest);
};

TEST_F(SingleFileStorageTest, constructor) {
    struct stat s;
    stat(path, &s);
    EXPECT_EQ(segmentSize * segmentFrames, s.st_size);
}

TEST_F(SingleFileStorageTest, openFails) {
    EXPECT_THROW(SingleFileStorage(segmentSize,
                                            segmentFrames,
                                            "/dev/null/cantcreate", 0),
                            BackupStorageException);
    EXPECT_STREQ("Not a directory", strerror(errno));
}

TEST_F(SingleFileStorageTest, allocate) {
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 0));
    EXPECT_EQ(0, storage->freeMap[0]);
    EXPECT_EQ(0U,
        static_cast<SingleFileStorage::Handle*>(handle.get())->
            getSegmentFrame());
}

TEST_F(SingleFileStorageTest, allocate_ensureFifoUse) {
    BackupStorage::Handle* handle = storage->allocate(99, 0);
    try {
        EXPECT_EQ(0, storage->freeMap[0]);
        EXPECT_EQ(0U,
            static_cast<SingleFileStorage::Handle*>(handle)->
                getSegmentFrame());
        storage->free(handle);
    } catch (...) {
        delete handle;
        throw;
    }

    handle = storage->allocate(99, 1);
    try {
        EXPECT_EQ(1, storage->freeMap[0]);
        EXPECT_EQ(0, storage->freeMap[1]);
        EXPECT_EQ(1U,
            static_cast<SingleFileStorage::Handle*>(handle)->
                getSegmentFrame());
        storage->free(handle);
    } catch (...) {
        delete handle;
        throw;
    }

    handle = storage->allocate(99, 2);
    try {
        EXPECT_EQ(0, storage->freeMap[0]);
        EXPECT_EQ(1, storage->freeMap[1]);
        EXPECT_EQ(0U,
            static_cast<SingleFileStorage::Handle*>(handle)->
                getSegmentFrame());
        storage->free(handle);
    } catch (...) {
        delete handle;
        throw;
    }
}

TEST_F(SingleFileStorageTest, allocate_noFreeFrames) {
    delete storage->allocate(99, 0);
    delete storage->allocate(99, 1);
    EXPECT_THROW(
        std::unique_ptr<BackupStorage::Handle>(storage->allocate(99, 2)),
        BackupStorageException);
}

TEST_F(SingleFileStorageTest, free) {
    BackupStorage::Handle* handle = storage->allocate(99, 0);
    storage->free(handle);

    EXPECT_EQ(1, storage->freeMap[0]);

    char buf[4];
    lseek(storage->fd, storage->offsetOfSegmentFrame(0), SEEK_SET);
    read(storage->fd, &buf[0], 4);
    EXPECT_EQ('F', buf[0]);
    EXPECT_EQ('E', buf[3]);
}

TEST_F(SingleFileStorageTest, getSegment) {
    delete storage->allocate(99, 0);  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 1));

    const char* src = "1234567";
    char dst[segmentSize];

    storage->putSegment(handle.get(), src);
    storage->getSegment(handle.get(), dst);

    EXPECT_STREQ("1234567", dst);
}

TEST_F(SingleFileStorageTest, putSegment) {
    delete storage->allocate(99, 0);  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 1));

    const char* src = "1234567";
    EXPECT_EQ(8U, segmentSize);
    char buf[segmentSize];

    storage->putSegment(handle.get(), src);

    lseek(storage->fd, storage->offsetOfSegmentFrame(1), SEEK_SET);
    read(storage->fd, &buf[0], segmentSize);
    EXPECT_STREQ("1234567", buf);
}

TEST_F(SingleFileStorageTest, putSegment_seekFailed) {
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 1));
    close(storage->fd);
    char buf[segmentSize];
    memset(buf, 0, sizeof(buf));
    EXPECT_THROW(
        storage->putSegment(handle.get(), buf),
        BackupStorageException);
    storage->fd = open(path, O_CREAT | O_RDWR, 0666); // supresses LOG ERROR
}

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
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorageTest);
};

TEST_F(InMemoryStorageTest, allocate) {
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 0));
    EXPECT_TRUE(0 !=
        static_cast<InMemoryStorage::Handle*>(handle.get())->
            getAddress());
}

TEST_F(InMemoryStorageTest, allocate_noFreeFrames) {
    delete storage->allocate(99, 0);
    delete storage->allocate(99, 1);
    EXPECT_THROW(
        std::unique_ptr<BackupStorage::Handle>(storage->allocate(99, 2)),
        BackupStorageException);
}

TEST_F(InMemoryStorageTest, free) {
    BackupStorage::Handle* handle = storage->allocate(99, 0);
    storage->free(handle);
}

TEST_F(InMemoryStorageTest, getSegment) {
    delete storage->allocate(99, 0);  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 1));

    const char* src = "1234567";
    char dst[segmentSize];

    storage->putSegment(handle.get(), src);
    storage->getSegment(handle.get(), dst);

    EXPECT_STREQ("1234567", dst);
}

TEST_F(InMemoryStorageTest, putSegment) {
    delete storage->allocate(99, 0);  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate(99, 1));

    const char* src = "1234567";
    storage->putSegment(handle.get(), src);
    EXPECT_STREQ("1234567",
        static_cast<InMemoryStorage::Handle*>(handle.get())->getAddress());
}

} // namespace RAMCloud
