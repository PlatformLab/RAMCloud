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

class SingleFileStorageTest : public ::testing::Test {
  public:
    const char* path;
    uint32_t segmentFrames;
    uint32_t segmentSize;
    Tub<SingleFileStorage> storage;

    SingleFileStorageTest()
        : path("/tmp/ramcloud-backup-storage-test-delete-this")
        , segmentFrames(2)
        , segmentSize(8)
        , storage()
    {
        storage.construct(segmentSize, segmentFrames, path, 0);
    }

    ~SingleFileStorageTest()
    {
        unlink(path);
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    /**
     * Places data in an aligned buffer before saving it in order to make it
     * work with test cases that use O_DIRECT.
     */
    void writeSegment(uint64_t masterId, uint64_t segmentId, const char* data)
    {
        std::unique_ptr<char, void (*)(void*)> block( // NOLINT
            static_cast<char*>(Memory::xmemalign(HERE,
                               segmentSize,
                               segmentSize)), std::free);
        memcpy(block.get(), data, segmentSize);
        std::unique_ptr<BackupStorage::Handle>
            handle(storage->allocate());
        storage->putSegment(handle.get(), block.get());
    }

    DISALLOW_COPY_AND_ASSIGN(SingleFileStorageTest);
};

TEST_F(SingleFileStorageTest, constructor) {
    struct stat s;
    stat(path, &s);
    EXPECT_EQ(segmentSize * segmentFrames, s.st_size);
}

TEST_F(SingleFileStorageTest, openFails) {
    TestLog::Enable _;
    EXPECT_THROW(SingleFileStorage(segmentSize,
                                            segmentFrames,
                                            "/dev/null/cantcreate", 0),
                            BackupStorageException);
    EXPECT_EQ("SingleFileStorage: Failed to open backup storage file "
              "/dev/null/cantcreate: Not a directory", TestLog::get());
}

TEST_F(SingleFileStorageTest, allocate) {
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate());
    EXPECT_EQ(0, storage->freeMap[0]);
    EXPECT_EQ(0U,
        static_cast<SingleFileStorage::Handle*>(handle.get())->
            getSegmentFrame());
}

TEST_F(SingleFileStorageTest, allocate_ensureFifoUse) {
    BackupStorage::Handle* handle = storage->allocate();
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

    handle = storage->allocate();
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

    handle = storage->allocate();
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
    delete storage->allocate();
    delete storage->allocate();
    EXPECT_THROW(
        std::unique_ptr<BackupStorage::Handle>(storage->allocate()),
        BackupStorageException);
}

TEST_F(SingleFileStorageTest, free) {
    BackupStorage::Handle* handle = storage->allocate();
    storage->free(handle);

    EXPECT_EQ(1, storage->freeMap[0]);

    char buf[4];
    lseek(storage->fd, storage->offsetOfSegmentFrame(0), SEEK_SET);
    read(storage->fd, &buf[0], 4);
    EXPECT_EQ('\0', buf[0]);
    EXPECT_EQ('E', buf[3]);
}

TEST_F(SingleFileStorageTest, getSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate());

    const char* src = "1234567";
    char dst[segmentSize];

    storage->putSegment(handle.get(), src);
    storage->getSegment(handle.get(), dst);

    EXPECT_STREQ("1234567", dst);
}

TEST_F(SingleFileStorageTest, putSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate());

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
        handle(storage->allocate());
    close(storage->fd);
    char buf[segmentSize];
    memset(buf, 0, sizeof(buf));
    EXPECT_THROW(
        storage->putSegment(handle.get(), buf),
        BackupStorageException);
    storage->fd = open(path, O_CREAT | O_RDWR, 0666); // supresses LOG ERROR
}

TEST_F(SingleFileStorageTest, offsetOfSegmentFrame) {
    storage->segmentSize = 1 << 23;
    uint64_t offset = storage->offsetOfSegmentFrame(512);
    EXPECT_NE(0lu, offset);
    EXPECT_EQ(1lu << 32, offset);
}

TEST_F(SingleFileStorageTest, getAllHeadersAndFooters) {
    /*
     * Setups with O_DIRECT must be at least block sized (512 on most
     * filesystems).  Any test involving O_DIRECT is going to be slow.
     * (100 ms or more just for single block-sized segments)
     */
    struct {
        uint32_t segmentSize;
        int openFlags;
    } setups[] {
        { 8, 0 },
        { 512, O_DIRECT | O_SYNC },
        // { 1 << 23, O_DIRECT | O_SYNC }, // Standard seg size, takes 500 ms.
        { ~0u, 0 }
    };

    segmentFrames = 4;
    for (auto* setup = &setups[0]; setup->segmentSize != ~0u; ++setup) {
        segmentSize = setup->segmentSize;
        storage.construct(segmentSize, segmentFrames, path, setup->openFlags);

        char buf[setup->segmentSize];
        memset(buf, 'q', sizeof(buf));
        buf[0] = '0';
        buf[setup->segmentSize - 1] = 'a';
        writeSegment(99, 0, buf);
        buf[0] = '1';
        buf[setup->segmentSize - 1] = 'b';
        writeSegment(99, 1, buf);
        buf[0] = '2';
        buf[setup->segmentSize - 1] = 'c';
        writeSegment(99, 2, buf);
        buf[0] = '3';
        buf[setup->segmentSize - 1] = 'd';
        writeSegment(99, 3, buf);

        storage.construct(segmentSize, segmentFrames, path, setup->openFlags);

        const size_t headerSize = 2;
        const size_t footerSize = 3;
        std::unique_ptr<char[]> entries(
            storage->getAllHeadersAndFooters(headerSize, footerSize));
        char* p = entries.get();

        const size_t totalSize = (headerSize + footerSize) * segmentFrames;
        EXPECT_EQ(0, memcmp(p, "0qqqa1qqqb2qqqc3qqqd", totalSize));
    }
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
        handle(storage->allocate());
    EXPECT_TRUE(0 !=
        static_cast<InMemoryStorage::Handle*>(handle.get())->
            getAddress());
}

TEST_F(InMemoryStorageTest, allocate_noFreeFrames) {
    delete storage->allocate();
    delete storage->allocate();
    EXPECT_THROW(
        std::unique_ptr<BackupStorage::Handle>(storage->allocate()),
        BackupStorageException);
}

TEST_F(InMemoryStorageTest, free) {
    BackupStorage::Handle* handle = storage->allocate();
    storage->free(handle);
}

TEST_F(InMemoryStorageTest, getSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate());

    const char* src = "1234567";
    char dst[segmentSize];

    storage->putSegment(handle.get(), src);
    storage->getSegment(handle.get(), dst);

    EXPECT_STREQ("1234567", dst);
}

TEST_F(InMemoryStorageTest, putSegment) {
    delete storage->allocate();  // skip the first segment frame
    std::unique_ptr<BackupStorage::Handle>
        handle(storage->allocate());

    const char* src = "1234567";
    storage->putSegment(handle.get(), src);
    EXPECT_STREQ("1234567",
        static_cast<InMemoryStorage::Handle*>(handle.get())->getAddress());
}

} // namespace RAMCloud
