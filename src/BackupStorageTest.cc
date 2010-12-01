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
#include <boost/scoped_ptr.hpp>

#include "TestUtil.h"

#include "BackupStorage.h"

namespace RAMCloud {

namespace {
const char* path = "/tmp/ramcloud-backup-storage-test-delete-this";
};

class SingleFileStorageTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(SingleFileStorageTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_constructor_openFails);
    CPPUNIT_TEST(test_allocate);
    CPPUNIT_TEST(test_allocate_ensureFifoUse);
    CPPUNIT_TEST(test_allocate_noFreeFrames);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_getSegment);
    CPPUNIT_TEST(test_putSegment);
    CPPUNIT_TEST(test_putSegment_seekFailed);
    CPPUNIT_TEST_SUITE_END();

    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    SingleFileStorage* storage;

  public:
    SingleFileStorageTest()
        : segmentFrames(2)
        , segmentSize(8)
        , storage(NULL)
    {
    }

    void
    setUp()
    {
        storage = new SingleFileStorage(segmentSize, segmentFrames, path, 0);
    }

    void
    tearDown()
    {
        delete storage;
        unlink(path);
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_constructor()
    {
        struct stat s;
        stat(path, &s);
        CPPUNIT_ASSERT_EQUAL(segmentSize * segmentFrames, s.st_size);
    }

    void
    test_constructor_openFails()
    {
        CPPUNIT_ASSERT_THROW(SingleFileStorage(segmentSize,
                                               segmentFrames,
                                               "/dev/null/cantcreate", 0),
                             BackupStorageException);
        CPPUNIT_ASSERT_EQUAL("Not a directory", strerror(errno));
    }

    void
    test_allocate()
    {
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 0));
        CPPUNIT_ASSERT_EQUAL(0, storage->freeMap[0]);
        CPPUNIT_ASSERT_EQUAL(0,
            static_cast<SingleFileStorage::Handle*>(handle.get())->
                getSegmentFrame());
    }

    void
    test_allocate_ensureFifoUse()
    {
        BackupStorage::Handle* handle = storage->allocate(99, 0);
        try {
            CPPUNIT_ASSERT_EQUAL(0, storage->freeMap[0]);
            CPPUNIT_ASSERT_EQUAL(0,
                static_cast<SingleFileStorage::Handle*>(handle)->
                    getSegmentFrame());
            storage->free(handle);
        } catch (...) {
            delete handle;
            throw;
        }

        handle = storage->allocate(99, 1);
        try {
            CPPUNIT_ASSERT_EQUAL(1, storage->freeMap[0]);
            CPPUNIT_ASSERT_EQUAL(0, storage->freeMap[1]);
            CPPUNIT_ASSERT_EQUAL(1,
                static_cast<SingleFileStorage::Handle*>(handle)->
                    getSegmentFrame());
            storage->free(handle);
        } catch (...) {
            delete handle;
            throw;
        }

        handle = storage->allocate(99, 2);
        try {
            CPPUNIT_ASSERT_EQUAL(0, storage->freeMap[0]);
            CPPUNIT_ASSERT_EQUAL(1, storage->freeMap[1]);
            CPPUNIT_ASSERT_EQUAL(0,
                static_cast<SingleFileStorage::Handle*>(handle)->
                    getSegmentFrame());
            storage->free(handle);
        } catch (...) {
            delete handle;
            throw;
        }
    }

    void
    test_allocate_noFreeFrames()
    {
        delete storage->allocate(99, 0);
        delete storage->allocate(99, 1);
        CPPUNIT_ASSERT_THROW(
            boost::scoped_ptr<BackupStorage::Handle>(storage->allocate(99, 2)),
            BackupStorageException);
    }

    void
    test_free()
    {
        BackupStorage::Handle* handle = storage->allocate(99, 0);
        storage->free(handle);

        CPPUNIT_ASSERT_EQUAL(1, storage->freeMap[0]);

        char buf[4];
        lseek(storage->fd, storage->offsetOfSegmentFrame(0), SEEK_SET);
        read(storage->fd, &buf[0], 4);
        CPPUNIT_ASSERT_EQUAL('F', buf[0]);
        CPPUNIT_ASSERT_EQUAL('E', buf[3]);
    }

    void
    test_getSegment()
    {
        delete storage->allocate(99, 0);  // skip the first segment frame
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 1));

        const char* src = "1234567";
        char dst[segmentSize];

        storage->putSegment(handle.get(), src);
        storage->getSegment(handle.get(), dst);

        CPPUNIT_ASSERT_EQUAL("1234567", dst);
    }

    void
    test_putSegment()
    {
        delete storage->allocate(99, 0);  // skip the first segment frame
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 1));

        const char* src = "1234567";
        CPPUNIT_ASSERT_EQUAL(8, segmentSize);
        char buf[segmentSize];

        storage->putSegment(handle.get(), src);

        lseek(storage->fd, storage->offsetOfSegmentFrame(1), SEEK_SET);
        read(storage->fd, &buf[0], segmentSize);
        CPPUNIT_ASSERT_EQUAL("1234567", buf);
    }

    void
    test_putSegment_seekFailed()
    {
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 1));
        close(storage->fd);
        CPPUNIT_ASSERT_THROW(
            storage->putSegment(handle.get(), NULL),
            BackupStorageException);
        storage->fd = open(path, O_CREAT | O_RDWR, 0666); // supresses LOG ERROR
    }

    // offsetOfSegmentFrame: correct by proof by construction

    // reserveSpace: tested by test_constructor

    DISALLOW_COPY_AND_ASSIGN(SingleFileStorageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(SingleFileStorageTest);

class InMemoryStorageTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(InMemoryStorageTest);
    CPPUNIT_TEST(test_allocate);
    CPPUNIT_TEST(test_allocate_noFreeFrames);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_getSegment);
    CPPUNIT_TEST(test_putSegment);
    CPPUNIT_TEST_SUITE_END();

    const uint32_t segmentFrames;
    const uint32_t segmentSize;
    InMemoryStorage* storage;

  public:
    InMemoryStorageTest()
        : segmentFrames(2)
        , segmentSize(8)
        , storage(NULL)
    {
    }

    void
    setUp()
    {
        storage = new InMemoryStorage(segmentSize, segmentFrames);
    }

    void
    tearDown()
    {
        delete storage;
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    test_allocate()
    {
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 0));
        CPPUNIT_ASSERT(0 !=
            static_cast<InMemoryStorage::Handle*>(handle.get())->
                getAddress());
    }

    void
    test_allocate_noFreeFrames()
    {
        delete storage->allocate(99, 0);
        delete storage->allocate(99, 1);
        CPPUNIT_ASSERT_THROW(
            boost::scoped_ptr<BackupStorage::Handle>(storage->allocate(99, 2)),
            BackupStorageException);
    }

    void
    test_free()
    {
        BackupStorage::Handle* handle = storage->allocate(99, 0);
        storage->free(handle);
    }

    void
    test_getSegment()
    {
        delete storage->allocate(99, 0);  // skip the first segment frame
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 1));

        const char* src = "1234567";
        char dst[segmentSize];

        storage->putSegment(handle.get(), src);
        storage->getSegment(handle.get(), dst);

        CPPUNIT_ASSERT_EQUAL("1234567", dst);
    }

    void
    test_putSegment()
    {
        delete storage->allocate(99, 0);  // skip the first segment frame
        boost::scoped_ptr<BackupStorage::Handle>
            handle(storage->allocate(99, 1));

        const char* src = "1234567";
        storage->putSegment(handle.get(), src);
        CPPUNIT_ASSERT_EQUAL("1234567",
            static_cast<InMemoryStorage::Handle*>(handle.get())->getAddress());
    }

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorageTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryStorageTest);

} // namespace RAMCloud
