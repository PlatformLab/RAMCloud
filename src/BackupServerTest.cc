/* Copyright (c) 2009 Stanford University
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

#include <string>
#include <cstring>

#include "TestUtil.h"
#include "BackupServer.h"
#include "Log.h"
#include "Master.h"
#include "MockTransport.h"
#include "Rpc.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Unit tests for BackupServer.
 */
class BackupServerTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_commitSegment);
    CPPUNIT_TEST(test_commitSegment_segmentNotOpen);
    CPPUNIT_TEST(test_findSegmentInfo);
    CPPUNIT_TEST(test_findSegmentInfo_notIn);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_freeSegment_stillOpen);
    CPPUNIT_TEST(test_freeSegment_noSuchSegment);
    CPPUNIT_TEST(test_getRecoveryData);
    CPPUNIT_TEST(test_openSegment);
    CPPUNIT_TEST(test_openSegment_alreadyOpen);
    CPPUNIT_TEST(test_openSegment_outOfStorage);
    CPPUNIT_TEST(test_startReadingData);
    CPPUNIT_TEST(test_startReadingData_empty);
    CPPUNIT_TEST(test_writeSegment);
    CPPUNIT_TEST(test_writeSegment_segmentNotOpen);
    CPPUNIT_TEST(test_writeSegment_badOffset);
    CPPUNIT_TEST(test_writeSegment_badLength);
    CPPUNIT_TEST(test_writeSegment_badOffsetPlusLength);
    CPPUNIT_TEST_SUITE_END();

    BackupServer* backup;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    BackupStorage* storage;
    MockTransport* transport;

  public:
    BackupServerTest()
        : backup(NULL)
        , segmentSize(1 << 16)
        , segmentFrames(2)
        , storage(NULL)
        , transport(NULL)
    {
    }

    void
    setUp()
    {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        storage = new InMemoryStorage(segmentSize, segmentFrames);
        backup = new BackupServer(*storage);
    }

    void
    tearDown()
    {
        delete backup;
        delete storage;
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    void
    rpc(const char* input)
    {
        transport->setInput(input);
        backup->handleRpc<BackupServer>();
    }

    void
    freeStorageHandle(uint64_t masterId, uint64_t segmentId)
    {
        BackupServer::SegmentInfo &info =
            backup->segments[BackupServer::MasterSegmentIdPair(masterId,
                                                               segmentId)];
        if (info.storageHandle)
            backup->storage.free(info.storageHandle);
    }

    void
    test_findSegmentInfo()
    {
        BackupServer::SegmentInfo& info =
            backup->segments[BackupServer::MasterSegmentIdPair(99, 88)];
        BackupServer::SegmentInfo* infop = backup->findSegmentInfo(99, 88);
        CPPUNIT_ASSERT(infop != NULL);
        CPPUNIT_ASSERT_EQUAL(&info, infop);
    }

    void
    test_findSegmentInfo_notIn()
    {
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    static bool
    inMemoryStorageFreePred(string s)
    {
        return s == "virtual void RAMCloud::InMemoryStorage::free("
                       "RAMCloud::BackupStorage::Handle*)";
    }

    void
    test_freeSegment()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("133 0 99 0 88 0 10 4 test");   // write 99,88 at 10 for 4 bytes
        rpc("128 0 99 0 88 0");             // commit 99,88
        {
            TestLog::Enable _(&inMemoryStorageFreePred);
            rpc("129 0 99 0 88 0");             // free 99,88
            CPPUNIT_ASSERT_EQUAL("virtual void "
                "RAMCloud::InMemoryStorage::free("
                "RAMCloud::BackupStorage::Handle*): called", TestLog::get());
        }
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 | "
                             "serverReply: 0 0 | serverReply: 0 0",
                             transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    void
    test_freeSegment_stillOpen()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("129 0 99 0 88 0");             // free 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 12 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_freeSegment_noSuchSegment()
    {
        rpc("129 0 99 0 88 0");             // free 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 11 0",
                             transport->outputLog);
    }

    void
    test_getRecoveryData()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("133 0 99 0 88 0 0 16 0x72646873 8 88 0"); // write 99,88 header
        // write 99,88 object
        rpc("133 0 99 0 88 0 16 52 0x216a626f 44 1 0 0 0 2 0 99 99 4 test");
        rpc("133 0 99 0 88 0 68 16 0x6b686373 8 99 99"); // write 99,88 footer
        rpc("128 0 99 0 88 0");             // commit 99,88
        rpc("132 0 99 0");                  // startReadingData 99
        rpc("130 0 99 0");                  // getRecoveryData 99
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 | "
                             "serverReply: 0 0 | serverReply: 0 0 | "
                             "serverReply: 0 0 | serverReply: 0 0 1 88 0 | "
                             "serverReply: 0 0 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_commitSegment()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("133 0 99 0 88 0 10 4 test");   // write 99,88 at 10 for 4 bytes
        rpc("128 0 99 0 88 0");             // commit 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 | "
                             "serverReply: 0 0",
                             transport->outputLog);
        BackupServer::SegmentInfo &info =
            backup->segments[BackupServer::MasterSegmentIdPair(99, 88)];
        char* storageAddress =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        CPPUNIT_ASSERT(NULL != storageAddress);
        CPPUNIT_ASSERT_EQUAL("test", &storageAddress[10]);
        CPPUNIT_ASSERT_EQUAL(NULL, static_cast<void*>(info.segment));
        freeStorageHandle(99, 88);
    }

    void
    test_commitSegment_segmentNotOpen()
    {
        rpc("128 0 99 0 88 0");             // commit 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 11 0",
                             transport->outputLog);
    }

    void
    test_openSegment()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0", transport->outputLog);
        BackupServer::SegmentInfo &info =
            backup->segments[BackupServer::MasterSegmentIdPair(99, 88)];
        CPPUNIT_ASSERT(NULL != info.segment);
        char* address =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        CPPUNIT_ASSERT(NULL != address);
        freeStorageHandle(99, 88);
    }

    void
    test_openSegment_alreadyOpen()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("131 0 99 0 88 0");             // open 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 12 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_openSegment_outOfStorage()
    {
        rpc("131 0 99 0 86 0");             // open 99,86
        rpc("131 0 99 0 87 0");             // open 99,87
        CPPUNIT_ASSERT_THROW(
            rpc("131 0 99 0 88 0"),         // open 99,88
            BackupStorageException);
        freeStorageHandle(99, 87);
        freeStorageHandle(99, 86);
    }

    void
    test_startReadingData()
    {
        rpc("131 0 99 0 88 0"),         // open 99,88
        rpc("132 0 99 0 88 0"),         // startReadingData 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0 1 88 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_startReadingData_empty()
    {
        rpc("132 0 99 0 88 0"),         // startReadingData 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 0",
                             transport->outputLog);
    }

    void
    test_writeSegment()
    {
        rpc("131 0 99 0 88 0");             // open 99,88
        rpc("133 0 99 0 88 0 10 4 test");   // write 99,88 at 10 for 4 bytes
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 0 0",
                             transport->outputLog);
        BackupServer::SegmentInfo &info =
            backup->segments[BackupServer::MasterSegmentIdPair(99, 88)];
        CPPUNIT_ASSERT(NULL != info.segment);
        CPPUNIT_ASSERT_EQUAL("test", &info.segment[10]);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_segmentNotOpen()
    {
        rpc("133 0 99 0 88 0 0 0");             // write 99,88
        CPPUNIT_ASSERT_EQUAL("serverReply: 11 0",
                             transport->outputLog);
    }

    void
    test_writeSegment_badOffset()
    {
        rpc("131 0 99 0 88 0");                 // open 99,88
        rpc("133 0 99 0 88 0 9999999 4 test");  // write 99,88 out of bounds
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 13 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_badLength()
    {
        rpc("131 0 99 0 88 0");                 // open 99,88
        rpc("133 0 99 0 88 0 0 9999999 test");  // write 99,88 too long
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 13 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_badOffsetPlusLength()
    {
        rpc("131 0 99 0 88 0");                   // open 99,88
        rpc("133 0 99 0 88 0 50000 50000 test");  // write 99,88 too far/long
        CPPUNIT_ASSERT_EQUAL("serverReply: 0 0 | serverReply: 13 0",
                             transport->outputLog);
        freeStorageHandle(99, 88);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

} // namespace RAMCloud
