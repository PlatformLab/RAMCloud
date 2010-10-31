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

#include <cstring>

#include "TestUtil.h"
#include "BackupServer.h"
#include "BindTransport.h"
#include "CoordinatorServer.h"
#include "Log.h"
#include "Logging.h"
#include "MasterServer.h"
#include "MockTransport.h"
#include "Rpc.h"
#include "SegmentIterator.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Unit tests for BackupServer.
 */
class BackupServerTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_closeSegment);
    CPPUNIT_TEST(test_closeSegment_segmentNotOpen);
    CPPUNIT_TEST(test_findSegmentInfo);
    CPPUNIT_TEST(test_findSegmentInfo_notIn);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_freeSegment_stillOpen);
    CPPUNIT_TEST(test_freeSegment_noSuchSegment);
    CPPUNIT_TEST(test_getRecoveryData);
    CPPUNIT_TEST(test_getRecoveryData_moreThanOneSegmentStored);
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
    BackupClient* client;
    CoordinatorServer* coordinatorServer;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    BackupStorage* storage;
    BackupServer::Config* config;
    BindTransport* transport;

  public:
    BackupServerTest()
        : backup(NULL)
        , client(NULL)
        , coordinatorServer(NULL)
        , segmentSize(1 << 16)
        , segmentFrames(2)
        , storage(NULL)
        , config(NULL)
        , transport(NULL)
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
    }

    void
    setUp()
    {
        config = new BackupServer::Config();
        config->coordinatorLocator = "mock:host=coordinator";
        storage = new InMemoryStorage(segmentSize, segmentFrames);

        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer();
        transport->addServer(*coordinatorServer, "mock:host=coordinator");
        backup = new BackupServer(*config, *storage);
        transport->addServer(*backup, "mock:host=backup");
        client =
            new BackupClient(transportManager.getSession("mock:host=backup"));
    }

    void
    tearDown()
    {
        delete client;
        delete backup;
        delete coordinatorServer;
        transportManager.unregisterMock();
        delete transport;
        delete storage;
        delete config;
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
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
    test_closeSegment()
    {
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        client->closeSegment(99, 88);
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
    test_closeSegment_segmentNotOpen()
    {
        CPPUNIT_ASSERT_THROW(client->closeSegment(99, 88),
                             BackupBadSegmentIdException);
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
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        client->closeSegment(99, 88);
        {
            TestLog::Enable _(&inMemoryStorageFreePred);
            client->freeSegment(99, 88);
            CPPUNIT_ASSERT_EQUAL("virtual void "
                "RAMCloud::InMemoryStorage::free("
                "RAMCloud::BackupStorage::Handle*): called", TestLog::get());
        }
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    void
    test_freeSegment_stillOpen()
    {
        client->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(client->freeSegment(99, 88),
                             BackupSegmentAlreadyOpenException);
        freeStorageHandle(99, 88);
    }

    void
    test_freeSegment_noSuchSegment()
    {
        CPPUNIT_ASSERT_THROW(client->freeSegment(99, 88),
                             BackupBadSegmentIdException);
    }

    void
    test_getRecoveryData()
    {
        struct MockSegment {
            MockSegment()
                : hdre()
                , hdr()
                , oe()
                , o()
                , cksume()
                , cksum()
            {}
            SegmentEntry hdre;
            SegmentHeader hdr;
            SegmentEntry oe;
            Object o;
            char data[16];
            // TODO(stutsman) test tombstones
            SegmentEntry cksume;
            SegmentFooter cksum;
        } seg;

        seg.hdre.type = LOG_ENTRY_TYPE_SEGHEADER;
        seg.hdre.length = sizeof(seg.hdr);
        seg.hdr.logId = 99;
        seg.hdr.segmentId = 88;
        seg.hdr.segmentCapacity = segmentSize;
        seg.oe.type = LOG_ENTRY_TYPE_OBJ;
        seg.oe.length = sizeof(seg.o) + sizeof(seg.data);
        seg.o.id = 777;
        seg.o.table = 99;
        seg.o.version = 42;
        seg.o.checksum = 0xff00ff00ff00;
        seg.o.data_len = sizeof(seg.data);
        const char* data = "0123456789ABCDEF";
        memcpy(seg.data, data, sizeof(seg.data));
        seg.cksume.type = LOG_ENTRY_TYPE_SEGFOOTER;
        seg.cksume.length = sizeof(SegmentFooter);
        seg.cksum.checksum = 0xDEADBEEFCAFEBABE;

        client->openSegment(99, 88);
        client->writeSegment(99, 88, 0, &seg, sizeof(seg));
        client->closeSegment(99, 88);
        client->startReadingData(99);
        Buffer response;
        client->getRecoveryData(99, 88, ProtoBuf::Tablets(), response);

        SegmentIterator it(response.getRange(0, response.getTotalLength()),
                           segmentSize);

        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, it.getType());
        it.next();
        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
        it.next();
        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
        it.next();
        CPPUNIT_ASSERT(it.isDone());

        freeStorageHandle(99, 88);
    }

    uint32_t
    writeEntry(uint64_t masterId, uint64_t segmentId, LogEntryType type,
               uint32_t offset, const void *data, uint32_t bytes)
    {
        SegmentEntry entry;
        entry.type = type;
        entry.length = bytes;
        client->writeSegment(masterId, segmentId,
                             offset, &entry, sizeof(entry));
        client->writeSegment(masterId, segmentId, offset + sizeof(entry),
                             data, bytes);
        return sizeof(entry) + bytes;
    }

    uint32_t
    writeObject(uint64_t masterId, uint64_t segmentId,
                uint32_t offset, const char *data, uint32_t bytes,
                uint64_t objectId)
    {
        char objectMem[sizeof(Object) + bytes];
        Object* obj = reinterpret_cast<Object*>(objectMem);
        obj->id = objectId;
        obj->table = 0;
        obj->version = 0;
        obj->checksum = 0xff00ff00ff00;
        obj->data_len = bytes;
        memcpy(objectMem + sizeof(*obj), data, bytes);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJ, offset,
                          objectMem, sizeof(Object) + bytes);
    }

    uint32_t
    writeHeader(uint64_t masterId, uint64_t segmentId)
    {
        SegmentHeader header;
        header.logId = masterId;
        header.segmentId = segmentId;
        header.segmentCapacity = segmentSize;
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGHEADER, 0,
                          &header, sizeof(header));
    }

    uint32_t
    writeFooter(uint64_t masterId, uint64_t segmentId, uint32_t offset)
    {
        SegmentFooter footer;
        footer.checksum = 0xff00ff00ff00;
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGFOOTER, offset,
                          &footer, sizeof(footer));
    }

    void
    test_getRecoveryData_moreThanOneSegmentStored()
    {
        uint32_t offset;
        client->openSegment(99, 87);
        offset = writeHeader(99, 87);
        offset += writeObject(99, 87, offset, "test1", 6, 999);
        offset += writeFooter(99, 87, offset);
        client->closeSegment(99, 87);

        client->openSegment(99, 88);
        offset = writeHeader(99, 88);
        offset += writeObject(99, 88, offset, "test2", 6, 999);
        offset += writeFooter(99, 88, offset);
        client->closeSegment(99, 88);


        {
            Buffer response;
            client->getRecoveryData(99, 88, ProtoBuf::Tablets(), response);

            SegmentIterator it(response.getRange(0, response.getTotalLength()),
                               segmentSize);
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, it.getType());
            it.next();
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
            CPPUNIT_ASSERT_EQUAL("test2",
                                 static_cast<const Object*>(it.getPointer())->
                                    data);
            it.next();
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
            it.next();
            CPPUNIT_ASSERT(it.isDone());
        }{
            Buffer response;
            client->getRecoveryData(99, 87, ProtoBuf::Tablets(), response);

            SegmentIterator it(response.getRange(0, response.getTotalLength()),
                               segmentSize);
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGHEADER, it.getType());
            it.next();
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
            CPPUNIT_ASSERT_EQUAL("test1",
                                 static_cast<const Object*>(it.getPointer())->
                                    data);
            it.next();
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_SEGFOOTER, it.getType());
            it.next();
            CPPUNIT_ASSERT(it.isDone());
        }

        client->freeSegment(99, 87);
        client->freeSegment(99, 88);
    }

    void
    test_openSegment()
    {
        client->openSegment(99, 88);
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
        client->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(
            client->openSegment(99, 88),
            BackupSegmentAlreadyOpenException);
        freeStorageHandle(99, 88);
    }

    void
    test_openSegment_outOfStorage()
    {
        client->openSegment(99, 86);
        client->openSegment(99, 87);
        CPPUNIT_ASSERT_THROW(
            client->openSegment(99, 88),
            BackupStorageException);
        freeStorageHandle(99, 87);
        freeStorageHandle(99, 86);
    }

    void
    test_startReadingData()
    {
        client->openSegment(99, 88);
        vector<uint64_t> result = client->startReadingData(99);
        CPPUNIT_ASSERT_EQUAL(1, result.size());
        CPPUNIT_ASSERT_EQUAL(88, result[0]);
        freeStorageHandle(99, 88);
    }

    void
    test_startReadingData_empty()
    {
        vector<uint64_t> result = client->startReadingData(99);
        CPPUNIT_ASSERT_EQUAL(0, result.size());
    }

    void
    test_writeSegment()
    {
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        BackupServer::SegmentInfo &info =
            backup->segments[BackupServer::MasterSegmentIdPair(99, 88)];
        CPPUNIT_ASSERT(NULL != info.segment);
        CPPUNIT_ASSERT_EQUAL("test", &info.segment[10]);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_segmentNotOpen()
    {
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 0, "test", 4),
            BackupBadSegmentIdException);
    }

    void
    test_writeSegment_badOffset()
    {
        client->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 500000, "test", 0),
            BackupSegmentOverflowException);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_badLength()
    {
        client->openSegment(99, 88);
        char junk[70000];
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 0, junk, sizeof(junk)),
            BackupSegmentOverflowException);
        freeStorageHandle(99, 88);
    }

    void
    test_writeSegment_badOffsetPlusLength()
    {
        client->openSegment(99, 88);
        char junk[50000];
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 50000, junk, 50000),
            BackupSegmentOverflowException);
        freeStorageHandle(99, 88);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

} // namespace RAMCloud
