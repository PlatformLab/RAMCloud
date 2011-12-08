/* Copyright (c) 2009-2011 Stanford University
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
#include "BackupService.h"
#include "BindTransport.h"
#include "CoordinatorService.h"
#include "Log.h"
#include "MasterService.h"
#include "Memory.h"
#include "MockTransport.h"
#include "RecoverySegmentIterator.h"
#include "Rpc.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Unit tests for BackupService.
 */
class BackupServiceTest : public ::testing::Test {
  public:
    BackupService* backup;
    BackupClient* client;
    CoordinatorService* coordinatorService;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    BackupStorage* storage;
    BackupService::Config* config;
    BindTransport* transport;

    BackupServiceTest()
        : backup(NULL)
        , client(NULL)
        , coordinatorService(NULL)
        , segmentSize(1 << 10)
        , segmentFrames(4)
        , storage(NULL)
        , config(NULL)
        , transport(NULL)
    {
        config = new BackupService::Config();
        config->coordinatorLocator = "mock:host=coordinator";
        storage = new InMemoryStorage(segmentSize, segmentFrames);

        transport = new BindTransport();
        Context::get().transportManager->registerMock(transport);
        coordinatorService = new CoordinatorService();
        transport->addService(*coordinatorService, "mock:host=coordinator",
                    COORDINATOR_SERVICE);
        backup = new BackupService(*config, *storage);
        transport->addService(*backup, "mock:host=backup", BACKUP_SERVICE);
        backup->init();
        client =
            new BackupClient(Context::get().transportManager->getSession(
                                 "mock:host=backup"));
    }

    ~BackupServiceTest()
    {
        delete client;
        delete backup;
        delete coordinatorService;
        Context::get().transportManager->unregisterMock();
        delete transport;
        delete storage;
        delete config;
        EXPECT_EQ(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    uint32_t
    writeEntry(uint64_t masterId, uint64_t segmentId, LogEntryType type,
               uint32_t offset, const void *data, uint32_t bytes)
    {
        SegmentEntry entry(type, bytes);
        client->writeSegment(masterId, segmentId,
                             offset, &entry,
                             downCast<uint32_t>(sizeof(entry)));
        client->writeSegment(masterId, segmentId,
                             downCast<uint32_t>(offset + sizeof(entry)),
                             data, bytes);
        return downCast<uint32_t>(sizeof(entry)) + bytes;
    }

    uint32_t
    writeObject(uint64_t masterId, uint64_t segmentId,
                uint32_t offset, const char *data, uint32_t bytes,
                uint64_t tableId, uint64_t objectId)
    {
        char objectMem[sizeof(Object) + bytes];
        Object* obj = reinterpret_cast<Object*>(objectMem);
        memset(obj, 'A', sizeof(*obj));
        obj->id.objectId = objectId;
        obj->id.tableId = tableId;
        obj->version = 0;
        memcpy(objectMem + sizeof(*obj), data, bytes);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJ, offset,
                          objectMem, obj->objectLength(bytes));
    }

    uint32_t
    writeTombstone(uint64_t masterId, uint64_t segmentId,
                   uint32_t offset, uint64_t tableId, uint64_t objectId)
    {
        ObjectTombstone tombstone(segmentId, tableId, objectId, 0);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJTOMB, offset,
                          &tombstone, sizeof(tombstone));
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
        footer.checksum =
            static_cast<Segment::Checksum::ResultType>(0xff00ff00ff00);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGFOOTER, offset,
                          &footer, sizeof(footer));
    }

    void
    appendTablet(ProtoBuf::Tablets& tablets,
                    uint64_t partitionId,
                    uint32_t tableId,
                    uint64_t start, uint64_t end)
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_object_id(start);
        tablet.set_end_object_id(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
    }

    void
    createTabletList(ProtoBuf::Tablets& tablets)
    {
        // partition 0
        appendTablet(tablets, 0, 123, 0, 9);
        appendTablet(tablets, 0, 123, 10, 19);
        appendTablet(tablets, 0, 123, 20, 29);
        appendTablet(tablets, 0, 124, 20, 100);

        // partition 1
        appendTablet(tablets, 1, 123, 30, 39);
        appendTablet(tablets, 1, 125, 0, std::numeric_limits<uint64_t>::max());
    }

    static bool
    inMemoryStorageFreePred(string s)
    {
        return s == "free";
    }

    // Helper method for the LogDigest tests. This writes a proper Segment
    // with a LogDigest containing the given IDs.
    void
    writeDigestedSegment(uint64_t masterId, uint64_t segmentId,
        vector<uint64_t> digestIds)
    {
        void* segBuf = Memory::xmemalign(HERE, 1024 * 1024, 1024 * 1024);
        Segment s((uint64_t)0, segmentId, segBuf, 1024 * 1024);

        char digestBuf[LogDigest::getBytesFromCount
                            (downCast<uint32_t>(digestIds.size()))];
        LogDigest src(downCast<uint32_t>(digestIds.size()),
                        digestBuf,
                        downCast<uint32_t>(sizeof(digestBuf)));
        for (uint32_t i = 0; i < digestIds.size(); i++)
            src.addSegment(digestIds[i]);

        SegmentEntryHandle seh = s.append(LOG_ENTRY_TYPE_LOGDIGEST,
            digestBuf, downCast<uint32_t>(sizeof(digestBuf)));
        uint32_t segmentLength = seh->logTime().second + seh->totalLength();
        client->writeSegment(masterId, segmentId, 0, s.getBaseAddress(),
            segmentLength);

        free(segBuf);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServiceTest);
};


TEST_F(BackupServiceTest, findSegmentInfo) {
    EXPECT_TRUE(NULL == backup->findSegmentInfo(99, 88));
    client->openSegment(99, 88);
    client->closeSegment(99, 88);
    BackupService::SegmentInfo* infop = backup->findSegmentInfo(99, 88);
    EXPECT_TRUE(infop != NULL);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, findSegmentInfo_notIn) {
    EXPECT_TRUE(NULL == backup->findSegmentInfo(99, 88));
}

TEST_F(BackupServiceTest, freeSegment) {
    client->openSegment(99, 88);
    client->writeSegment(99, 88, 10, "test", 4);
    client->closeSegment(99, 88);
    {
        TestLog::Enable _(&inMemoryStorageFreePred);
        client->freeSegment(99, 88);
        EXPECT_EQ("free: called", TestLog::get());
    }
    EXPECT_TRUE(NULL == backup->findSegmentInfo(99, 88));
    client->freeSegment(99, 88);
    EXPECT_TRUE(NULL == backup->findSegmentInfo(99, 88));
}

TEST_F(BackupServiceTest, freeSegment_stillOpen) {
    client->openSegment(99, 88);
    client->freeSegment(99, 88);
    EXPECT_TRUE(NULL == backup->findSegmentInfo(99, 88));
}

TEST_F(BackupServiceTest, getRecoveryData) {
    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    uint32_t offset = 0;
    client->openSegment(99, 88);
    offset = writeHeader(99, 88);
    // Objects
    // Barely in tablet
    offset += writeObject(99, 88, offset, "test1", 6, 123, 29);
    // Barely out of tablets
    offset += writeObject(99, 88, offset, "test2", 6, 123, 30);
    // In on other table
    offset += writeObject(99, 88, offset, "test3", 6, 124, 20);
    // Not in any table
    offset += writeObject(99, 88, offset, "test4", 6, 125, 20);
    // Tombstones
    // Barely in tablet
    offset += writeTombstone(99, 88, offset, 123, 29);
    // Barely out of tablets
    offset += writeTombstone(99, 88, offset, 123, 30);
    // In on other table
    offset += writeTombstone(99, 88, offset, 124, 20);
    // Not in any table
    offset += writeTombstone(99, 88, offset, 125, 20);
    offset += writeFooter(99, 88, offset);
    client->closeSegment(99, 88);
    BackupClient::StartReadingData::Result result;
    client->startReadingData(ServerId(99), tablets, &result);

    Buffer response;
    while (true) {
        try {
            BackupClient::GetRecoveryData(*client, 99, 88, 0, response)();
        } catch (const RetryException& e) {
            response.reset();
            continue;
        }
        break;
    }

    RecoverySegmentIterator it(
        response.getRange(0, response.getTotalLength()),
        response.getTotalLength());

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(123U, it.get<Object>()->id.tableId);
    EXPECT_EQ(29U, it.get<Object>()->id.objectId);
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(124U, it.get<Object>()->id.tableId);
    EXPECT_EQ(20U, it.get<Object>()->id.objectId);
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
    EXPECT_EQ(123U, it.get<ObjectTombstone>()->id.tableId);
    EXPECT_EQ(29U, it.get<ObjectTombstone>()->id.objectId);
    it.next();

    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
    EXPECT_EQ(124U, it.get<ObjectTombstone>()->id.tableId);
    EXPECT_EQ(20U, it.get<ObjectTombstone>()->id.objectId);
    it.next();

    EXPECT_TRUE(it.isDone());
}

TEST_F(BackupServiceTest, getRecoveryData_moreThanOneSegmentStored) {
    uint32_t offset = 0;
    client->openSegment(99, 87);
    offset = writeHeader(99, 87);
    offset += writeObject(99, 87, offset, "test1", 6, 123, 9);
    offset += writeFooter(99, 87, offset);
    client->closeSegment(99, 87);

    client->openSegment(99, 88);
    offset = writeHeader(99, 88);
    offset += writeObject(99, 88, offset, "test2", 6, 123, 10);
    offset += writeFooter(99, 88, offset);
    client->closeSegment(99, 88);

    ProtoBuf::Tablets tablets;
    createTabletList(tablets);

    BackupClient::StartReadingData::Result result;
    client->startReadingData(ServerId(99), tablets, &result);

    {
        Buffer response;
        while (true) {
            try {
                BackupClient::GetRecoveryData(*client, 99, 88, 0,
                                                response)();
            } catch (const RetryException& e) {
                response.reset();
                continue;
            }
            break;
        }

        RecoverySegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_STREQ("test2", static_cast<const Object*>(it.getPointer())->
                           data);
        it.next();
        EXPECT_TRUE(it.isDone());
    }
    {
        Buffer response;
        while (true) {
            try {
                BackupClient::GetRecoveryData(*client, 99, 87, 0,
                                                response)();
            } catch (const RetryException& e) {
                response.reset();
                continue;
            }
            break;
        }

        RecoverySegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());
        EXPECT_FALSE(it.isDone());
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
        EXPECT_STREQ("test1", static_cast<const Object*>(it.getPointer())->
                           data);
        it.next();
        EXPECT_TRUE(it.isDone());
    }

    client->freeSegment(99, 87);
    client->freeSegment(99, 88);
}

TEST_F(BackupServiceTest, getRecoveryData_malformedSegment) {
    client->openSegment(99, 88);
    client->closeSegment(99, 88);

    BackupClient::StartReadingData::Result result;
    client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);

    while (true) {
        Buffer response;
        BackupClient::GetRecoveryData cont(*client, 99, 88,
                                            0, response);
        EXPECT_THROW(
            try {
                cont();
            } catch (const RetryException& e) {
                continue;
            },
            SegmentRecoveryFailedException);
        break;
    }

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, getRecoveryData_notRecovered) {
    uint32_t offset = 0;
    client->openSegment(99, 88);
    offset += writeHeader(99, 88);
    offset += writeObject(99, 88, offset, "test2", 6, 123, 10);
    offset += writeFooter(99, 88, offset);
    Buffer response;

    BackupClient::GetRecoveryData cont(*client, 99, 88,
                                        0, response);
    EXPECT_THROW(cont(), BackupBadSegmentIdException);

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, recoverySegmentBuilder) {
    uint32_t offset = 0;
    client->openSegment(99, 87);
    offset = writeHeader(99, 87);
    offset += writeObject(99, 87, offset, "test1", 6, 123, 9);
    offset += writeFooter(99, 87, offset);
    client->closeSegment(99, 87);

    client->openSegment(99, 88);
    offset = writeHeader(99, 88);
    offset += writeObject(99, 88, offset, "test2", 6, 123, 30);
    offset += writeFooter(99, 88, offset);
    client->closeSegment(99, 88);

    vector<BackupService::SegmentInfo*> toBuild;
    auto info = backup->findSegmentInfo(99, 87);
    EXPECT_TRUE(NULL != info);
    info->setRecovering();
    info->startLoading();
    toBuild.push_back(info);
    info = backup->findSegmentInfo(99, 88);
    EXPECT_TRUE(NULL != info);
    info->setRecovering();
    info->startLoading();
    toBuild.push_back(info);

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    AtomicInt recoveryThreadCount{0};
    BackupService::RecoverySegmentBuilder builder(Context::get(),
                                                  toBuild,
                                                  partitions,
                                                  recoveryThreadCount);
    builder();

    EXPECT_EQ(BackupService::SegmentInfo::RECOVERING,
                            toBuild[0]->state);
    EXPECT_TRUE(NULL != toBuild[0]->recoverySegments);
    Buffer* buf = &toBuild[0]->recoverySegments[0];
    RecoverySegmentIterator it(buf->getRange(0, buf->getTotalLength()),
                                buf->getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_STREQ("test1", it.get<Object>()->data);
    it.next();
    EXPECT_TRUE(it.isDone());

    EXPECT_EQ(BackupService::SegmentInfo::RECOVERING,
              toBuild[1]->state);
    EXPECT_TRUE(NULL != toBuild[1]->recoverySegments);
    buf = &toBuild[1]->recoverySegments[1];
    RecoverySegmentIterator it2(buf->getRange(0, buf->getTotalLength()),
                                buf->getTotalLength());
    EXPECT_FALSE(it2.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it2.getType());
    EXPECT_STREQ("test2", it2.get<Object>()->data);
    it2.next();
    EXPECT_TRUE(it2.isDone());
}

TEST_F(BackupServiceTest, startReadingData) {
    MockRandom _(1);
    client->openSegment(99, 88);
    client->writeSegment(99, 88, 0, "test", 4);
    client->openSegment(99, 89);
    client->openSegment(99, 98, false);
    client->openSegment(99, 99, false);

    BackupClient::StartReadingData::Result result;
    client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
    EXPECT_EQ(4U, result.segmentIdAndLength.size());

    EXPECT_EQ(88U, result.segmentIdAndLength[0].first);
    EXPECT_EQ(4U, result.segmentIdAndLength[0].second);
    {
        BackupService::SegmentInfo& info = *backup->findSegmentInfo(99, 88);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
    }

    EXPECT_EQ(89U, result.segmentIdAndLength[1].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[1].second);
    {
        BackupService::SegmentInfo& info = *backup->findSegmentInfo(99, 89);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
    }

    EXPECT_EQ(98U, result.segmentIdAndLength[2].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[2].second);
    {
        BackupService::SegmentInfo& info = *backup->findSegmentInfo(99, 98);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
        EXPECT_TRUE(info.recoveryPartitions);
    }

    EXPECT_EQ(99U, result.segmentIdAndLength[3].first);
    EXPECT_EQ(0U, result.segmentIdAndLength[3].second);
    EXPECT_TRUE(backup->findSegmentInfo(99, 99)->recoveryPartitions);
    {
        BackupService::SegmentInfo& info = *backup->findSegmentInfo(99, 99);
        BackupService::SegmentInfo::Lock lock(info.mutex);
        EXPECT_EQ(BackupService::SegmentInfo::RECOVERING, info.state);
        EXPECT_TRUE(info.recoveryPartitions);
    }

    EXPECT_EQ(4, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, startReadingData_empty) {
    BackupClient::StartReadingData::Result result;
    client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
    EXPECT_EQ(0U, result.segmentIdAndLength.size());
    EXPECT_EQ(0U, result.logDigestBytes);
    EXPECT_TRUE(NULL == result.logDigestBuffer);
}

TEST_F(BackupServiceTest, startReadingData_logDigest_simple) {
    // ensure that we get the LogDigest back at all.
    client->openSegment(99, 88);
    {
        writeDigestedSegment(99, 88, { 0x3f17c2451f0cafUL });

        BackupClient::StartReadingData::Result result;
        client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
        EXPECT_EQ(LogDigest::getBytesFromCount(1),
            result.logDigestBytes);
        EXPECT_EQ(88U, result.logDigestSegmentId);
        EXPECT_EQ(52U, result.logDigestSegmentLen);
        LogDigest ld(result.logDigestBuffer, result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x3f17c2451f0cafUL, ld.getSegmentIds()[0]);
    }

    // add a newer Segment and check that we get its LogDigest instead.
    client->openSegment(99, 89);
    {
        writeDigestedSegment(99, 89, { 0x5d8ec445d537e15UL });

        BackupClient::StartReadingData::Result result;
        client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
        EXPECT_EQ(LogDigest::getBytesFromCount(1),
            result.logDigestBytes);
        EXPECT_EQ(89U, result.logDigestSegmentId);
        EXPECT_EQ(52U, result.logDigestSegmentLen);
        LogDigest ld(result.logDigestBuffer, result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x5d8ec445d537e15UL, ld.getSegmentIds()[0]);
    }
}

TEST_F(BackupServiceTest, startReadingData_logDigest_latest) {
    client->openSegment(99, 88);
    writeDigestedSegment(99, 88, { 0x39e874a1e85fcUL });

    client->openSegment(99, 89);
    writeDigestedSegment(99, 89, { 0xbe5fbc1e62af6UL });

    // close the new one. we should get the old one now.
    client->closeSegment(99, 89);
    {
        BackupClient::StartReadingData::Result result;
        client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
        EXPECT_EQ(88U, result.logDigestSegmentId);
        EXPECT_EQ(52U, result.logDigestSegmentLen);
        EXPECT_EQ(LogDigest::getBytesFromCount(1),
            result.logDigestBytes);
        LogDigest ld(result.logDigestBuffer, result.logDigestBytes);
        EXPECT_EQ(1, ld.getSegmentCount());
        EXPECT_EQ(0x39e874a1e85fcUL, ld.getSegmentIds()[0]);
    }
}

TEST_F(BackupServiceTest, startReadingData_logDigest_none) {
    // closed segments don't count.
    client->openSegment(99, 88);
    writeDigestedSegment(99, 88, { 0xe966e17be4aUL });

    client->closeSegment(99, 88);
    {
        BackupClient::StartReadingData::Result result;
        client->startReadingData(ServerId(99), ProtoBuf::Tablets(), &result);
        EXPECT_EQ(1U, result.segmentIdAndLength.size());
        EXPECT_EQ(0U, result.logDigestBytes);
        EXPECT_TRUE(NULL == result.logDigestBuffer);
    }
}

TEST_F(BackupServiceTest, writeSegment) {
    client->openSegment(99, 88);
    // test for idempotence
    for (int i = 0; i < 2; ++i) {
        client->writeSegment(99, 88, 10, "test", 5);
        BackupService::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        EXPECT_TRUE(NULL != info.segment);
        EXPECT_STREQ("test", &info.segment[10]);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_segmentNotOpen) {
    EXPECT_THROW(
        client->writeSegment(99, 88, 0, "test", 4),
        BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_segmentClosed) {
    client->openSegment(99, 88);
    client->closeSegment(99, 88);
    EXPECT_THROW(
        client->writeSegment(99, 88, 0, "test", 4),
        BackupBadSegmentIdException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_segmentClosedRedundantClosingWrite) {
    client->openSegment(99, 88);
    client->closeSegment(99, 88);
    BackupClient::WriteSegment::WriteSegment(*client, 99, 88, 0, "test", 4,
                                                BackupWriteRpc::CLOSE)();
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffset) {
    client->openSegment(99, 88);
    EXPECT_THROW(
        client->writeSegment(99, 88, 500000, "test", 0),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badLength) {
    client->openSegment(99, 88);
    char junk[70000];
    EXPECT_THROW(
        client->writeSegment(99, 88, 0, junk,
                                downCast<uint32_t>(sizeof(junk))),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_badOffsetPlusLength) {
    client->openSegment(99, 88);
    char junk[50000];
    EXPECT_THROW(
        client->writeSegment(99, 88, 50000, junk, 50000),
        BackupSegmentOverflowException);
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(BackupServiceTest, writeSegment_closeSegment) {
    client->openSegment(99, 88);
    client->writeSegment(99, 88, 10, "test", 5);
    // loop to test for idempotence
    for (int i = 0; i > 2; ++i) {
        client->closeSegment(99, 88);
        BackupService::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        char* storageAddress =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        {
            BackupService::SegmentInfo::Lock lock(info.mutex);
            while (info.segment)
                info.condition.wait(lock);
        }
        EXPECT_TRUE(NULL != storageAddress);
        EXPECT_EQ("test", &storageAddress[10]);
        EXPECT_TRUE(NULL == static_cast<void*>(info.segment));
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_closeSegmentSegmentNotOpen) {
    EXPECT_THROW(client->closeSegment(99, 88),
                            BackupBadSegmentIdException);
}

TEST_F(BackupServiceTest, writeSegment_openSegment) {
    // loop to test for idempotence
    for (int i = 0; i < 2; ++i) {
        client->openSegment(99, 88);
        BackupService::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        EXPECT_TRUE(NULL != info.segment);
        EXPECT_EQ(0, *info.segment);
        EXPECT_TRUE(info.primary);
        char* address =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        EXPECT_TRUE(NULL != address);
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
}

TEST_F(BackupServiceTest, writeSegment_openSegmentSecondary) {
    client->openSegment(99, 88, false);
    BackupService::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
    EXPECT_TRUE(!info.primary);
}

TEST_F(BackupServiceTest, writeSegment_openSegmentOutOfStorage) {
    client->openSegment(99, 86);
    client->openSegment(99, 87);
    client->openSegment(99, 88);
    client->openSegment(99, 89);
    EXPECT_THROW(
        client->openSegment(99, 90),
        BackupStorageException);
    EXPECT_EQ(4, BackupStorage::Handle::getAllocatedHandlesCount());
}

class SegmentInfoTest : public ::testing::Test {
  public:
    typedef BackupService::SegmentInfo SegmentInfo;
    typedef BackupStorage::Handle Handle;
    SegmentInfoTest()
        : segmentSize(64 * 1024)
        , pool{segmentSize}
        , storage{segmentSize, 2}
        , ioScheduler()
        , ioThread(boost::ref(ioScheduler))
        , info{storage, pool, ioScheduler, 99, 88, segmentSize, true}
    {
    }

    ~SegmentInfoTest()
    {
        ioScheduler.shutdown(ioThread);
    }

    uint32_t segmentSize;
    BackupService::ThreadSafePool pool;
    InMemoryStorage storage;
    BackupService::IoScheduler ioScheduler;
    boost::thread ioThread;
    SegmentInfo info;
};

TEST_F(SegmentInfoTest, destructor) {
    TestLog::Enable _;
    {
        SegmentInfo info{storage, pool, ioScheduler, 99, 88, segmentSize, true};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
    EXPECT_EQ("~SegmentInfo: Backup shutting down with open segment <99,88>, "
              "closing out to storage", TestLog::get());
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    ASSERT_EQ(static_cast<char*>(NULL), info.segment);
}

TEST_F(SegmentInfoTest, destructorLoading) {
    {
        SegmentInfo info{storage, pool, ioScheduler, 99, 88, segmentSize, true};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
        info.close();
        info.startLoading();
    }
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
}

void
appendTablet(ProtoBuf::Tablets& tablets,
             uint64_t partitionId,
             uint32_t tableId,
             uint64_t start, uint64_t end)
{
    ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_object_id(start);
    tablet.set_end_object_id(end);
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(partitionId);
}

void
createTabletList(ProtoBuf::Tablets& tablets)
{
    appendTablet(tablets, 0, 123, 0, 9);
    appendTablet(tablets, 0, 123, 10, 19);
    appendTablet(tablets, 0, 123, 20, 29);
    appendTablet(tablets, 0, 124, 20, 100);
    appendTablet(tablets, 1, 123, 30, 39);
    appendTablet(tablets, 1, 125, 0, std::numeric_limits<uint64_t>::max());
}

TEST_F(SegmentInfoTest, appendRecoverySegment) {
    info.open();
    Segment segment(123, 88, info.segment, segmentSize);

    SegmentHeader header = { 99, 88, segmentSize };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    Object object(sizeof(object));
    object.id.objectId = 10;
    object.id.tableId = 123;
    object.version = 0;
    segment.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));

    segment.close();
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Status status = info.appendRecoverySegment(0, buffer);
    ASSERT_EQ(STATUS_OK, status);
    RecoverySegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                                 buffer.getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(sizeof(Object), it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentSecondarySegment) {
    SegmentInfo info{storage, pool, ioScheduler, 99, 88, segmentSize, false};
    info.open();
    Segment segment(123, 88, info.segment, segmentSize);

    SegmentHeader header = { 99, 88, segmentSize };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    Object object(sizeof(object));
    object.id.objectId = 10;
    object.id.tableId = 123;
    object.version = 0;
    segment.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));

    segment.close();
    info.close();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.setRecovering(partitions);

    Buffer buffer;
    while (true) {
        Status status = info.appendRecoverySegment(0, buffer);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    buffer.reset();
    while (true) {
        Status status = info.appendRecoverySegment(0, buffer);
        if (status == STATUS_RETRY) {
            buffer.reset();
            continue;
        }
        ASSERT_EQ(status, STATUS_OK);
        break;
    }
    RecoverySegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                                 buffer.getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(sizeof(Object), it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentMalformedSegment) {
    info.open();
    memcpy(info.segment, "garbage", 7);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);
    info.buildRecoverySegments(partitions);

    Buffer buffer;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 SegmentRecoveryFailedException);
    EXPECT_EQ(STATUS_OK, status);
}

TEST_F(SegmentInfoTest, appendRecoverySegmentNotYetRecovered) {
    Buffer buffer;
    TestLog::Enable _;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for segment <99,88> which isn't "
              "recovering", TestLog::get());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentPartitionOutOfBounds) {
    info.open();
    Segment segment(123, 88, info.segment, segmentSize);
    segment.close();
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    info.buildRecoverySegments(partitions);

    EXPECT_EQ(0u, info.recoverySegmentsLength);
    Buffer buffer;
    TestLog::Enable _;
    Status status;
    EXPECT_THROW(status = info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ("appendRecoverySegment: Asked for recovery segment 0 from "
              "segment <99,88> but there are only 0 partitions",
              TestLog::get());
}

TEST_F(SegmentInfoTest, whichPartition) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    Object object(sizeof(object));
    object.id.objectId = 10;
    object.id.tableId = 123;
    object.version = 0;

    auto r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(0u, *r);

    object.id.objectId = 30;
    r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(1u, *r);

    TestLog::Enable _;
    object.id.objectId = 40;
    r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_FALSE(r);
    EXPECT_EQ("whichPartition: Couldn't place object <123,40> into any of the "
              "given tablets for recovery; hopefully it belonged to a deleted "
              "tablet or lives in another log now", TestLog::get());
}

TEST_F(SegmentInfoTest, buildRecoverySegment) {
    info.open();
    Segment segment(123, 88, info.segment, segmentSize);

    SegmentHeader header = { 99, 88, segmentSize };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    Object object(sizeof(object));
    object.id.objectId = 10;
    object.id.tableId = 123;
    object.version = 0;
    segment.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));

    segment.close();
    info.close();
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions);

    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(2u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
    EXPECT_EQ(sizeof(object) + sizeof(SegmentEntry),
              info.recoverySegments[0].getTotalLength());
    EXPECT_EQ(0u,
              info.recoverySegments[1].getTotalLength());
}

TEST_F(SegmentInfoTest, buildRecoverySegmentMalformedSegment) {
    info.open();
    memcpy(info.segment, "garbage", 7);
    info.setRecovering();
    info.startLoading();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions);
    EXPECT_TRUE(info.recoveryException);
    EXPECT_FALSE(info.recoverySegments);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
}

TEST_F(SegmentInfoTest, buildRecoverySegmentNoTablets) {
    info.open();
    Segment segment(123, 88, info.segment, segmentSize);
    segment.close();
    info.setRecovering();
    info.startLoading();
    info.buildRecoverySegments(ProtoBuf::Tablets());
    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
}

TEST_F(SegmentInfoTest, close) {
    info.open();
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
    ASSERT_TRUE(pool.is_from(info.segment));
    const char* magic = "kitties!";
    snprintf(info.segment, segmentSize, "%s", magic);

    info.close();
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
    {
        // wait for the store op to complete
        SegmentInfo::Lock lock(info.mutex);
        info.waitForOngoingOps(lock);
    }
    EXPECT_FALSE(pool.is_from(info.segment));

    char seg[segmentSize];
    storage.getSegment(info.storageHandle, seg);
    EXPECT_STREQ(magic, seg);

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(SegmentInfoTest, closeWhileNotOpen) {
    EXPECT_THROW(info.close(), BackupBadSegmentIdException);
}

TEST_F(SegmentInfoTest, free) {
    info.open();
    info.close();
    {
        // wait for the store op to complete
        SegmentInfo::Lock lock(info.mutex);
        info.waitForOngoingOps(lock);
    }
    EXPECT_FALSE(info.inMemory());
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    info.free();
    EXPECT_FALSE(pool.is_from(info.segment));
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    EXPECT_EQ(SegmentInfo::FREED, info.state);
}

TEST_F(SegmentInfoTest, freeRecoveringSecondary) {
    SegmentInfo info{storage, pool, ioScheduler, 99, 88, segmentSize, false};
    info.open();
    info.close();
    info.setRecovering(ProtoBuf::Tablets());
    info.free();
    EXPECT_FALSE(pool.is_from(info.segment));
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    EXPECT_EQ(SegmentInfo::FREED, info.state);
}

TEST_F(SegmentInfoTest, open) {
    info.open();
    ASSERT_NE(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ('\0', info.segment[0]);
    EXPECT_NE(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
}

TEST_F(SegmentInfoTest, openStorageAllocationFailure) {
    InMemoryStorage storage{segmentSize, 0};
    SegmentInfo info{storage, pool, ioScheduler, 99, 88, segmentSize, true};
    EXPECT_THROW(info.open(), BackupStorageException);
    ASSERT_EQ(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::UNINIT, info.state);
}

TEST_F(SegmentInfoTest, startLoading) {
    info.open();
    info.close();
    info.startLoading();
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
}

} // namespace RAMCloud
