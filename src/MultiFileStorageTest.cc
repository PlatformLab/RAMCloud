/* Copyright (c) 2010-2016 Stanford University
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
#include "BackupMasterRecovery.h"
#include "MultiFileStorage.h"
#include "StringUtil.h"

namespace RAMCloud {

class MultiFileStorageTest : public ::testing::Test {
  public:
    typedef char* bytes;
    enum { BLOCK_SIZE = MultiFileStorage::BLOCK_SIZE };
    enum { METADATA_SIZE = MultiFileStorage::METADATA_SIZE };
    typedef MultiFileStorage::Frame Frame;

    const char* test;
    uint32_t testLength;
    Buffer testSource;
    uint32_t segmentFrames;
    uint32_t segmentSize;
    std::vector<MultiFileStorage*> storages;
    Tub<MultiFileStorage> storage1;
    Tub<MultiFileStorage> storage2;
    Tub<MultiFileStorage> storage3;
    const char* filePath1;
    const char* filePath21;
    const char* filePath22;
    const char* filePath31;
    const char* filePath32;
    const char* filePath33;
    mode_t oldUmask;

    MultiFileStorageTest()
        : test("test")
        , testLength(downCast<uint32_t>(strlen(test)))
        , testSource()
        , segmentFrames(4)
        // Needed when testing with O_DIRECT
        , segmentSize(BLOCK_SIZE * 4)
        , storages()
        , storage1()
        , storage2()
        , storage3()
        , filePath1("/tmp/ramcloud-backup-storage-test-delete-this-1-1")
        , filePath21("/tmp/ramcloud-backup-storage-test-delete-this-2-1")
        , filePath22("/tmp/ramcloud-backup-storage-test-delete-this-2-2")
        , filePath31("/tmp/ramcloud-backup-storage-test-delete-this-3-1")
        , filePath32("/tmp/ramcloud-backup-storage-test-delete-this-3-2")
        , filePath33("/tmp/ramcloud-backup-storage-test-delete-this-3-3")
        , oldUmask(umask(0))
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        testSource.appendExternal(test, testLength + 1);

        storage1.construct(segmentSize, segmentFrames, 0, segmentFrames,
                                 filePath1, O_DIRECT | O_SYNC);
        std::string twoFiles = std::string(filePath21) + "," + filePath22;
        storage2.construct(segmentSize, segmentFrames, 0, segmentFrames,
                                  twoFiles.c_str(), O_DIRECT | O_SYNC);
        std::string threeFiles = std::string(filePath31) + "," + filePath32
                                 + "," + filePath33;
        storage3.construct(segmentSize, segmentFrames, 0, segmentFrames,
                                    threeFiles.c_str(), O_DIRECT | O_SYNC);
        storages.push_back(storage1.get());
        storages.push_back(storage2.get());
        storages.push_back(storage3.get());

        Frame::testingSkipRealIo = true;
    }

    ~MultiFileStorageTest()
    {
        Frame::testingSkipRealIo = false;
        umask(oldUmask);
        unlink(filePath1);
        unlink(filePath21);
        unlink(filePath22);
        unlink(filePath31);
        unlink(filePath32);
        unlink(filePath33);
    }

    /**
     * This method will generate fake data for a segment replica and
     * write it to the disk file used for storage. Metadata will also
     * be written for the replica
     * \param frameIndex
     *      Index of the frame in which the replicas to be stored.
     * \param length
     *      Number of bytes of data in the replica (not including metadata).
     * \param logId
     *      Identifier for the log (master id).
     * \param segmentId
     *      Identifier for the segment within the log.
     * \param closed
     *      True means the replica had been closed.
     * \param primary
     *      True means the replica is the primary replica for its segment.
     * @return
     */
    void
    writeReplica(size_t frameIndex, int length, uint64_t logId,
            uint64_t segmentId, bool closed, bool primary)
    {
        Buffer data;

        // Write the replica data.
        TestUtil::fillLargeBuffer(&data, length);
        FILE *f = fopen(filePath1, "r+");
        off_t offset = storage1->offsetOfFramelet(frameIndex);
        fseek(f, offset, SEEK_SET);
        data.write(0, length, f);

        // Next, write the corresponding metadata.
        SegmentCertificate certificate;
        certificate.segmentLength = length;
        BackupReplicaMetadata metadata(certificate, logId, segmentId,
                segmentSize, 0, closed, primary);
        offset = storage1->offsetOfFrameMetadata(frameIndex);
        fseek(f, offset, SEEK_SET);
        fwrite(&metadata, 1, sizeof(metadata), f);
        fclose(f);
    }

    DISALLOW_COPY_AND_ASSIGN(MultiFileStorageTest);
};

TEST_F(MultiFileStorageTest, Frame_loadMetadata) {
    Frame::testingSkipRealIo = false;
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 0, 0, test, testLength + 1);
    frame->free();
    frame->loadMetadata();
    char* metadata = bytes(const_cast<void*>(frame->getMetadata()));
    EXPECT_STREQ(test, metadata);
}

TEST_F(MultiFileStorageTest, Frame_startLoading) {
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->startLoading();
    EXPECT_TRUE(frame->loadRequested);
    EXPECT_FALSE(frame->isScheduled());
    frame->deschedule();
    frame->buffer.reset();
    EXPECT_TRUE(frame->loadRequested);
    EXPECT_FALSE(frame->isScheduled());
}

TEST_F(MultiFileStorageTest, Frame_load) {
    Frame::testingSkipRealIo = false;
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, testSource.size(),
                  0, test, testLength + 1);
    frame->close();
    EXPECT_FALSE(frame->buffer);
    EXPECT_TRUE(frame->isSynced());
    bytes(frame->appendedMetadata.get())[0] = '\0';
    char* replica = bytes(frame->load());
    EXPECT_TRUE(frame->loadRequested);
    EXPECT_TRUE(frame->testingHadToWaitForBufferOnLoad);
    EXPECT_FALSE(frame->testingHadToWaitForSyncOnLoad);
    EXPECT_STREQ(test, replica);
    // Make sure metadata wasn't loaded, should be as tweaked before load.
    char* metadata = bytes(const_cast<void*>(frame->getMetadata()));
    EXPECT_STREQ("", metadata);
}

TEST_F(MultiFileStorageTest, Frame_loadDirty) {
    Frame::testingSkipRealIo = false;
    // stutsman: This test races. It can be easily made so the race causes the
    // check to pass but be incomplete, but I've flipped it so the if the race
    // doesn't cover the properties I want then it fails. File a bug on it if
    // this test causes issues and I'll try to come up with a compromise.
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, testSource.size(),
                  0, test, testLength + 1);
    EXPECT_TRUE(frame->buffer);
    char* replica = bytes(frame->load());
    EXPECT_TRUE(frame->loadRequested);
    EXPECT_FALSE(frame->testingHadToWaitForBufferOnLoad);
    EXPECT_TRUE(frame->testingHadToWaitForSyncOnLoad);
    EXPECT_TRUE(frame->isSynced());
    EXPECT_STREQ(test, replica);

    // Hack to force an additional disk read to make sure that it is really
    // on disk after a load.
    frame->buffer.reset();
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->performRead(lock);
    }
    replica = bytes(frame->load());
    EXPECT_STREQ(test, replica);
}

TEST_F(MultiFileStorageTest, Frame_appendNotOpen) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 0, 0, NULL, 0);
    frame->close();
    EXPECT_THROW(frame->append(testSource, 0, 0, 0, NULL, 0),
                 BackupBadSegmentIdException);
}

TEST_F(MultiFileStorageTest, Frame_appendLoading) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 0, 0, NULL, 0);
    frame->load();
    EXPECT_THROW(frame->append(testSource, 0, 0, 0, NULL, 0),
                 BackupBadSegmentIdException);
}

TEST_F(MultiFileStorageTest, Frame_appendOutOfBounds) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 1, segmentSize - 1, NULL, 0);
    EXPECT_THROW(frame->append(testSource, 0, 1, segmentSize, NULL, 0),
                 BackupSegmentOverflowException);
}

TEST_F(MultiFileStorageTest, Frame_appendMetadataTooBig) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 0, 0 , NULL, MultiFileStorage::METADATA_SIZE);
    EXPECT_THROW(frame->append(testSource, 0, 0, 0 ,
                               NULL, MultiFileStorage::METADATA_SIZE + 1),
                 BackupSegmentOverflowException);
}

TEST_F(MultiFileStorageTest, Frame_appendOrderIndependence) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    storage1->ioQueue.halt();
    Buffer source;
    source.appendExternal("0123456789", 10);
    frame->append(source, 6, 4, 6, test, testLength + 1);
    frame->append(source, 0, 6, 0, test, testLength + 1);
    EXPECT_EQ(10LU, frame->appendedLength);
    string data(static_cast<char*>(frame->buffer.get()), 10);
    EXPECT_EQ("0123456789", data);
}

TEST_F(MultiFileStorageTest, Frame_appendSync) {
    Frame::testingSkipRealIo = false;
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    EXPECT_TRUE(frame->isSynced());

    // Force a read from disk.
    frame->buffer.reset();
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->loadRequested = true;
        frame->performRead(lock);
    }
    char* replica = bytes(frame->load());
    EXPECT_STREQ(test, replica);
    char* metadata = bytes(const_cast<void*>(frame->getMetadata()));
    EXPECT_STREQ(test, metadata);
}

TEST_F(MultiFileStorageTest, Frame_appendNothingAdded) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 0, 0, NULL, 0);
    EXPECT_EQ(0lu, frame->appendedLength);
    EXPECT_EQ(0lu, frame->appendedMetadataVersion);
    EXPECT_FALSE(frame->isScheduled());
}

TEST_F(MultiFileStorageTest, Frame_appendOnlyNewMetadata) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    storage1->ioQueue.halt();
    frame->append(testSource, 0, 0, 0, test, testLength + 1);
    EXPECT_EQ(0lu, frame->appendedLength);
    EXPECT_EQ(1lu, frame->appendedMetadataVersion);
    EXPECT_TRUE(frame->isScheduled());
}

TEST_F(MultiFileStorageTest, Frame_appendIdempotence) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    storage1->ioQueue.halt();
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    EXPECT_EQ(5lu, frame->appendedLength);
    EXPECT_EQ(1lu, frame->appendedMetadataVersion);
    EXPECT_TRUE(frame->isScheduled());
    storage1->ioQueue.start();
    while (!frame->isSynced());
    EXPECT_FALSE(frame->isScheduled());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    EXPECT_EQ(5lu, frame->appendedLength);
    EXPECT_EQ(2lu, frame->appendedMetadataVersion);
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    EXPECT_EQ(5lu, frame->appendedLength);
    EXPECT_EQ(3lu, frame->appendedMetadataVersion);
}

TEST_F(MultiFileStorageTest, Frame_close) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->close();
    EXPECT_FALSE(frame->isOpen);
    EXPECT_TRUE(frame->isClosed);
}

TEST_F(MultiFileStorageTest, Frame_closeSync) {
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->close();
    EXPECT_FALSE(frame->isOpen);
    EXPECT_TRUE(frame->isClosed);
    EXPECT_FALSE(frame->buffer);
    EXPECT_TRUE(frame->isSynced());
    EXPECT_EQ(0lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, Frame_closeSyncNotWriteBuffer) {
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->isWriteBuffer = false;
    frame->close();
    EXPECT_EQ(1lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, Frame_closeLoading) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->load();
    EXPECT_THROW(frame->close(),
                 BackupBadSegmentIdException);
}

TEST_F(MultiFileStorageTest, Frame_free) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->free();

    EXPECT_TRUE(frame->isSynced());
    EXPECT_FALSE(frame->isOpen);
    EXPECT_FALSE(frame->isClosed);
    EXPECT_FALSE(frame->loadRequested);
    EXPECT_FALSE(frame->buffer);
    EXPECT_EQ(1, storage1->freeMap[0]);
    EXPECT_EQ(0lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, Frame_freeNotWriteBuffer) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->isWriteBuffer = false;
    frame->free();

    EXPECT_EQ(1lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, Frame_reopen) {
    Frame::testingSkipRealIo = false;
    writeReplica(2, 50, 99LU, 1000LU, false, true);
    std::vector<BackupStorage::FrameRef> allFrames =
            storage1->loadAllMetadata();
    Frame* frame = static_cast<Frame*>(allFrames[2].get());
    const BackupReplicaMetadata* metadata =
            static_cast<const BackupReplicaMetadata*>(frame->getMetadata());
    frame->reopen(metadata->certificate.segmentLength);

    EXPECT_EQ(50LU, frame->appendedLength);
    EXPECT_EQ(50LU, frame->committedLength);
    EXPECT_TRUE(frame->isOpen);
    EXPECT_FALSE(frame->isClosed);
    EXPECT_FALSE(frame->loadRequested);
    EXPECT_TRUE(frame->buffer);
    EXPECT_STREQ("word 1, word 2, word 3, word 4, word 5; word 6, wo",
            static_cast<char*>(frame->buffer.get()));

    // Make sure we can successfully append to this frame.
    Buffer source;
    source.appendExternal("0123456789", 10);
    frame->append(source, 4, 6, 50, NULL, 0);
    EXPECT_EQ(56LU, frame->appendedLength);
    EXPECT_EQ(50LU, frame->committedLength);
    EXPECT_STREQ("word 1, word 2, word 3, word 4, word 5; word 6, wo456789",
            static_cast<char*>(frame->buffer.get()));
}

TEST_F(MultiFileStorageTest, Frame_open) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    EXPECT_TRUE(frame->buffer);
    EXPECT_TRUE(frame->isOpen);
    EXPECT_FALSE(frame->isClosed);
    EXPECT_FALSE(frame->sync);
    EXPECT_EQ(1lu, storage1->writeBuffersInUse);

    frame->open(true, ServerId(), 0);
    EXPECT_FALSE(frame->sync);

    frame->close();
    frame->open(true, ServerId(), 0);
    EXPECT_FALSE(frame->sync);

    frame->load();
    frame->open(true, ServerId(), 0);
    EXPECT_FALSE(frame->sync);

    frame->free();
    frame->open(true, ServerId(), 0);
    EXPECT_TRUE(frame->sync);
    EXPECT_EQ(1lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, unlockedWrite) {
    // This test also implicitly tests unlockedRead.
    Frame::testingSkipRealIo = false;
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    while (!frame->isSynced());

    // Force a read from disk.
    frame->buffer.reset();
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->loadRequested = true;
        frame->performRead(lock);
    }
    char* replica = bytes(frame->load());
    EXPECT_STREQ(test, replica);
    char* metadata = bytes(const_cast<void*>(frame->getMetadata()));
    EXPECT_STREQ(test, metadata);
}

TEST_F(MultiFileStorageTest, unlockedWriteWholeSegment) {
    // This test also implicitly tests unlockedRead.
    Memory::unique_ptr_free data(
        Memory::xmemalign(HERE, getpagesize(), segmentSize),
        std::free);
    memset(data.get(), 'x', segmentSize - 1);
    static_cast<char*>(data.get())[segmentSize - 1] = '\0';

    size_t metadataLen = storage1->getMetadataSize();
    Memory::unique_ptr_free metadata(
        Memory::xmemalign(HERE, getpagesize(), metadataLen),
        std::free);
    memset(metadata.get(), 'y', metadataLen - 1);
    static_cast<char*>(data.get())[metadataLen - 1] = '\0';

    Buffer source;
    source.appendExternal(data.get(), segmentSize);

    Frame::testingSkipRealIo = false;

    for (size_t i = 0; i < storages.size(); i++) {
        BackupStorage::FrameRef frameRef = storages[i]->open(false,
                ServerId(), 0);
        Frame* frame = static_cast<Frame*>(frameRef.get());
        frame->append(source, 0, segmentSize, 0, metadata.get(), metadataLen);
        while (!frame->isSynced());

        // Force a read from disk.
        frame->buffer.reset();
        {
            Frame::Lock lock(frame->storage->mutex);
            frame->loadRequested = true;
            frame->performRead(lock);
        }
        char* replica = bytes(frame->load());
        EXPECT_STREQ(bytes(data.get()), replica);
        char* metadataRead = bytes(const_cast<void*>(frame->getMetadata()));
        EXPECT_STREQ(bytes(metadata.get()), metadataRead);
    }
}

TEST_F(MultiFileStorageTest, unlockedWriteMiddleOfSegment) {
    // This test also implicitly tests unlockedRead.
    size_t dataLen1 = BLOCK_SIZE + BLOCK_SIZE / 2;
    size_t dataLen2 = BLOCK_SIZE;
    Memory::unique_ptr_free data(
        Memory::xmemalign(HERE, getpagesize(), segmentSize),
        std::free);
    memset(data.get(), 'x', dataLen1);
    memset(static_cast<char*>(data.get()) + dataLen1, 'y', dataLen2);
    static_cast<char*>(data.get())[dataLen1 + dataLen2] = '\0';

    size_t metadataLen = storage1->getMetadataSize();
    Memory::unique_ptr_free metadata(
        Memory::xmemalign(HERE, getpagesize(), metadataLen),
        std::free);
    memset(metadata.get(), 'z', metadataLen - 1);
    static_cast<char*>(data.get())[metadataLen - 1] = '\0';

    Buffer source;
    source.appendExternal(data.get(), segmentSize);

    Frame::testingSkipRealIo = false;

    for (size_t i = 0; i < storages.size(); i++) {
        BackupStorage::FrameRef frameRef = storages[i]->open(false,
                ServerId(), 0);
        Frame* frame = static_cast<Frame*>(frameRef.get());
        frame->append(source, 0, dataLen1, 0, metadata.get(), metadataLen);
        storages[i]->quiesce();
        frame->append(source, dataLen1, dataLen2 + 1, dataLen1, NULL, 0);
        while (!frame->isSynced());

        // Force a read from disk.
        frame->buffer.reset();
        {
            Frame::Lock lock(frame->storage->mutex);
            frame->loadRequested = true;
            frame->performRead(lock);
        }
        char* replica = bytes(frame->load());
        EXPECT_STREQ(bytes(data.get()), replica);
        char* metadataRead = bytes(const_cast<void*>(frame->getMetadata()));
        EXPECT_STREQ(bytes(metadata.get()), metadataRead);
    }
}

TEST_F(MultiFileStorageTest, Frame_performWrite) {
    storage1->ioQueue.halt();
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    TestLog::Enable _;
    frame->deschedule();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 512 frameIndex 0",
              TestLog::get());
    EXPECT_FALSE(frame->isScheduled());
    EXPECT_EQ(5lu, frame->appendedLength);
    EXPECT_EQ(1lu, frame->appendedMetadataVersion);
    EXPECT_EQ(5lu, frame->committedLength);
    EXPECT_EQ(1lu, frame->committedMetadataVersion);
    EXPECT_TRUE(frame->buffer);
}

TEST_F(MultiFileStorageTest, Frame_performWriteReleasesBufferAtTheRightTimes) {
    storage1->ioQueue.halt();
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    frame->close();
    frame->loadRequested = true;
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->performWrite(lock);
    }
    EXPECT_TRUE(frame->buffer);
    EXPECT_EQ(1lu, storage1->writeBuffersInUse);
    frame->loadRequested = false;
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->performWrite(lock);
    }
    EXPECT_FALSE(frame->buffer);
    EXPECT_EQ(0lu, storage1->writeBuffersInUse);
}

TEST_F(MultiFileStorageTest, Frame_performWriteLoadWaiting) {
    storage1->ioQueue.halt();
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, 5, 0, test, testLength + 1);
    frame->startLoading();
    frame->deschedule();
    TestLog::Enable _;
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 512 frameIndex 0",
              TestLog::get());
    EXPECT_TRUE(frame->isScheduled());
}

TEST_F(MultiFileStorageTest, Frame_performWriteSmokeTestOffsets) {
    testSource.reset();
    char garbage[1024];
    testSource.appendExternal(garbage, sizeof(garbage));

    storage1->ioQueue.halt();
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    TestLog::Enable _;

    // Force !isSynced to get performTask() to call performWrite() by
    // touching metadata.
    frame->append(testSource, 0, 0, 0, test, 1);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 0 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 1, 0, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 512 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 510, 1, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 512 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 1, 511, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 0 count 512 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 512, 512, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 512 count 512 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 513, 1024, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 1024 count 1024 frameIndex 0",
              TestLog::get());

    frame->append(testSource, 0, 1, 1537, NULL, 0);
    frame->deschedule();
    TestLog::reset();
    frame->performTask();
    EXPECT_EQ("performWrite: sourceBufferOffset 1536 count 512 frameIndex 0",
              TestLog::get());
}

TEST_F(MultiFileStorageTest, Frame_performRead) {
    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->close();
    storage1->ioQueue.halt();

    EXPECT_FALSE(frame->buffer);
    TestLog::Enable _;
    {
        Frame::Lock lock(frame->storage->mutex);
        frame->loadRequested = true;
        frame->performRead(lock);
    }
    EXPECT_EQ("performRead: count 2048 frameIndex 0", TestLog::get());
    EXPECT_TRUE(frame->buffer);
}

TEST_F(MultiFileStorageTest, constructor) {
    struct stat s;
    stat(filePath1, &s);
    EXPECT_EQ(storage1->offsetOfFramelet(segmentFrames),
              uint32_t(s.st_size));
}

TEST_F(MultiFileStorageTest, openFails) {
    TestLog::Enable _;
    EXPECT_THROW(MultiFileStorage(segmentSize,
                                  segmentFrames,
                                  0,
                                  segmentFrames,
                                  "/dev/null/cantcreate", 0),
                            BackupStorageException);
    EXPECT_EQ("MultiFileStorage: Failed to open backup storage file "
              "/dev/null/cantcreate: Not a directory", TestLog::get());
}

TEST_F(MultiFileStorageTest, open) {
    BackupStorage::FrameRef frameRef = storage1->open(false,
            ServerId(1, 2), 123);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    EXPECT_EQ(0, storage1->freeMap[0]);
    EXPECT_EQ(0U, frame->frameIndex);
    EXPECT_EQ("1.2", frame->masterId.toString());
    EXPECT_EQ(123LU, frame->segmentId);
}

TEST_F(MultiFileStorageTest, open_ensureFifoUse) {
    for (uint32_t f = 0; f < segmentFrames; ++f) {
        BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
        Frame* frame = static_cast<Frame*>(frameRef.get());
        EXPECT_EQ(0, storage1->freeMap[f]);
        EXPECT_EQ(f, frame->frameIndex);
        frame->free();
    }

    BackupStorage::FrameRef frameRef = storage1->open(false, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    EXPECT_EQ(0, storage1->freeMap[0]);
    EXPECT_EQ(1, storage1->freeMap[1]);
    EXPECT_EQ(0U, frame->frameIndex);
    frame->free();
}

TEST_F(MultiFileStorageTest, open_noFreeFrames) {
    TestLog::Enable _;
    std::vector<BackupStorage::FrameRef> frames;
    for (uint32_t f = 0; f < segmentFrames; ++f)
        frames.push_back(storage1->open(false, ServerId(), 0));
    storage1->writeBuffersInUse = 0;
    EXPECT_THROW(storage1->open(false, ServerId(), 0),
                 BackupOpenRejectedException);
    EXPECT_EQ("open: Master tried to open a storage frame but there are no "
            "frames free (all 4 frames are in use); rejecting", TestLog::get());
}

TEST_F(MultiFileStorageTest, open_tooManyBuffersInUse) {
    TestLog::Enable _;
    storage1->writeBuffersInUse = storage1->maxWriteBuffers + 1;
    EXPECT_THROW(storage1->open(false, ServerId(), 0),
                 BackupOpenRejectedException);
}

TEST_F(MultiFileStorageTest, loadAllMetadata) {
    uint8_t ones[storage1->getMetadataSize()];
    memset(ones, 0xff, sizeof(ones));
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    Buffer empty;
    frame->append(empty, 0, 0, 0, ones, sizeof(ones));
    const uint8_t *metadata = static_cast<const uint8_t*>(frame->getMetadata());
    EXPECT_EQ(uint8_t(0xff), metadata[0]);
    frame->free();
    auto frames = storage1->loadAllMetadata();
    EXPECT_EQ(uint8_t(0), metadata[0]);
    EXPECT_EQ(metadata, frames[0]->getMetadata());
    EXPECT_EQ(storage1->frames.size(), frames.size());
}

TEST_F(MultiFileStorageTest, resetSuperblock) {
    for (uint32_t expectedVersion = 1; expectedVersion < 3; ++expectedVersion) {
        storage1->resetSuperblock({9999, expectedVersion}, "hasso");
        for (uint32_t frame = 0; frame < 2; ++frame) {
            auto superblock = storage1->tryLoadSuperblock(frame);
            ASSERT_TRUE(superblock);
            EXPECT_EQ(ServerId(9999, expectedVersion),
                      superblock->getServerId());
            EXPECT_STREQ("hasso", superblock->getClusterName());
            EXPECT_EQ(expectedVersion, superblock->version);
            EXPECT_EQ(expectedVersion, storage1->superblock.version);
            EXPECT_EQ(1u, storage1->lastSuperblockFrame);
        }
    }
}

struct WaitForQuiesce {
    explicit WaitForQuiesce(MultiFileStorage& storage)
        : storage(storage)
    {}
    void operator()() {
        RAMCLOUD_TEST_LOG("about to start");
        storage.quiesce();
    }
    MultiFileStorage& storage;
};

TEST_F(MultiFileStorageTest, quiesce)
{
    storage1->ioQueue.halt();
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    frame->append(testSource, 0, testSource.size(),
                  0, test, testLength + 1);
    WaitForQuiesce waitForQuiesce(*storage1);
    TestLog::Enable _;
    std::thread thread(waitForQuiesce);
    while (TestLog::get() == "");
    storage1->ioQueue.start();
    thread.join();
}

TEST_F(MultiFileStorageTest, fry)
{
    Frame::testingSkipRealIo = false;
    uint8_t ones[storage1->getMetadataSize()];
    memset(ones, 0xff, sizeof(ones));
    BackupStorage::FrameRef frameRef = storage1->open(true, ServerId(), 0);
    Frame* frame = static_cast<Frame*>(frameRef.get());
    Buffer empty;
    frame->append(empty, 0, 0, 0, ones, sizeof(ones));
    frame->loadMetadata();
    const uint8_t *metadata = static_cast<const uint8_t*>(frame->getMetadata());
    EXPECT_EQ(uint8_t(0xff), metadata[0]);
    storage1->fry();
    frame->loadMetadata();
    EXPECT_EQ(uint8_t(0), metadata[0]);
}

TEST_F(MultiFileStorageTest, loadSuperblockBothEqual) {
    storage1->resetSuperblock({9998, 1}, "gruuuu");
    auto superblock = storage1->loadSuperblock();
    EXPECT_TRUE(!memcmp(&superblock, &storage1->superblock,
                        sizeof(superblock)));
    EXPECT_EQ(ServerId(9998, 1), superblock.getServerId());
    EXPECT_STREQ("gruuuu", superblock.getClusterName());
    EXPECT_EQ(1u, superblock.version);
    EXPECT_EQ(0u, storage1->lastSuperblockFrame);
}

TEST_F(MultiFileStorageTest, loadSuperblockLeftGreater) {
    // "0x1" means skip writing superblock frame 0.
    storage1->resetSuperblock({9997, 2}, "fruuuu", 0x1);
    // "0x2" means skip writing superblock frame 1.
    storage1->resetSuperblock({9997, 1}, "gruuuu", 0x2);
    auto superblock = storage1->loadSuperblock();
    EXPECT_TRUE(!memcmp(&superblock, &storage1->superblock,
                        sizeof(superblock)));
    EXPECT_EQ(ServerId(9997, 1), superblock.getServerId());
    EXPECT_STREQ("gruuuu", superblock.getClusterName());
    EXPECT_EQ(2u, superblock.version);
    EXPECT_EQ(0u, storage1->lastSuperblockFrame);
}

TEST_F(MultiFileStorageTest, loadSuperblockRightGreater) {
    // "0x2" means skip writing superblock frame 1.
    storage1->resetSuperblock({9996, 2}, "fruuuu", 0x2);
    // "0x1" means skip writing superblock frame 0.
    storage1->resetSuperblock({9996, 1}, "gruuuu", 0x1);
    auto superblock = storage1->loadSuperblock();
    EXPECT_TRUE(!memcmp(&superblock, &storage1->superblock,
                        sizeof(superblock)));
    EXPECT_EQ(ServerId(9996, 1), superblock.getServerId());
    EXPECT_STREQ("gruuuu", superblock.getClusterName());
    EXPECT_EQ(2u, superblock.version);
    EXPECT_EQ(1u, storage1->lastSuperblockFrame);
}

namespace {
bool loadSuperblockFilter(string s) { return s == "loadSuperblock"; }
}

TEST_F(MultiFileStorageTest, loadSuperblockNoneFound) {
    TestLog::Enable _(loadSuperblockFilter);
    auto superblock = storage1->loadSuperblock();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
                "loadSuperblock: Backup couldn't find existing superblock;"));
    EXPECT_TRUE(!memcmp(&superblock, &storage1->superblock,
                        sizeof(superblock)));
    EXPECT_EQ(ServerId(), superblock.getServerId());
    EXPECT_STREQ("__unnamed__", superblock.getClusterName());
    EXPECT_EQ(0u, superblock.version);
    EXPECT_EQ(1u, storage1->lastSuperblockFrame);
}

TEST_F(MultiFileStorageTest, tryLoadSuperblock) {
    // "0x2" means skip writing superblock frame 1.
    storage1->resetSuperblock({9994, 1}, "fhqwhgads", 0x2);
    auto superblock = storage1->tryLoadSuperblock(0);
    ASSERT_TRUE(superblock);
    EXPECT_EQ(ServerId(9994, 1),
              superblock->getServerId());
    EXPECT_STREQ("fhqwhgads", superblock->getClusterName());
    EXPECT_EQ(1u, superblock->version);
    EXPECT_EQ(1u, storage1->superblock.version);
    EXPECT_EQ(1u, storage1->lastSuperblockFrame);

    TestLog::Enable _;
    superblock = storage1->tryLoadSuperblock(1);
    ASSERT_FALSE(superblock);
    EXPECT_EQ("tryLoadSuperblock: Stored superblock had a bad checksum: "
              "stored checksum was 0, but stored data had checksum 88a5c087",
              TestLog::get());
}

TEST_F(MultiFileStorageTest, tryLoadSuperblockCannotReadFile) {
    close(storage1->fds[0]);
    TestLog::Enable _;
    auto superblock = storage1->tryLoadSuperblock(0);
    ASSERT_FALSE(superblock);
    EXPECT_EQ("tryLoadSuperblock: Couldn't read superblock from superblock "
              "frame 0: Bad file descriptor",
              TestLog::get());
    // supress destructor error message on file close
    storage1->fds[0] = creat(filePath1, 0666);
}

namespace {
bool tryLoadSuperblockFilter(string s) { return s == "tryLoadSuperblock"; }
}

TEST_F(MultiFileStorageTest, tryLoadSuperblockBadChecksum) {
    TestLog::Enable _(tryLoadSuperblockFilter);
    storage1->resetSuperblock({9994, 1}, "fhqwhgads");
    Memory::unique_ptr_free buffer(
        Memory::xmemalign(HERE, getpagesize(), BLOCK_SIZE),
        std::free);
    ASSERT_EQ(BLOCK_SIZE,
              pread(storage1->fds[0], buffer.get(), BLOCK_SIZE, 0));
    static_cast<char*>(buffer.get())[0] = ' ';
    ASSERT_EQ(BLOCK_SIZE, pwrite(storage1->fds[0], buffer.get(),
                                 BLOCK_SIZE, 0));
    auto superblock = storage1->tryLoadSuperblock(0);
    ASSERT_FALSE(superblock);
    EXPECT_EQ("tryLoadSuperblock: Stored superblock had a bad checksum: "
              "stored checksum was 6c19c3f1, but stored data had checksum "
              "9d232a61", TestLog::get());
    superblock = storage1->tryLoadSuperblock(1);
    ASSERT_TRUE(superblock);
}

TEST_F(MultiFileStorageTest, offsetOfFramelet) {
    for (size_t i = 0; i < storages.size(); i++) {
        uint64_t offset = storages[i]->offsetOfFramelet(0);
        EXPECT_EQ(2lu * BLOCK_SIZE, offset);
        offset = storages[i]->offsetOfFramelet(1);
        // This line will cause failures if you test with more than 5 files
        size_t divider = segmentSize % (i + 1) == 0 ? i + 1 : i;
        EXPECT_EQ(2lu * BLOCK_SIZE + segmentSize/divider + METADATA_SIZE,
                  offset);
        offset = storages[i]->offsetOfFramelet(2);
        EXPECT_EQ(2lu * BLOCK_SIZE + 2 * (segmentSize/divider + METADATA_SIZE),
                  offset);
    }

    // Check for 32-bit overflow.
    storage1->segmentSize = 1 << 23;
    uint64_t offset = storage1->offsetOfFramelet(512);
    EXPECT_NE(0lu, offset);
    EXPECT_EQ(1024lu + 512lu * ((1lu << 23) + 512lu), offset);
}

} // namespace RAMCloud
