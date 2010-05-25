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

/**
 * \file
 * Unit tests for BackupServer.
 */

#include <BackupServer.h>

#include <backuprpc.h>
#include <Log.h>
#include <Server.h>
#include <MockTransport.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

namespace RAMCloud {

static const char *LOG_PATH = "/tmp/rctest.log";

/**
 * Unit tests for BackupServer.
 */
class BackupServerTest : public CppUnit::TestFixture {
    Service *service;
    MockTransport *transport;

    const std::string testMessage;
    uint32_t testMessageLen;
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest); // NOLINT

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_constructor_normal);
    CPPUNIT_TEST(test_constructor_badPath);
    CPPUNIT_TEST(test_writeSegment_badSegmentNumber);
    CPPUNIT_TEST(test_writeSegment_normal);
    CPPUNIT_TEST(test_writeSegment_dataTooLarge);
    CPPUNIT_TEST(test_writeSegment_offsetTooLarge);
    CPPUNIT_TEST(test_writeSegment_overflow);
    CPPUNIT_TEST(test_commitSegment_badSegmentNumber);
    CPPUNIT_TEST(test_commitSegment_normal);
    //CPPUNIT_TEST(test_flushSegment_noFreeFrames);
    CPPUNIT_TEST(test_flushSegment_normal);
    //CPPUNIT_TEST(test_flushSegment_logClosed);
    //CPPUNIT_TEST(test_commitSegment_quirky);
    CPPUNIT_TEST(test_commitSegment_commitWithoutWriteSegment);
    CPPUNIT_TEST(test_freeSegment_badSegmentNumber);
    CPPUNIT_TEST(test_freeSegment_nonexistantSegmentNumber);
    CPPUNIT_TEST(test_freeSegment_normal);
    CPPUNIT_TEST(test_freeSegment_previouslyFreeSegment);
    CPPUNIT_TEST(test_retrieveSegment_normal);
    CPPUNIT_TEST(test_retrieveSegment_badSegNum);
    CPPUNIT_TEST(test_getSegmentList_normal);
    CPPUNIT_TEST(test_getSegmentList_listSizeTooShort);
    CPPUNIT_TEST(test_extractMetadata_normal);
    CPPUNIT_TEST(test_getSegmentMetadata_segmentNotHere);
    CPPUNIT_TEST(test_getSegmentMetadata_noObjects);
    CPPUNIT_TEST(test_getSegmentMetadata_normal);
    CPPUNIT_TEST(test_getSegmentMetadata_nonexistentSegmentId);
    CPPUNIT_TEST(test_frameForSegNum_matchFound);
    CPPUNIT_TEST(test_frameForSegNum_noMatchFound);
    CPPUNIT_TEST_SUITE_END();

  public:
    BackupServerTest()
            : service(0), transport(0), testMessage("God hates ponies."),
              testMessageLen(0)
    {
        testMessageLen = static_cast<uint32_t>(testMessage.length());
    }

    void
    setUp()
    {
        service = new Service();
        transport = new MockTransport();
    }

    void
    tearDown()
    {
        unlink(LOG_PATH);
        delete service;
        delete transport;
    }

    // Construct a backup and check members to verify our
    // understanding
    void
    test_constructor_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        CPPUNIT_ASSERT(backup.logFD != -1);

        const int pagesize = getpagesize();
        CPPUNIT_ASSERT(backup.seg);
        CPPUNIT_ASSERT(!((uintptr_t)backup.seg & (pagesize - 1)));

        CPPUNIT_ASSERT_EQUAL(INVALID_SEGMENT_NUM, backup.openSegNum);

        for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
            CPPUNIT_ASSERT(backup.freeMap.get(i));
            CPPUNIT_ASSERT_EQUAL(INVALID_SEGMENT_NUM,
                                 backup.segmentAtFrame[i]);
        }
    }

    void
    test_constructor_badPath()
    {
        try {
            BackupServer backup(service, transport, "/god/hates/ponies/");
            CPPUNIT_ASSERT_MESSAGE("Backup construction should fail on "
                                   "non-existant directory for log path.",
                                   false);
            CPPUNIT_ASSERT(false);
        } catch (BackupLogIOException e) {}
    }

    // TODO(stutsman) There are some other things to test in the
    // constructor but they are only compile time selectable for now

    // Ensure INVALID_SEGMENT_NUM cannot be used
    void
    test_writeSegment_badSegmentNumber()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.writeSegment(INVALID_SEGMENT_NUM, 0, "test", 5);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    // Ensure that segments must be committed before new writeSegments
    // then check to see if data is correct from first writeSegment
    // TODO(stutsman) This single open segment condition should be
    // relaxed relatively early in development.
    void
    test_writeSegment_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.writeSegment(0, 0,
                                testMessage.c_str(), testMessageLen);
            backup.writeSegment(1, 0,
                                testMessage.c_str(), testMessageLen);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                backup.seg, testMessageLen));
    }

    void
    test_writeSegment_dataTooLarge()
    {
        BackupServer backup(service, transport, LOG_PATH);

        char buf[SEGMENT_SIZE + 256];
        try {
            backup.writeSegment(0, 0, buf, SEGMENT_SIZE + 256);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    void
    test_writeSegment_offsetTooLarge()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.writeSegment(0, SEGMENT_SIZE,
                         testMessage.c_str(), testMessageLen);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    // Test offset is ok but runs off the end
    void
    test_writeSegment_overflow()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.writeSegment(0, SEGMENT_SIZE - 2,
                                testMessage.c_str(), testMessageLen);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    // Make sure flush fails if we have no free segment frames
    void
    test_flushSegment_noFreeFrames()
    {
        BackupServer backup(service, transport, LOG_PATH);
        // TODO(stutsman) these is somekind of problem with clearing
        // all the values in the freeMap resulting in some kind of
        // exception
        backup.freeMap.clearAll();
        backup.writeSegment(12, 0,
                            testMessage.c_str(), testMessageLen);
        try {
            backup.flushSegment();
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_flushSegment_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(12, 0,
                            testMessage.c_str(), testMessageLen);
        backup.flushSegment();

        char buf[SEGMENT_SIZE];
        backup.retrieveSegment(12, &buf[0]);
        CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                &buf[0], testMessageLen));
    }

    // See what happens if the log is closed during operation
    void
    test_flushSegment_logClosed()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(12, 0,
                            testMessage.c_str(), testMessageLen);

        close(backup.logFD);

        try {
            backup.flushSegment();
            CPPUNIT_ASSERT(false);
        } catch (BackupLogIOException e) {}
    }

    void
    test_commitSegment_badSegmentNumber()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.commitSegment(INVALID_SEGMENT_NUM);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_commitSegment_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        // Simple test - should work
        char buf[SEGMENT_SIZE];
        uint64_t targetFrame = 0;
        for (uint64_t seg = 20; seg < 24; seg++) {
            backup.writeSegment(seg, 0,
                                testMessage.c_str(), testMessageLen);
            backup.commitSegment(seg);
            // Check to make sure it's in the right slot
            CPPUNIT_ASSERT_EQUAL(seg, backup.segmentAtFrame[targetFrame]);
            targetFrame++;

            backup.retrieveSegment(seg, &buf[0]);
            CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                    &buf[0], testMessageLen));
        }

    }

    void
    test_commitSegment_quirky()
    {
        BackupServer backup(service, transport, LOG_PATH);

        // TODO(stutsman) Finish me
        CPPUNIT_ASSERT(false);

        backup.freeMap.clear(63);
        backup.freeMap.clear(64);
        // Test what happens when we run out of seg frames
        //for (uint64_t i = 0; i < SEGMENT_FRAMES - 1; i++)
        for (int64_t i = 0; i < 64; i++)
            backup.freeMap.clear(i);
        backup.writeSegment(0, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(0);
        try {
            backup.writeSegment(1, 0,
                                testMessage.c_str(), testMessageLen);
            backup.commitSegment(1);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_commitSegment_commitWithoutWriteSegment()
    {
        BackupServer backup(service, transport, LOG_PATH);

        // Try to commit a segment that isn't open
        try {
            backup.commitSegment(0);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_badSegmentNumber()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.freeSegment(INVALID_SEGMENT_NUM);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_nonexistantSegmentNumber()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.freeSegment(77);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);
        backup.freeSegment(76);
    }

    void
    test_freeSegment_previouslyFreeSegment()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);
        backup.freeSegment(76);

        try {
            // Try to freeSegment non-existent
            backup.freeSegment(76);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_retrieveSegment_badSegNum()
    {
        BackupServer backup(service, transport, LOG_PATH);
        char buf[SEGMENT_SIZE];
        try {
            // Try to fetch INVALID_SEGMENT_NUM
            backup.retrieveSegment(INVALID_SEGMENT_NUM, &buf[0]);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_retrieveSegment_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        backup.writeSegment(14, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(14);

        char buf[SEGMENT_SIZE];
        backup.retrieveSegment(76, &buf[0]);
        CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                &buf[0], testMessageLen));
        backup.retrieveSegment(14, &buf[0]);
        CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                &buf[0], testMessageLen));
    }

    // TODO(stutsman) test to ensure we can't freeSegment the currently
    // pinned segframe

    void
    test_getSegmentList_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        backup.writeSegment(82, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(82);

        uint64_t list[2];
        uint32_t count = 2;
        backup.getSegmentList(&list[0], count);

        CPPUNIT_ASSERT(list[0] == 76);
        CPPUNIT_ASSERT(list[1] == 82);
    }

    void
    test_getSegmentList_listSizeTooShort()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        backup.writeSegment(82, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(82);

        uint64_t list[1];
        uint32_t count = 1;
        try {
            backup.getSegmentList(&list[0], count);
            CPPUNIT_ASSERT_MESSAGE("getSegmentList with too short "
                                   " list space should have failed", false);
        } catch (BackupException e) {}
    }

    void
    test_extractMetadata_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        RecoveryObjectMetadata meta;
        DECLARE_OBJECT(o, 23);
        o->id = 11;
        o->table = 13;
        o->version = 19;
        o->data_len = 23;
        backup.extractMetadata(o, 29, &meta);

        CPPUNIT_ASSERT_EQUAL((uint64_t) 11lu, meta.key);
        CPPUNIT_ASSERT_EQUAL((uint64_t) 13lu, meta.table);
        CPPUNIT_ASSERT_EQUAL((uint64_t) 19lu, meta.version);
        CPPUNIT_ASSERT_EQUAL((uint64_t) 29lu, meta.offset);
        CPPUNIT_ASSERT_EQUAL((uint64_t) 23lu, meta.length);
    }

    /**
     * Write a segment header to the beginning of a segment on the
     * backup instance given.
     *
     * \param[in] backup
     *     The backup instance to be written to.
     * \param[in] segNum
     *     The segment number to place in the segment header
     *     to be defined.
     * \return
     *     The offset into the segment at which more data can be
     *     appended.  This is also the length of the header.
     */
    uint32_t
    writeMockHeader(BackupServer *backup, uint64_t segNum)
    {
        log_entry logEntry;
        segment_header segmentHeader;

        logEntry.type = LOG_ENTRY_TYPE_SEGMENT_HEADER;
        logEntry.length = sizeof(segmentHeader);
        backup->writeSegment(segNum, 0, &logEntry, sizeof(logEntry));

        segmentHeader.id = segNum;
        backup->writeSegment(segNum, sizeof(logEntry),
                            &segmentHeader, sizeof(segmentHeader));
        return sizeof(logEntry) + sizeof(segmentHeader);
    }

    /**
     * Write a mock object to the given segment number on the
     * backup instance given.  The object will contain some fake
     * metadata and data which should be apparent from the body of the
     * method.
     *
     * \param[in] backup
     *     The backup instance to be written to.
     * \param[in] segNum
     *     The segment number to place in the segment header
     *     to be defined.
     * \param[in] key
     *     The key to place in the mock object.
     * \param[in] offset
     *     Where in the backup segment to write this object.
     * \return
     *     The offset into the segment at which more data can be
     *     appended.
     */
    uint32_t
    writeMockObject(BackupServer *backup,
                    uint64_t segNum,
                    uint64_t key,
                    uint32_t offset)
    {
        log_entry logEntry;
        DECLARE_OBJECT(o, 8);

        o->id = key;
        o->table = 13;
        o->version = 15;
        o->checksum = 0xbeef;
        o->data_len = 8;
        *(reinterpret_cast<uint64_t *>(o->data)) = 0x1234567890123456ULL;

        logEntry.type = LOG_ENTRY_TYPE_OBJECT;
        logEntry.length = sizeof(*o) + 8;
        backup->writeSegment(segNum, offset, &logEntry, sizeof(logEntry));

        backup->writeSegment(segNum,
                             offset + static_cast<uint32_t>(sizeof(logEntry)),
                             o,
                             sizeof(*o) + 8);
        return offset +
            static_cast<uint32_t>(sizeof(logEntry) + sizeof(*o)) + 8;
    }

    void
    test_getSegmentMetadata_segmentNotHere()
    {
        BackupServer backup(service, transport, LOG_PATH);

        const uint32_t listSize = 0;
        RecoveryObjectMetadata list[listSize];

        try {
            uint32_t listElements = backup.getSegmentMetadata(76, &list[0],
                                                              listSize);
            listElements++;             // suppress unused warning
            CPPUNIT_ASSERT_MESSAGE("getSegmentMetadata should fail "
                                   "when there is no such stored segment",
                                   false);
        } catch (BackupException e) {}
    }

    void
    test_getSegmentMetadata_noObjects()
    {
        BackupServer backup(service, transport, LOG_PATH);

        writeMockHeader(&backup, 76);
        backup.commitSegment(76);

        char testbuf[SEGMENT_SIZE];
        backup.retrieveSegment(76, &testbuf[0]);

        const uint32_t listSize = 1;
        RecoveryObjectMetadata list[listSize];

        try {
            uint32_t listElements = backup.getSegmentMetadata(76, &list[0],
                                                              listSize);
            CPPUNIT_ASSERT_EQUAL((uint32_t)0, listElements);
        } catch (BackupException e) {
            CPPUNIT_ASSERT_MESSAGE(e.message, false);
        }
    }

    void
    test_getSegmentMetadata_normal()
    {
        BackupServer backup(service, transport, LOG_PATH);

        uint32_t offset = writeMockHeader(&backup, 76);
        offset = writeMockObject(&backup, 76, 5, offset);
        offset = writeMockObject(&backup, 76, 7, offset);
        backup.commitSegment(76);

        offset = writeMockHeader(&backup, 82);
        offset = writeMockObject(&backup, 82, 3, offset);
        backup.commitSegment(82);

        const uint32_t listSize = 2;
        RecoveryObjectMetadata list[listSize];

        try {
            uint32_t listElements = backup.getSegmentMetadata(76, &list[0],
                                                              listSize);
            CPPUNIT_ASSERT_EQUAL((uint32_t)2, listElements);
            listElements = backup.getSegmentMetadata(82, &list[0],
                                                     listSize);
            CPPUNIT_ASSERT_EQUAL((uint32_t)1, listElements);
        } catch (BackupException e) {
            CPPUNIT_ASSERT_MESSAGE(e.message, false);
        }
    }

    void
    test_getSegmentMetadata_nonexistentSegmentId()
    {
        BackupServer backup(service, transport, LOG_PATH);

        const uint32_t listSize = 2;
        RecoveryObjectMetadata list[listSize];

        try {
            uint32_t listElements = backup.getSegmentMetadata(0, &list[0],
                                                              listSize);
            listElements++;             // supressed unused warning
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_frameForSegNum_matchFound()
    {
        BackupServer backup(service, transport, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        CPPUNIT_ASSERT_EQUAL((uint64_t) 0ul, backup.frameForSegNum(76));
    }

    void
    test_frameForSegNum_noMatchFound()
    {
        BackupServer backup(service, transport, LOG_PATH);

        try {
            backup.frameForSegNum(76);
            CPPUNIT_ASSERT_MESSAGE("Should have thrown an exception", false);
        } catch (BackupException e) {}
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

} // namespace RAMCloud
