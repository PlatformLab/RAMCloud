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
#include <Net.h>
#include <MockNet.h>
#include <Log.h>
#include <Server.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

#define BACKUPSVRADDR "127.0.0.1"
#define BACKUPSVRPORT  11111

#define BACKUPCLNTADDR "127.0.0.1"
#define BACKUPCLNTPORT  11111

namespace RAMCloud {

static const char *LOG_PATH = "/tmp/rctest.log";

/**
 * Unit tests for BackupServer.
 */
class BackupServerTest : public CppUnit::TestFixture {
    Net *net;

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
    CPPUNIT_TEST(test_commit_normal);
    //CPPUNIT_TEST(test_commit_quirky);
    CPPUNIT_TEST(test_commit_commitWithoutWriteSegment);
    CPPUNIT_TEST(test_freeSegment_badSegmentNumber);
    CPPUNIT_TEST(test_freeSegment_nonexistantSegmentNumber);
    CPPUNIT_TEST(test_freeSegment_normal);
    CPPUNIT_TEST(test_freeSegment_previouslyFreeSegment);
    //CPPUNIT_TEST(test_retrieveSegment_normal);
    CPPUNIT_TEST(test_getSegmentList);
    //CPPUNIT_TEST(test_getSegmentMetadata_normal);
    CPPUNIT_TEST(test_frameForSegNum_matchFound);
    CPPUNIT_TEST(test_frameForSegNum_noMatchFound);
    //CPPUNIT_TEST(test_flush);
    //CPPUNIT_TEST(test_segFrameOff);
    CPPUNIT_TEST_SUITE_END();

  public:
    BackupServerTest() :
        net(0), testMessage("God hates ponies."), testMessageLen(0)
    {
        testMessageLen = static_cast<uint32_t>(testMessage.length());
    }

    static void noOp(const char *buf, size_t len)
    {
        printf(">>>> %s", buf);
    }

    void
    setUp()
    {
        net = new MockNet(noOp);
    }

    void
    tearDown()
    {
        unlink(LOG_PATH);
        delete net;
    }

    void
    test_constructor_normal()
    {
        BackupServer backup(net, LOG_PATH);
        // TODO(stutsman) Probably best to check here that the
        // internal state of the private members are intitalized to
        // the expected starting state
    }

    void
    test_constructor_badPath()
    {
        bool threw = false;
        try {
            BackupServer backup(net, "/god/hates/ponies/");
            CPPUNIT_ASSERT_MESSAGE("Backup construction should fail on "
                                   "non-existant directory for log path.",
                                   false);
        } catch (BackupLogIOException e) {
            threw = true;
        }
        CPPUNIT_ASSERT(threw);
    }

    // TODO(stutsman) There are some other things to test in the
    // constructor but they are only compile time selectable for now

    // Ensure INVALID_SEGMENT_NUM cannot be used
    void
    test_writeSegment_badSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

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
        BackupServer backup(net, LOG_PATH);

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
        BackupServer backup(net, LOG_PATH);

        char buf[SEGMENT_SIZE + 256];
        try {
            backup.writeSegment(0, 0, buf, SEGMENT_SIZE + 256);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    void
    test_writeSegment_offsetTooLarge()
    {
        BackupServer backup(net, LOG_PATH);

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
        BackupServer backup(net, LOG_PATH);

        try {
            backup.writeSegment(0, SEGMENT_SIZE - 2,
                         testMessage.c_str(), testMessageLen);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    // TODO(stutsman) Test commit invalid seg num

    void
    test_commit_normal()
    {
        BackupServer backup(net, LOG_PATH);

        /*
        BackupClient *client = &backup;
        ServerConfig sconfig;
        MockNet net(noOp);
        Server server(&sconfig, &net, client);
        char buf[SEGMENT_SIZE];
        Log(SEGMENT_SIZE, &buf[0], SEGMENT_SIZE, &backup);

        DECLARE_OBJECT(new_o, buf_len);
        */

        // Simple test - should work
        uint64_t targetFrame = 0;
        for (uint64_t seg = 20; seg < 24; seg++) {
            backup.writeSegment(seg, 0,
                                testMessage.c_str(), testMessageLen);
            backup.commitSegment(seg);
            // Check to make sure it's in the right slot
            CPPUNIT_ASSERT_EQUAL(seg, backup.segmentAtFrame[targetFrame]);
            targetFrame++;
        }

        // TODO(stutsman) Need to check file to ensure it really
        // worked
    }

    void
    test_commit_quirky()
    {
        BackupServer backup(net, LOG_PATH);

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
    test_commit_commitWithoutWriteSegment()
    {
        BackupServer backup(net, LOG_PATH);

        // Try to commit a segment that isn't open
        try {
            backup.commitSegment(0);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_badSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.freeSegment(INVALID_SEGMENT_NUM);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_nonexistantSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.freeSegment(77);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_freeSegment_normal()
    {
        BackupServer backup(net, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);
        backup.freeSegment(76);
    }

    void
    test_freeSegment_previouslyFreeSegment()
    {
        BackupServer backup(net, LOG_PATH);

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
    test_retrieveSegment_normal()
    {
        BackupServer backup(net, LOG_PATH);

        const int pagesize = getpagesize();
        char logBuf[SEGMENT_SIZE + pagesize];
        void *segStart = reinterpret_cast<void *>(
            ((reinterpret_cast<intptr_t>(&logBuf[0]) +
              pagesize - 1) /
             pagesize) * pagesize);

    }

    // TODO(stutsman) test to ensure we can't freeSegment the currently
    // pinned segframe

    void
    test_getSegmentList()
    {
        BackupServer backup(net, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        backup.writeSegment(82, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(82);

        uint64_t list[2];
        uint64_t count = 2;
        backup.getSegmentList(&list[0], &count);

        CPPUNIT_ASSERT(list[0] == 76);
        CPPUNIT_ASSERT(list[1] == 82);
    }

    void
    test_getSegmentMetadata_normal()
    {
        BackupServer backup(net, LOG_PATH);

        // TODO(stutsman) This is insufficent, must be in server
        // format
        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        backup.writeSegment(82, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(82);

        const size_t listSize = 2;
        RecoveryObjectMetadata list[listSize];

        try {
            size_t listElements = backup.getSegmentMetadata(76, &list[0],
                                                            listSize);
            CPPUNIT_ASSERT_EQUAL((size_t)1, listElements);
            listElements = backup.getSegmentMetadata(82, &list[0],
                                                     listSize);
            CPPUNIT_ASSERT_EQUAL((size_t)1, listElements);
        } catch (BackupException e) {
            CPPUNIT_ASSERT_MESSAGE(e.message, false);
        }
    }

    void
    test_getSegmentMetadata_nonexistentSegmentId()
    {
        BackupServer backup(net, LOG_PATH);

        const size_t listSize = 2;
        RecoveryObjectMetadata list[listSize];

        try {
            size_t listElements = backup.getSegmentMetadata(0, &list[0],
                                                            listSize);
            listElements++;             // supressed unused warning
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_frameForSegNum_matchFound()
    {
        BackupServer backup(net, LOG_PATH);

        backup.writeSegment(76, 0, testMessage.c_str(), testMessageLen);
        backup.commitSegment(76);

        CPPUNIT_ASSERT_EQUAL(0lu, backup.frameForSegNum(76));
    }

    void
    test_frameForSegNum_noMatchFound()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.frameForSegNum(76);
            CPPUNIT_ASSERT_MESSAGE("Should have thrown an exception", false);
        } catch (BackupException e) {}
    }

    void
    test_flush()
    {
        CPPUNIT_ASSERT(false);
    }

    void
    test_segFrameOff()
    {
        CPPUNIT_ASSERT(false);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

} // namespace RAMCloud
