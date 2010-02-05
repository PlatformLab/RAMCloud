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
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest);

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_constructor_normal);
    CPPUNIT_TEST(test_constructor_badPath);
    CPPUNIT_TEST(test_write_badSegmentNumber);
    CPPUNIT_TEST(test_write_normal);
    CPPUNIT_TEST(test_write_dataTooLarge);
    CPPUNIT_TEST(test_write_offsetTooLarge);
    CPPUNIT_TEST(test_write_overflow);
    CPPUNIT_TEST(test_commit_normal);
    CPPUNIT_TEST(test_commit_quirky);
    CPPUNIT_TEST(test_commit_commitWithoutWrite);
    CPPUNIT_TEST(test_free_badSegmentNumber);
    CPPUNIT_TEST(test_free_nonexistantSegmentNumber);
    CPPUNIT_TEST(test_free_normal);
    CPPUNIT_TEST(test_free_previouslyFreed);
    CPPUNIT_TEST(test_getSegmentList);
    CPPUNIT_TEST(test_frameForSegNum);
    CPPUNIT_TEST(test_flush);
    CPPUNIT_TEST(test_segFrameOff);
    CPPUNIT_TEST_SUITE_END();

  public:
    BackupServerTest() :
        net(0), testMessage("God hates ponies.")
    {}

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
    test_write_badSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Write(INVALID_SEGMENT_NUM, 0, "test", 5);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    // Ensure that segments must be committed before new writes
    // then check to see if data is correct from first write
    // TODO(stutsman) This single open segment condition should be
    // relaxed relatively early in development.
    void
    test_write_normal()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Write(0, 0, testMessage.c_str(), testMessage.length());
            backup.Write(1, 0, testMessage.c_str(), testMessage.length());
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        CPPUNIT_ASSERT(!strncmp(testMessage.c_str(),
                                backup.seg, testMessage.length()));
    }

    void
    test_write_dataTooLarge()
    {
        BackupServer backup(net, LOG_PATH);

        char buf[SEGMENT_SIZE + 256];
        try {
            backup.Write(0, 0, buf, SEGMENT_SIZE + 256);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    void
    test_write_offsetTooLarge()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Write(0, SEGMENT_SIZE,
                         testMessage.c_str(), testMessage.length());
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    // Test offset is ok but runs off the end
    void
    test_write_overflow()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Write(0, SEGMENT_SIZE - 2,
                         testMessage.c_str(), testMessage.length());
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    void
    test_commit_normal()
    {
        BackupServer backup(net, LOG_PATH);

        // Simple test - should work
        for (uint64_t seg = 0; seg < 4; seg++) {
            backup.Write(seg, 0, testMessage.c_str(), testMessage.length());
            backup.Commit(seg);
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

        backup.free_map.clear(63);
        backup.free_map.clear(64);
        // Test what happens when we run out of seg frames
        //for (uint64_t i = 0; i < SEGMENT_FRAMES - 1; i++)
        for (int64_t i = 0; i < 64; i++)
            backup.free_map.clear(i);
        backup.Write(0, 0, testMessage.c_str(), testMessage.length());
        backup.Commit(0);
        try {
            backup.Write(1, 0, testMessage.c_str(), testMessage.length());
            backup.Commit(1);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_commit_commitWithoutWrite()
    {
        BackupServer backup(net, LOG_PATH);

        // Try to commit a segment that isn't open
        try {
            backup.Commit(0);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_free_badSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Free(INVALID_SEGMENT_NUM);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_free_nonexistantSegmentNumber()
    {
        BackupServer backup(net, LOG_PATH);

        try {
            backup.Free(77);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_free_normal()
    {
        BackupServer backup(net, LOG_PATH);

        backup.Write(76, 0, testMessage.c_str(), testMessage.length());
        backup.Commit(76);
        backup.Free(76);
    }

    void
    test_free_previouslyFreed()
    {
        BackupServer backup(net, LOG_PATH);

        backup.Write(76, 0, testMessage.c_str(), testMessage.length());
        backup.Commit(76);
        backup.Free(76);

        try {
            // Try to free non-existent
            backup.Free(76);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    // TODO(stutsman) test to ensure we can't free the currently
    // pinned segframe

    void
    test_getSegmentList()
    {
        BackupServer backup(net, LOG_PATH);

        backup.Write(76, 0, testMessage.c_str(), testMessage.length());
        backup.Commit(76);

        backup.Write(82, 0, testMessage.c_str(), testMessage.length());
        backup.Commit(82);

        uint64_t list[2];
        uint64_t count = 2;
        backup.GetSegmentList(&list[0], &count);

        CPPUNIT_ASSERT(list[0] == 76);
        CPPUNIT_ASSERT(list[1] == 82);
    }

    void
    test_frameForSegNum()
    {
        CPPUNIT_ASSERT(false);
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
