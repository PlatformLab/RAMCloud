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

#include <BackupServer.h>

#include <backuprpc.h>
#include <Net.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

#define BACKUPSVRADDR "127.0.0.1"
#define BACKUPSVRPORT  11111

#define BACKUPCLNTADDR "127.0.0.1"
#define BACKUPCLNTPORT  11111

namespace RAMCloud {

static const char *LOG_PATH = "/tmp/rctest.log";

class BackupServerTest : public CppUnit::TestFixture {
    Net *net;
    BackupServer *backup;
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest);

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_commit);
    CPPUNIT_TEST(test_free);
    CPPUNIT_TEST(test_getSegmentList);
    CPPUNIT_TEST_SUITE_END();

  public:
    BackupServerTest() : net(0), backup(0) {}

    void
    setUp()
    {
    }

    void
    tearDown()
    {
        unlink(LOG_PATH);
    }

    static void NoOp(const char *buf, size_t len)
    {
        printf(">>>> %s", buf);
    }

    void
    test_constructor()
    {
        MockNet net(NoOp);

        // Normal construction
        {
            BackupServer backup(&net, LOG_PATH);
        }
        unlink(LOG_PATH);

        // Bad path
        bool threw = false;
        try {
            BackupServer backup(&net, "/god/hates/ponies/");
            CPPUNIT_ASSERT(false);
        } catch (BackupLogIOException e) {
            threw = true;
        }
        CPPUNIT_ASSERT(threw);

        // TODO(stutsman) There are some other things to test but they are
        // only compile time selectable for now
    }

    void
    test_write()
    {
        MockNet net(NoOp);

        BackupServer backup(&net, LOG_PATH);

        const char *msg = "test";
        const size_t len = strlen(msg) + 1;

        // Ensure INVALID_SEGMENT_NUM cannot be used
        try {
            backup.Write(INVALID_SEGMENT_NUM, 0, "test", 5);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        // Ensure that segments must be committed before new writes
        try {
            backup.Write(0, 0, msg, len);
            backup.Write(1, 0, msg, len);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        // Check to see if data is correct from first write
        CPPUNIT_ASSERT(!strncmp(msg, backup.seg, len));

        // Test write is too long
        char buf[SEGMENT_SIZE + 256];
        try {
            backup.Write(0, 0, buf, SEGMENT_SIZE + 256);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}

        // Test offset is bad
        try {
            backup.Write(0, SEGMENT_SIZE, msg, len);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}

        // Test offset is ok but runs off the end
        try {
            backup.Write(0, SEGMENT_SIZE - 2, msg, len);
            CPPUNIT_ASSERT(false);
        } catch (BackupSegmentOverflowException e) {}
    }

    void
    test_commit()
    {
        MockNet net(NoOp);

        BackupServer backup(&net, LOG_PATH);

        const char *msg = "test";
        const size_t len = strlen(msg) + 1;

        // Simple test - should work
        for (uint64_t seg = 0; seg < 4; seg++) {
            backup.Write(seg, 0, msg, len);
            backup.Commit(seg);
        }

        /*
        backup.free_map.Clear(63);
        backup.free_map.Clear(64);
        // Test what happens when we run out of seg frames
        //for (uint64_t i = 0; i < SEGMENT_FRAMES - 1; i++)
        for (int64_t i = 0; i < 64; i++)
        backup.free_map.Clear(i);
        backup.Write(0, 0, msg, len);
        backup.Commit(0);
        try {
        backup.Write(1, 0, msg, len);
        backup.Commit(1);
        CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
        */

        // Try to commit a segment that isn't open
        try {
            backup.Commit(0);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}
    }

    void
    test_free()
    {
        MockNet net(NoOp);

        BackupServer backup(&net, LOG_PATH);

        // Try to free invalid segment
        try {
            backup.Free(INVALID_SEGMENT_NUM);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        // Try to free non-existent
        try {
            backup.Free(77);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        const char *msg = "test";
        const size_t len = strlen(msg) + 1;

        backup.Write(76, 0, msg, len);
        backup.Commit(76);
        // Normal free
        backup.Free(76);
        try {
            // Try to free non-existent
            backup.Free(77);
            CPPUNIT_ASSERT(false);
        } catch (BackupException e) {}

        // TODO(stutsman) test to ensure we can't free the currently
        // pinned segframe
    }

    void
    test_getSegmentList()
    {
        MockNet net(NoOp);

        BackupServer backup(&net, LOG_PATH);

        const char *msg = "test";
        const size_t len = strlen(msg) + 1;

        backup.Write(76, 0, msg, len);
        backup.Commit(76);

        backup.Write(82, 0, msg, len);
        backup.Commit(82);

        uint64_t list[2];
        uint64_t count = 2;
        backup.GetSegmentList(&list[0], &count);

        CPPUNIT_ASSERT(list[0] == 76);
        CPPUNIT_ASSERT(list[1] == 82);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

} // namespace RAMCloud
