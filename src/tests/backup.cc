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

#include <backup/backup.h>

#include <shared/backuprpc.h>
#include <server/net.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

#define BACKUPSVRADDR "127.0.0.1"
#define BACKUPSVRPORT  11111

#define BACKUPCLNTADDR "127.0.0.1"
#define BACKUPCLNTPORT  11111

namespace RAMCloud {

class BackupTest : public CppUnit::TestFixture {
  public:
    BackupTest() : net(0), backup(0) {}
    void setUp();
    void tearDown();

    // Constructor tests
    void TestConstructor();

    // RPC tests
    void TestWrite();
    void TestCommit();
    void TestFree();
    void TestGetSegmentList();

    // Other Tests
    void TestFreeBitmap();
  private:
    CPPUNIT_TEST_SUITE(BackupTest);
    CPPUNIT_TEST(TestConstructor);
    CPPUNIT_TEST(TestWrite);
    CPPUNIT_TEST(TestCommit);
    CPPUNIT_TEST(TestFree);
    CPPUNIT_TEST(TestGetSegmentList);
    CPPUNIT_TEST(TestFreeBitmap);
    CPPUNIT_TEST_SUITE_END();
    Net *net;
    BackupServer *backup;
    DISALLOW_COPY_AND_ASSIGN(BackupTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupTest);

void
BackupTest::setUp()
{
}

void
BackupTest::tearDown()
{
}

static void NoOp(const char *buf, size_t len)
{
    printf(">>>> %s", buf);
}

static const char *LOG_PATH = "/tmp/rctest.log";
void
BackupTest::TestConstructor()
try
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
} catch (...) {
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
}

void
BackupTest::TestWrite()
try
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
} catch (...) {
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
}

void
BackupTest::TestCommit()
try
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
} catch (...) {
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
}

void
BackupTest::TestFree()
try
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
} catch (...) {
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
}

void
BackupTest::TestGetSegmentList()
try
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
} catch (...) {
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
}

void
BackupTest::TestFreeBitmap()
{
    FreeBitmap<8192> free(false);

    free.Set(63);
    CPPUNIT_ASSERT(!free.Get(62));
    CPPUNIT_ASSERT(free.Get(63));
    free.Clear(63);

    free.Set(0);
    CPPUNIT_ASSERT(free.NextFree(0) == 0);
    CPPUNIT_ASSERT(free.NextFree(1) == 0);
    free.Set(0);
    free.Clear(1);
    CPPUNIT_ASSERT(free.NextFree(1) == 0);
    CPPUNIT_ASSERT(free.NextFree(2) == 0);
    CPPUNIT_ASSERT(free.NextFree(63) == 0);
    CPPUNIT_ASSERT(free.NextFree(64) == 0);
    free.Set(64);
    free.Clear(0);
    CPPUNIT_ASSERT(free.NextFree(64) == 64);
    CPPUNIT_ASSERT(free.NextFree(1) == 64);
    free.Clear(0);
    free.Set(1);
    CPPUNIT_ASSERT(free.NextFree(1) == 1);
    CPPUNIT_ASSERT(free.NextFree(64) == 1);
    free.Set(0);
    for (uint32_t i = 1; i < 8192; i++)
        free.Clear(i);
    CPPUNIT_ASSERT(free.NextFree(8191) == 0);
    free.Clear(0);
    CPPUNIT_ASSERT(free.NextFree(0) == -1);
    CPPUNIT_ASSERT(free.NextFree(2812) == -1);
    CPPUNIT_ASSERT(free.NextFree(8191) == -1);


    free.Clear(0);
    CPPUNIT_ASSERT(!free.Get(0));
    free.Set(0);
    free.Set(1);
    CPPUNIT_ASSERT(free.Get(0));

    free.Clear(0);
    CPPUNIT_ASSERT(!free.Get(0));
    free.Set(0);
    CPPUNIT_ASSERT(free.Get(0));

    FreeBitmap<8> f(true);

    CPPUNIT_ASSERT(f.Get(0));
    CPPUNIT_ASSERT(f.Get(7));
    CPPUNIT_ASSERT(f.NextFree(0) == 0);
    CPPUNIT_ASSERT(f.NextFree(1) == 0);
    f.ClearAll();
    CPPUNIT_ASSERT(f.NextFree(0) == -1);
    f.Set(5);
    CPPUNIT_ASSERT(f.NextFree(0) == 5);
}

} // namespace RAMCloud
