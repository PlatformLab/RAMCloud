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
    void setUp();
    void tearDown();
    void TestHeartbeat();
    void TestFreeBitmap();
  private:
    CPPUNIT_TEST_SUITE(BackupTest);
    //CPPUNIT_TEST(TestHeartbeat);
    CPPUNIT_TEST(TestFreeBitmap);
    CPPUNIT_TEST_SUITE_END();
    Net *net;
    BackupServer *backup;
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

static const char *LOG_PATH = "/tmp/rctest.log";
void
BackupTest::TestHeartbeat()
try
{
    Net net(BACKUPSVRADDR, BACKUPSVRPORT,
            BACKUPCLNTADDR, BACKUPCLNTPORT);
    BackupServer backup(&net, LOG_PATH);

    backup_rpc req, resp;

    req.hdr.type = BACKUP_RPC_HEARTBEAT_REQ;
    req.hdr.len  = static_cast<uint32_t>(BACKUP_RPC_HEARTBEAT_REQ_LEN);

    backup.HandleHeartbeat(&req, &resp);

    CPPUNIT_ASSERT(resp.hdr.type == BACKUP_RPC_HEARTBEAT_RESP);
    CPPUNIT_ASSERT(resp.hdr.len == BACKUP_RPC_HEARTBEAT_RESP_LEN);

    unlink(LOG_PATH);
} catch (BackupException e) {
    printf("%s: Exception: %s\n", e.message.c_str());
    CPPUNIT_ASSERT(false);
    unlink(LOG_PATH);
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
