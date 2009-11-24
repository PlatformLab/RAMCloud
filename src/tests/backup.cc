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
  private:
    CPPUNIT_TEST_SUITE(BackupTest);
    CPPUNIT_TEST(TestHeartbeat);
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

    backup.Heartbeat(&req, &resp);

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

} // namespace RAMCloud
