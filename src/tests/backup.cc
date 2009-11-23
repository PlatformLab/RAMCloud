/*-
 * Copyright (c) 2009 Ryan Stutsman
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE FOUNDATION OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
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
