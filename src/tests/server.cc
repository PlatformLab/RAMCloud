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

#include <config.h>

#include <server/server.h>
#include <server/net.h>
#include <shared/rcrpc.h>
#include <backup/backup.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

// Override settings in config.h
#define SVRADDR "127.0.0.1"
#define SVRPORT  11111

#define CLNTADDR "127.0.0.1"
#define CLNTPORT  11111

#define BACKUPSVRADDR "127.0.0.1"
#define BACKUPSVRPORT  11111

#define BACKUPCLNTADDR "127.0.0.1"
#define BACKUPCLNTPORT  11111

class ServerTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestPing();
    void TestWriteRead100();
    void TestCreateTable();
  private:
    CPPUNIT_TEST_SUITE(ServerTest);
    //CPPUNIT_TEST(TestPing);
    // TODO(stutsman) Doesn't work until BackupClient is mocked out
    //CPPUNIT_TEST(TestWriteRead100);
    //CPPUNIT_TEST(TestCreateTable);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Net *net;
    RAMCloud::Server *server;
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServerTest);

void
ServerTest::setUp()
{
    net = new RAMCloud::Net(SVRADDR, SVRPORT,
                            CLNTADDR, CLNTPORT);
    server = new RAMCloud::Server(net);
}

void
ServerTest::tearDown()
{
    delete server;
    delete net;
}

void
ServerTest::TestPing()
{
    struct rcrpc req, resp;

    req.type = RCRPC_PING_REQUEST;
    req.len  = static_cast<uint32_t>(RCRPC_PING_REQUEST_LEN);

    server->Ping(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_PING_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_PING_RESPONSE_LEN);
}

void
ServerTest::TestWriteRead100()
{
    char reqbuf[1024];
    char respbuf[1024];
    rcrpc *req = reinterpret_cast<rcrpc *>(reqbuf);
    rcrpc *resp = reinterpret_cast<rcrpc *>(respbuf);

    std::string value = "God hates ponies";
    uint64_t table = 0;
    int key = 0;

    memcpy(req->write_request.var, value.c_str(), value.length());
    req->type = RCRPC_WRITE_REQUEST;
    req->len = static_cast<uint32_t>(RCRPC_WRITE_REQUEST_LEN_WODATA) +
            value.length();
    req->write_request.table = table;
    req->write_request.key = key;
    req->write_request.index_entries_len = 0;
    req->write_request.buf_len = value.length() + 1;
    strcpy(req->write_request.var, value.c_str());
    server->Write(req, resp);

    CPPUNIT_ASSERT(resp->type == RCRPC_WRITE_RESPONSE);
    CPPUNIT_ASSERT(resp->len == RCRPC_WRITE_RESPONSE_LEN);

    // --- read ---

    req->type = RCRPC_READ_REQUEST;
    req->len  = static_cast<uint32_t>(RCRPC_READ_REQUEST_LEN);
    req->read_request.table = table;
    req->read_request.key = key;
    server->Read(req, resp);

    CPPUNIT_ASSERT(resp->type == RCRPC_READ_RESPONSE);
    CPPUNIT_ASSERT(resp->len == RCRPC_READ_RESPONSE_LEN_WODATA +
                   resp->read_response.buf_len);
    CPPUNIT_ASSERT(value == resp->read_response.var);

}

void
ServerTest::TestCreateTable()
{
    struct rcrpc req, resp;
    const char *name = "Testing";

    req.type = RCRPC_CREATE_TABLE_REQUEST;
    req.len  = static_cast<uint32_t>(RCRPC_CREATE_TABLE_REQUEST_LEN);
    strncpy(req.create_table_request.name,
            name,
            sizeof(req.create_table_request.name));
    int i = sizeof(req.create_table_request.name) - 1;
    req.create_table_request.name[i] = '\0';

    server->CreateTable(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_CREATE_TABLE_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_CREATE_TABLE_RESPONSE_LEN);
}
