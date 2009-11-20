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

#include <server/server.h>
#include <server/net.h>
#include <shared/rcrpc.h>

#include <cppunit/extensions/HelperMacros.h>

#include <string>
#include <cstring>

class ServerTest : public CppUnit::TestFixture {
  public:
    void setUp();
    void tearDown();
    void TestPing();
    void TestWriteRead100();
    void TestCreateTable();
  private:
    CPPUNIT_TEST_SUITE(ServerTest);
    CPPUNIT_TEST(TestPing);
    CPPUNIT_TEST(TestWriteRead100);
    CPPUNIT_TEST(TestCreateTable);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::Net *net;
    RAMCloud::Server *server;
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServerTest);

void
ServerTest::setUp()
{
    net = new RAMCloud::UDPNet(true);
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

    server->ping(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_PING_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_PING_RESPONSE_LEN);
}

void
ServerTest::TestWriteRead100()
{
    struct rcrpc req, resp;

    std::string value = "God hates ponies";
    uint64_t table = 0;
    int key = 0;

    memset(req.write100_request.buf, 0, sizeof(req.write100_request.buf));
    memcpy(req.write100_request.buf, value.c_str(), value.length());
    req.type = RCRPC_WRITE100_REQUEST;
    req.len  = static_cast<uint32_t>(RCRPC_WRITE100_REQUEST_LEN);
    req.write100_request.table = table;
    req.write100_request.key = key;
    server->write100(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_WRITE100_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_WRITE100_RESPONSE_LEN);

    // --- read ---

    req.type = RCRPC_READ100_REQUEST;
    req.len  = static_cast<uint32_t>(RCRPC_READ100_REQUEST_LEN);
    req.read100_request.table = table;
    req.read100_request.key = key;
    server->read100(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_READ100_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_READ100_RESPONSE_LEN);
    CPPUNIT_ASSERT(value == resp.read100_response.buf);

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

    server->createTable(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_CREATE_TABLE_RESPONSE);
    CPPUNIT_ASSERT(resp.len == RCRPC_CREATE_TABLE_RESPONSE_LEN);
}
