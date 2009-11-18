#include <cppunit/extensions/HelperMacros.h>
#include <string>
#include <cstring>

#include <server/server.h>
#include <shared/rcrpc.h>

#include <tests/server.h>

void
ServerTest::setUp()
{
    server = new RAMCloud::Server();
}

void
ServerTest::tearDown()
{
    delete server;
}

void
ServerTest::testPing()
{
    using namespace RAMCloud;

    struct rcrpc req, resp;

    req.type = RCRPC_PING_REQUEST;
    req.len  = (uint32_t) RCRPC_PING_REQUEST_LEN;

    server->ping(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_PING_RESPONSE);
    CPPUNIT_ASSERT(resp.len == (uint32_t) RCRPC_PING_RESPONSE_LEN);
}

void
ServerTest::testWriteRead100()
{
    using namespace RAMCloud;

    struct rcrpc req, resp;

    std::string value = "God hates ponies";
    uint64_t table = 0;
    int key = 0;

    memset(req.write100_request.buf, 0, sizeof(req.write100_request.buf));
    memcpy(req.write100_request.buf, value.c_str(), value.length());
    req.type = RCRPC_WRITE100_REQUEST;
    req.len  = (uint32_t) RCRPC_WRITE100_REQUEST_LEN;
    req.write100_request.table = table;
    req.write100_request.key = key;
    server->write100(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_WRITE100_RESPONSE);
    CPPUNIT_ASSERT(resp.len == (uint32_t) RCRPC_WRITE100_RESPONSE_LEN);

    // --- read ---

    req.type = RCRPC_READ100_REQUEST;
    req.len  = (uint32_t) RCRPC_READ100_REQUEST_LEN;
    req.read100_request.table = table;
    req.read100_request.key = key;
    server->read100(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_READ100_RESPONSE);
    CPPUNIT_ASSERT(resp.len == (uint32_t) RCRPC_READ100_RESPONSE_LEN);
    CPPUNIT_ASSERT(value == resp.read100_response.buf);

}

void
ServerTest::testCreateTable()
{
    using namespace RAMCloud;

    struct rcrpc req, resp;
    const char *name = "Testing";

    req.type = RCRPC_CREATE_TABLE_REQUEST;
    req.len  = (uint32_t) RCRPC_CREATE_TABLE_REQUEST_LEN;
    strncpy(req.create_table_request.name,
            name,
            sizeof(req.create_table_request.name));
    req.create_table_request.name[sizeof(req.create_table_request.name) - 1] = '\0';

    server->createTable(&req, &resp);

    CPPUNIT_ASSERT(resp.type == RCRPC_CREATE_TABLE_RESPONSE);
    CPPUNIT_ASSERT(resp.len == (uint32_t) RCRPC_CREATE_TABLE_RESPONSE_LEN);
}
