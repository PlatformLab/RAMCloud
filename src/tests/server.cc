#include <cppunit/extensions/HelperMacros.h>
#include <string>
#include <cstring>

#include <server/server.h>

#include <tests/server.h>

extern struct rcrpc blobs[256];

void
ServerTest::setUp()
{
    init();
}

void
ServerTest::tearDown()
{
}

void
ServerTest:: testPing()
{
    CPPUNIT_ASSERT(true);
}

void
ServerTest::testRead100()
{
    CPPUNIT_ASSERT(true);
}
