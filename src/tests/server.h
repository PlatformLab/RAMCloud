#ifndef RAMCLOUD_TESTS_SERVER_H
#define RAMCLOUD_TESTS_SERVER_H

#include <cppunit/extensions/HelperMacros.h>

#include <server/server.h>

class ServerTest : public CppUnit::TestFixture  {
    CPPUNIT_TEST_SUITE(ServerTest);
    CPPUNIT_TEST(testPing);
    CPPUNIT_TEST(testWriteRead100);
    CPPUNIT_TEST(testCreateTable);
    CPPUNIT_TEST_SUITE_END();
private:
    RAMCloud::Server *server;
public:
    void setUp();
    void tearDown();
    void testPing();
    void testWriteRead100();
    void testCreateTable();
};

#endif
