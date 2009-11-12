#ifndef RAMCLOUD_TESTS_SERVER_H
#define RAMCLOUD_TESTS_SERVER_H

#include <cppunit/extensions/HelperMacros.h>
#include <string>

class ServerTest : public CppUnit::TestFixture  {
    CPPUNIT_TEST_SUITE(ServerTest);
    CPPUNIT_TEST(testPing);
    CPPUNIT_TEST(testRead100);
    CPPUNIT_TEST_SUITE_END();
private:
    std::string specialStuff;
public:
    void setUp();
    void tearDown();
    void testPing();
    void testRead100();
};

#endif
