/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Unit tests for #RAMCloud::ServiceLocator.
 */

#include "TestUtil.h"
#include "ServiceLocator.h"

namespace RAMCloud {

class ServiceLocatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ServiceLocatorTest);
    CPPUNIT_TEST(test_usageExample);
    CPPUNIT_TEST(test_constructor_normal);
    CPPUNIT_TEST(test_constructor_goodInput);
    CPPUNIT_TEST(test_constructor_badInput);
    CPPUNIT_TEST(test_constructor_escapingControl);
    CPPUNIT_TEST(test_constructor_escapingData);
    CPPUNIT_TEST(test_protocolStack);
    CPPUNIT_TEST(test_getOption);
    CPPUNIT_TEST_SUITE_END();

/**
 * Assert the service locator string parses with the right number of protocols
 * and options.
 */
#define CONSTRUCTOR_OK(numProtocols, numOptions, string) \
    CPPUNIT_ASSERT_NO_THROW( \
        ServiceLocator sl(string); \
        CPPUNIT_ASSERT_EQUAL(numProtocols, \
                             sl.getProtocolStack().size()); \
        CPPUNIT_ASSERT_EQUAL(numOptions, \
                             sl.options.size());)

/**
 * Make sure the service locator string doesn't parse.
 */
#define CONSTRUCTOR_BAD(string) \
    CPPUNIT_ASSERT_THROW(ServiceLocator(string), \
                         ServiceLocator::BadServiceLocatorException)

  public:
    ServiceLocatorTest() {}

    void test_usageExample() {
        ServiceLocator sl("tcp+ip: host=example.org, port=8081");
        CPPUNIT_ASSERT_EQUAL("tcp", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("ip", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL(8081, sl.getOption<uint16_t>("port"));
        CPPUNIT_ASSERT_EQUAL(0x8001,
                             sl.getOption<uint16_t>("etherType", 0x8001));
    }

    void test_constructor_normal() {
        string s("tcp+ip: host=example.org, port=8081, port=8082");
        ServiceLocator sl(s);
        CPPUNIT_ASSERT_EQUAL(s, sl.originalString);
        CPPUNIT_ASSERT_EQUAL("tcp+ip", sl.getOriginalProtocol());
        CPPUNIT_ASSERT_EQUAL("tcp", sl.getProtocolStack().at(0));
        CPPUNIT_ASSERT_EQUAL("ip", sl.getProtocolStack().at(1));
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL("8082", sl.getOption("port"));
    }

    void test_constructor_goodInput() {
        CONSTRUCTOR_OK(1, 0, "tcp:");
        CONSTRUCTOR_OK(1, 2, " tcp : x = y , z = \"a\" , ");
        CONSTRUCTOR_OK(1, 1, "tcp: x=\"=\\\",\"");
        CONSTRUCTOR_OK(2, 0, "tcp+ip:");
        CONSTRUCTOR_OK(2, 0, "3tcp+ip_V4:");
        CONSTRUCTOR_OK(1, 1, "tcp: 3x_Y7=2");
        CONSTRUCTOR_OK(1, 1, "tcp: port=");
    }

    void test_constructor_badInput() {
        CONSTRUCTOR_BAD("tcp");
        CONSTRUCTOR_BAD("tcp: x");
        CONSTRUCTOR_BAD("tcp ,");
        CONSTRUCTOR_BAD("tcp x,");
        CONSTRUCTOR_BAD("tcp: x=y,y");
        CONSTRUCTOR_BAD("tcp + ip: ");
    }

    void test_constructor_escapingControl() {
        ServiceLocator sl("tcp: a=\\\"\\,\\\", b=\"\\\"\"");
        CPPUNIT_ASSERT_EQUAL("\",\"", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\"", sl.getOption("b"));
    }

    void test_constructor_escapingData() {
        ServiceLocator sl("tcp: a=\\x, b=\"\\x\"");
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("b"));
    }

    void test_protocolStack() {
        ServiceLocator sl("tcp+ip:");
        CPPUNIT_ASSERT_EQUAL("tcp", sl.peekProtocol());
        CPPUNIT_ASSERT_EQUAL("tcp", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("ip", sl.popProtocol());
        CPPUNIT_ASSERT_THROW(sl.peekProtocol(),
                             ServiceLocator::NoMoreProtocolsException);
        CPPUNIT_ASSERT_THROW(sl.popProtocol(),
                             ServiceLocator::NoMoreProtocolsException);
        sl.resetProtocolStack();
        CPPUNIT_ASSERT_EQUAL("tcp", sl.peekProtocol());
    }

    void test_getOption() {
        ServiceLocator sl("tcp: host=example.org, port=8081");
        CPPUNIT_ASSERT_THROW(sl.getOption<uint16_t>("host"),
                             StringConverter::BadFormatException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint16_t>("host", 4),
                             StringConverter::BadFormatException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port"),
                             StringConverter::OutOfRangeException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port", 4),
                             StringConverter::OutOfRangeException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("foo"),
                             ServiceLocator::NoSuchKeyException);
        CPPUNIT_ASSERT_EQUAL(7, sl.getOption<uint8_t>("foo", 7));
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServiceLocatorTest);

}  // namespace RAMCloud
