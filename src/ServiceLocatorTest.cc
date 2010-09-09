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
    CPPUNIT_TEST(test_hasOption);
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
        ServiceLocator sl("fast+udp: host=example.org, port=8081");
        CPPUNIT_ASSERT_EQUAL("fast", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("udp", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL(8081, sl.getOption<uint16_t>("port"));
        CPPUNIT_ASSERT_EQUAL(0x8001,
                             sl.getOption<uint16_t>("etherType", 0x8001));
    }

    void test_constructor_normal() {
        string s("fast+udp: host=example.org, port=8081, port=8082");
        ServiceLocator sl(s);
        CPPUNIT_ASSERT_EQUAL(s, sl.originalString);
        CPPUNIT_ASSERT_EQUAL("fast+udp", sl.getOriginalProtocol());
        CPPUNIT_ASSERT_EQUAL("fast", sl.getProtocolStack().at(0));
        CPPUNIT_ASSERT_EQUAL("udp", sl.getProtocolStack().at(1));
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL("8082", sl.getOption("port"));
    }

    void test_constructor_goodInput() {
        CONSTRUCTOR_OK(1, 0, "fast:");
        CONSTRUCTOR_OK(1, 2, " fast : x = y , z = \"a\" , ");
        CONSTRUCTOR_OK(1, 1, "fast: x=\"=\\\",\"");
        CONSTRUCTOR_OK(2, 0, "fast+udp:");
        CONSTRUCTOR_OK(2, 0, "3fast+udp_V4:");
        CONSTRUCTOR_OK(1, 1, "fast: 3x_Y7=2");
        CONSTRUCTOR_OK(1, 1, "fast: port=");
    }

    void test_constructor_badInput() {
        CONSTRUCTOR_BAD("fast");
        CONSTRUCTOR_BAD("fast: x");
        CONSTRUCTOR_BAD("fast ,");
        CONSTRUCTOR_BAD("fast x,");
        CONSTRUCTOR_BAD("fast: x=y,y");
        CONSTRUCTOR_BAD("fast + udp: ");
    }

    void test_constructor_escapingControl() {
        ServiceLocator sl("fast: a=\\\"\\,\\\", b=\"\\\"\"");
        CPPUNIT_ASSERT_EQUAL("\",\"", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\"", sl.getOption("b"));
    }

    void test_constructor_escapingData() {
        ServiceLocator sl("fast: a=\\x, b=\"\\x\"");
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("b"));
    }

    void test_protocolStack() {
        ServiceLocator sl("fast+udp:");
        CPPUNIT_ASSERT_EQUAL("fast", sl.peekProtocol());
        CPPUNIT_ASSERT_EQUAL("fast", sl.popProtocol());
        CPPUNIT_ASSERT_EQUAL("udp", sl.popProtocol());
        CPPUNIT_ASSERT_THROW(sl.peekProtocol(),
                             ServiceLocator::NoMoreProtocolsException);
        CPPUNIT_ASSERT_THROW(sl.popProtocol(),
                             ServiceLocator::NoMoreProtocolsException);
        sl.resetProtocolStack();
        CPPUNIT_ASSERT_EQUAL("fast", sl.peekProtocol());
    }

    void test_getOption() {
        ServiceLocator sl("fast: host=example.org, port=8081");
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

    void test_hasOption() {
        ServiceLocator sl("cat: meow=1");
        CPPUNIT_ASSERT(sl.hasOption("meow"));
        CPPUNIT_ASSERT(!sl.hasOption("moo"));
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServiceLocatorTest);

}  // namespace RAMCloud
