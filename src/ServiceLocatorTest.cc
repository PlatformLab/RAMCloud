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

#include "TestUtil.h"
#include "ServiceLocator.h"

namespace RAMCloud {

class ServiceLocatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(ServiceLocatorTest);
    CPPUNIT_TEST(test_usageExample);
    CPPUNIT_TEST(test_BadServiceLocatorException_constructor);
    CPPUNIT_TEST(test_NoSuchKeyException_constructor);
    CPPUNIT_TEST(test_parseServiceLocators_goodInput);
    CPPUNIT_TEST(test_parseServiceLocators_badInput);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_init_branches);
    CPPUNIT_TEST(test_init_goodInput);
    CPPUNIT_TEST(test_init_badInput);
    CPPUNIT_TEST(test_init_escapingControl);
    CPPUNIT_TEST(test_init_escapingData);
    CPPUNIT_TEST(test_getOptionCastNoDefault);
    CPPUNIT_TEST(test_getOptionCastDefault);
    CPPUNIT_TEST(test_getOptionNoCastNoDefault);
    CPPUNIT_TEST(test_getOptionNoCastDefault);
    CPPUNIT_TEST(test_getOptionCStrNoDefault);
    CPPUNIT_TEST(test_getOptionCStrDefault);
    CPPUNIT_TEST(test_hasOption);
    CPPUNIT_TEST_SUITE_END();

/**
 * Assert the service locator string parses with the right number of protocols
 * and options.
 */
#define CONSTRUCTOR_OK(numOptions, string) \
    CPPUNIT_ASSERT_NO_THROW( \
        CPPUNIT_ASSERT_EQUAL(numOptions, \
                             ServiceLocator(string).options.size());)

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
        CPPUNIT_ASSERT_EQUAL("fast+udp", sl.getProtocol());
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL(8081, sl.getOption<uint16_t>("port"));
        CPPUNIT_ASSERT_EQUAL(0x8001,
                             sl.getOption<uint16_t>("etherType", 0x8001));
    }

    void test_BadServiceLocatorException_constructor() {
        ServiceLocator::BadServiceLocatorException e(HERE, "o", "r");
        CPPUNIT_ASSERT_EQUAL("The ServiceLocator string 'o' could not be "
                             "parsed, starting at 'r'", e.message);
        CPPUNIT_ASSERT_EQUAL("o", e.original);
        CPPUNIT_ASSERT_EQUAL("r", e.remaining);
    }

    void test_NoSuchKeyException_constructor() {
        ServiceLocator::NoSuchKeyException e(HERE, "k");
        CPPUNIT_ASSERT_EQUAL("The option with key 'k' was not found in the "
                             "ServiceLocator.", e.message);
        CPPUNIT_ASSERT_EQUAL("k", e.key);
    }

    void test_parseServiceLocators_goodInput() {
        std::vector<ServiceLocator> locators;
        ServiceLocator::parseServiceLocators("fast: ; tcp: port=3;x:",
                                             &locators);
        CPPUNIT_ASSERT_EQUAL(3, locators.size());
        CPPUNIT_ASSERT_EQUAL("fast:", locators.at(0).getOriginalString());
        CPPUNIT_ASSERT_EQUAL("tcp: port=3",
                             locators.at(1).getOriginalString());
        CPPUNIT_ASSERT_EQUAL("x:", locators.at(2).getOriginalString());
    }

    void test_parseServiceLocators_badInput() {
        std::vector<ServiceLocator> locators;
        CPPUNIT_ASSERT_THROW(
            ServiceLocator::parseServiceLocators("fast: ; tcp: port=3;!",
                                                 &locators),
            ServiceLocator::BadServiceLocatorException);
        CPPUNIT_ASSERT_EQUAL(0, locators.size());
    }

    void test_constructor() {
        string s("fast+udp: host=example.org, port=8081, port=8082");
        ServiceLocator sl(s);
        CPPUNIT_ASSERT_EQUAL(s, sl.originalString);
        CPPUNIT_ASSERT_EQUAL("fast+udp", sl.getProtocol());
        CPPUNIT_ASSERT_EQUAL("example.org", sl.getOption("host"));
        CPPUNIT_ASSERT_EQUAL("8082", sl.getOption("port"));

        CONSTRUCTOR_OK(0, "fast: ;");
        CONSTRUCTOR_BAD("fast: ; slow: ;");
    }

    void test_init_branches() {
        // parsing protocol fails
        CONSTRUCTOR_BAD("!");

        { // parsing protocol sets sentinel
            ServiceLocator sl("foo:;");
            CPPUNIT_ASSERT_EQUAL("foo:", sl.getOriginalString());
        }

        { // parsing protocol is end of string
            ServiceLocator sl("foo:");
            CPPUNIT_ASSERT_EQUAL("foo:", sl.getOriginalString());
        }

        // parsing key-value fails
        CONSTRUCTOR_BAD("foo:!");

        { // parsing key-value sets sentinel
            ServiceLocator sl("foo: x=y;");
            CPPUNIT_ASSERT_EQUAL("foo: x=y", sl.getOriginalString());
            CPPUNIT_ASSERT_EQUAL("y", sl.getOption("x"));
        }

        { // parsing key-value is end of string
            ServiceLocator sl("foo: x=y");
            CPPUNIT_ASSERT_EQUAL("foo: x=y", sl.getOriginalString());
            CPPUNIT_ASSERT_EQUAL("y", sl.getOption("x"));
        }

        { // value is empty
            ServiceLocator sl("foo: x=");
            CPPUNIT_ASSERT_EQUAL("", sl.getOption("x"));
        }

        { // value is quoted
            ServiceLocator sl("foo: x=\",\\\";\"");
            CPPUNIT_ASSERT_EQUAL("foo: x=\",\\\";\"", sl.getOriginalString());
            CPPUNIT_ASSERT_EQUAL(",\";", sl.getOption("x"));
        }

        { // value is not quoted
            ServiceLocator sl("foo: x=\\,\\\"\\;");
            CPPUNIT_ASSERT_EQUAL("foo: x=\\,\\\"\\;", sl.getOriginalString());
            CPPUNIT_ASSERT_EQUAL(",\";", sl.getOption("x"));
        }

        { // some leading or trailing whitespace
            ServiceLocator sl("  foo:x=y  ");
            CPPUNIT_ASSERT_EQUAL("foo:x=y", sl.getOriginalString());
        }
    }

    void test_init_goodInput() {
        CONSTRUCTOR_OK(0, "fast:");
        CONSTRUCTOR_OK(2, " fast : x = y , z = \"a\" , ");
        CONSTRUCTOR_OK(1, "fast: x=\"=\\\",\"");
        CONSTRUCTOR_OK(0, "fast+udp:");
        CONSTRUCTOR_OK(0, "3fast+udp_V4:");
        CONSTRUCTOR_OK(1, "fast: 3x_Y7=2");
        CONSTRUCTOR_OK(1, "fast: port=");
    }

    void test_init_badInput() {
        CONSTRUCTOR_BAD("fast");
        CONSTRUCTOR_BAD("fast: x");
        CONSTRUCTOR_BAD("fast ,");
        CONSTRUCTOR_BAD("fast x,");
        CONSTRUCTOR_BAD("fast: x=y,y");
        CONSTRUCTOR_BAD("fast + udp: ");
    }

    void test_init_escapingControl() {
        ServiceLocator sl("fast: a=\\\"\\,\\\", b=\"\\\"\"");
        CPPUNIT_ASSERT_EQUAL("\",\"", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\"", sl.getOption("b"));
    }

    void test_init_escapingData() {
        ServiceLocator sl("fast: a=\\x, b=\"\\x\"");
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("a"));
        CPPUNIT_ASSERT_EQUAL("\\x", sl.getOption("b"));
    }

    void test_getOptionCastNoDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("foo"),
                             ServiceLocator::NoSuchKeyException);
        CPPUNIT_ASSERT_EQUAL(8081, sl.getOption<uint16_t>("port"));
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port"),
                             boost::bad_lexical_cast);
    }

    void test_getOptionCastDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_EQUAL(7, sl.getOption<uint8_t>("foo", 7));
        CPPUNIT_ASSERT_EQUAL(8081, sl.getOption<uint16_t>("port", 0));
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port", 4),
                             boost::bad_lexical_cast);
    }

    void test_getOptionNoCastNoDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_THROW(sl.getOption("foo"),
                             ServiceLocator::NoSuchKeyException);
        CPPUNIT_ASSERT_EQUAL("8081", sl.getOption("port"));
    }

    void test_getOptionNoCastDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_EQUAL("7", sl.getOption("foo", "7"));
        CPPUNIT_ASSERT_EQUAL("8081", sl.getOption("port", "0"));
    }

    void test_getOptionCStrNoDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_THROW(sl.getOption<const char*>("foo"),
                             ServiceLocator::NoSuchKeyException);
        CPPUNIT_ASSERT_EQUAL("8081", sl.getOption<const char*>("port"));
    }

    void test_getOptionCStrDefault() {
        ServiceLocator sl("fast: host=example.org, port=8081");
        CPPUNIT_ASSERT_EQUAL("7", sl.getOption<const char*>("foo", "7"));
        CPPUNIT_ASSERT_EQUAL("8081", sl.getOption<const char*>("port", "0"));
    }

    void test_hasOption() {
        ServiceLocator sl("cat: meow=1");
        CPPUNIT_ASSERT(sl.hasOption("meow"));
        CPPUNIT_ASSERT(!sl.hasOption("moo"));
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServiceLocatorTest);

}  // namespace RAMCloud
