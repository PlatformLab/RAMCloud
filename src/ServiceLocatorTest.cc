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

class ServiceLocatorTest : public ::testing::Test {

/**
 * Assert the service locator string parses with the right number of protocols
 * and options.
 */
#define CONSTRUCTOR_OK(numOptions, string) \
    EXPECT_EQ(numOptions, ServiceLocator(string).options.size())

/**
 * Make sure the service locator string doesn't parse.
 */
#define CONSTRUCTOR_BAD(string) \
    EXPECT_THROW(ServiceLocator(string), \
                         ServiceLocator::BadServiceLocatorException)

  public:
    ServiceLocatorTest() {}
};

TEST_F(ServiceLocatorTest, usageExample) {
    ServiceLocator sl("fast+udp: host=example.org, port=8081");
    EXPECT_EQ("fast+udp", sl.getProtocol());
    EXPECT_EQ("example.org", sl.getOption("host"));
    EXPECT_EQ(8081, sl.getOption<uint16_t>("port"));
    EXPECT_EQ(0x8001, sl.getOption<uint16_t>("etherType", 0x8001));
}

TEST_F(ServiceLocatorTest, BadServiceLocatorException_constructor) {
    ServiceLocator::BadServiceLocatorException e(HERE, "o", "r");
    EXPECT_EQ("The ServiceLocator string 'o' could not be "
                            "parsed, starting at 'r'", e.message);
    EXPECT_EQ("o", e.original);
    EXPECT_EQ("r", e.remaining);
}

TEST_F(ServiceLocatorTest, NoSuchKeyException_constructor) {
    ServiceLocator::NoSuchKeyException e(HERE, "k");
    EXPECT_EQ("The option with key 'k' was not found in the "
                            "ServiceLocator.", e.message);
    EXPECT_EQ("k", e.key);
}

TEST_F(ServiceLocatorTest, parseServiceLocators_goodInput) {
    std::vector<ServiceLocator> locators =
        ServiceLocator::parseServiceLocators("fast: ; tcp: port=3;x:");
    EXPECT_EQ(3U, locators.size());
    EXPECT_EQ("fast:", locators.at(0).getOriginalString());
    EXPECT_EQ("tcp: port=3", locators.at(1).getOriginalString());
    EXPECT_EQ("x:", locators.at(2).getOriginalString());
}

TEST_F(ServiceLocatorTest, parseServiceLocators_badInput) {
    std::vector<ServiceLocator> locators;
    EXPECT_THROW(
        locators =
            ServiceLocator::parseServiceLocators("fast: ; tcp: port=3;!"),
        ServiceLocator::BadServiceLocatorException);
    EXPECT_EQ(0U, locators.size());
}

TEST_F(ServiceLocatorTest, constructor) {
    string s("fast+udp: host=example.org, port=8081, port=8082");
    ServiceLocator sl(s);
    EXPECT_EQ(s, sl.originalString);
    EXPECT_EQ("fast+udp", sl.getProtocol());
    EXPECT_EQ("example.org", sl.getOption("host"));
    EXPECT_EQ("8082", sl.getOption("port"));

    CONSTRUCTOR_OK(0U, "fast: ;");
    CONSTRUCTOR_BAD("fast: ; slow: ;");
}

TEST_F(ServiceLocatorTest, init_branches) {
    // parsing protocol fails
    CONSTRUCTOR_BAD("!");

    { // parsing protocol sets sentinel
        ServiceLocator sl("foo:;");
        EXPECT_EQ("foo:", sl.getOriginalString());
    }

    { // parsing protocol is end of string
        ServiceLocator sl("foo:");
        EXPECT_EQ("foo:", sl.getOriginalString());
    }

    // parsing key-value fails
    CONSTRUCTOR_BAD("foo:!");

    { // parsing key-value sets sentinel
        ServiceLocator sl("foo: x=y;");
        EXPECT_EQ("foo: x=y", sl.getOriginalString());
        EXPECT_EQ("y", sl.getOption("x"));
    }

    { // parsing key-value is end of string
        ServiceLocator sl("foo: x=y");
        EXPECT_EQ("foo: x=y", sl.getOriginalString());
        EXPECT_EQ("y", sl.getOption("x"));
    }

    { // value is empty
        ServiceLocator sl("foo: x=");
        EXPECT_EQ("", sl.getOption("x"));
    }

    { // value is quoted
        ServiceLocator sl("foo: x=\",\\\";\"");
        EXPECT_EQ("foo: x=\",\\\";\"", sl.getOriginalString());
        EXPECT_EQ(",\";", sl.getOption("x"));
    }

    { // value is not quoted
        ServiceLocator sl("foo: x=\\,\\\"\\;");
        EXPECT_EQ("foo: x=\\,\\\"\\;", sl.getOriginalString());
        EXPECT_EQ(",\";", sl.getOption("x"));
    }

    { // some leading or trailing whitespace
        ServiceLocator sl("  foo:x=y  ");
        EXPECT_EQ("foo:x=y", sl.getOriginalString());
    }
}

TEST_F(ServiceLocatorTest, init_goodInput) {
    CONSTRUCTOR_OK(0U, "fast:");
    CONSTRUCTOR_OK(2U, " fast : x = y , z = \"a\" , ");
    CONSTRUCTOR_OK(1U, "fast: x=\"=\\\",\"");
    CONSTRUCTOR_OK(0U, "fast+udp:");
    CONSTRUCTOR_OK(0U, "3fast+udp_V4:");
    CONSTRUCTOR_OK(1U, "fast: 3x_Y7=2");
    CONSTRUCTOR_OK(1U, "fast: port=");
}

TEST_F(ServiceLocatorTest, init_badInput) {
    CONSTRUCTOR_BAD("fast");
    CONSTRUCTOR_BAD("fast: x");
    CONSTRUCTOR_BAD("fast ,");
    CONSTRUCTOR_BAD("fast x,");
    CONSTRUCTOR_BAD("fast: x=y,y");
    CONSTRUCTOR_BAD("fast + udp: ");
}

TEST_F(ServiceLocatorTest, init_escapingControl) {
    ServiceLocator sl("fast: a=\\\"\\,\\\", b=\"\\\"\"");
    EXPECT_EQ("\",\"", sl.getOption("a"));
    EXPECT_EQ("\"", sl.getOption("b"));
}

TEST_F(ServiceLocatorTest, init_escapingData) {
    ServiceLocator sl("fast: a=\\x, b=\"\\x\"");
    EXPECT_EQ("\\x", sl.getOption("a"));
    EXPECT_EQ("\\x", sl.getOption("b"));
}

TEST_F(ServiceLocatorTest, getOptionCastNoDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_THROW(sl.getOption<uint8_t>("foo"),
                 ServiceLocator::NoSuchKeyException);
    EXPECT_EQ(8081, sl.getOption<uint16_t>("port"));
    EXPECT_THROW(sl.getOption<uint8_t>("port"),
                            boost::bad_lexical_cast);
}

TEST_F(ServiceLocatorTest, getOptionCastDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_EQ(7, sl.getOption<uint8_t>("foo", 7));
    EXPECT_EQ(8081, sl.getOption<uint16_t>("port", 0));
    EXPECT_THROW(sl.getOption<uint8_t>("port", 4),
                 boost::bad_lexical_cast);
}

TEST_F(ServiceLocatorTest, getOptionNoCastNoDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_THROW(sl.getOption("foo"),
                 ServiceLocator::NoSuchKeyException);
    EXPECT_EQ("8081", sl.getOption("port"));
}

TEST_F(ServiceLocatorTest, getOptionNoCastDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_EQ("7", sl.getOption("foo", "7"));
    EXPECT_STREQ("8081", sl.getOption("port", "0"));
}

TEST_F(ServiceLocatorTest, getOptionCStrNoDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_THROW(sl.getOption<const char*>("foo"),
                 ServiceLocator::NoSuchKeyException);
    EXPECT_STREQ("8081", sl.getOption<const char*>("port"));
}

TEST_F(ServiceLocatorTest, getOptionCStrDefault) {
    ServiceLocator sl("fast: host=example.org, port=8081");
    EXPECT_EQ("7", sl.getOption<const char*>("foo", "7"));
    EXPECT_STREQ("8081", sl.getOption<const char*>("port", "0"));
}

TEST_F(ServiceLocatorTest, hasOption) {
    ServiceLocator sl("cat: meow=1");
    EXPECT_TRUE(sl.hasOption("meow"));
    EXPECT_FALSE(sl.hasOption("moo"));
}

TEST_F(ServiceLocatorTest, assignmentOperator) {
    // Ensure this works as expected to narrow down some other bug...
    ServiceLocator sl("foo: bar=314159");
    ServiceLocator copy = sl;
    EXPECT_TRUE(sl == copy);

    ServiceLocator sl2("mock:");
    ServiceLocator copy2 = sl2;
    EXPECT_TRUE(sl2 == copy2);
}

}  // namespace RAMCloud
