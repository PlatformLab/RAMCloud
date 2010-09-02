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

#include <math.h>

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
    CPPUNIT_TEST(test_convertValue_toStrings);
    CPPUNIT_TEST(test_convertValue_toBool);
    CPPUNIT_TEST(test_convertValue_toFloat);
    CPPUNIT_TEST(test_convertValue_toDouble);
    CPPUNIT_TEST(test_convertValue_toSigned);
    CPPUNIT_TEST(test_convertValue_toUnsigned);
    CPPUNIT_TEST(test_convertValue_fromEmpty);
    CPPUNIT_TEST(test_convertValue_from0);
    CPPUNIT_TEST(test_convertValue_from1);
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

/**
 * Assert a convertValue call matches the expected value.
 */
#define CV_EQUAL(type, expected, string) \
    CPPUNIT_ASSERT_NO_THROW( \
        CPPUNIT_ASSERT_EQUAL(expected, \
                             ServiceLocator::convertValue<type>(string)))

/**
 * Assert the given string is out of range for a convertValue call.
 */
#define CV_OUT_OF_RANGE(type, string) \
    CPPUNIT_ASSERT_THROW(ServiceLocator::convertValue<type>(string), \
                         ServiceLocator::OutOfRangeException)

/**
 * Assert the given string has a bad format for a convertValue call.
 */
#define CV_BAD_FORMAT(type, string) \
    CPPUNIT_ASSERT_THROW(ServiceLocator::convertValue<type>(string), \
                         ServiceLocator::BadFormatException)

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
                             ServiceLocator::BadFormatException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint16_t>("host", 4),
                             ServiceLocator::BadFormatException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port"),
                             ServiceLocator::OutOfRangeException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("port", 4),
                             ServiceLocator::OutOfRangeException);
        CPPUNIT_ASSERT_THROW(sl.getOption<uint8_t>("foo"),
                             ServiceLocator::NoSuchKeyException);
        CPPUNIT_ASSERT_EQUAL(7, sl.getOption<uint8_t>("foo", 7));
    }

    void test_convertValue_toStrings() {
        const std::string s("hi");
        CPPUNIT_ASSERT_EQUAL(&s,
                             &ServiceLocator::convertValue<const string&>(s));
        CV_EQUAL(const char*, s.c_str(), s);
    }

    void test_convertValue_toBool() {
        CV_EQUAL(bool, false, "0");
        CV_EQUAL(bool, false, "0");
        CV_EQUAL(bool, false, "n");
        CV_EQUAL(bool, false, "N");
        CV_EQUAL(bool, false, "f");
        CV_EQUAL(bool, false, "F");
        CV_EQUAL(bool, false, "nO");
        CV_EQUAL(bool, false, "FaLsE");

        CV_EQUAL(bool, true, "1");
        CV_EQUAL(bool, true, "y");
        CV_EQUAL(bool, true, "Y");
        CV_EQUAL(bool, true, "t");
        CV_EQUAL(bool, true, "T");
        CV_EQUAL(bool, true, "yEs");
        CV_EQUAL(bool, true, "tRUe");

        CV_BAD_FORMAT(bool, "2");
        CV_BAD_FORMAT(bool, "x");
        CV_BAD_FORMAT(bool, "nope");
    }

    void test_convertValue_toFloat() {
        CV_EQUAL(float, 0, "0.");
        CV_EQUAL(float, 0.5, "0.5");
        CV_EQUAL(float, 0.5, "+.5");
        CV_EQUAL(float, -0.5, "-.5");

        CPPUNIT_ASSERT(isnan(ServiceLocator::convertValue<float>("nan")));
        CPPUNIT_ASSERT(isinf(ServiceLocator::convertValue<float>("inf")));
        CPPUNIT_ASSERT(isinf(ServiceLocator::convertValue<float>("infinity")));

        CV_BAD_FORMAT(float, "x");
        CV_BAD_FORMAT(float, ".");
        CV_BAD_FORMAT(float, "4.x");
        CV_BAD_FORMAT(float, "infcopter");
    }

    void test_convertValue_toDouble() {
        CV_EQUAL(double, 0, "0.");
        CV_EQUAL(double, 0.5, "0.5");
        CV_EQUAL(double, 0.5, "+.5");
        CV_EQUAL(double, -0.5, "-.5");

        CPPUNIT_ASSERT(isnan(ServiceLocator::convertValue<double>("nan")));
        CPPUNIT_ASSERT(isinf(ServiceLocator::convertValue<double>("inf")));
        CPPUNIT_ASSERT(
            isinf(ServiceLocator::convertValue<double>("infinity")));

        CV_BAD_FORMAT(double, "x");
        CV_BAD_FORMAT(double, ".");
        CV_BAD_FORMAT(double, "4.x");
        CV_BAD_FORMAT(double, "infcopter");
    }

    void test_convertValue_toSigned() {
        CV_EQUAL(int8_t, 0, "-0");
        CV_EQUAL(int8_t, 0, "0");
        CV_EQUAL(int8_t, 0, "+0");
        CV_EQUAL(int8_t, INT8_MIN, "-128");
        CV_EQUAL(int8_t, INT8_MIN, "-0x80");
        CV_EQUAL(int8_t, INT8_MAX, "127");
        CV_EQUAL(int8_t, INT8_MAX, "0x7f");
        CV_EQUAL(int8_t, INT8_MAX, "+127");
        CV_EQUAL(int8_t, INT8_MAX, "+0x7f");
        CV_OUT_OF_RANGE(int8_t, "-129");
        CV_OUT_OF_RANGE(int8_t, "128");
        CV_BAD_FORMAT(int8_t, "x");
        CV_BAD_FORMAT(int8_t, "0x");
        CV_BAD_FORMAT(int8_t, "0xy");
        CV_BAD_FORMAT(int8_t, "1x");

        CV_EQUAL(int16_t, 0, "-0");
        CV_EQUAL(int16_t, 0, "0");
        CV_EQUAL(int16_t, 0, "+0");
        CV_EQUAL(int16_t, INT16_MIN, "-32768");
        CV_EQUAL(int16_t, INT16_MIN, "-0x8000");
        CV_EQUAL(int16_t, INT16_MAX, "32767");
        CV_EQUAL(int16_t, INT16_MAX, "0x7fff");
        CV_EQUAL(int16_t, INT16_MAX, "+32767");
        CV_EQUAL(int16_t, INT16_MAX, "+0x7fff");
        CV_OUT_OF_RANGE(int16_t, "-32769");
        CV_OUT_OF_RANGE(int16_t, "32768");
        CV_BAD_FORMAT(int16_t, "x");
        CV_BAD_FORMAT(int16_t, "0x");
        CV_BAD_FORMAT(int16_t, "0xy");
        CV_BAD_FORMAT(int16_t, "1x");

        CV_EQUAL(int32_t, 0, "-0");
        CV_EQUAL(int32_t, 0, "0");
        CV_EQUAL(int32_t, 0, "+0");
        CV_EQUAL(int32_t, INT32_MIN, "-2147483648");
        CV_EQUAL(int32_t, INT32_MIN, "-0x80000000");
        CV_EQUAL(int32_t, INT32_MAX, "2147483647");
        CV_EQUAL(int32_t, INT32_MAX, "0x7fffffff");
        CV_EQUAL(int32_t, INT32_MAX, "+2147483647");
        CV_EQUAL(int32_t, INT32_MAX, "+0x7fffffff");
        CV_OUT_OF_RANGE(int32_t, "-2147483649");
        CV_OUT_OF_RANGE(int32_t, "2147483648");
        CV_BAD_FORMAT(int32_t, "x");
        CV_BAD_FORMAT(int32_t, "0x");
        CV_BAD_FORMAT(int32_t, "0xy");
        CV_BAD_FORMAT(int32_t, "1x");

        CV_EQUAL(int64_t, 0, "-0");
        CV_EQUAL(int64_t, 0, "0");
        CV_EQUAL(int64_t, 0, "+0");
        CV_EQUAL(int64_t, INT64_MIN, "-9223372036854775808");
        CV_EQUAL(int64_t, INT64_MIN, "-0x8000000000000000");
        CV_EQUAL(int64_t, INT64_MAX, "9223372036854775807");
        CV_EQUAL(int64_t, INT64_MAX, "0x7fffffffffffffff");
        CV_EQUAL(int64_t, INT64_MAX, "+9223372036854775807");
        CV_EQUAL(int64_t, INT64_MAX, "+0x7fffffffffffffff");
        CV_OUT_OF_RANGE(int64_t, "-9223372036854775809");
        CV_OUT_OF_RANGE(int64_t, "9223372036854775808");
        CV_BAD_FORMAT(int64_t, "x");
        CV_BAD_FORMAT(int64_t, "0x");
        CV_BAD_FORMAT(int64_t, "0xy");
        CV_BAD_FORMAT(int64_t, "1x");
    }

    void test_convertValue_toUnsigned() {
        CV_EQUAL(uint8_t, 0, "0");
        CV_EQUAL(uint8_t, 0, "+0");
        CV_EQUAL(uint8_t, UINT8_MAX, "255");
        CV_EQUAL(uint8_t, UINT8_MAX, "0xff");
        CV_EQUAL(uint8_t, UINT8_MAX, "+255");
        CV_EQUAL(uint8_t, UINT8_MAX, "+0xff");
        CV_OUT_OF_RANGE(uint8_t, "256");
        CV_BAD_FORMAT(uint8_t, "-0");
        CV_BAD_FORMAT(uint8_t, "x");
        CV_BAD_FORMAT(uint8_t, "0x");
        CV_BAD_FORMAT(uint8_t, "0xy");
        CV_BAD_FORMAT(uint8_t, "1x");

        CV_EQUAL(uint16_t, 0, "0");
        CV_EQUAL(uint16_t, 0, "+0");
        CV_EQUAL(uint16_t, UINT16_MAX, "65535");
        CV_EQUAL(uint16_t, UINT16_MAX, "0xffff");
        CV_EQUAL(uint16_t, UINT16_MAX, "+65535");
        CV_EQUAL(uint16_t, UINT16_MAX, "+0xffff");
        CV_OUT_OF_RANGE(uint16_t, "65536");
        CV_BAD_FORMAT(uint16_t, "-0");
        CV_BAD_FORMAT(uint16_t, "x");
        CV_BAD_FORMAT(uint16_t, "0x");
        CV_BAD_FORMAT(uint16_t, "0xy");
        CV_BAD_FORMAT(uint16_t, "1x");

        CV_EQUAL(uint32_t, 0, "0");
        CV_EQUAL(uint32_t, 0, "+0");
        CV_EQUAL(uint32_t, UINT32_MAX, "4294967295");
        CV_EQUAL(uint32_t, UINT32_MAX, "0xffffffff");
        CV_EQUAL(uint32_t, UINT32_MAX, "+4294967295");
        CV_EQUAL(uint32_t, UINT32_MAX, "+0xffffffff");
        CV_OUT_OF_RANGE(uint32_t, "4294967296");
        CV_BAD_FORMAT(uint32_t, "-0");
        CV_BAD_FORMAT(uint32_t, "x");
        CV_BAD_FORMAT(uint32_t, "0x");
        CV_BAD_FORMAT(uint32_t, "0xy");
        CV_BAD_FORMAT(uint32_t, "1x");

        CV_EQUAL(uint64_t, 0, "0");
        CV_EQUAL(uint64_t, 0, "+0");
        CV_EQUAL(uint64_t, UINT64_MAX, "18446744073709551615");
        CV_EQUAL(uint64_t, UINT64_MAX, "0xffffffffffffffff");
        CV_EQUAL(uint64_t, UINT64_MAX, "+18446744073709551615");
        CV_EQUAL(uint64_t, UINT64_MAX, "+0xffffffffffffffff");
        CV_OUT_OF_RANGE(uint64_t, "18446744073709551616");
        CV_BAD_FORMAT(uint64_t, "-0");
        CV_BAD_FORMAT(uint64_t, "x");
        CV_BAD_FORMAT(uint64_t, "0x");
        CV_BAD_FORMAT(uint64_t, "0xy");
        CV_BAD_FORMAT(uint64_t, "1x");
    }

    void test_convertValue_fromEmpty() {
        CPPUNIT_ASSERT_EQUAL(0,
            ServiceLocator::convertValue<const string&>("").length());
        CV_EQUAL(const char*, "", "");
        CV_BAD_FORMAT(bool,     "");
        CV_BAD_FORMAT(float,    "");
        CV_BAD_FORMAT(double,   "");
        CV_BAD_FORMAT(int8_t,   "");
        CV_BAD_FORMAT(uint8_t,  "");
        CV_BAD_FORMAT(int16_t,  "");
        CV_BAD_FORMAT(uint16_t, "");
        CV_BAD_FORMAT(int32_t,  "");
        CV_BAD_FORMAT(uint32_t, "");
        CV_BAD_FORMAT(int64_t,  "");
        CV_BAD_FORMAT(uint64_t, "");
    }
    void test_convertValue_from0() {
        CV_EQUAL(bool,     0, "0");
        CV_EQUAL(float,    0, "0");
        CV_EQUAL(double,   0, "0");
        CV_EQUAL(int8_t,   0, "0");
        CV_EQUAL(uint8_t,  0, "0");
        CV_EQUAL(int16_t,  0, "0");
        CV_EQUAL(uint16_t, 0, "0");
        CV_EQUAL(int32_t,  0, "0");
        CV_EQUAL(uint32_t, 0, "0");
        CV_EQUAL(int64_t,  0, "0");
        CV_EQUAL(uint64_t, 0, "0");
    }

    void test_convertValue_from1() {
        CV_EQUAL(bool,     1, "1");
        CV_EQUAL(float,    1, "1");
        CV_EQUAL(double,   1, "1");
        CV_EQUAL(int8_t,   1, "1");
        CV_EQUAL(uint8_t,  1, "1");
        CV_EQUAL(int16_t,  1, "1");
        CV_EQUAL(uint16_t, 1, "1");
        CV_EQUAL(int32_t,  1, "1");
        CV_EQUAL(uint32_t, 1, "1");
        CV_EQUAL(int64_t,  1, "1");
        CV_EQUAL(uint64_t, 1, "1");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(ServiceLocatorTest);

}  // namespace RAMCloud
