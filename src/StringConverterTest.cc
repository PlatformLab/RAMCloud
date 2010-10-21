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

#include <cmath>

#include <boost/lexical_cast.hpp>
#include "TestUtil.h"

using boost::lexical_cast;
using boost::bad_lexical_cast;

namespace RAMCloud {

class StringConverterTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(StringConverterTest);
    CPPUNIT_TEST(test_convert_toStrings);
    CPPUNIT_TEST(test_convert_toBool);
    CPPUNIT_TEST(test_convert_toFloat);
    CPPUNIT_TEST(test_convert_toDouble);
    CPPUNIT_TEST(test_convert_toSigned);
    CPPUNIT_TEST(test_convert_toUnsigned);
    CPPUNIT_TEST(test_convert_fromEmpty);
    CPPUNIT_TEST(test_convert_from0);
    CPPUNIT_TEST(test_convert_from1);
    CPPUNIT_TEST_SUITE_END();

/**
 * Assert a conversion matches the expected value.
 */
#define CV_EQUAL(type, expected, string) \
  CPPUNIT_ASSERT_NO_THROW(                                              \
      CPPUNIT_ASSERT_EQUAL(expected,                                    \
                           boost::lexical_cast<type>(string)))

/**
 * Assert the given string is out of range for a conversion.
 */
#define CV_OUT_OF_RANGE(type, string) \
  CPPUNIT_ASSERT_THROW(boost::lexical_cast<type>(string),       \
                       boost::bad_lexical_cast)

/**
 * Assert the given string has a bad format for a conversion.
 */
#define CV_BAD_FORMAT(type, string) \
  CPPUNIT_ASSERT_THROW(boost::lexical_cast<type>(string),   \
                       boost::bad_lexical_cast)

  public:
    StringConverterTest() {}

    void test_convert_toStrings() {
        const std::string s("hi");
        CV_EQUAL(std::string, s, std::string("hi"));
    }

    void test_convert_toBool() {
        CV_EQUAL(bool, false, "0");
        CV_EQUAL(bool, false, '0');
        CV_EQUAL(bool, false, false);

        CV_EQUAL(bool, true, "1");
        CV_EQUAL(bool, true, '1');
        CV_EQUAL(bool, true, true);

        CV_BAD_FORMAT(bool, "true");
        CV_BAD_FORMAT(bool, "2");
        CV_BAD_FORMAT(bool, "x");
        CV_BAD_FORMAT(bool, "nope");
    }

    void test_convert_toFloat() {
        CV_EQUAL(float, 0, "0.");
        CV_EQUAL(float, 0.5, "0.5");
        CV_EQUAL(float, 0.5, "+.5");
        CV_EQUAL(float, -0.5, "-.5");

        // lexical_cast cannot handle NAN or INF unlike strtof().

        CV_BAD_FORMAT(float, "x");
        CV_BAD_FORMAT(float, ".");
        CV_BAD_FORMAT(float, "4.x");
        CV_BAD_FORMAT(float, "infcopter");
    }

    void test_convert_toDouble() {
        CV_EQUAL(double, 0, "0.");
        CV_EQUAL(double, 0.5, "0.5");
        CV_EQUAL(double, 0.5, "+.5");
        CV_EQUAL(double, -0.5, "-.5");

        CV_BAD_FORMAT(double, "x");
        CV_BAD_FORMAT(double, ".");
        CV_BAD_FORMAT(double, "4.x");
        CV_BAD_FORMAT(double, "infcopter");
    }

    void test_convert_toSigned() {
        CV_EQUAL(int16_t, -1, "-1");
        CV_EQUAL(int16_t, 1, "1");
        CV_EQUAL(int16_t, 1, "+1");
        CV_EQUAL(int16_t, INT16_MIN, "-32768");
        CV_EQUAL(int16_t, INT16_MAX, "32767");
        CV_EQUAL(int16_t, INT16_MAX, "+32767");
        CV_OUT_OF_RANGE(int16_t, "-32769");
        CV_OUT_OF_RANGE(int16_t, "32768");
        CV_BAD_FORMAT(int16_t, "x");
        CV_BAD_FORMAT(int16_t, "0x");
        CV_BAD_FORMAT(int16_t, "0xy");
        CV_BAD_FORMAT(int16_t, "1x");

        CV_EQUAL(int32_t, -1, "-1");
        CV_EQUAL(int32_t, 1, "1");
        CV_EQUAL(int32_t, 1, "+1");
        CV_EQUAL(int32_t, INT32_MIN, "-2147483648");
        CV_EQUAL(int32_t, INT32_MAX, "2147483647");
        CV_EQUAL(int32_t, INT32_MAX, "+2147483647");
        CV_OUT_OF_RANGE(int32_t, "-2147483649");
        CV_OUT_OF_RANGE(int32_t, "2147483648");
        CV_BAD_FORMAT(int32_t, "x");
        CV_BAD_FORMAT(int32_t, "0x");
        CV_BAD_FORMAT(int32_t, "0xy");
        CV_BAD_FORMAT(int32_t, "1x");

        CV_EQUAL(int64_t, -1, "-1");
        CV_EQUAL(int64_t, 1, "1");
        CV_EQUAL(int64_t, 1, "+1");
        CV_EQUAL(int64_t, INT64_MIN, "-9223372036854775808");
        CV_EQUAL(int64_t, INT64_MAX, "9223372036854775807");
        CV_EQUAL(int64_t, INT64_MAX, "+9223372036854775807");
        CV_OUT_OF_RANGE(int64_t, "-9223372036854775809");
        CV_OUT_OF_RANGE(int64_t, "9223372036854775808");
        CV_BAD_FORMAT(int64_t, "x");
        CV_BAD_FORMAT(int64_t, "0x");
        CV_BAD_FORMAT(int64_t, "0xy");
        CV_BAD_FORMAT(int64_t, "1x");
    }

    void test_convert_toUnsigned() {
        CV_EQUAL(uint8_t, '0', "0");
        CV_EQUAL(uint8_t, '1', "1");
        CV_OUT_OF_RANGE(uint8_t, "256");
        CV_OUT_OF_RANGE(uint8_t, "-1");
        CV_BAD_FORMAT(uint8_t, "0x");
        CV_BAD_FORMAT(uint8_t, "0xy");
        CV_BAD_FORMAT(uint8_t, "1x");

        CV_EQUAL(uint16_t, 0, "0");
        CV_EQUAL(uint16_t, 1, "+1");
        CV_EQUAL(uint16_t, UINT16_MAX, "65535");
        CV_EQUAL(uint16_t, UINT16_MAX, "+65535");
        CV_OUT_OF_RANGE(uint16_t, "65536");
        // "-1" is not out of range - it wraps around.
        CV_BAD_FORMAT(uint16_t, "x");
        CV_BAD_FORMAT(uint16_t, "0x");
        CV_BAD_FORMAT(uint16_t, "0xy");
        CV_BAD_FORMAT(uint16_t, "1x");

        CV_EQUAL(uint32_t, 0, "0");
        CV_EQUAL(uint32_t, 0, "+0");
        CV_EQUAL(uint32_t, UINT32_MAX, "4294967295");
        CV_EQUAL(uint32_t, UINT32_MAX, "+4294967295");
        CV_OUT_OF_RANGE(uint32_t, "4294967296");
        CV_BAD_FORMAT(uint32_t, "x");
        CV_BAD_FORMAT(uint32_t, "0x");
        CV_BAD_FORMAT(uint32_t, "0xy");
        CV_BAD_FORMAT(uint32_t, "1x");

        CV_EQUAL(uint64_t, 1, "1");
        CV_EQUAL(uint64_t, 1, "+1");
        CV_EQUAL(uint64_t, UINT64_MAX, "18446744073709551615");
        CV_EQUAL(uint64_t, UINT64_MAX, "+18446744073709551615");
        CV_OUT_OF_RANGE(uint64_t, "18446744073709551616");
        CV_BAD_FORMAT(uint64_t, "x");
        CV_BAD_FORMAT(uint64_t, "0x");
        CV_BAD_FORMAT(uint64_t, "0xy");
        CV_BAD_FORMAT(uint64_t, "1x");
    }

    void test_convert_fromEmpty() {
        CV_EQUAL(std::string, "", "");
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
    void test_convert_from0() {
        CV_EQUAL(bool,     false, "0");
        CV_EQUAL(float,    0, "0");
        CV_EQUAL(double,   0, "0");
        CV_EQUAL(int8_t,   '0', "0");
        CV_EQUAL(uint8_t,  '0', "0");
        CV_EQUAL(int16_t,  0, "0");
        CV_EQUAL(uint16_t, 0, "0");
        CV_EQUAL(int32_t,  0, "0");
        CV_EQUAL(uint32_t, 0, "0");
        CV_EQUAL(int64_t,  0, "0");
        CV_EQUAL(uint64_t, 0, "0");
    }

    void test_convert_from1() {
        CV_EQUAL(bool,     true, "1");
        CV_EQUAL(float,    1, "1");
        CV_EQUAL(double,   1, "1");
        CV_EQUAL(int8_t,   '1', "1");
        CV_EQUAL(uint8_t,  '1', "1");
        CV_EQUAL(int16_t,  1, "1");
        CV_EQUAL(uint16_t, 1, "1");
        CV_EQUAL(int32_t,  1, "1");
        CV_EQUAL(uint32_t, 1, "1");
        CV_EQUAL(int64_t,  1, "1");
        CV_EQUAL(uint64_t, 1, "1");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(StringConverterTest);

}  // namespace RAMCloud
