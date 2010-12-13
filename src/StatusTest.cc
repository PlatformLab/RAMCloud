/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.lie
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "Status.h"

namespace RAMCloud {

class StatusTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(StatusTest);
    CPPUNIT_TEST(test_statusToString);
    CPPUNIT_TEST(test_statusToSymbol);
    CPPUNIT_TEST_SUITE_END();

  public:
    StatusTest() { }

    void test_statusToString() {
        // Make sure that Status value 0 always corresponds to success.
        CPPUNIT_ASSERT_EQUAL("operation succeeded",
                statusToString(Status(0)));
        CPPUNIT_ASSERT_EQUAL("object has wrong version",
                statusToString(STATUS_WRONG_VERSION));
        CPPUNIT_ASSERT(statusToString(Status(STATUS_MAX_VALUE)) !=
                       statusToString(Status(STATUS_MAX_VALUE + 1)));
        CPPUNIT_ASSERT_EQUAL("unrecognized RAMCloud error",
                statusToString(Status(STATUS_MAX_VALUE+1)));
    }

    void test_statusToSymbol() {
        // Make sure that Status value 0 always corresponds to success.
        CPPUNIT_ASSERT_EQUAL("STATUS_OK",
                statusToSymbol(Status(0)));
        CPPUNIT_ASSERT_EQUAL("STATUS_WRONG_VERSION",
                statusToSymbol(STATUS_WRONG_VERSION));
        CPPUNIT_ASSERT(statusToSymbol(Status(STATUS_MAX_VALUE)) !=
                       statusToSymbol(Status(STATUS_MAX_VALUE + 1)));
        CPPUNIT_ASSERT_EQUAL("STATUS_UNKNOWN",
                statusToSymbol(Status(STATUS_MAX_VALUE+1)));
    }

    DISALLOW_COPY_AND_ASSIGN(StatusTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(StatusTest);

}  // namespace RAMCloud
