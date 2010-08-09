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

/**
 * \file
 * Unit tests for everything defined by ClientException.h.
 */

#include <ClientException.h>
#include <TestUtil.h>

namespace RAMCloud {

class ClientExceptionTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(ClientExceptionTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_throwException_basics);
    CPPUNIT_TEST(test_throwException_unknownStatus);
    CPPUNIT_TEST(test_throwException_useSubclass);
    CPPUNIT_TEST(test_toString);
    CPPUNIT_TEST(test_toSymbol);
    CPPUNIT_TEST_SUITE_END();

  public:
    ClientExceptionTest() { }

    void test_constructor() {
        ClientException e(STATUS_WRONG_VERSION);
        CPPUNIT_ASSERT_EQUAL(STATUS_WRONG_VERSION, e.status);
    }

    // No tests for destructor: nothing to test.

    void test_throwException_basics() {
        // Try throwing all possible exceptions:
        // * Make sure each exception is thrown
        // * Check which exceptions also belong to interesting subclasses
        //   such as RejectRulesException or InternalError.
        string internal, reject;
        for (int i = 0; i <= STATUS_MAX_VALUE; i++) {
            string expected = "caught exception ";
            expected += statusToSymbol(Status(i));
            string got;
            try {
                ClientException::throwException(Status(i));
            } catch (RejectRulesException e) {
                if (reject.length() != 0) {
                    reject += " ";
                }
                reject += e.toSymbol();
                got += "caught exception ";
                got += e.toSymbol();
            } catch (InternalError e) {
                if (internal.length() != 0) {
                    internal += " ";
                }
                internal += e.toSymbol();
                got += "caught exception ";
                got += e.toSymbol();
            } catch (ClientException e) {
                got += "caught exception ";
                got += e.toSymbol();
            }
            CPPUNIT_ASSERT_EQUAL(expected, got);
        }
        CPPUNIT_ASSERT_EQUAL("STATUS_OBJECT_DOESNT_EXIST "
                "STATUS_OBJECT_EXISTS STATUS_WRONG_VERSION", reject);
        CPPUNIT_ASSERT_EQUAL("STATUS_MESSAGE_TOO_SHORT "
                "STATUS_UNIMPLEMENTED_REQUEST STATUS_REQUEST_FORMAT_ERROR "
                "STATUS_RESPONSE_FORMAT_ERROR", internal);
    }
    void test_throwException_useSubclass() {
        // Make sure that a specific subclass is used, for at least one
        // particular status value.
        string msg = "no exception occurred";
        try {
            ClientException::throwException(STATUS_WRONG_VERSION);
        } catch (WrongVersionException e) {
            msg = "WrongVersionException thrown";
        } catch (ClientException e) {
            msg = "ClientException thrown";
        }
        CPPUNIT_ASSERT_EQUAL("WrongVersionException thrown", msg);
    }
    void test_throwException_unknownStatus() {
        string msg = "no exception occurred";
        Status status;
        try {
            ClientException::throwException(Status(STATUS_MAX_VALUE+1));
        } catch (InternalError e) {
            msg = "InternalError thrown";
            status = e.status;
        } catch (ClientException e) {
            msg = "ClientException thrown";
            status = e.status;
        }
        CPPUNIT_ASSERT_EQUAL("InternalError thrown", msg);
        CPPUNIT_ASSERT_EQUAL(STATUS_MAX_VALUE+1, status);
    }

    void test_toString() {
        ClientException e(STATUS_WRONG_VERSION);
        CPPUNIT_ASSERT_EQUAL("object has wrong version", e.toString());
    }

    void test_toSymbol() {
        ClientException e(STATUS_WRONG_VERSION);
        CPPUNIT_ASSERT_EQUAL("STATUS_WRONG_VERSION", e.toSymbol());
    }

    DISALLOW_COPY_AND_ASSIGN(ClientExceptionTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(ClientExceptionTest);

}  // namespace RAMCloud
