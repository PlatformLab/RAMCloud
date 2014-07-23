/* Copyright (c) 2010-2014 Stanford University
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
#include "ClientException.h"

namespace RAMCloud {

class ClientExceptionTest : public ::testing::Test {
  public:
    ClientExceptionTest() { }

    DISALLOW_COPY_AND_ASSIGN(ClientExceptionTest);
};

TEST_F(ClientExceptionTest, constructor) {
    ClientException e(HERE, STATUS_WRONG_VERSION);
    EXPECT_EQ(STATUS_WRONG_VERSION, e.status);
}

// No tests for destructor: nothing to test.

TEST_F(ClientExceptionTest, throwException_basics) {
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
            ClientException::throwException(HERE, Status(i));
        } catch (RejectRulesException& e) {
            if (reject.length() != 0) {
                reject += " ";
            }
            reject += e.toSymbol();
            got += "caught exception ";
            got += e.toSymbol();
        } catch (InternalError& e) {
            if (internal.length() != 0) {
                internal += " ";
            }
            internal += e.toSymbol();
            got += "caught exception ";
            got += e.toSymbol();
        } catch (ClientException& e) {
            got += "caught exception ";
            got += e.toSymbol();
        }
        EXPECT_EQ(expected, got);
    }
    EXPECT_EQ("STATUS_OBJECT_DOESNT_EXIST "
              "STATUS_OBJECT_EXISTS STATUS_WRONG_VERSION", reject);
    EXPECT_EQ("STATUS_MESSAGE_TOO_SHORT "
              "STATUS_UNIMPLEMENTED_REQUEST STATUS_REQUEST_FORMAT_ERROR "
              "STATUS_RESPONSE_FORMAT_ERROR STATUS_INTERNAL_ERROR",
              internal);
}
TEST_F(ClientExceptionTest, throwException_useSubclass) {
    // Make sure that a specific subclass is used, for at least one
    // particular status value.
    string msg = "no exception occurred";
    try {
        ClientException::throwException(HERE, STATUS_WRONG_VERSION);
    } catch (WrongVersionException& e) {
        msg = "WrongVersionException thrown";
    } catch (ClientException& e) {
        msg = "ClientException thrown";
    }
    EXPECT_EQ("WrongVersionException thrown", msg);
}
TEST_F(ClientExceptionTest, throwException_unknownStatus) {
    string msg = "no exception occurred";
    Status status;
    try {
        ClientException::throwException(HERE, Status(STATUS_MAX_VALUE+1));
    } catch (InternalError& e) {
        msg = "InternalError thrown";
        status = e.status;
    } catch (ClientException& e) {
        msg = "ClientException thrown";
        status = e.status;
    }
    EXPECT_EQ("InternalError thrown", msg);
    EXPECT_EQ(STATUS_MAX_VALUE+1, status);
}

TEST_F(ClientExceptionTest, toString) {
    ClientException e(HERE, STATUS_WRONG_VERSION);
    EXPECT_STREQ("object has wrong version", e.toString());
}

TEST_F(ClientExceptionTest, toSymbol) {
    ClientException e(HERE, STATUS_WRONG_VERSION);
    EXPECT_STREQ("STATUS_WRONG_VERSION", e.toSymbol());
}

}  // namespace RAMCloud
