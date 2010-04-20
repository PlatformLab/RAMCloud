/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <Common.h>
#include <TCPTransport.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

/**
 * Unit tests for TCPTransport.
 */
class TCPTransportTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(TCPTransportTest); // NOLINT

    CPPUNIT_TEST_SUITE(TCPTransportTest);
    CPPUNIT_TEST(test_foo);
    CPPUNIT_TEST_SUITE_END();

  public:
    TCPTransportTest() {}

    void test_foo()
    {
        TCPTransport x(SVRADDR, SVRPORT);
        static_assert(Transport::ServerToken::BUF_SIZE >=
                      sizeof(TCPTransport::TCPServerToken));
        static_assert(Transport::ClientToken::BUF_SIZE >=
                      sizeof(TCPTransport::TCPClientToken));

        // TODO(ongaro): testing
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(TCPTransportTest);

} // namespace RAMCloud
