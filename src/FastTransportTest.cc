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

/**
 * \file
 * Unit tests for #RAMCloud::FastTransport.
 */

#include <TestUtil.h>
#include <FastTransport.h>

namespace RAMCloud {

class FastTransportTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(FastTransportTest);
    CPPUNIT_TEST(test_serverRecv);
    CPPUNIT_TEST_SUITE_END();

  public:
    FastTransportTest() {}

    void test_serverRecv() {
        FastTransport transport(NULL);
        FastTransport::ServerRPC rpc(NULL, 0);
        TAILQ_INSERT_TAIL(&transport.serverReadyQueue, &rpc, readyQueueEntries);
        CPPUNIT_ASSERT_EQUAL(&rpc, transport.serverRecv());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(FastTransportTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(FastTransportTest);

}  // namespace RAMCloud
