/* Copyright (c) 2011 Stanford University
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

#include "TestUtil.h"
#include "InfUdDriver.h"
#include "IpAddress.h"
#include "MockFastTransport.h"

namespace RAMCloud {
class InfUdDriverTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(InfUdDriverTest);
    CPPUNIT_TEST(test_basics);
    CPPUNIT_TEST_SUITE_END();

  public:

    InfUdDriverTest() {}

    void setUp() {
    }

    void tearDown() {
    }

    // Used to wait for data to arrive on a driver by invoking the
    // dispatcher's polling loop; gives up if a long time goes by with
    // no data.
    const char *receivePacket(MockFastTransport *transport) {
        transport->packetData.clear();
        uint64_t start = rdtsc();
        while (true) {
            Dispatch::poll();
            if (transport->packetData.size() != 0) {
                return transport->packetData.c_str();
            }
            if (cyclesToSeconds(rdtsc() - start) > .1) {
                return "no packet arrived";
            }
        }
    }

    void test_basics() {
        // Send a packet from a client-style driver to a server-style
        // driver.
        ServiceLocator serverLocator("fast+infud:");
        InfUdDriver<RealInfiniband> *server =
                new InfUdDriver<RealInfiniband>(&serverLocator);
        MockFastTransport serverTransport(server);
        InfUdDriver<RealInfiniband> *client =
                new InfUdDriver<RealInfiniband>();
        MockFastTransport clientTransport(client);
        Driver::Address* serverAddress =
                client->newAddress(server->getServiceLocator());

        Buffer message;
        const char *testString = "This is a sample message";
        Buffer::Chunk::appendToBuffer(&message, testString,
                downCast<uint32_t>(strlen(testString)));
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL("header:This is a sample message",
                receivePacket(&serverTransport));

        // Send a response back in the other direction.
        message.reset();
        Buffer::Chunk::appendToBuffer(&message, "response", 8);
        Buffer::Iterator iterator2(message);
        server->sendPacket(serverTransport.sender, "h:", 2, &iterator2);
        CPPUNIT_ASSERT_EQUAL("h:response", receivePacket(&clientTransport));
        delete serverAddress;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(InfUdDriverTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(InfUdDriverTest);

}  // namespace RAMCloud
