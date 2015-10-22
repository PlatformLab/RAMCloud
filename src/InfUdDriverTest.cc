/* Copyright (c) 2011-2015 Stanford University
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
class InfUdDriverTest : public ::testing::Test {
  public:
    Context context;
    TestLog::Enable logEnabler;

    InfUdDriverTest()
        : context()
        , logEnabler()
    {}

    // Used to wait for data to arrive on a driver by invoking the
    // dispatcher's polling loop; gives up if a long time goes by with
    // no data.
    const char *receivePacket(MockFastTransport *transport) {
        transport->packetData.clear();
        uint64_t start = Cycles::rdtsc();
        while (true) {
            context.dispatch->poll();
            if (transport->packetData.size() != 0) {
                return transport->packetData.c_str();
            }
            if (Cycles::toSeconds(Cycles::rdtsc() - start) > .1) {
                return "no packet arrived";
            }
        }
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(InfUdDriverTest);
};

TEST_F(InfUdDriverTest, basics) {
    // Send a packet from a client-style driver to a server-style
    // driver.
    ServiceLocator serverLocator("fast+infud:");
    InfUdDriver *server =
            new InfUdDriver(&context, &serverLocator, false);
    MockFastTransport serverTransport(&context, server);
    InfUdDriver *client =
            new InfUdDriver(&context, NULL, false);
    MockFastTransport clientTransport(&context, client);
    ServiceLocator sl(server->getServiceLocator());
    Driver::Address* serverAddress = client->newAddress(&sl);

    Buffer message;
    const char *testString = "This is a sample message";
    message.appendExternal(testString, downCast<uint32_t>(strlen(testString)));
    Buffer::Iterator iterator(&message);
    client->sendPacket(serverAddress, "header:", 7, &iterator);
    TestLog::reset();
    EXPECT_STREQ("header:This is a sample message",
            receivePacket(&serverTransport));
    EXPECT_EQ("", TestLog::get());

    // Send a response back in the other direction.
    message.reset();
    message.appendExternal("response", 8);
    Buffer::Iterator iterator2(&message);
    server->sendPacket(serverTransport.sender, "h:", 2, &iterator2);
    EXPECT_STREQ("h:response", receivePacket(&clientTransport));
    delete serverAddress;
}

}  // namespace RAMCloud
