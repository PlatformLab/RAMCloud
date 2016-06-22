/* Copyright (c) 2010-2016 Stanford University
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
#include "MockPacketHandler.h"
#include "MockSyscall.h"
#include "Tub.h"
#include "UdpDriver.h"

namespace RAMCloud {
class UdpDriverTest : public ::testing::Test {
  public:
    Context context;
    string exceptionMessage;
    ServiceLocator serverLocator;
    IpAddress serverAddress;
    UdpDriver server;
    UdpDriver client;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable logEnabler;
    MockPacketHandler *clientHandler, *serverHandler;

    UdpDriverTest()
        : context()
        , exceptionMessage("no exception")
        , serverLocator("udp: host=localhost, port=8100")
        , serverAddress(&serverLocator)
        , server(&context, &serverLocator)
        , client(&context)
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler()
        , clientHandler(NULL)
        , serverHandler(NULL)
    {
        savedSyscall = UdpDriver::sys;
        sys = new MockSyscall();
        UdpDriver::sys = sys;
        clientHandler = new MockPacketHandler(&client);
        serverHandler = new MockPacketHandler(&server);
    }

    ~UdpDriverTest() {
        // Note: don't delete clientHandler or serverHandler; they
        // get deleted automatically by the driver.
        delete sys;
        sys = NULL;
        UdpDriver::sys = savedSyscall;
    }

    void sendMessage(UdpDriver *driver, IpAddress *address,
            const char *header, const char *payload) {
        Buffer message;
        message.appendExternal(payload, downCast<uint32_t>(strlen(payload)));
        Buffer::Iterator iterator(&message);
        driver->sendPacket(address, header, downCast<uint32_t>(strlen(header)),
                           &iterator);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(UdpDriverTest);
};

TEST_F(UdpDriverTest, basics) {
    // Send a packet from a client-style driver to a server-style
    // driver.
    Buffer message;
    const char *testString = "This is a sample message";
    message.appendExternal(testString, downCast<uint32_t>(strlen(testString)));
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_STREQ("header:This is a sample message",
            serverHandler->receivePacket(&context));

    // Send a response back in the other direction.
    message.reset();
    message.appendExternal("response", 8);
    Buffer::Iterator iterator2(&message);
    server.sendPacket(serverHandler->sender, "h:", 2, &iterator2);
    EXPECT_STREQ("h:response", clientHandler->receivePacket(&context));
}

TEST_F(UdpDriverTest, constructor_errorInSocketCall) {
    sys->socketErrno = EPERM;
    try {
        UdpDriver server2(&context, &serverLocator);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("UdpDriver couldn't create socket: "
                "Operation not permitted", exceptionMessage);
}

TEST_F(UdpDriverTest, constructor_socketInUse) {
    try {
        UdpDriver server2(&context, &serverLocator);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("UdpDriver couldn't bind to locator "
            "'udp: host=localhost, port=8100': Address already in use",
            exceptionMessage);
}

TEST_F(UdpDriverTest, destructor_closeSocket) {
    // If the socket isn't closed, we won't be able to create another
    // UdpDriver that binds to the same socket.
    Tub<UdpDriver> driver2;
    ServiceLocator locator("udp: host=localhost, port=8101");
    driver2.construct(&context, &locator);
    driver2.destroy();
    try {
        driver2.construct(&context, &locator);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("no exception", exceptionMessage);
}

TEST_F(UdpDriverTest, close_closeSocket) {
    // If the socket isn't closed, we won't be able to create another
    // UdpDriver that binds to the same socket.
    server.close();
    EXPECT_EQ(-1, server.socketFd);
    try {
        UdpDriver duplicate(&context, &serverLocator);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("no exception", exceptionMessage);
}

TEST_F(UdpDriverTest, close_deleteReadHandler) {
    server.close();
    EXPECT_FALSE(server.readHandler);
}

TEST_F(UdpDriverTest, sendPacket_alreadyClosed) {
    sys->sendmsgErrno = EPERM;
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.close();
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(UdpDriverTest, sendPacket_headerEmpty) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "", 0, &iterator);
    EXPECT_STREQ("xyzzy", serverHandler->receivePacket(&context));
}

TEST_F(UdpDriverTest, sendPacket_payloadEmpty) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_STREQ("header:xyzzy", serverHandler->receivePacket(&context));
}

TEST_F(UdpDriverTest, sendPacket_multipleChunks) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    message.appendExternal("0123456789", 10);
    message.appendExternal("abc", 3);
    Buffer::Iterator iterator(&message, 1, 23);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_STREQ("header:yzzy0123456789abc",
            serverHandler->receivePacket(&context));
}

TEST_F(UdpDriverTest, sendPacket_errorInSend) {
    sys->sendmsgErrno = EPERM;
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("sendPacket: UdpDriver error sending to socket: "
            "Operation not permitted", TestLog::get());
}

TEST_F(UdpDriverTest, ReadHandler_errorInRecv) {
    sys->recvfromErrno = EPERM;
    Driver::Received received;
    server.readHandler->handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_EQ("handleFileEvent: UdpDriver error receiving from socket: "
            "Operation not permitted", TestLog::get());
}

TEST_F(UdpDriverTest, ReadHandler_noPacketAvailable) {
    server.readHandler->handleFileEvent(
            Dispatch::FileEvent::READABLE);
    EXPECT_EQ("", serverHandler->packetData);
}

TEST_F(UdpDriverTest, ReadHandler_multiplePackets) {
    sendMessage(&client, &serverAddress, "header:", "first");
    sendMessage(&client, &serverAddress, "header:", "second");
    sendMessage(&client, &serverAddress, "header:", "third");
    EXPECT_STREQ("header:first, header:second, header:third",
            serverHandler->receivePacket(&context));
    EXPECT_STREQ("no packet arrived", serverHandler->receivePacket(&context));
}

}  // namespace RAMCloud
