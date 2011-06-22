/* Copyright (c) 2010-2011 Stanford University
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
#include "MockFastTransport.h"
#include "MockSyscall.h"
#include "Tub.h"
#include "UdpDriver.h"

namespace RAMCloud {
class UdpDriverTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(UdpDriverTest);
    CPPUNIT_TEST(test_basics);
    CPPUNIT_TEST(test_constructor_errorInSocketCall);
    CPPUNIT_TEST(test_constructor_socketInUse);
    CPPUNIT_TEST(test_destructor_closeSocket);
    CPPUNIT_TEST(test_sendPacket_headerEmpty);
    CPPUNIT_TEST(test_sendPacket_payloadEmpty);
    CPPUNIT_TEST(test_sendPacket_multipleChunks);
    CPPUNIT_TEST(test_sendPacket_errorInSend);
    CPPUNIT_TEST(test_ReadHandler_errorInRecv);
    CPPUNIT_TEST(test_ReadHandler_noPacketAvailable);
    CPPUNIT_TEST(test_ReadHandler_multiplePackets);
    CPPUNIT_TEST_SUITE_END();

  public:
    string exceptionMessage;
    ServiceLocator *serverLocator;
    IpAddress *serverAddress;
    UdpDriver *server;
    UdpDriver *client;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable* logEnabler;
    MockFastTransport *clientTransport, *serverTransport;

    UdpDriverTest()
        : exceptionMessage()
        , serverLocator(NULL)
        , serverAddress(NULL)
        , server(NULL)
        , client(NULL)
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler(NULL)
        , clientTransport(NULL)
        , serverTransport(NULL)
    {}

    void setUp() {
        savedSyscall = UdpDriver::sys;
        sys = new MockSyscall();
        UdpDriver::sys = sys;
        exceptionMessage = "no exception";
        serverLocator = new ServiceLocator("udp: host=localhost, port=8100");
        serverAddress = new IpAddress(*serverLocator);
        server = new UdpDriver(serverLocator);
        client = new UdpDriver;
        logEnabler = new TestLog::Enable();
        clientTransport = new MockFastTransport(client);
        serverTransport = new MockFastTransport(server);
    }

    void tearDown() {
        delete serverLocator;
        serverLocator = NULL;
        delete serverAddress;
        serverAddress = NULL;
        // Note: deleting the transport deletes the driver implicitly.
        if (serverTransport != NULL) {
            delete serverTransport;
            serverTransport = NULL;
            server = NULL;
        }
        delete clientTransport;
        clientTransport = NULL;
        client = NULL;
        delete sys;
        sys = NULL;
        UdpDriver::sys = savedSyscall;
        delete logEnabler;
    }

    void sendMessage(UdpDriver *driver, IpAddress *address,
            const char *header, const char *payload) {
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, payload,
                                      downCast<uint32_t>(strlen(payload)));
        Buffer::Iterator iterator(message);
        driver->sendPacket(address, header, downCast<uint32_t>(strlen(header)),
                           &iterator);
    }

    // Used to wait for data to arrive on a driver by invoking the
    // dispatcher's polling loop; gives up if a long time goes by with
    // no data.
    const char *receivePacket(MockFastTransport *transport) {
        transport->packetData.clear();
        uint64_t start = rdtsc();
        while (true) {
            dispatch->poll();
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
        Buffer message;
        const char *testString = "This is a sample message";
        Buffer::Chunk::appendToBuffer(&message, testString,
                downCast<uint32_t>(strlen(testString)));
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL("header:This is a sample message",
                receivePacket(serverTransport));

        // Send a response back in the other direction.
        message.reset();
        Buffer::Chunk::appendToBuffer(&message, "response", 8);
        Buffer::Iterator iterator2(message);
        server->sendPacket(serverTransport->sender, "h:", 2, &iterator2);
        CPPUNIT_ASSERT_EQUAL("h:response", receivePacket(clientTransport));
    }

    void test_constructor_errorInSocketCall() {
        sys->socketErrno = EPERM;
        try {
            UdpDriver server2(serverLocator);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("UdpDriver couldn't create socket: "
                    "Operation not permitted", exceptionMessage);
    }

    void test_constructor_socketInUse() {
        try {
            UdpDriver server2(serverLocator);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("UdpDriver couldn't bind to locator "
                "'udp: host=localhost, port=8100': Address already in use",
                exceptionMessage);
    }

    void test_destructor_closeSocket() {
        // If the socket isn't closed, we won't be able to create another
        // UdpDriver that binds to the same socket.
        delete serverTransport;
        serverTransport = NULL;
        try {
            server = new UdpDriver(serverLocator);
            serverTransport = new MockFastTransport(server);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("no exception", exceptionMessage);
    }

    void test_sendPacket_headerEmpty() {
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "", 0, &iterator);
        CPPUNIT_ASSERT_EQUAL("xyzzy", receivePacket(serverTransport));
    }

    void test_sendPacket_payloadEmpty() {
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL("header:xyzzy", receivePacket(serverTransport));
    }

    void test_sendPacket_multipleChunks() {
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Chunk::appendToBuffer(&message, "0123456789", 10);
        Buffer::Chunk::appendToBuffer(&message, "abc", 3);
        Buffer::Iterator iterator(message, 1, 23);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL("header:yzzy0123456789abc",
                receivePacket(serverTransport));
    }

    void test_sendPacket_errorInSend() {
        sys->sendmsgErrno = EPERM;
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Iterator iterator(message);
        try {
            client->sendPacket(serverAddress, "header:", 7, &iterator);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("UdpDriver error sending to socket: "
                "Operation not permitted", exceptionMessage);
    }

    void test_ReadHandler_errorInRecv() {
        sys->recvfromErrno = EPERM;
        Driver::Received received;
        try {
            server->readHandler->handleFileEvent();
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("UdpDriver error receiving from socket: "
                "Operation not permitted", exceptionMessage);
    }

    void test_ReadHandler_noPacketAvailable() {
        server->readHandler->handleFileEvent();
        CPPUNIT_ASSERT_EQUAL("", serverTransport->packetData);
    }

    void test_ReadHandler_multiplePackets() {
        sendMessage(client, serverAddress, "header:", "first");
        sendMessage(client, serverAddress, "header:", "second");
        sendMessage(client, serverAddress, "header:", "third");
        CPPUNIT_ASSERT_EQUAL("header:first", receivePacket(serverTransport));
        CPPUNIT_ASSERT_EQUAL("header:second", receivePacket(serverTransport));
        CPPUNIT_ASSERT_EQUAL("header:third", receivePacket(serverTransport));
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(UdpDriverTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(UdpDriverTest);

}  // namespace RAMCloud
