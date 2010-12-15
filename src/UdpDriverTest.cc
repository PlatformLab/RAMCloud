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

#include "TestUtil.h"
#include "MockSyscall.h"
#include "ObjectTub.h"
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
    CPPUNIT_TEST(test_tryRecvPacket_errorInRecv);
    CPPUNIT_TEST(test_tryRecvPacket_noPacketAvailable);
    CPPUNIT_TEST(test_tryRecvPacket_multiplePackets);
    CPPUNIT_TEST_SUITE_END();

  public:
    string exceptionMessage;
    ServiceLocator *serverLocator;
    IpAddress *serverAddress;
    UdpDriver *server;
    UdpDriver *client;
    MockSyscall* sys;
    Syscall *savedSyscall;
    ObjectTub<TestLog::Enable> logEnabler;

    UdpDriverTest()
        : exceptionMessage()
        , serverLocator(NULL)
        , serverAddress(NULL)
        , server(NULL)
        , client(NULL)
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler()
    {}

    void setUp() {
        exceptionMessage = "no exception";
        serverLocator = new ServiceLocator("udp: host=localhost, port=8100");
        serverAddress = new IpAddress(*serverLocator);
        server = new UdpDriver(serverLocator);
        client = new UdpDriver;
        sys = new MockSyscall();
        savedSyscall = UdpDriver::sys;
        UdpDriver::sys = sys;
        logEnabler.construct();
    }

    void tearDown() {
        delete serverLocator;
        serverLocator = NULL;
        delete serverAddress;
        serverAddress = NULL;
        if (server != NULL) {
            delete server;
            server = NULL;
        }
        delete client;
        client = NULL;
        delete sys;
        sys = NULL;
        UdpDriver::sys = savedSyscall;
        logEnabler.destroy();
    }

    void sendMessage(UdpDriver *driver, IpAddress *address,
            const char *header, const char *payload) {
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, payload, strlen(payload));
        Buffer::Iterator iterator(message);
        driver->sendPacket(address, header, strlen(header), &iterator);
    }

    void test_basics() {
        // Send a packet from a client-style driver to a server-style
        // driver.
        Driver::Received received;
        Buffer message;
        const char *testString = "This is a sample message";
        Buffer::Chunk::appendToBuffer(&message, testString,
                strlen(testString));
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received));
        CPPUNIT_ASSERT_EQUAL("header:This is a sample message",
                toString(received.payload, received.len));

        // Send a response back in the other direction.
        message.reset();
        Buffer::Chunk::appendToBuffer(&message, "response", 8);
        Buffer::Iterator iterator2(message);
        server->sendPacket(received.sender, "h:", 2, &iterator2);
        Driver::Received received2;
        CPPUNIT_ASSERT_EQUAL(true, client->tryRecvPacket(&received2));
        CPPUNIT_ASSERT_EQUAL("h:response",
                toString(received2.payload, received2.len));
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
        delete server;
        server = NULL;
        try {
            server = new UdpDriver(serverLocator);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("no exception", exceptionMessage);
    }

    void test_sendPacket_headerEmpty() {
        Driver::Received received;
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "", 0, &iterator);
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received));
        CPPUNIT_ASSERT_EQUAL("xyzzy",
                toString(received.payload, received.len));
    }

    void test_sendPacket_payloadEmpty() {
        Driver::Received received;
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Iterator iterator(message);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received));
        CPPUNIT_ASSERT_EQUAL("header:xyzzy",
                toString(received.payload, received.len));
    }

    void test_sendPacket_multipleChunks() {
        Driver::Received received;
        Buffer message;
        Buffer::Chunk::appendToBuffer(&message, "xyzzy", 5);
        Buffer::Chunk::appendToBuffer(&message, "0123456789", 10);
        Buffer::Chunk::appendToBuffer(&message, "abc", 3);
        Buffer::Iterator iterator(message, 1, 23);
        client->sendPacket(serverAddress, "header:", 7, &iterator);
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received));
        CPPUNIT_ASSERT_EQUAL("header:yzzy0123456789abc",
                toString(received.payload, received.len));
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

    void test_tryRecvPacket_errorInRecv() {
        sys->recvfromErrno = EPERM;
        Driver::Received received;
        try {
            server->tryRecvPacket(&received);
        } catch (DriverException& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("UdpDriver error receiving from socket: "
                "Operation not permitted", exceptionMessage);
    }

    void test_tryRecvPacket_noPacketAvailable() {
        Driver::Received received;
        CPPUNIT_ASSERT_EQUAL(false, server->tryRecvPacket(&received));
    }

    void test_tryRecvPacket_multiplePackets() {
        Driver::Received received1, received2, received3;
        sendMessage(client, serverAddress, "header:", "first");
        sendMessage(client, serverAddress, "header:", "second");
        sendMessage(client, serverAddress, "header:", "third");
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received1));
        CPPUNIT_ASSERT_EQUAL("header:first",
                toString(received1.payload, received1.len));
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received2));
        CPPUNIT_ASSERT_EQUAL("header:second",
                toString(received2.payload, received2.len));
        CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received3));
        CPPUNIT_ASSERT_EQUAL("header:third",
                toString(received3.payload, received3.len));
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(UdpDriverTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(UdpDriverTest);

}  // namespace RAMCloud
