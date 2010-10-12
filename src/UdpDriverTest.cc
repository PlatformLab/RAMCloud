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
#include "UdpDriver.h"

namespace RAMCloud {

class UdpDriverTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(UdpDriverTest);
    CPPUNIT_TEST(test_basics);
    CPPUNIT_TEST(test_constructor_socketInUse);
    CPPUNIT_TEST(test_destructor_closeSocket);
    CPPUNIT_TEST(test_sendPacket_headerEmpty);
    CPPUNIT_TEST(test_sendPacket_payloadEmpty);
    CPPUNIT_TEST(test_sendPacket_multipleChunks);
    CPPUNIT_TEST(test_tryRecvPacket_noPacketAvailable);
    CPPUNIT_TEST(test_tryRecvPacket_multiplePackets);
    CPPUNIT_TEST(test_freePacketBufs);
    CPPUNIT_TEST_SUITE_END();

  public:
    char exceptionMessage[200];
    ServiceLocator *serverLocator;
    IpAddress *serverAddress;
    UdpDriver *server;
    UdpDriver *client;

    UdpDriverTest() : serverLocator(NULL), serverAddress(NULL),
            server(NULL), client(NULL) {}

    void setUp() {
        snprintf(exceptionMessage, sizeof(exceptionMessage), "%s",
                "no exception");
        serverLocator = new ServiceLocator("udp: host=localhost, port=8100");
        serverAddress = new IpAddress(*serverLocator);
        server = new UdpDriver(serverLocator);
        client = new UdpDriver;
    }

    void tearDown() {
        delete serverLocator;
        delete serverAddress;
        delete server;
        delete client;
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

    void test_constructor_socketInUse() {
        try {
            UdpDriver server2(serverLocator);
        } catch (UnrecoverableDriverException& e) {
            snprintf(exceptionMessage, sizeof(exceptionMessage), "%s",
                    e.message.c_str());
        }
        CPPUNIT_ASSERT_EQUAL("Address already in use", exceptionMessage);
    }

    void test_destructor_closeSocket() {
        delete server;
        server = NULL;
        try {
            server = new UdpDriver(serverLocator);
        } catch (UnrecoverableDriverException& e) {
            snprintf(exceptionMessage, sizeof(exceptionMessage), "%s",
                    e.message.c_str());
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

    void test_freePacketBufs() {
        // This test exercises the facilities for reusing old packet buffers
        // without having to call malloc for each received packet.

        // First, force the allocation of 3 packet buffers and make sure they
        // get freed when the Received structures are destroyed.
        Driver::Received received1;
        {
            Driver::Received received2, received3;
            sendMessage(client, serverAddress, "header:", "first");
            sendMessage(client, serverAddress, "header:", "second");
            sendMessage(client, serverAddress, "header:", "third");
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received1));
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received2));
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received3));
            CPPUNIT_ASSERT_EQUAL(0, server->freePacketBufs.size());
        }
        CPPUNIT_ASSERT_EQUAL(2, server->freePacketBufs.size());

        // Now receive 3 more messages and make sure they use existing buffers,
        // if available.
        {
            Driver::Received received4, received5, received6;
            sendMessage(client, serverAddress, "header:", "fourth");
            sendMessage(client, serverAddress, "header:", "fifth");
            sendMessage(client, serverAddress, "header:", "sixth");
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received4));
            CPPUNIT_ASSERT_EQUAL(1, server->freePacketBufs.size());
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received5));
            CPPUNIT_ASSERT_EQUAL(0, server->freePacketBufs.size());
            CPPUNIT_ASSERT_EQUAL(true, server->tryRecvPacket(&received6));
            CPPUNIT_ASSERT_EQUAL(0, server->freePacketBufs.size());
        }
        CPPUNIT_ASSERT_EQUAL(3, server->freePacketBufs.size());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(UdpDriverTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(UdpDriverTest);

}  // namespace RAMCloud
