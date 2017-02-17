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
    Driver::Received *recv;

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
        , recv(NULL)
    {
        savedSyscall = UdpDriver::sys;
        sys = new MockSyscall();
        UdpDriver::sys = sys;
    }

    ~UdpDriverTest() {
        delete sys;
        sys = NULL;
        UdpDriver::sys = savedSyscall;
        delete recv;
    }

    // Used to wait for data to arrive on a driver by invoking the
    // dispatcher's receivePackets method; gives up if a long time
    // goes by with no data. Returns the contents of all the incoming
    // packets, separated by commas.
    string receivePackets(UdpDriver* driver, int maxPackets = 5) {
        std::vector<Driver::Received> receivedPackets;
        for (int i = 0; i < 1000; i++) {
            driver->receivePackets(maxPackets, &receivedPackets);
            if (receivedPackets.size() > 0) {
                break;
            }
            usleep(1000);
        }
        if (receivedPackets.size() == 0) {
            return "no packet arrived";
        }
        string result;
        for (uint32_t i = 0; i < receivedPackets.size(); i++) {
            if (i != 0) {
                result.append(", ");
            }
            result.append(receivedPackets[i].payload,
                    receivedPackets[i].len);
        }
        delete recv;
        recv = new Driver::Received(std::move(receivedPackets[0]));
        return result;
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
    // Send a packet from a "client" to a "server" and back again.
    Buffer message;
    const char *testString = "This is a sample message";
    message.appendExternal(testString, downCast<uint32_t>(strlen(testString)));
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("header:This is a sample message",
            receivePackets(&server));

    // Send a response back in the other direction.
    message.reset();
    message.appendExternal("response", 8);
    Buffer::Iterator iterator2(&message);
    server.sendPacket(recv->sender, "h:", 2, &iterator2);
    EXPECT_EQ("h:response", receivePackets(&client));
}

TEST_F(UdpDriverTest, constructor_gbsOption) {
    Cycles::mockCyclesPerSec = 2e09;
    ServiceLocator serverLocator("basic+udp:host=localhost,port=8101,gbs=40");
    UdpDriver driver(&context, &serverLocator);
    EXPECT_EQ(2.5, driver.queueEstimator.bandwidth);
    EXPECT_EQ(40, driver.bandwidthGbps);
    EXPECT_EQ(10000u, driver.maxTransmitQueueSize);

    ServiceLocator serverLocator2("basic+udp:host=localhost,port=8102,gbs=1");
    UdpDriver driver2(&context, &serverLocator2);
    EXPECT_EQ(2800u, driver2.maxTransmitQueueSize);
    Cycles::mockCyclesPerSec = 0;
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
TEST_F(UdpDriverTest, constructor_errorInServerSocketBind) {
    sys->bindErrno = EPERM;
    ServiceLocator serverLocator("basic+udp:host=localhost,port=8101");
    try {
        UdpDriver server2(&context, &serverLocator);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("UdpDriver couldn't bind to locator "
            "'udp:host=localhost,port=8101': Operation not permitted",
            exceptionMessage);
}
TEST_F(UdpDriverTest, constructor_errorInClientSocketBind) {
    sys->bindErrno = EPERM;
    try {
        UdpDriver client2(&context, NULL);
    } catch (DriverException& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("UdpDriver couldn't bind client socket: "
            "Operation not permitted", exceptionMessage);
}

TEST_F(UdpDriverTest, destructor_closeSocket) {
    // If the socket isn't closed, we won't be able to create another
    // UdpDriver that binds to the same socket.
    Tub<UdpDriver> driver2;
    ServiceLocator locator("udp: host=localhost, port=8101");
    driver2.construct(&context, &locator);
    TestLog::reset();
    driver2.destroy();
    EXPECT_EQ("readerThreadMain: reader thread exited", TestLog::get());
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

TEST_F(UdpDriverTest, getTransmitQueueSpace) {
    Cycles::mockTscValue = 10000;
    client.maxTransmitQueueSize = 1000;
    EXPECT_EQ(1000, client.getTransmitQueueSpace(10000));
    sendMessage(&client, &serverAddress, "0123456789", "abcdefghij");
    EXPECT_EQ(980, client.getTransmitQueueSpace(10000));
    sendMessage(&client, &serverAddress, "0123456789", "abcdefghij");
    EXPECT_EQ(960, client.getTransmitQueueSpace(10000));
    EXPECT_EQ(1000, client.getTransmitQueueSpace(1000000));
    Cycles::mockTscValue = 0;
}

TEST_F(UdpDriverTest, receivePackets_noPacketsAvailable) {
    std::vector<Driver::Received> received;
    server.receivePackets(10, &received);
    EXPECT_EQ(0lu, received.size());
}
TEST_F(UdpDriverTest, receivePackets_receivePartialBatches) {
    // First, stall the reader thread, so we can queue up a bunch
    // of packets.
    server.packetBatches[1].packetsAvailable = 2;
    client.sendPacket(&serverAddress, "packet1", 7, NULL);
    EXPECT_EQ("packet1", receivePackets(&server));

    // Queue up a bunch of packets.
    client.sendPacket(&serverAddress, "packet2", 7, NULL);
    client.sendPacket(&serverAddress, "packet3", 7, NULL);
    client.sendPacket(&serverAddress, "packet4", 7, NULL);
    client.sendPacket(&serverAddress, "packet5", 7, NULL);
    client.sendPacket(&serverAddress, "packet6", 7, NULL);

    // Receive packets in 3 separate calls to receivePackets.
    server.packetBatches[1].packetsAvailable = 0;
    EXPECT_EQ("packet2, packet3, packet4", receivePackets(&server, 3));
    EXPECT_EQ(5, server.packetBatches[1].packetsAvailable);
    EXPECT_EQ(3, server.packetBatches[1].packetsRemoved);
    EXPECT_EQ(1, server.currentBatch);
    EXPECT_EQ("packet5", receivePackets(&server, 1));
    EXPECT_EQ(5, server.packetBatches[1].packetsAvailable);
    EXPECT_EQ(4, server.packetBatches[1].packetsRemoved);
    EXPECT_EQ(1, server.currentBatch);
    EXPECT_EQ("packet6", receivePackets(&server));
    EXPECT_EQ(0, server.packetBatches[1].packetsAvailable);
    EXPECT_EQ(0, server.packetBatches[1].packetsRemoved);
    EXPECT_EQ(0, server.currentBatch);
}

TEST_F(UdpDriverTest, sendPacket_alreadyClosed) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.close();
    TestLog::reset();
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(UdpDriverTest, sendPacket_headerEmpty) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "", 0, &iterator);
    EXPECT_EQ("xyzzy", receivePackets(&server));
}

TEST_F(UdpDriverTest, sendPacket_payloadEmpty) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    Buffer::Iterator iterator(&message);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("header:xyzzy", receivePackets(&server));
}

TEST_F(UdpDriverTest, sendPacket_multipleChunks) {
    Buffer message;
    message.appendExternal("xyzzy", 5);
    message.appendExternal("0123456789", 10);
    message.appendExternal("abc", 3);
    Buffer::Iterator iterator(&message, 1, 23);
    client.sendPacket(&serverAddress, "header:", 7, &iterator);
    EXPECT_EQ("header:yzzy0123456789abc", receivePackets(&server));
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

TEST_F(UdpDriverTest, stopReaderThread_basics) {
    client.stopReaderThread();
    TestUtil::waitForLog();
    EXPECT_EQ("readerThreadMain: reader thread exited", TestLog::get());
}
TEST_F(UdpDriverTest, stopReaderThread_errorInGetsockname) {
    sys->getsocknameErrno = EPERM;
    client.stopReaderThread();
    TestUtil::waitForLog();
    EXPECT_EQ("stopReaderThread: getsockname returned error: "
            "Operation not permitted", TestLog::get());
    TestLog::reset();

    // Second time should work fine.
    client.stopReaderThread();
    TestUtil::waitForLog();
    EXPECT_EQ("readerThreadMain: reader thread exited", TestLog::get());
}

TEST_F(UdpDriverTest, readerThreadMain_waitForDispatchThread) {
    server.packetBatches[1].packetsAvailable = 2;
    client.sendPacket(&serverAddress, "packet1", 7, NULL);
    EXPECT_EQ("packet1", receivePackets(&server));

    // The server should now be stuck waiting for batch 1 to become
    // available, so it shouldn't receive the following packet.
    client.sendPacket(&serverAddress, "packet2", 7, NULL);
    usleep(1000);
    EXPECT_TRUE(TestUtil::contains(TestLog::get(), "not keeping up"));
    EXPECT_EQ(2, server.packetBatches[1].packetsAvailable);
    EXPECT_TRUE(server.packetBatches[1].buffers[0] == NULL);
    EXPECT_TRUE(server.packetBatches[1].buffers[1] == NULL);

    // Release batch 1 and make sure that the second packet now arrives.
    server.packetBatches[1].packetsAvailable = 0;
    EXPECT_EQ("packet2", receivePackets(&server));
}
TEST_F(UdpDriverTest, readerThreadMain_exitWhileWaitingForDispatchThread) {
    server.packetBatches[1].packetsAvailable = 2;
    client.sendPacket(&serverAddress, "packet1", 7, NULL);
    EXPECT_EQ("packet1", receivePackets(&server));

    // The server should now be stuck waiting for batch 1 to become
    // available, so it shouldn't receive the following packet.
    usleep(1000);
    EXPECT_TRUE(server.packetBatches[1].buffers[0] == NULL);

    // Tell the thread to exit, and make sure it does exit.
    server.readerThreadExit = true;
    TestUtil::waitForLog("reader thread exited");
    EXPECT_TRUE(TestUtil::contains(TestLog::get(), "reader thread exited"));
}
TEST_F(UdpDriverTest, readerThreadMain_initializeMsgHdrs) {
    client.sendPacket(&serverAddress, "packet1", 7, NULL);
    EXPECT_EQ("packet1", receivePackets(&server));
    EXPECT_EQ(20lu, server.packetBufPool.outstandingObjects);
    EXPECT_TRUE(server.packetBatches[0].buffers[0] == NULL);
    EXPECT_FALSE(server.packetBatches[0].buffers[1] == NULL);
    EXPECT_FALSE(server.packetBatches[1].buffers[0] == NULL);
}
TEST_F(UdpDriverTest, readerThreadMain_errorInRecvmmsg) {
    sys->recvmmsgErrno = EPERM;
    client.sendPacket(&serverAddress, "packet1", 7, NULL);
    EXPECT_EQ("packet1", receivePackets(&server));
    TestUtil::waitForLog();
    EXPECT_EQ("readerThreadMain: UdpDriver error receiving from socket: "
            "Operation not permitted", TestLog::get());

    // Make sure subsequent packets can still be received, even after
    // the error.
    client.sendPacket(&serverAddress, "packet2", 7, NULL);
    EXPECT_EQ("packet2", receivePackets(&server));
}

}  // namespace RAMCloud
