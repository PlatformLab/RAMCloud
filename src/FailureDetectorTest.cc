/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "TestUtil.h"

#include "FailureDetector.h"
#include "ServerList.pb.h"

namespace RAMCloud {

static char alertSyscallsBuf[16384];

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(FailureDetectorTest); // NOLINT

    CPPUNIT_TEST_SUITE(FailureDetectorTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_serviceLocatorStringToSockaddrIn);
    CPPUNIT_TEST(test_handleIncomingPing);
    CPPUNIT_TEST(test_handleIncomingResponse);
    CPPUNIT_TEST(test_handleCoordinatorResponse);
    CPPUNIT_TEST(test_pingRandomServer);
    CPPUNIT_TEST(test_alertCoordinator);
    CPPUNIT_TEST(test_processPacket);
    CPPUNIT_TEST(test_requestServerList);
    CPPUNIT_TEST(test_mainLoop);
    CPPUNIT_TEST_SUITE_END();

  public:
    TestLog::Enable* logEnabler;

    FailureDetectorTest()
        : logEnabler(NULL),
          alertSyscalls(),
          processPacketSyscalls()
    {
    }

    void
    setUp()
    {
        logEnabler = new TestLog::Enable();
    }

    void
    tearDown()
    {
        delete logEnabler;
    }

    static bool
    failureDetectorFilter(string s)
    {
        return s == "FailureDetector";
    }

    void
    test_constructor()
    {
        TestLog::Enable _(&failureDetectorFilter);

        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        CPPUNIT_ASSERT_EQUAL(MASTER, fd.type);
        CPPUNIT_ASSERT_EQUAL("tcp:host=10.0.0.1,port=242", fd.coordinator);
        CPPUNIT_ASSERT_EQUAL("fast+udp:host=127.0.0.1,port=30293",
            fd.localLocator);
        CPPUNIT_ASSERT_EQUAL(0, fd.serverList.server_size());
        CPPUNIT_ASSERT_EQUAL(false, fd.terminate);
        CPPUNIT_ASSERT_EQUAL(0, fd.queue.entries.size());
        CPPUNIT_ASSERT_EQUAL(false, fd.haveLoggedNoServers);

        CPPUNIT_ASSERT_EQUAL("FailureDetector: listening on UDP socket "
            "127.0.0.1:32404 for incoming pings", TestLog::get());
    }

    void
    test_serviceLocatorStringToSockaddrIn()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        sockaddr_in sin;
        sin = fd.serviceLocatorStringToSockaddrIn("infrc:host=1.2.3.4,port=52");
        CPPUNIT_ASSERT_EQUAL("1.2.3.4", inet_ntoa(sin.sin_addr));
        CPPUNIT_ASSERT_EQUAL(52 + 2111, ntohs(sin.sin_port));

        sin = fd.serviceLocatorStringToSockaddrIn("tcp:host=4.3.2.1,port=0;"
            "fast+udp:host=1.9.8.5,port=59;infrc:host=1.2.3.4,port=52");
        CPPUNIT_ASSERT_EQUAL("1.2.3.4", inet_ntoa(sin.sin_addr));
        CPPUNIT_ASSERT_EQUAL(52 + 2111, ntohs(sin.sin_port));

        sin = fd.serviceLocatorStringToSockaddrIn("tcp:host=4.3.2.1,port=0;"
            "fast+udp:host=1.9.8.5,port=59");
        CPPUNIT_ASSERT_EQUAL("1.9.8.5", inet_ntoa(sin.sin_addr));
        CPPUNIT_ASSERT_EQUAL(59 + 2111, ntohs(sin.sin_port));

        CPPUNIT_ASSERT_THROW(fd.serviceLocatorStringToSockaddrIn("bogus:foo=bar"),
            Exception);
    }

    void
    test_handleIncomingPing()
    {
        // not sure it's worth testing such a simple method
    }

    static bool
    handleIncomingResponse(string s)
    {
        return s == "handleIncomingResponse";
    }

    void
    test_handleIncomingResponse()
    {
#if 0
        TestLog::Enable _(&handleIncomingResponse);

        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        uint64_t nonce = 10;
        sockaddr_in sin;
        memset(&sin, 0xcc, sizeof(sin));
        fd.handleIncomingResponse(reinterpret_cast<char*>(&nonce),
            sizeof(nonce), &sin);
        CPPUNIT_ASSERT_EQUAL("handleIncomingResponse: incoming ping response "
            "from 204.204.204.204:52428 | handleIncomingResponse: received "
            "invalid nonce -- too late?",
            TestLog::get());

        fd.lastNonce = 10;
        fd.handleIncomingResponse(reinterpret_cast<char*>(&nonce),
            sizeof(nonce), &sin);
        CPPUNIT_ASSERT_EQUAL(true, fd.lastResponded);
#endif
    }

    void
    test_handleCoordinatorResponse()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        ProtoBuf::ServerList localList;
        ProtoBuf::ServerList_Entry& server(*localList.add_server());
        server.set_server_type(ProtoBuf::ServerType::MASTER);
        server.set_server_id(12);
        server.set_service_locator("hello!");

        std::ostringstream ostream;
        localList.SerializePartialToOstream(&ostream);
        string str(ostream.str());
        uint32_t length = str.length();

        char buf[length + sizeof(GetServerListRpc::Response)];
        GetServerListRpc::Response* resp =
            reinterpret_cast<GetServerListRpc::Response*>(buf);
        resp->serverListLength = length;
        memcpy(&buf[sizeof(*resp)], str.c_str(), length);

        struct sockaddr_in sin;
        fd.handleCoordinatorResponse(buf,
            length + sizeof(GetServerListRpc::Response), &sin);

        CPPUNIT_ASSERT_EQUAL(1, fd.serverList.server_size());
        const ProtoBuf::ServerList_Entry& entry(fd.serverList.server(0));
        CPPUNIT_ASSERT_EQUAL("hello!", entry.service_locator());
        CPPUNIT_ASSERT_EQUAL(12, entry.server_id());
        CPPUNIT_ASSERT_EQUAL(ProtoBuf::ServerType::MASTER, entry.server_type());

        // ensure the list gets cleared each time
        fd.handleCoordinatorResponse(buf,
            length + sizeof(GetServerListRpc::Response), &sin);
        CPPUNIT_ASSERT_EQUAL(1, fd.serverList.server_size());
    }

    static bool
    pingRandomServerFilter(string s)
    {
        return s == "pingRandomServer";
    }

    void
    test_pingRandomServer()
    {
        TestLog::Enable _(&pingRandomServerFilter);

        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        fd.pingRandomServer();
        CPPUNIT_ASSERT_EQUAL("pingRandomServer: No servers besides myself "
            "to probe! List has 0 entries.",
            TestLog::get());

        fd.haveLoggedNoServers = false;
        ProtoBuf::ServerList_Entry& me(*fd.serverList.add_server());
        me.set_service_locator("fast+udp:host=127.0.0.1,port=30293");
        TestLog::reset();
        fd.pingRandomServer();
        CPPUNIT_ASSERT_EQUAL("pingRandomServer: No servers besides myself "
            "to probe! List has 1 entries.",
            TestLog::get());

        // is it worth interposing somehow to ensure that the call to
        // sendto does the right thing?
    }

    class AlertSyscalls : public Syscall {
        ssize_t
        sendto(int socket, const void *buffer, size_t length, int flags,
            const struct sockaddr *dest_addr, socklen_t dest_len)
        {
            memcpy(alertSyscallsBuf, buffer,
                MIN(length, sizeof(alertSyscallsBuf)));
            return length;
        }
    } alertSyscalls;

    void
    test_alertCoordinator()
    {
#if 0
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        fd.sys = &alertSyscalls;
        ProtoBuf::ServerList_Entry& me(*fd.serverList.add_server());
        me.set_service_locator("fast+udp:host=1.2.4.6,port=51525");
        fd.lastIndex = 0;
        fd.alertCoordinator();

        HintServerDownRpc::Request* rpc = 
            reinterpret_cast<HintServerDownRpc::Request*>(alertSyscallsBuf);
        CPPUNIT_ASSERT_EQUAL(HINT_SERVER_DOWN, rpc->common.type);
        CPPUNIT_ASSERT_EQUAL(33, rpc->serviceLocatorLength);
        CPPUNIT_ASSERT_EQUAL(32, strlen(&alertSyscallsBuf[sizeof(*rpc)]));
        CPPUNIT_ASSERT_EQUAL('\0', alertSyscallsBuf[sizeof(*rpc) + 32]);
        CPPUNIT_ASSERT_EQUAL("fast+udp:host=1.2.4.6,port=51525",
            &alertSyscallsBuf[sizeof(*rpc)]);
#endif
    }

    class ProcessPacketSyscalls : public Syscall {
        ssize_t
        recvfrom(int sockfd, void *buf, size_t len, int flags,
                 sockaddr *from, socklen_t* fromLen)
                
        {
            memset(from, 0xcc, *fromLen);
            return 0;
        }

        ssize_t
        sendto(int socket, const void *buffer, size_t length, int flags,
            const struct sockaddr *destAddr, socklen_t destLen)
        {
            return length;
        }
    } processPacketSyscalls;

    void
    test_processPacket()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        TestLog::reset();
        fd.sys = &processPacketSyscalls;
        fd.processPacket(fd.serverFd);
        fd.processPacket(fd.clientFd);
        fd.processPacket(fd.coordFd);
        CPPUNIT_ASSERT_EQUAL("handleIncomingPing: incoming ping from "
            "204.204.204.204:52428 | handleIncomingResponse: incoming ping "
            "response from 204.204.204.204:52428 | handleIncomingResponse: "
            "payload isn't 8 bytes! | handleCoordinatorResponse: incoming "
            "coordinator response from 204.204.204.204:52428 | "
            "handleCoordinatorResponse: impossibly small coordinator response: "
            "0 bytes",
            TestLog::get()); 
    }

    class RequestServerListSyscalls : public Syscall {
        ssize_t
        sendto(int socket, const void *buffer, size_t length, int flags,
               const struct sockaddr *destAddr, socklen_t destLen)
        {
            return 0;
        }
    };

    void
    test_requestServerList()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        TestLog::reset();
        fd.requestServerList();
        CPPUNIT_ASSERT_EQUAL("",
            TestLog::get());
    }

    void
    test_mainLoop()
    {
        // this is the big boy. lots to do here.

    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(FailureDetectorTest);

} // namespace RAMCloud
