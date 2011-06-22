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

#include "BenchUtil.h"
#include "FailureDetector.h"
#include "ServerList.pb.h"

namespace RAMCloud {

static char saveSendtoBuf[16384];

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public CppUnit::TestFixture {

    DISALLOW_COPY_AND_ASSIGN(FailureDetectorTest); // NOLINT

    CPPUNIT_TEST_SUITE(FailureDetectorTest);
    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_serviceLocatorStringToSockaddrIn);
    CPPUNIT_TEST(test_handleIncomingRequest);
    CPPUNIT_TEST(test_handleIncomingResponse);
    CPPUNIT_TEST(test_handleCoordinatorResponse);
    CPPUNIT_TEST(test_pingRandomServer);
    CPPUNIT_TEST(test_alertCoordinator);
    CPPUNIT_TEST(test_processPacket);
    CPPUNIT_TEST(test_requestServerList);
    CPPUNIT_TEST(test_mainLoop);
    CPPUNIT_TEST(test_tq_constructor);
    CPPUNIT_TEST(test_tq_enqueue);
    CPPUNIT_TEST(test_tq_dequeue);
    CPPUNIT_TEST(test_tq_dequeue_arg);
    CPPUNIT_TEST(test_tq_microsUntilNextTimeout);
    CPPUNIT_TEST_SUITE_END();

  public:
    TestLog::Enable* logEnabler;

    FailureDetectorTest()
        : logEnabler(NULL),
          saveSendtoSyscalls(),
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

///////////////////////////////////////////
// Tests for FailureDetector
///////////////////////////////////////////

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

        CPPUNIT_ASSERT_THROW(
            fd.serviceLocatorStringToSockaddrIn("bogus:foo=bar"),
            Exception);
    }

    class SaveSendtoSyscalls : public Syscall {
        ssize_t
        sendto(int socket, const void *buffer, size_t length, int flags,
            const struct sockaddr *dest_addr, socklen_t dest_len)
        {
            memcpy(saveSendtoBuf, buffer,
                MIN(length, sizeof(saveSendtoBuf)));
            return length;
        }
    } saveSendtoSyscalls;

    static bool
    handleIncomingRequest(string s)
    {
        return s == "handleIncomingRequest";
    }

    void
    test_handleIncomingRequest()
    {
        TestLog::Enable _(&handleIncomingRequest);

        sockaddr_in sin;
        memset(&sin, 0xcc, sizeof(sin));

        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        fd.sys = &saveSendtoSyscalls;

        {
            PingRpc::Request req;
            req.common.opcode = PING;
            req.nonce = 0x1122334455667788UL;
            fd.handleIncomingRequest(reinterpret_cast<char*>(&req),
                sizeof(req), &sin);
            CPPUNIT_ASSERT_EQUAL("handleIncomingRequest: incoming request "
                "from 204.204.204.204:52428", TestLog::get());
            PingRpc::Response* resp =
                reinterpret_cast<PingRpc::Response*>(saveSendtoBuf);
            CPPUNIT_ASSERT_EQUAL(STATUS_OK, resp->common.status);
            CPPUNIT_ASSERT_EQUAL(0x1122334455667788UL, resp->nonce);
        }

        TestLog::reset();

        {
            char buf[500];
            ProxyPingRpc::Request* req =
                reinterpret_cast<ProxyPingRpc::Request*>(buf);
            req->common.opcode = PROXY_PING;
            req->timeoutNanoseconds = -1;    // we ignore this
            req->serviceLocatorLength = 31;
            memcpy(&buf[sizeof(*req)], "tcp:host=71.53.23.21,port=5723\0", 31);
            fd.handleIncomingRequest(reinterpret_cast<char*>(req),
                sizeof(*req) + 31, &sin);
            CPPUNIT_ASSERT_EQUAL("handleIncomingRequest: incoming request from "
                "204.204.204.204:52428 | handleIncomingRequest: sending proxy "
                "ping to tcp:host=71.53.23.21,port=5723",
                TestLog::get());
            PingRpc::Request* proxiedReq =
                reinterpret_cast<PingRpc::Request*>(saveSendtoBuf);
            CPPUNIT_ASSERT_EQUAL(PING, proxiedReq->common.opcode);
            CPPUNIT_ASSERT(proxiedReq->nonce & 0x8000000000000000UL);
            CPPUNIT_ASSERT_EQUAL(true, fd.queue.dequeue(proxiedReq->nonce));
        }
    }

    static bool
    handleIncomingResponse(string s)
    {
        return s == "handleIncomingResponse";
    }

    void
    test_handleIncomingResponse()
    {
        TestLog::Enable _(&handleIncomingResponse);

        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        PingRpc::Response resp;
        resp.common.status = STATUS_OK;
        resp.nonce = 10;

        sockaddr_in sin;
        memset(&sin, 0xcc, sizeof(sin));
        fd.handleIncomingResponse(reinterpret_cast<char*>(&resp),
            sizeof(resp), &sin);
        CPPUNIT_ASSERT_EQUAL("handleIncomingResponse: incoming ping response "
            "from 204.204.204.204:52428 | handleIncomingResponse: received "
            "invalid nonce -- too late?",
            TestLog::get());

        TestLog::reset();

        fd.queue.enqueue("blah", resp.nonce);
        fd.handleIncomingResponse(reinterpret_cast<char*>(&resp),
            sizeof(resp), &sin);
        CPPUNIT_ASSERT_EQUAL("handleIncomingResponse: incoming ping response "
            "from 204.204.204.204:52428 | handleIncomingResponse: received "
            "response from 204.204.204.204:52428",
            TestLog::get());
        CPPUNIT_ASSERT_EQUAL(false, fd.queue.dequeue());

        TestLog::reset();

        resp.nonce = 0x8000000000000000UL | 10;
        fd.queue.enqueue("blah", resp.nonce);
        fd.handleIncomingResponse(reinterpret_cast<char*>(&resp),
            sizeof(resp), &sin);
        CPPUNIT_ASSERT_EQUAL("handleIncomingResponse: incoming ping response "
            "from 204.204.204.204:52428 | handleIncomingResponse: received "
            "response from 204.204.204.204:52428 | handleIncomingResponse: "
            "issued reply to coordinator",
            TestLog::get());
        CPPUNIT_ASSERT_EQUAL(false, fd.queue.dequeue());
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
        uint32_t length = downCast<uint32_t>(str.length());

        char buf[length + sizeof(GetServerListRpc::Response)];
        GetServerListRpc::Response* resp =
            reinterpret_cast<GetServerListRpc::Response*>(buf);
        resp->serverListLength = length;
        memcpy(&buf[sizeof(*resp)], str.c_str(), length);

        struct sockaddr_in sin;
        memset(&sin, 0xcc, sizeof(sin));
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
        fd.sys = &saveSendtoSyscalls;

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

        ProtoBuf::ServerList_Entry& other(*fd.serverList.add_server());
        other.set_service_locator("tcp:host=134.23.42.25,port=2734");
        fd.pingRandomServer();
        PingRpc::Request* req =
            reinterpret_cast<PingRpc::Request*>(saveSendtoBuf);
        CPPUNIT_ASSERT_EQUAL(PING, req->common.opcode);
        CPPUNIT_ASSERT_EQUAL(0, req->common.opcode & 0x8000000000000000UL);
        CPPUNIT_ASSERT_EQUAL(1, fd.queue.entries.size());
    }

    void
    test_alertCoordinator()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);

        fd.sys = &saveSendtoSyscalls;

        // first try the random ping case
        FailureDetector::TimeoutQueue::TimeoutEntry te(
            0, "fast+udp:host=1.2.4.6,port=51525", 10);
        fd.alertCoordinator(&te);
        HintServerDownRpc::Request* rpc =
            reinterpret_cast<HintServerDownRpc::Request*>(saveSendtoBuf);
        CPPUNIT_ASSERT_EQUAL(HINT_SERVER_DOWN, rpc->common.opcode);
        CPPUNIT_ASSERT_EQUAL(33, rpc->serviceLocatorLength);
        CPPUNIT_ASSERT_EQUAL(32, strlen(&saveSendtoBuf[sizeof(*rpc)]));
        CPPUNIT_ASSERT_EQUAL('\0', saveSendtoBuf[sizeof(*rpc) + 32]);
        CPPUNIT_ASSERT_EQUAL("fast+udp:host=1.2.4.6,port=51525",
            &saveSendtoBuf[sizeof(*rpc)]);

        // next try the proxied ping case
        FailureDetector::TimeoutQueue::TimeoutEntry te2(
            0, "fast+udp:host=9.2.3.8,port=21173", 0x8000000000000000UL | 10);
        fd.alertCoordinator(&te2);
        ProxyPingRpc::Response* resp =
            reinterpret_cast<ProxyPingRpc::Response*>(saveSendtoBuf);
        CPPUNIT_ASSERT_EQUAL(STATUS_OK, resp->common.status);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, resp->replyNanoseconds);
    }

    class ProcessPacketSyscalls : public Syscall {
        ssize_t
        recvfrom(int sockfd, void *buf, size_t len, int flags,
                 sockaddr *from, socklen_t* fromLen)
        {
            memset(from, 0xcc, *fromLen);
            memset(buf, 0xcc, len);
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
        CPPUNIT_ASSERT_EQUAL("handleIncomingRequest: incoming request from "
            "204.204.204.204:52428 | handleIncomingRequest: unknown request "
            "encountered (52428); ignoring | handleIncomingResponse: "
            "incoming ping response from 204.204.204.204:52428 | "
            "handleIncomingResponse: payload isn't 16 bytes, but 0! | "
            "handleCoordinatorResponse: incoming coordinator response from "
            "204.204.204.204:52428 | handleCoordinatorResponse: impossibly "
            "small coordinator response: 0 bytes",
            TestLog::get());
    }

    void
    test_requestServerList()
    {
        FailureDetector fd("tcp:host=10.0.0.1,port=242",
            "fast+udp:host=127.0.0.1,port=30293", MASTER);
        fd.sys = &saveSendtoSyscalls;
        TestLog::reset();
        fd.requestServerList();
        CPPUNIT_ASSERT_EQUAL("requestServerList: requesting server list",
            TestLog::get());
        GetServerListRpc::Request* req =
            reinterpret_cast<GetServerListRpc::Request*>(saveSendtoBuf);
        CPPUNIT_ASSERT_EQUAL(GET_SERVER_LIST, req->common.opcode);
        CPPUNIT_ASSERT_EQUAL(MASTER, req->serverType);
    }

    void
    test_mainLoop()
    {
        // XXX- fill me in. there isn't much to do here, but we should
        //      ensure that the select timeout is sensible
    }

///////////////////////////////////////////
// Tests for FailureDetector::TimeoutQueue
///////////////////////////////////////////

    typedef FailureDetector::TimeoutQueue TimeoutQueue;

    void
    test_tq_constructor()
    {
        TimeoutQueue q(523);
        CPPUNIT_ASSERT_EQUAL(0, q.entries.size());
        CPPUNIT_ASSERT_EQUAL(523, q.timeoutUsecs);
    }

    void
    test_tq_enqueue()
    {
        TimeoutQueue q(523);
        mockTSCValue = nanosecondsToCycles(12 * 1000);
        q.enqueue("hello, there", 8734723);
        CPPUNIT_ASSERT_EQUAL(1, q.entries.size());
        CPPUNIT_ASSERT_EQUAL("hello, there", q.entries.front().locator);
        CPPUNIT_ASSERT_EQUAL(8734723, q.entries.front().nonce);
        CPPUNIT_ASSERT_EQUAL(12, q.entries.front().startUsec);
        mockTSCValue = 0;
    }

    void
    test_tq_dequeue()
    {
        TimeoutQueue q(523);
        mockTSCValue = nanosecondsToCycles(12 * 1000);
        q.enqueue("hello, there", 8734723);
        CPPUNIT_ASSERT_EQUAL(false, q.dequeue());
        mockTSCValue += nanosecondsToCycles(522 * 1000);
        CPPUNIT_ASSERT_EQUAL(false, q.dequeue());
        mockTSCValue += nanosecondsToCycles(1 * 1000);
        CPPUNIT_ASSERT_EQUAL(true, q.dequeue());
        CPPUNIT_ASSERT_EQUAL(false, q.dequeue());
        CPPUNIT_ASSERT_EQUAL(0, q.entries.size());
        mockTSCValue = 0;
    }

    void
    test_tq_dequeue_arg()
    {
        TimeoutQueue q(523);
        q.enqueue("hello, there", 8734723);
        CPPUNIT_ASSERT_EQUAL(1, q.entries.size());
        CPPUNIT_ASSERT_EQUAL(true, q.dequeue(8734723));
        CPPUNIT_ASSERT_EQUAL(false, q.dequeue(8734723));
        CPPUNIT_ASSERT_EQUAL(0, q.entries.size());
    }

    void
    test_tq_microsUntilNextTimeout()
    {
        TimeoutQueue q(523);
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, q.microsUntilNextTimeout());
        mockTSCValue = nanosecondsToCycles(12 * 1000);
        q.enqueue("hi", 12347234);
        CPPUNIT_ASSERT_EQUAL(523, q.microsUntilNextTimeout());
        mockTSCValue += nanosecondsToCycles(522 * 1000);
        CPPUNIT_ASSERT_EQUAL(1, q.microsUntilNextTimeout());
        mockTSCValue += nanosecondsToCycles(1 * 1000);
        CPPUNIT_ASSERT_EQUAL(0, q.microsUntilNextTimeout());
        mockTSCValue += nanosecondsToCycles(1000 * 1000);
        CPPUNIT_ASSERT_EQUAL(0, q.microsUntilNextTimeout());
        q.dequeue();
        CPPUNIT_ASSERT_EQUAL((uint64_t)-1, q.microsUntilNextTimeout());
        mockTSCValue = 0;
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(FailureDetectorTest);

} // namespace RAMCloud
