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

static char saveSendtoBuf[16384];

/**
 * Unit tests for FailureDetector.
 */
class FailureDetectorTest : public ::testing::Test {
  public:
    TestLog::Enable* logEnabler;

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

    FailureDetectorTest()
        : logEnabler(NULL),
          saveSendtoSyscalls(),
          processPacketSyscalls()
    {
        logEnabler = new TestLog::Enable();
    }

    ~FailureDetectorTest()
    {
        delete logEnabler;
    }

    static bool
    failureDetectorFilter(string s)
    {
        return s == "FailureDetector";
    }

    DISALLOW_COPY_AND_ASSIGN(FailureDetectorTest);
};

TEST_F(FailureDetectorTest, constructor) {
    TestLog::Enable _(&failureDetectorFilter);

    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);
    EXPECT_EQ(MASTER, fd.type);
    EXPECT_EQ("tcp:host=10.0.0.1,port=242", fd.coordinator);
    EXPECT_EQ("fast+udp:host=127.0.0.1,port=30293",
        fd.localLocator);
    EXPECT_EQ(0, fd.serverList.server_size());
    EXPECT_FALSE(fd.terminate);
    EXPECT_EQ(0U, fd.queue.entries.size());
    EXPECT_FALSE(fd.haveLoggedNoServers);

    EXPECT_EQ("FailureDetector: listening on UDP socket "
        "127.0.0.1:32404 for incoming pings", TestLog::get());
}

TEST_F(FailureDetectorTest, serviceLocatorStringToSockaddrIn) {
    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);
    sockaddr_in sin;
    sin = fd.serviceLocatorStringToSockaddrIn("infrc:host=1.2.3.4,port=52");
    EXPECT_STREQ("1.2.3.4", inet_ntoa(sin.sin_addr));
    EXPECT_EQ(52 + 2111, NTOHS(sin.sin_port));

    sin = fd.serviceLocatorStringToSockaddrIn("tcp:host=4.3.2.1,port=0;"
        "fast+udp:host=1.9.8.5,port=59;infrc:host=1.2.3.4,port=52");
    EXPECT_STREQ("1.2.3.4", inet_ntoa(sin.sin_addr));
    EXPECT_EQ(52 + 2111, NTOHS(sin.sin_port));

    sin = fd.serviceLocatorStringToSockaddrIn("tcp:host=4.3.2.1,port=0;"
        "fast+udp:host=1.9.8.5,port=59");
    EXPECT_STREQ("1.9.8.5", inet_ntoa(sin.sin_addr));
    EXPECT_EQ(59 + 2111, NTOHS(sin.sin_port));

    EXPECT_THROW(
        fd.serviceLocatorStringToSockaddrIn("bogus:foo=bar"),
        Exception);
}

static bool
handleIncomingRequest(string s)
{
    return s == "handleIncomingRequest";
}

TEST_F(FailureDetectorTest, handleIncomingRequest) {
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
        EXPECT_EQ("handleIncomingRequest: incoming request "
            "from 204.204.204.204:52428", TestLog::get());
        PingRpc::Response* resp =
            reinterpret_cast<PingRpc::Response*>(saveSendtoBuf);
        EXPECT_EQ(STATUS_OK, resp->common.status);
        EXPECT_EQ(0x1122334455667788UL, resp->nonce);
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
        EXPECT_EQ("handleIncomingRequest: incoming request from "
            "204.204.204.204:52428 | handleIncomingRequest: sending proxy "
            "ping to tcp:host=71.53.23.21,port=5723",
            TestLog::get());
        PingRpc::Request* proxiedReq =
            reinterpret_cast<PingRpc::Request*>(saveSendtoBuf);
        EXPECT_EQ(PING, proxiedReq->common.opcode);
        EXPECT_TRUE(proxiedReq->nonce & 0x8000000000000000UL);
        EXPECT_TRUE(fd.queue.dequeue(proxiedReq->nonce));
    }
}

static bool
handleIncomingResponse(string s)
{
    return s == "handleIncomingResponse";
}

TEST_F(FailureDetectorTest, handleIncomingResponse) {
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
    EXPECT_EQ("handleIncomingResponse: incoming ping response "
        "from 204.204.204.204:52428 | handleIncomingResponse: received "
        "invalid nonce -- too late?",
        TestLog::get());

    TestLog::reset();

    fd.queue.enqueue("blah", resp.nonce);
    fd.handleIncomingResponse(reinterpret_cast<char*>(&resp),
        sizeof(resp), &sin);
    EXPECT_EQ("handleIncomingResponse: incoming ping response "
        "from 204.204.204.204:52428 | handleIncomingResponse: received "
        "response from 204.204.204.204:52428",
        TestLog::get());
    EXPECT_FALSE(fd.queue.dequeue());

    TestLog::reset();

    resp.nonce = 0x8000000000000000UL | 10;
    fd.queue.enqueue("blah", resp.nonce);
    fd.handleIncomingResponse(reinterpret_cast<char*>(&resp),
        sizeof(resp), &sin);
    EXPECT_EQ("handleIncomingResponse: incoming ping response "
        "from 204.204.204.204:52428 | handleIncomingResponse: received "
        "response from 204.204.204.204:52428 | handleIncomingResponse: "
        "issued reply to coordinator",
        TestLog::get());
    EXPECT_FALSE(fd.queue.dequeue());
}

TEST_F(FailureDetectorTest, handleCoordinatorResponse) {
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

    EXPECT_EQ(1, fd.serverList.server_size());
    const ProtoBuf::ServerList_Entry& entry(fd.serverList.server(0));
    EXPECT_EQ("hello!", entry.service_locator());
    EXPECT_EQ(12U, entry.server_id());
    EXPECT_EQ(ProtoBuf::ServerType::MASTER, entry.server_type());

    // ensure the list gets cleared each time
    fd.handleCoordinatorResponse(buf,
        length + sizeof(GetServerListRpc::Response), &sin);
    EXPECT_EQ(1, fd.serverList.server_size());
}

static bool
pingRandomServerFilter(string s)
{
    return s == "pingRandomServer";
}

TEST_F(FailureDetectorTest, pingRandomServer) {
    TestLog::Enable _(&pingRandomServerFilter);

    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);
    fd.sys = &saveSendtoSyscalls;

    fd.pingRandomServer();
    EXPECT_EQ("pingRandomServer: No servers besides myself "
        "to probe! List has 0 entries.",
        TestLog::get());

    fd.haveLoggedNoServers = false;
    ProtoBuf::ServerList_Entry& me(*fd.serverList.add_server());
    me.set_service_locator("fast+udp:host=127.0.0.1,port=30293");
    TestLog::reset();
    fd.pingRandomServer();
    EXPECT_EQ("pingRandomServer: No servers besides myself "
        "to probe! List has 1 entries.",
        TestLog::get());

    ProtoBuf::ServerList_Entry& other(*fd.serverList.add_server());
    other.set_service_locator("tcp:host=134.23.42.25,port=2734");
    fd.pingRandomServer();
    PingRpc::Request* req =
        reinterpret_cast<PingRpc::Request*>(saveSendtoBuf);
    EXPECT_EQ(PING, req->common.opcode);
    EXPECT_EQ(0U, req->common.opcode & 0x8000000000000000UL);
    EXPECT_EQ(1U, fd.queue.entries.size());
}

TEST_F(FailureDetectorTest, alertCoordinator) {
    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);

    fd.sys = &saveSendtoSyscalls;

    // first try the random ping case
    FailureDetector::TimeoutQueue::TimeoutEntry te(
        0, "fast+udp:host=1.2.4.6,port=51525", 10);
    fd.alertCoordinator(&te);
    HintServerDownRpc::Request* rpc =
        reinterpret_cast<HintServerDownRpc::Request*>(saveSendtoBuf);
    EXPECT_EQ(HINT_SERVER_DOWN, rpc->common.opcode);
    EXPECT_EQ(33U, rpc->serviceLocatorLength);
    EXPECT_EQ(32U, strlen(&saveSendtoBuf[sizeof(*rpc)]));
    EXPECT_EQ('\0', saveSendtoBuf[sizeof(*rpc) + 32]);
    EXPECT_STREQ("fast+udp:host=1.2.4.6,port=51525",
        &saveSendtoBuf[sizeof(*rpc)]);

    // next try the proxied ping case
    FailureDetector::TimeoutQueue::TimeoutEntry te2(
        0, "fast+udp:host=9.2.3.8,port=21173", 0x8000000000000000UL | 10);
    fd.alertCoordinator(&te2);
    ProxyPingRpc::Response* resp =
        reinterpret_cast<ProxyPingRpc::Response*>(saveSendtoBuf);
    EXPECT_EQ(STATUS_OK, resp->common.status);
    EXPECT_EQ((uint64_t)-1, resp->replyNanoseconds);
}

TEST_F(FailureDetectorTest, processPacket) {
    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);
    TestLog::reset();
    fd.sys = &processPacketSyscalls;
    fd.processPacket(fd.serverFd);
    fd.processPacket(fd.clientFd);
    fd.processPacket(fd.coordFd);
    EXPECT_EQ("handleIncomingRequest: incoming request from "
        "204.204.204.204:52428 | handleIncomingRequest: unknown request "
        "encountered (52428); ignoring | handleIncomingResponse: "
        "incoming ping response from 204.204.204.204:52428 | "
        "handleIncomingResponse: payload isn't 16 bytes, but 0! | "
        "handleCoordinatorResponse: incoming coordinator response from "
        "204.204.204.204:52428 | handleCoordinatorResponse: impossibly "
        "small coordinator response: 0 bytes",
        TestLog::get());
}

TEST_F(FailureDetectorTest, requestServerList) {
    FailureDetector fd("tcp:host=10.0.0.1,port=242",
        "fast+udp:host=127.0.0.1,port=30293", MASTER);
    fd.sys = &saveSendtoSyscalls;
    TestLog::reset();
    fd.requestServerList();
    EXPECT_EQ("requestServerList: requesting server list",
        TestLog::get());
    GetServerListRpc::Request* req =
        reinterpret_cast<GetServerListRpc::Request*>(saveSendtoBuf);
    EXPECT_EQ(GET_SERVER_LIST, req->common.opcode);
    EXPECT_EQ(MASTER, req->serverType);
}

TEST_F(FailureDetectorTest, mainLoop) {
    // XXX- fill me in. there isn't much to do here, but we should
    //      ensure that the select timeout is sensible
}

///////////////////////////////////////////
// Tests for FailureDetector::TimeoutQueue
///////////////////////////////////////////

typedef FailureDetector::TimeoutQueue TimeoutQueue;

TEST_F(FailureDetectorTest, tq_constructor) {
    TimeoutQueue q(523);
    EXPECT_EQ(0U, q.entries.size());
    EXPECT_EQ(523U, q.timeoutUsecs);
}

TEST_F(FailureDetectorTest, tq_enqueue) {
    TimeoutQueue q(523);
    Cycles::mockTscValue = Cycles::fromNanoseconds(12 * 1000 + 100);
    q.enqueue("hello, there", 8734723);
    EXPECT_EQ(1U, q.entries.size());
    EXPECT_EQ("hello, there", q.entries.front().locator);
    EXPECT_EQ(8734723U, q.entries.front().nonce);
    EXPECT_EQ(12U, q.entries.front().startUsec);
    Cycles::mockTscValue = 0;
}

TEST_F(FailureDetectorTest, tq_dequeue) {
    TimeoutQueue q(523);
    Cycles::mockTscValue = Cycles::fromNanoseconds(12 * 1000);
    q.enqueue("hello, there", 8734723);
    EXPECT_FALSE(q.dequeue());
    Cycles::mockTscValue += Cycles::fromNanoseconds(522 * 1000);
    EXPECT_FALSE(q.dequeue());
    Cycles::mockTscValue += Cycles::fromNanoseconds(1 * 1000);
    EXPECT_TRUE(q.dequeue());
    EXPECT_FALSE(q.dequeue());
    EXPECT_EQ(0U, q.entries.size());
    Cycles::mockTscValue = 0;
}

TEST_F(FailureDetectorTest, tq_dequeue_arg) {
    TimeoutQueue q(523);
    q.enqueue("hello, there", 8734723);
    EXPECT_EQ(1U, q.entries.size());
    EXPECT_TRUE(q.dequeue(8734723));
    EXPECT_FALSE(q.dequeue(8734723));
    EXPECT_EQ(0U, q.entries.size());
}

TEST_F(FailureDetectorTest, tq_microsUntilNextTimeout) {
    TimeoutQueue q(523);
    EXPECT_EQ((uint64_t)-1, q.microsUntilNextTimeout());
    Cycles::mockTscValue = Cycles::fromNanoseconds(12 * 1000);
    q.enqueue("hi", 12347234);
    EXPECT_EQ(523U, q.microsUntilNextTimeout());
    Cycles::mockTscValue += Cycles::fromNanoseconds(522 * 1000);
    EXPECT_EQ(1U, q.microsUntilNextTimeout());
    Cycles::mockTscValue += Cycles::fromNanoseconds(1 * 1000);
    EXPECT_EQ(0U, q.microsUntilNextTimeout());
    Cycles::mockTscValue += Cycles::fromNanoseconds(1000 * 1000);
    EXPECT_EQ(0U, q.microsUntilNextTimeout());
    q.dequeue();
    EXPECT_EQ((uint64_t)-1, q.microsUntilNextTimeout());
    Cycles::mockTscValue = 0;
}

} // namespace RAMCloud
