/* Copyright (c) 2011-2012 Stanford University
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
#include "CoordinatorServerList.h"
#include "MockTransport.h"
#include "ServerTracker.h"
#include "ServerListUpdater.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {


namespace {
/// Deletes all messages on an updater
static void
deleteMessageQueue(ServerListUpdater& updater) {
    while (!updater.msgQueue.empty())
        updater.msgQueue.pop();
}
}

class ServerListUpdaterTest : public ::testing::Test {
  public:
    Context context;
    CoordinatorServerList csl;
    ServerListUpdater updater;
    ServerId id1, id2;
    ProtoBuf::ServerList psl1, psl2;
    ServerListUpdater::Message msg;
    MockTransport transport;
    TransportManager::MockRegistrar registrar;

    ServerListUpdaterTest()
        : context()
        , csl(context)
        , updater(context, csl)
        , id1()
        , id2()
        , psl1()
        , psl2()
        , msg(ServerListUpdater::Opcode::STOP)
        , transport(context)
        , registrar(context, transport)
    {
        csl.updater.halt();
        psl1.set_version_number(4);
        psl2.set_version_number(99);
        id1 = csl.generateUniqueId();
        csl.add(id1, "mock:host=server1", {WireFormat::MEMBERSHIP_SERVICE}, 0);
        id2 = csl.generateUniqueId();
        csl.add(id2, "mock:host=server2", {WireFormat::MEMBERSHIP_SERVICE}, 0);

        // Prevents cross contamination of MembershipUpdates and transport logs
        deleteMessageQueue(csl.updater);
        transport.output.clear();
        transport.outputLog.clear();
    }
};

TEST_F(ServerListUpdaterTest, constructor) {
    EXPECT_TRUE(updater.stop);
    EXPECT_TRUE(updater.msgQueue.empty());
    EXPECT_FALSE(updater.thread);
}

TEST_F(ServerListUpdaterTest, start) {
    updater.start();
    EXPECT_TRUE(updater.thread);
    EXPECT_FALSE(updater.stop);
    EXPECT_TRUE(updater.msgQueue.empty());
    EXPECT_TRUE(updater.thread->joinable());
}

TEST_F(ServerListUpdaterTest, start_multisafe) {
    updater.start();
    updater.start();
    updater.start();
    EXPECT_TRUE(updater.thread);
    EXPECT_FALSE(updater.stop);
    EXPECT_TRUE(updater.msgQueue.empty());
    EXPECT_TRUE(updater.thread->joinable());


    updater.halt();
    EXPECT_FALSE(updater.thread);
}

TEST_F(ServerListUpdaterTest, start_flushesQueue) {
    updater.halt();
    updater.sendFullList(id1, psl1);
    transport.setInput("0 0");          // Receive list okay
    updater.sendFullList(id2, psl2);
    transport.setInput("0 0");          // Receive list okay

    EXPECT_EQ("", transport.outputLog);
    updater.start();
    updater.flush();
    EXPECT_EQ("sendRequest: 0x40023 9 1041 0 /0 | "
            "sendRequest: 0x40023 9 0x6311 0 /0",
            transport.outputLog);

}

TEST_F(ServerListUpdaterTest, halt_normal) {
    updater.start();
    updater.halt();

    EXPECT_FALSE(updater.thread);
    EXPECT_TRUE(updater.stop);
    EXPECT_TRUE(updater.msgQueue.empty());
}

TEST_F(ServerListUpdaterTest, halt_multisafe) {
    updater.start();
    updater.halt();
    updater.halt();
    updater.halt();

    EXPECT_FALSE(updater.thread);
}

TEST_F(ServerListUpdaterTest, sendFullList) {
    updater.halt();

    EXPECT_EQ(0U, updater.msgQueue.size());
    updater.sendFullList(id1, psl1);
    EXPECT_EQ(1U, updater.msgQueue.size());
    updater.sendFullList(id2, psl2);
    EXPECT_EQ(2U, updater.msgQueue.size());
}

TEST_F(ServerListUpdaterTest, sendUpdate) {
    updater.halt();

    std::vector<ServerId> ids1, ids2;
    ids1.push_back(id1);
    ids1.push_back(id2);
    ids2.push_back(id1);

    EXPECT_EQ(0U, updater.msgQueue.size());
    updater.sendUpdate(ids1, psl1);
    EXPECT_EQ(1U, updater.msgQueue.size());
}

TEST_F(ServerListUpdaterTest, send_interlaced) {
    updater.halt();

    updater.sendFullList(id1, psl1);
    EXPECT_EQ(1U, updater.msgQueue.size());

    std::vector<ServerId> ids1, ids2;
    ids1.push_back(id1);
    ids1.push_back(id2);
    ids2.push_back(id1);

    updater.sendUpdate(ids1, psl1);
    EXPECT_EQ(2U, updater.msgQueue.size());

    // Check sendFullList
    msg = updater.msgQueue.front();
    updater.msgQueue.pop();
    EXPECT_EQ(ServerListUpdater::Opcode::FULL_LIST, msg.opcode);
    EXPECT_EQ(1U, msg.recipients.size());
    EXPECT_EQ(id1, msg.recipients.front());
    EXPECT_EQ(psl1.version_number(), msg.update.version_number());

    // Check sendUpdate
    msg = updater.msgQueue.front();
    updater.msgQueue.pop();
    EXPECT_EQ(ServerListUpdater::Opcode::UPDATE, msg.opcode);
    EXPECT_EQ(ids1, msg.recipients);
    EXPECT_EQ(psl1.version_number(), msg.update.version_number());

}

TEST_F(ServerListUpdaterTest, flushIfRunning) {
    std::vector<ServerId> noIds;
    updater.sendUpdate(noIds, psl1);
    updater.start();
    updater.flush();
    EXPECT_EQ(0U, updater.msgQueue.size());
}

//Note: If this test hangs, that's an error...
TEST_F(ServerListUpdaterTest, getWork) {
    updater.stop = false;
    updater.sendFullList(id1, psl1);
    ServerListUpdater::Message& msg = updater.getWork();
    EXPECT_EQ(ServerListUpdater::Opcode::FULL_LIST, msg.opcode);
    EXPECT_EQ(1U, msg.recipients.size());
    EXPECT_EQ(id1, msg.recipients.front());
    EXPECT_EQ(psl1.version_number(), msg.update.version_number());
}

TEST_F(ServerListUpdaterTest, getWork_stopped) {
    updater.stop = true;

    ServerListUpdater::Message& msg = updater.getWork();
    EXPECT_EQ(ServerListUpdater::Opcode::STOP, msg.opcode);
}

TEST_F(ServerListUpdaterTest, workDone) {
    updater.halt();

    updater.sendFullList(id1, psl1);
    updater.sendFullList(id2, psl2);
    EXPECT_EQ(2U, updater.msgQueue.size());
    updater.workDone();
    EXPECT_EQ(1U, updater.msgQueue.size());
    updater.workDone();
    EXPECT_EQ(0U, updater.msgQueue.size());

}

TEST_F(ServerListUpdaterTest, loop_queued_stop) {
    updater.stop = true;
    updater.sendFullList(id2, psl1);
    updater.loop();
    EXPECT_EQ(1U, updater.msgQueue.size());
    EXPECT_EQ(ServerListUpdater::Opcode::FULL_LIST,
              updater.msgQueue.front().opcode);
}

TEST_F(ServerListUpdaterTest, loop) {
    updater.stop = false;
    updater.sendFullList(id1, psl1);
    updater.sendFullList(id2, psl2);
    transport.setInput("0 0");          // Receive list okay
    transport.setInput("0 0");          // Receive list okay

    updater.msgQueue.emplace(ServerListUpdater::Opcode::STOP); // mimics stop
    updater.loop();
    EXPECT_EQ(1U, updater.msgQueue.size());
    EXPECT_EQ(ServerListUpdater::Opcode::STOP,
              updater.msgQueue.front().opcode);
}


TEST_F(ServerListUpdaterTest, handleRequest) {
    std::vector<ServerId> ids;
    ids.push_back(id1);
    ids.push_back(id2);

    ServerListUpdater::Message msg1(ServerListUpdater::Opcode::UPDATE, ids,
                                    psl1);
    ServerListUpdater::Message msg2(ServerListUpdater::Opcode::FULL_LIST,
                                    id2, psl2);
    ServerListUpdater::Message msg3(ServerListUpdater::Opcode::STOP);

    // Receive Lists Okay
    transport.setInput("0 0");
    transport.setInput("0 0");
    transport.setInput("0 0");

    updater.handleRequest(msg1);
    updater.handleRequest(msg2);
    updater.handleRequest(msg3);

    EXPECT_EQ("sendRequest: 0x40024 9 1041 0 /0 | "
            "sendRequest: 0x40024 9 1041 0 /0 | "
            "sendRequest: 0x40023 9 0x6311 0 /0",
        transport.outputLog);

    // make sure it doesn't send to downed/removed servers.
    transport.outputLog.clear();
    csl.iget(id2.indexNumber())->status = ServerStatus::DOWN;

    TestLog::Enable _;
    updater.handleRequest(msg2);
    EXPECT_EQ("", transport.outputLog);
    EXPECT_EQ("handleRequest: Async sendUpdate to 2.0 occured after it was "
            "removed/downed in the CoordinatorServerList.", TestLog::get());
}

TEST_F(ServerListUpdaterTest, sendMembershipUpdate) {
    Tub<ProtoBuf::ServerList> serializedServerList;

    transport.setInput("0 0");  // Receive Full List okay
    transport.setInput("0 1");  // Missed update
    transport.setInput("0");    // Retry Full List okay

    updater.sendMembershipUpdate(id1, psl1, serializedServerList);
    updater.sendMembershipUpdate(id2, psl2, serializedServerList);

    EXPECT_EQ("sendRequest: 0x40024 9 1041 0 /0 | "
            "sendRequest: 0x40024 9 0x6311 0 /0 | "
            "sendRequest: 0x40023 9 529 0 /0",
    transport.outputLog);
}

} // namespace RAMCloud
