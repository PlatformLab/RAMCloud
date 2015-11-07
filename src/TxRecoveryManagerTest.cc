/* Copyright (c) 2015 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "ClientLeaseAgent.h"
#include "MockCluster.h"
#include "TxRecoveryManager.h"

namespace RAMCloud {

class MockTxRecoveryRpcWrapper
        : public TxRecoveryManager::RecoveryTask::TxRecoveryRpcWrapper {
  public:
    MockTxRecoveryRpcWrapper(Context* context,
            Transport::SessionRef session,
            TxRecoveryManager::RecoveryTask* task)
        : TxRecoveryRpcWrapper(context,
                               session,
                               task,
                               sizeof(WireFormat::ResponseCommon))
        , actualParticipantCount(0)
    {
        participantCount = &actualParticipantCount;
    }

    uint32_t actualParticipantCount;
};

class TxRecoveryManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    Server* server;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    TxRecoveryManager txRecoveryManager;
    Tub<TxRecoveryManager::RecoveryTask> task;
    Tub<MockTxRecoveryRpcWrapper> txRecoveryRpc;
    Tub<TxRecoveryManager::RecoveryTask::DecisionRpc> decisionRpc;
    Tub<TxRecoveryManager::RecoveryTask::RequestAbortRpc> raRpc;

    TxRecoveryManagerTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , server()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , txRecoveryManager(&context)
        , task()
        , txRecoveryRpc()
        , decisionRpc()
        , raRpc()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        server = cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        context.services[WireFormat::MASTER_SERVICE] = server->master.get();

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                server->context->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = server->context->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = server->context->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");

        Buffer dummy;
        task.construct(&context, 42, 1, dummy, 0);
        txRecoveryRpc.construct(&context, session, task.get());
        decisionRpc.construct(&context, session, task.get());
        raRpc.construct(&context, session, task.get());
    }

    void fillPList()
    {
        task->participants.emplace_back(tableId1, 1, 2);
        task->participants.emplace_back(tableId1, 2, 3);
        task->participants.emplace_back(tableId1, 3, 4);

        task->participants.emplace_back(tableId1, 4, 5);
        task->participants.emplace_back(tableId1, 5, 6);

        task->participants.emplace_back(tableId2, 1, 7);

        task->participants.emplace_back(tableId3, 1, 8);

        task->nextParticipantEntry = task->participants.begin();
    }

    void insertRecovery(uint64_t leaseId,
                        uint64_t transactionId,
                        uint32_t participantCount) {
        Buffer buffer;
        TxRecoveryManager::RecoveryId recoveryId = {leaseId, transactionId};
        for (uint32_t i = 1; i <= participantCount; ++i) {
            buffer.emplaceAppend<WireFormat::TxParticipant>(tableId1,
                                                            transactionId + i,
                                                            transactionId + i);
        }

        txRecoveryManager.recoveringIds.insert(recoveryId);
        txRecoveryManager.recoveries.emplace_back(
                &context, leaseId, transactionId, buffer, participantCount);
    }

    string rpcToString(
            TxRecoveryManager::RecoveryTask::DecisionRpc* rpc) {
        string s;
        s.append(
                format("DecisionRpc :: lease{%lu}", rpc->reqHdr->leaseId));
        s.append(
                format(" participantCount{%u}", rpc->reqHdr->participantCount));
        uint32_t offset = sizeof(WireFormat::TxDecision::Request);

        s.append(format(" ParticipantList["));
        for (uint32_t i = 0; i < rpc->reqHdr->participantCount; i++) {
            WireFormat::TxParticipant* entry =
                    rpc->request.getOffset<WireFormat::TxParticipant>(offset);
            s.append(format(" {%lu, %lu, %lu}",
                            entry->tableId, entry->keyHash, entry->rpcId));
            offset += sizeof32(WireFormat::TxParticipant);
        }
        s.append(format(" ]"));
        return s;
    }

    string rpcToString(
            TxRecoveryManager::RecoveryTask::RequestAbortRpc* rpc) {
        string s;
        s.append(
                format("RequestAbortRpc :: lease{%lu}", rpc->reqHdr->leaseId));
        s.append(
                format(" participantCount{%u}", rpc->reqHdr->participantCount));
        uint32_t offset = sizeof(WireFormat::TxRequestAbort::Request);

        s.append(format(" ParticipantList["));
        for (uint32_t i = 0; i < rpc->reqHdr->participantCount; i++) {
            WireFormat::TxParticipant* entry =
                    rpc->request.getOffset<WireFormat::TxParticipant>(offset);
            s.append(format(" {%lu, %lu, %lu}",
                            entry->tableId, entry->keyHash, entry->rpcId));
            offset += sizeof32(WireFormat::TxParticipant);
        }
        s.append(format(" ]"));
        return s;
    }

    DISALLOW_COPY_AND_ASSIGN(TxRecoveryManagerTest);
};

TEST_F(TxRecoveryManagerTest, handleTimerEvent) {
    insertRecovery(1, 1, 4);
    insertRecovery(2, 4, 1);

    EXPECT_EQ(2U, txRecoveryManager.recoveries.size());

    txRecoveryManager.handleTimerEvent();
    EXPECT_TRUE(txRecoveryManager.isRunning());
    txRecoveryManager.stop();
    EXPECT_EQ(2U, txRecoveryManager.recoveries.size());

    txRecoveryManager.handleTimerEvent();
    EXPECT_TRUE(txRecoveryManager.isRunning());
    txRecoveryManager.stop();
    EXPECT_EQ(1U, txRecoveryManager.recoveries.size());

    txRecoveryManager.handleTimerEvent();
    EXPECT_TRUE(txRecoveryManager.isRunning());
    txRecoveryManager.stop();
    EXPECT_EQ(1U, txRecoveryManager.recoveries.size());

    txRecoveryManager.handleTimerEvent();
    EXPECT_FALSE(txRecoveryManager.isRunning());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());

    txRecoveryManager.handleTimerEvent();
    EXPECT_FALSE(txRecoveryManager.isRunning());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
}

TEST_F(TxRecoveryManagerTest, handleTxHintFailed_basic) {
    Buffer buffer;
    WireFormat::TxHintFailed::Request req;
    req.leaseId = 42;
    req.participantCount = 2;
    buffer.appendCopy<WireFormat::TxHintFailed::Request>(&req);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId1, 2, 3);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId2, 5, 6);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    txRecoveryManager.handleTxHintFailed(&buffer);

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(1U, txRecoveryManager.recoveries.size());
    EXPECT_TRUE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, handleTxHintFailed_badRPC) {
    Buffer buffer;
    WireFormat::TxHintFailed::Request req;
    req.leaseId = 42;
    req.participantCount = 0;
    buffer.appendCopy<WireFormat::TxHintFailed::Request>(&req);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    EXPECT_THROW(txRecoveryManager.handleTxHintFailed(&buffer),
                 RequestFormatError);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, handleTxHintFailed_wrongServer) {
    Buffer buffer;
    WireFormat::TxHintFailed::Request req;
    req.leaseId = 42;
    req.participantCount = 2;
    buffer.appendCopy<WireFormat::TxHintFailed::Request>(&req);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId2, 2, 3);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId3, 5, 6);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    EXPECT_THROW(txRecoveryManager.handleTxHintFailed(&buffer),
                 UnknownTabletException);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, handleTxHintFailed_duplicate) {
    Buffer buffer;
    WireFormat::TxHintFailed::Request req;
    req.leaseId = 42;
    req.clientTxId = 1;
    req.participantCount = 2;
    buffer.appendCopy<WireFormat::TxHintFailed::Request>(&req);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId1, 2, 3);
    buffer.emplaceAppend<WireFormat::TxParticipant>(tableId2, 5, 6);

    txRecoveryManager.recoveringIds.insert({42, 1});

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    txRecoveryManager.handleTxHintFailed(&buffer);

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, isTxDecisionRecordNeeded) {
    TxDecisionRecord record(1, 2, 3, 4, WireFormat::TxDecision::ABORT, 100);
    record.addParticipant(1, 2, 5);

    EXPECT_FALSE(txRecoveryManager.isTxDecisionRecordNeeded(record));

    txRecoveryManager.recoveringIds.insert({3, 4});

    EXPECT_TRUE(txRecoveryManager.isTxDecisionRecordNeeded(record));
}

TEST_F(TxRecoveryManagerTest, recoverRecovery_basic) {
    TxDecisionRecord record(1, 2, 42, 4, WireFormat::TxDecision::ABORT, 100);
    record.addParticipant(1, 2, 5);
    record.addParticipant(4, 5, 7);

    EXPECT_EQ(0U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    EXPECT_TRUE(txRecoveryManager.recoverRecovery(record));

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(1U, txRecoveryManager.recoveries.size());
    EXPECT_TRUE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, recoverRecovery_duplicate) {
    TxDecisionRecord record(1, 2, 42, 4, WireFormat::TxDecision::ABORT, 100);
    record.addParticipant(1, 2, 5);
    record.addParticipant(4, 5, 7);

    txRecoveryManager.recoveringIds.insert({42, 4});

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());

    EXPECT_FALSE(txRecoveryManager.recoverRecovery(record));

    EXPECT_EQ(1U, txRecoveryManager.recoveringIds.size());
    EXPECT_EQ(0U, txRecoveryManager.recoveries.size());
    EXPECT_FALSE(txRecoveryManager.isRunning());
}

TEST_F(TxRecoveryManagerTest, RecoveryTask_constructor_initial) {
    Buffer participantBuffer;
    participantBuffer.emplaceAppend<WireFormat::TxParticipant>(1, 2, 3);
    participantBuffer.emplaceAppend<WireFormat::TxParticipant>(4, 5, 6);
    participantBuffer.emplaceAppend<WireFormat::TxParticipant>(7, 8, 9);

    TxRecoveryManager::RecoveryTask task(&context, 21, 2, participantBuffer, 3);
    EXPECT_TRUE(&context == task.context);
    EXPECT_EQ("RecoveryTask :: lease{21} transaction{2} state{REQUEST_ABORT} "
              "decision{INVALID} participants[ {1, 2, 3} {4, 5, 6} {7, 8, 9} ]",
              task.toString());
}

TEST_F(TxRecoveryManagerTest, RecoveryTask_constructor_recovered) {
    TxDecisionRecord record(1, 2, 21, 2, WireFormat::TxDecision::ABORT, 100);
    record.addParticipant(1, 2, 3);
    record.addParticipant(4, 5, 6);
    record.addParticipant(7, 8, 9);

    TxRecoveryManager::RecoveryTask task(&context, record);
    EXPECT_TRUE(&context == task.context);
    EXPECT_EQ("RecoveryTask :: lease{21} transaction{2} state{DECIDE} "
              "decision{ABORT} participants[ {1, 2, 3} {4, 5, 6} {7, 8, 9} ]",
              task.toString());
}

TEST_F(TxRecoveryManagerTest, RecoveryTask_performTask_basic) {
    fillPList();

    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 1 Sent, RPC 1 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 2 Sent, RPC 2 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 3 Sent, RPC 3 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 4 Sent, RPC 4 Processed
                                // RPC 1 Sent, RPC 1 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DECIDE, task->state);
    task->performTask();        // RPC 2 Sent, RPC 2 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DECIDE, task->state);
    task->performTask();        // RPC 3 Sent, RPC 3 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DECIDE, task->state);
    task->performTask();        // RPC 4 Sent, RPC 4 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
    task->performTask();
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
}

TEST_F(TxRecoveryManagerTest, RecoveryTask_performTask_abortEarly) {
    // Fake the ack id forward
    ramcloud->rpcTracker->firstMissing = 100;
    ramcloud->rpcTracker->nextRpcId = 100;
    ramcloud->clientLeaseAgent->getLease();
    ramcloud->clientLeaseAgent->lease.leaseId = task->leaseId;
    ramcloud->write(tableId2, "foo", 3, "bar", 3, NULL, NULL, false);

    fillPList();

    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 1 Sent, RPC 1 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 2 Sent, RPC 2 Processed
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::REQUEST_ABORT, task->state);
    task->performTask();        // RPC 3 Sent, RPC 3 Rejected
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
    task->performTask();
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
}

TEST_F(TxRecoveryManagerTest, RecoveryTask_performTask_setDecision) {
    task->state = TxRecoveryManager::RecoveryTask::REQUEST_ABORT;
    task->decision = WireFormat::TxDecision::COMMIT;
    task->performTask();
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
    EXPECT_EQ(WireFormat::TxDecision::COMMIT, task->decision);

    task->state = TxRecoveryManager::RecoveryTask::REQUEST_ABORT;
    task->decision = WireFormat::TxDecision::ABORT;
    task->performTask();
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
    EXPECT_EQ(WireFormat::TxDecision::ABORT, task->decision);

    task->state = TxRecoveryManager::RecoveryTask::REQUEST_ABORT;
    task->decision = WireFormat::TxDecision::UNDECIDED;
    task->performTask();
    EXPECT_EQ(TxRecoveryManager::RecoveryTask::DONE, task->state);
    EXPECT_EQ(WireFormat::TxDecision::COMMIT, task->decision);
}

TEST_F(TxRecoveryManagerTest, TxRecoveryRpcWrapper_send) {
    EXPECT_TRUE(RpcWrapper::NOT_STARTED == txRecoveryRpc->state);
    txRecoveryRpc->send();
    EXPECT_TRUE(RpcWrapper::NOT_STARTED != txRecoveryRpc->state);
}

TEST_F(TxRecoveryManagerTest, TxRecoveryRpcWrapper_checkStatus) {
    WireFormat::ResponseCommon resp;
    resp.status = STATUS_TABLE_DOESNT_EXIST;
    txRecoveryRpc->responseHeader = &resp;
    TestLog::reset();
    EXPECT_TRUE(txRecoveryRpc->checkStatus());
    EXPECT_EQ("", TestLog::get());
    resp.status = STATUS_UNKNOWN_TABLET;
    EXPECT_TRUE(txRecoveryRpc->checkStatus());
    EXPECT_EQ("markOpsForRetry: Retry marked.", TestLog::get());
}

TEST_F(TxRecoveryManagerTest, TxRecoveryRpcWrapper_handleTransportError) {
    TestLog::reset();
    EXPECT_TRUE(txRecoveryRpc->handleTransportError());
    EXPECT_TRUE(txRecoveryRpc->session == NULL);
    EXPECT_EQ("flushSession: flushing session for mock:host=master3 | "
              "markOpsForRetry: Retry marked.",
              TestLog::get());
}

TEST_F(TxRecoveryManagerTest, TxRecoveryRpcWrapper_markOpsForRetry) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->participants.emplace_back(tableId1, 2, 3);
    task->participants.emplace_back(tableId1, 3, 4);
    task->nextParticipantEntry = task->participants.begin();

    TxRecoveryManager::ParticipantList::iterator it =
            task->participants.begin();
    while (it != task->participants.end()) {
        txRecoveryRpc->appendOp(it, TxRecoveryManager::Participant::DECIDE);
        EXPECT_EQ(TxRecoveryManager::Participant::DECIDE, it->state);
        it++;
    }

    task->nextParticipantEntry = task->participants.end();
    txRecoveryRpc->markOpsForRetry();
    EXPECT_EQ(task->participants.begin(), task->nextParticipantEntry);

    it = task->participants.begin();
    while (it != task->participants.end()) {
        EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
        it++;
    }
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    task->leaseId = 21;
    task->decision = WireFormat::TxDecision::UNDECIDED;

    TxRecoveryManager::RecoveryTask::DecisionRpc
            rpc(&context, session, task.get());
    EXPECT_EQ(task->decision, rpc.reqHdr->decision);
    EXPECT_EQ(task->leaseId, rpc.reqHdr->leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->participantCount);
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_appendOp) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it =
            task->participants.begin();
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
    decisionRpc->appendOp(it);
    EXPECT_EQ(TxRecoveryManager::Participant::DECIDE, it->state);
    EXPECT_EQ(decisionRpc->ops[decisionRpc->reqHdr->participantCount - 1], it);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {1, 1, 2} ]",
              rpcToString(decisionRpc.get()));
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_wait) {
    Buffer respBuf;
    WireFormat::TxDecision::Response* respHdr =
            respBuf.emplaceAppend<WireFormat::TxDecision::Response>();
    decisionRpc->state = RpcWrapper::FAILED;
    EXPECT_THROW(decisionRpc->wait(), ServerNotUpException);

    decisionRpc->response = &respBuf;
    respHdr->common.status = STATUS_UNKNOWN_TABLET;
    decisionRpc->state = RpcWrapper::FINISHED;
    EXPECT_THROW(decisionRpc->wait(), UnknownTabletException);

    respHdr->common.status = STATUS_OK;
    decisionRpc->wait();
}

TEST_F(TxRecoveryManagerTest, processDecisionRpcResults_basic) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->decision = WireFormat::TxDecision::COMMIT;
    task->nextParticipantEntry = task->participants.begin();
    task->sendDecisionRpc();

    EXPECT_EQ(1U, task->decisionRpcs.size());
    TestLog::Enable _("processDecisionRpcResults");
    TestLog::reset();
    task->processDecisionRpcResults();
    EXPECT_EQ("processDecisionRpcResults: STATUS_OK", TestLog::get());
    EXPECT_EQ(0U, task->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processDecisionRpcResults_unknownTablet) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->decision = WireFormat::TxDecision::COMMIT;
    task->nextParticipantEntry = task->participants.begin();
    task->sendDecisionRpc();
    // Resend to wrong session
    task->decisionRpcs.begin()->session = session2;
    task->decisionRpcs.begin()->send();

    EXPECT_EQ(1U, task->decisionRpcs.size());
    TestLog::Enable _("processDecisionRpcResults");
    TestLog::reset();
    task->processDecisionRpcResults();
    EXPECT_EQ("processDecisionRpcResults: STATUS_UNKNOWN_TABLET",
              TestLog::get());
    EXPECT_EQ(0U, task->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processDecisionRpcResults_ServerNotUp) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->decision = WireFormat::TxDecision::COMMIT;
    task->nextParticipantEntry = task->participants.begin();
    task->sendDecisionRpc();
    // Fail the rpc.
    task->decisionRpcs.begin()->failed();

    EXPECT_EQ(1U, task->decisionRpcs.size());
    TestLog::Enable _("flushSession", "processDecisionRpcResults", NULL);
    TestLog::reset();
    task->processDecisionRpcResults();
    EXPECT_EQ("flushSession: flushing session for mock:host=master1 | "
              "processDecisionRpcResults: STATUS_SERVER_NOT_UP",
              TestLog::get());
    EXPECT_EQ(0U, task->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processDecisionRpcResults_notReady) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->decision = WireFormat::TxDecision::COMMIT;
    task->nextParticipantEntry = task->participants.begin();
    task->sendDecisionRpc();

    EXPECT_EQ(1U, task->decisionRpcs.size());
    task->decisionRpcs.begin()->state =
            TxRecoveryManager::RecoveryTask::DecisionRpc::IN_PROGRESS;
    task->processDecisionRpcResults();
    EXPECT_EQ(1U, task->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, sendDecisionRpc_basic) {
    TxRecoveryManager::RecoveryTask::DecisionRpc* rpc;
    fillPList();

    EXPECT_EQ(0U, task->decisionRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    task->sendDecisionRpc();
    EXPECT_EQ(1U, task->decisionRpcs.size());
    rpc = &task->decisionRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{3} "
              "ParticipantList[ {1, 1, 2} {1, 2, 3} {1, 3, 4} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    task->nextParticipantEntry = task->participants.begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    task->sendDecisionRpc();
    EXPECT_EQ(2U, task->decisionRpcs.size());
    rpc = &task->decisionRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{2} "
              "ParticipantList[ {1, 4, 5} {1, 5, 6} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    task->sendDecisionRpc();
    EXPECT_EQ(3U, task->decisionRpcs.size());
    rpc = &task->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {2, 1, 7} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    task->sendDecisionRpc();
    EXPECT_EQ(4U, task->decisionRpcs.size());
    rpc = &task->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {3, 1, 8} ]",
              rpcToString(rpc));

    // Should issue do nothing.
    task->sendDecisionRpc();
    EXPECT_EQ(4U, task->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    task->leaseId = 21;

    TxRecoveryManager::RecoveryTask::RequestAbortRpc
            rpc(&context, session, task.get());
    EXPECT_EQ(task->leaseId, rpc.reqHdr->leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->participantCount);
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_appendOp) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it =
            task->participants.begin();
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
    raRpc->appendOp(it);
    EXPECT_EQ(TxRecoveryManager::Participant::ABORT, it->state);
    EXPECT_EQ(raRpc->ops[raRpc->reqHdr->participantCount - 1], it);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {1, 1, 2} ]",
              rpcToString(raRpc.get()));
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_wait) {
    Buffer respBuf;
    WireFormat::TxRequestAbort::Response* respHdr =
            respBuf.emplaceAppend<WireFormat::TxRequestAbort::Response>();
    raRpc->state = RpcWrapper::FAILED;
    EXPECT_THROW(raRpc->wait(), ServerNotUpException);

    raRpc->response = &respBuf;
    respHdr->common.status = STATUS_UNKNOWN_TABLET;
    raRpc->state = RpcWrapper::FINISHED;
    EXPECT_THROW(raRpc->wait(), UnknownTabletException);

    respHdr->common.status = STATUS_OK;
    respHdr->vote = WireFormat::TxPrepare::ABORT;
    EXPECT_EQ(WireFormat::TxPrepare::ABORT, raRpc->wait());

    respHdr->common.status = STATUS_OK;
    respHdr->vote = WireFormat::TxPrepare::PREPARED;
    EXPECT_EQ(WireFormat::TxPrepare::PREPARED, raRpc->wait());

    respHdr->common.status = STATUS_OK;
    respHdr->vote = WireFormat::TxPrepare::COMMITTED;
    EXPECT_EQ(WireFormat::TxPrepare::COMMITTED, raRpc->wait());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_basic) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    task->processRequestAbortRpcResults();
    EXPECT_EQ(WireFormat::TxDecision::ABORT, task->decision);
    EXPECT_EQ(0U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_prepared) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();

    Buffer respBuf;
    WireFormat::TxRequestAbort::Response* respHdr =
            respBuf.emplaceAppend<WireFormat::TxRequestAbort::Response>();

    task->requestAbortRpcs.begin()->response = &respBuf;
    respHdr->common.status = STATUS_OK;
    respHdr->vote = WireFormat::TxPrepare::PREPARED;
    EXPECT_EQ(WireFormat::TxPrepare::PREPARED,
              task->requestAbortRpcs.begin()->wait());

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    task->processRequestAbortRpcResults();
    EXPECT_EQ(WireFormat::TxDecision::UNDECIDED, task->decision);
    EXPECT_EQ(0U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_committed) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();

    Buffer respBuf;
    WireFormat::TxRequestAbort::Response* respHdr =
            respBuf.emplaceAppend<WireFormat::TxRequestAbort::Response>();

    task->requestAbortRpcs.begin()->response = &respBuf;
    respHdr->common.status = STATUS_OK;
    respHdr->vote = WireFormat::TxPrepare::COMMITTED;
    EXPECT_EQ(WireFormat::TxPrepare::COMMITTED,
              task->requestAbortRpcs.begin()->wait());

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    task->processRequestAbortRpcResults();
    EXPECT_EQ(WireFormat::TxDecision::UNDECIDED, task->decision);
    EXPECT_EQ(0U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_unknownTablet) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();
    // Resend to wrong session
    task->requestAbortRpcs.begin()->session = session2;
    task->requestAbortRpcs.begin()->send();

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    TestLog::Enable _("processRequestAbortRpcResults");
    TestLog::reset();
    task->processRequestAbortRpcResults();
    EXPECT_EQ("processRequestAbortRpcResults: STATUS_UNKNOWN_TABLET",
              TestLog::get());
    EXPECT_EQ(0U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_ServerNotUp) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();
    // Fail the rpc.
    task->requestAbortRpcs.begin()->failed();

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    TestLog::Enable _("flushSession", "processRequestAbortRpcResults", NULL);
    TestLog::reset();
    task->processRequestAbortRpcResults();
    EXPECT_EQ("flushSession: flushing session for mock:host=master1 | "
              "processRequestAbortRpcResults: STATUS_SERVER_NOT_UP",
              TestLog::get());
    EXPECT_EQ(0U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_notReady) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    task->requestAbortRpcs.begin()->state =
            TxRecoveryManager::RecoveryTask::DecisionRpc::IN_PROGRESS;
    task->processRequestAbortRpcResults();
    EXPECT_EQ(1U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, processRequestAbortRpcResults_staleRpc) {
    // Fake the ack id forward
    ramcloud->rpcTracker->firstMissing = 100;
    ramcloud->rpcTracker->nextRpcId = 100;
    ramcloud->clientLeaseAgent->getLease();
    ramcloud->clientLeaseAgent->lease.leaseId = task->leaseId;
    ramcloud->write(tableId1, "foo", 3, "bar", 3, NULL, NULL, false);

    task->participants.emplace_back(tableId1, 1, 2);
    task->nextParticipantEntry = task->participants.begin();
    task->sendRequestAbortRpc();

    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    EXPECT_THROW(task->processRequestAbortRpcResults(), StaleRpcException);
    EXPECT_EQ(1U, task->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, sendRequestAbortRpc_basic) {
    task->participants.emplace_back(tableId1, 1, 2);
    task->participants.emplace_back(tableId1, 2, 3);
    task->participants.emplace_back(tableId1, 3, 4);
    task->participants.emplace_back(tableId1, 4, 5);
    task->participants.emplace_back(tableId1, 5, 6);
    task->participants.emplace_back(tableId2, 1, 7);
    task->participants.emplace_back(tableId3, 1, 8);

    TxRecoveryManager::RecoveryTask::RequestAbortRpc* rpc;
    task->nextParticipantEntry = task->participants.begin();

    EXPECT_EQ(0U, task->requestAbortRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    task->sendRequestAbortRpc();
    EXPECT_EQ(1U, task->requestAbortRpcs.size());
    rpc = &task->requestAbortRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{3} "
              "ParticipantList[ {1, 1, 2} {1, 2, 3} {1, 3, 4} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    task->nextParticipantEntry = task->participants.begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    task->sendRequestAbortRpc();
    EXPECT_EQ(2U, task->requestAbortRpcs.size());
    rpc = &task->requestAbortRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{2} "
              "ParticipantList[ {1, 4, 5} {1, 5, 6} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    task->sendRequestAbortRpc();
    EXPECT_EQ(3U, task->requestAbortRpcs.size());
    rpc = &task->requestAbortRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {2, 1, 7} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    task->sendRequestAbortRpc();
    EXPECT_EQ(4U, task->requestAbortRpcs.size());
    rpc = &task->requestAbortRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {3, 1, 8} ]",
              rpcToString(rpc));

    // Should issue do nothing.
    task->sendRequestAbortRpc();
    EXPECT_EQ(4U, task->requestAbortRpcs.size());
}

}  // namespace RAMCloud
