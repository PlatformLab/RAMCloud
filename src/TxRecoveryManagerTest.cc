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
#include "MockCluster.h"
#include "TxRecoveryManager.h"

namespace RAMCloud {

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
    TxRecoveryManager::ParticipantList pList;
    Tub<TxRecoveryManager::DecisionTask> decisionTask;
    Tub<TxRecoveryManager::DecisionTask::DecisionRpc> decisionRpc;
    Tub<TxRecoveryManager::RequestAbortTask> raTask;
    Tub<TxRecoveryManager::RequestAbortTask::RequestAbortRpc> raRpc;

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
        , pList()
        , decisionTask()
        , decisionRpc()
        , raTask()
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

        context.masterService = server->master.get();

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

        decisionTask.construct(
                &context, WireFormat::TxDecision::ABORT, 42, &pList);
        decisionRpc.construct(&context, session, decisionTask.get());
        raTask.construct(&context, 42, &pList);
        raRpc.construct(&context, session, raTask.get());
    }

    void fillPList()
    {
        pList.emplace_back(tableId1, 2, 3);
        pList.emplace_back(tableId1, 4, 5);
        pList.emplace_back(tableId1, 6, 7);
    }

    string rpcToString(
            TxRecoveryManager::DecisionTask::DecisionRpc* rpc) {
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
            TxRecoveryManager::RequestAbortTask::RequestAbortRpc* rpc) {
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

TEST_F(TxRecoveryManagerTest, DecisionRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    decisionTask->leaseId = 21;
    decisionTask->decision = WireFormat::TxDecision::INVALID;

    TxRecoveryManager::DecisionTask::DecisionRpc
            rpc(&context, session, decisionTask.get());
    EXPECT_EQ(decisionTask->decision, rpc.reqHdr->decision);
    EXPECT_EQ(decisionTask->leaseId, rpc.reqHdr->leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->participantCount);
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_checkStatus) {
    fillPList();
    decisionTask->nextParticipantEntry = decisionTask->pList->end();
    WireFormat::TxDecision::Response resp;
    resp.common.status = STATUS_TABLE_DOESNT_EXIST;
    decisionRpc->responseHeader = &resp.common;
    decisionRpc->checkStatus();
    EXPECT_EQ(decisionTask->pList->end(), decisionTask->nextParticipantEntry);
    resp.common.status = STATUS_UNKNOWN_TABLET;
    decisionRpc->checkStatus();
    EXPECT_EQ(decisionTask->pList->begin(), decisionTask->nextParticipantEntry);
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_handleTransportError) {
    fillPList();
    decisionTask->nextParticipantEntry = decisionTask->pList->end();
    TestLog::reset();
    decisionRpc->handleTransportError();
    EXPECT_TRUE(decisionRpc->session == NULL);
    EXPECT_EQ(decisionTask->pList->begin(), decisionTask->nextParticipantEntry);
    EXPECT_EQ("flushSession: flushing session for mock:host=master3",
              TestLog::get());
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_send) {
    EXPECT_TRUE(RpcWrapper::NOT_STARTED == decisionRpc->state);
    decisionRpc->send();
    EXPECT_TRUE(RpcWrapper::NOT_STARTED != decisionRpc->state);
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_appendOp) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it =
            decisionTask->pList->begin();
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
    decisionRpc->appendOp(it);
    EXPECT_EQ(TxRecoveryManager::Participant::DECIDE, it->state);
    EXPECT_EQ(decisionRpc->ops[decisionRpc->reqHdr->participantCount - 1], it);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {1, 2, 3} ]",
              rpcToString(decisionRpc.get()));
}

TEST_F(TxRecoveryManagerTest, DecisionRpc_retryRequest) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it =
            decisionTask->pList->begin();
    while (it != decisionTask->pList->end()) {
        decisionRpc->appendOp(it);
        EXPECT_EQ(TxRecoveryManager::Participant::DECIDE, it->state);
        it++;
    }

    decisionRpc->retryRequest();

    it = decisionTask->pList->begin();
    while (it != decisionTask->pList->end()) {
        EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
        it++;
    }
}

// TODO(cstlee) : Unit test DecisionTask::processDecisionRpcs()

TEST_F(TxRecoveryManagerTest, sendDecisionRpc_basic) {
    pList.emplace_back(tableId1, 1, 2);
    pList.emplace_back(tableId1, 2, 3);
    pList.emplace_back(tableId1, 3, 4);
    pList.emplace_back(tableId1, 4, 5);
    pList.emplace_back(tableId1, 5, 6);
    pList.emplace_back(tableId2, 1, 7);
    pList.emplace_back(tableId3, 1, 8);

    TxRecoveryManager::DecisionTask::DecisionRpc* rpc;
    decisionTask->nextParticipantEntry = decisionTask->pList->begin();

    EXPECT_EQ(0U, decisionTask->decisionRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(1U, decisionTask->decisionRpcs.size());
    rpc = &decisionTask->decisionRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{3} "
              "ParticipantList[ {1, 1, 2} {1, 2, 3} {1, 3, 4} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    decisionTask->nextParticipantEntry = decisionTask->pList->begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(2U, decisionTask->decisionRpcs.size());
    rpc = &decisionTask->decisionRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{2} "
              "ParticipantList[ {1, 4, 5} {1, 5, 6} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(3U, decisionTask->decisionRpcs.size());
    rpc = &decisionTask->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {2, 1, 7} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(4U, decisionTask->decisionRpcs.size());
    rpc = &decisionTask->decisionRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("DecisionRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {3, 1, 8} ]",
              rpcToString(rpc));

    // Should issue do nothing.
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(4U, decisionTask->decisionRpcs.size());
}

TEST_F(TxRecoveryManagerTest, sendDecisionRpc_TableDoesntExist) {
    pList.emplace_back(0, 1, 2);
    decisionTask->nextParticipantEntry = decisionTask->pList->begin();

    EXPECT_EQ(0U, decisionTask->decisionRpcs.size());
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING,
              decisionTask->pList->begin()->state);
    TestLog::reset();
    decisionTask->sendDecisionRpc();
    EXPECT_EQ(0U, decisionTask->decisionRpcs.size());
    EXPECT_EQ(TxRecoveryManager::Participant::FAILED,
              decisionTask->pList->begin()->state);
    EXPECT_EQ("sendDecisionRpc: trying to recover transaction for leaseId "
              "42 but table with id 0 does not exist.",
              TestLog::get());
}

// TODO(cstlee) : Unit test RequestAbortTask::performTask()
// TODO(cstlee) : Unit test RequestAbortTask::wait()

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_constructor) {
    Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
    raTask->leaseId = 21;

    TxRecoveryManager::RequestAbortTask::RequestAbortRpc
            rpc(&context, session, raTask.get());
    EXPECT_EQ(raTask->leaseId, rpc.reqHdr->leaseId);
    EXPECT_EQ(0U, rpc.reqHdr->participantCount);
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_checkStatus) {
    fillPList();
    raTask->nextParticipantEntry = raTask->pList->end();
    WireFormat::TxRequestAbort::Response resp;
    resp.common.status = STATUS_TABLE_DOESNT_EXIST;
    raRpc->responseHeader = &resp.common;
    raRpc->checkStatus();
    EXPECT_EQ(raTask->pList->end(), raTask->nextParticipantEntry);
    resp.common.status = STATUS_UNKNOWN_TABLET;
    raRpc->checkStatus();
    EXPECT_EQ(raTask->pList->begin(), raTask->nextParticipantEntry);
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_handleTransportError) {
    fillPList();
    raTask->nextParticipantEntry = raTask->pList->end();
    TestLog::reset();
    raRpc->handleTransportError();
    EXPECT_TRUE(raRpc->session == NULL);
    EXPECT_EQ(raTask->pList->begin(), raTask->nextParticipantEntry);
    EXPECT_EQ("flushSession: flushing session for mock:host=master3",
              TestLog::get());
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_send) {
    EXPECT_TRUE(RpcWrapper::NOT_STARTED == raRpc->state);
    raRpc->send();
    EXPECT_TRUE(RpcWrapper::NOT_STARTED != raRpc->state);
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_appendOp) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it = raTask->pList->begin();
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
    raRpc->appendOp(it);
    EXPECT_EQ(TxRecoveryManager::Participant::ABORT, it->state);
    EXPECT_EQ(raRpc->ops[raRpc->reqHdr->participantCount - 1], it);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {1, 2, 3} ]",
              rpcToString(raRpc.get()));
}

TEST_F(TxRecoveryManagerTest, RequestAbortRpc_retryRequest) {
    fillPList();
    TxRecoveryManager::ParticipantList::iterator it = raTask->pList->begin();
    while (it != raTask->pList->end()) {
        raRpc->appendOp(it);
        EXPECT_EQ(TxRecoveryManager::Participant::ABORT, it->state);
        it++;
    }

    raRpc->retryRequest();

    it = raTask->pList->begin();
    while (it != raTask->pList->end()) {
        EXPECT_EQ(TxRecoveryManager::Participant::PENDING, it->state);
        it++;
    }
}

// TODO(cstlee) : Unit test RequestAbortTask::processRequestAbortRpcs()

TEST_F(TxRecoveryManagerTest, sendRequestAbortRpc_basic) {
    pList.emplace_back(tableId1, 1, 2);
    pList.emplace_back(tableId1, 2, 3);
    pList.emplace_back(tableId1, 3, 4);
    pList.emplace_back(tableId1, 4, 5);
    pList.emplace_back(tableId1, 5, 6);
    pList.emplace_back(tableId2, 1, 7);
    pList.emplace_back(tableId3, 1, 8);

    TxRecoveryManager::RequestAbortTask::RequestAbortRpc* rpc;
    raTask->nextParticipantEntry = raTask->pList->begin();

    EXPECT_EQ(0U, raTask->requestAbortRpcs.size());

    // Should issue 1 rpc to master 1 with 3 objects in it.
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(1U, raTask->requestAbortRpcs.size());
    rpc = &raTask->requestAbortRpcs.back();
    EXPECT_EQ(3U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{3} "
              "ParticipantList[ {1, 1, 2} {1, 2, 3} {1, 3, 4} ]",
              rpcToString(rpc));

    // Rest nextCacheEntry to make see if processed ops will be skipped.
    raTask->nextParticipantEntry = raTask->pList->begin();

    // Should issue 1 rpc to master 1 with 2 objects in it.
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(2U, raTask->requestAbortRpcs.size());
    rpc = &raTask->requestAbortRpcs.back();
    EXPECT_EQ(2U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master1", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{2} "
              "ParticipantList[ {1, 4, 5} {1, 5, 6} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 2 with 1 objects in it.
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(3U, raTask->requestAbortRpcs.size());
    rpc = &raTask->requestAbortRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master2", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {2, 1, 7} ]",
              rpcToString(rpc));

    // Should issue 1 rpc to master 3 with 1 objects in it.
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(4U, raTask->requestAbortRpcs.size());
    rpc = &raTask->requestAbortRpcs.back();
    EXPECT_EQ(1U, rpc->reqHdr->participantCount);
    EXPECT_EQ("mock:host=master3", rpc->session.get()->serviceLocator);
    EXPECT_EQ("RequestAbortRpc :: lease{42} participantCount{1} "
              "ParticipantList[ {3, 1, 8} ]",
              rpcToString(rpc));

    // Should issue do nothing.
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(4U, raTask->requestAbortRpcs.size());
}

TEST_F(TxRecoveryManagerTest, sendRequestAbortRpc_TableDoesntExist) {
    pList.emplace_back(0, 1, 2);
    raTask->nextParticipantEntry = raTask->pList->begin();

    EXPECT_EQ(0U, raTask->requestAbortRpcs.size());
    EXPECT_EQ(TxRecoveryManager::Participant::PENDING,
              raTask->pList->begin()->state);
    TestLog::reset();
    raTask->sendRequestAbortRpc();
    EXPECT_EQ(0U, raTask->requestAbortRpcs.size());
    EXPECT_EQ(TxRecoveryManager::Participant::FAILED,
              raTask->pList->begin()->state);
    EXPECT_EQ("sendRequestAbortRpc: trying to recover transaction for leaseId "
              "42 but table with id 0 does not exist.",
              TestLog::get());
}

}  // namespace RAMCloud
