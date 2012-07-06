/* Copyright (c) 2011-2012 Stanford University
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

#include "TestUtil.h"
#include "Common.h"
#include "MockService.h"
#include "MockSyscall.h"
#include "MockTransport.h"
#include "ServiceManager.h"
#include "Tub.h"

namespace RAMCloud {

class ServiceManagerTest : public ::testing::Test {
  public:
    Context context;
    Tub<ServiceManager> manager;
    MockTransport transport;
    MockService service;
    TestLog::Enable logEnabler;
    Syscall *savedSyscall;
    MockSyscall sys;

    ServiceManagerTest() : context(), manager(), transport(context),
            service(), logEnabler(), savedSyscall(NULL), sys()
    {
        manager.construct(context);
        manager->addService(service, BACKUP_SERVICE);
        savedSyscall = ServiceManager::sys;
        ServiceManager::sys = &sys;
    }

    ~ServiceManagerTest() {
        service.gate = 0;
        // Must explicitly destroy the service manager (to ensure that the
        // worker thread exits); if we used implicit destruction, it's possible
        // that some other objects such as MockService might get destroyed while
        // the worker thread is still using them.
        manager.destroy();
        ServiceManager::sys = savedSyscall;
    }

    // Wait for a given number of the currently-executing RPCs (i.e. those
    // actually assigned to a worker) to complete, but give up if this
    // takes too long.
    void
    waitUntilDone(int count)
    {
        for (int i = 0; i < 1000; i++) {
            int completed = 0;
            foreach (Worker* worker, manager->busyThreads) {
                if (worker->state.load() != Worker::WORKING) {
                    completed++;
                }
            }
            if (completed >= count) {
                return;
            }
            usleep(1000);
        }
    }
    DISALLOW_COPY_AND_ASSIGN(ServiceManagerTest);
};

TEST_F(ServiceManagerTest, sanityCheck) {
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc);

    // Wait for the request to be processed for (but don't wait forever).
    for (int i = 0; i < 1000; i++) {
        context.dispatch->poll();
        if (!transport.outputLog.empty())
            break;
        usleep(1000);
    }
    EXPECT_EQ("rpc: 0x10000 3 4", service.log);
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
}

TEST_F(ServiceManagerTest, constructor) {
    MockService mock;
    ServiceManager manager1(context);
    ServiceManager manager2(context);
    manager2.addService(mock, BACKUP_SERVICE);
    EXPECT_EQ(0, manager1.serviceCount);
    EXPECT_EQ(1, manager2.serviceCount);
    EXPECT_TRUE(manager2.services[1]);
}

TEST_F(ServiceManagerTest, destructor_cleanupThreads) {
    TestLog::Enable _;

    // Start 2 requests in parallel, but don't let them finish yet.
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc1);
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 5");
    manager->handleRpc(rpc2);
    usleep(1000);
    EXPECT_EQ(2U, manager->busyThreads.size());
    EXPECT_EQ("", TestLog::get());

    // Allow the requests to finish, but destroy the ServiceManager before
    // they have been moved back to the idle list.
    service.gate = 3;
    manager.destroy();
    EXPECT_EQ("workerMain: exiting | workerMain: exiting | "
            "workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, addService) {
    MockService mock;
    ServiceManager manager1(context);
    EXPECT_EQ(0, manager1.serviceCount);
    EXPECT_FALSE(manager1.services[1]);
    EXPECT_EQ(0U, manager1.idleThreads.size());
    manager1.addService(mock, BACKUP_SERVICE);
    EXPECT_EQ(1, manager1.serviceCount);
    EXPECT_TRUE(manager1.services[1]);
    EXPECT_EQ(3U, manager1.idleThreads.size());
}

TEST_F(ServiceManagerTest, handleRpc_noHeader) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "");
    manager->handleRpc(rpc);
    EXPECT_EQ("handleRpc: Incoming RPC contains no header (message length 0)",
            TestLog::get());
    EXPECT_STREQ("STATUS_MESSAGE_TOO_SHORT",
            statusToSymbol(transport.status));
}

TEST_F(ServiceManagerTest, handleRpc_serviceUnavailable) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x70000");
    manager->handleRpc(rpc);
    EXPECT_EQ("handleRpc: Incoming RPC requested unavailable service 7",
            TestLog::get());
    EXPECT_STREQ("STATUS_SERVICE_NOT_AVAILABLE",
            statusToSymbol(transport.status));
}

TEST_F(ServiceManagerTest, handleRpc_concurrencyLimitExceeded) {
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10000 4");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(0U, manager->services[1]->waitingRpcs.size());
    manager->handleRpc(rpc4);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(1U, manager->services[1]->waitingRpcs.size());
}

TEST_F(ServiceManagerTest, handleRpc_handoffToWorker) {
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 2");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(3U, manager->idleThreads.size());
}

TEST_F(ServiceManagerTest, idle) {
    EXPECT_TRUE(manager->idle());
    // Start one RPC.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc);
    EXPECT_FALSE(manager->idle());

    // Wait for it to finish.
    waitUntilDone(1);
    manager->poll();
    EXPECT_TRUE(manager->idle());
}

TEST_F(ServiceManagerTest, poll_basics) {
    // Start 3 RPCs concurrently, with 2 more waiting.
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10000 4");
    MockTransport::MockServerRpc* rpc5 = new MockTransport::MockServerRpc(
            &transport, "0x10000 5");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    manager->handleRpc(rpc5);
    EXPECT_EQ(2U, manager->services[1]->waitingRpcs.size());

    // Allow 2 of the requests to complete, and make sure that the remaining
    // 2 start service.
    service.gate = 1;
    waitUntilDone(1);
    service.gate = 2;
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(0U, manager->services[1]->waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10001 3 | serverReply: 0x10001 2",
            transport.outputLog);

    // Allow the request in slot 0 of busyThreads to complete.
    transport.outputLog.clear();
    service.gate = 5;
    waitUntilDone(1);
    manager->poll();
    EXPECT_EQ("serverReply: 0x10001 6", transport.outputLog);
    EXPECT_EQ("0x10000 3", TestUtil::toString(
              &manager->busyThreads[0]->rpc->requestPayload));

    // Allow the remaining requests to complete.
    transport.outputLog.clear();
    service.gate = 0;
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ("serverReply: 0x10001 5 | serverReply: 0x10001 4",
            transport.outputLog);
}

TEST_F(ServiceManagerTest, poll_postprocessing) {
    // This test makes sure that the POSTPROCESSING state is handled
    // correctly (along with the subsequent POLLING state).
    service.gate = -1;
    service.sendReply = true;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc);
    waitUntilDone(1);

    // At this point the state of the worker should be POSTPROCESSING.
    manager->poll();
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
    EXPECT_EQ(2U, manager->idleThreads.size());

    // Now allow the worker to finish.
    service.gate = 0;
    for (int i = 0; i < 1000; i++) {
        manager->poll();
        if (!manager->idleThreads.empty())
            break;
        usleep(1000);
    }
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
    EXPECT_EQ(2U, manager->idleThreads.size());
}

// No tests for waitForRpc: this method is only used in tests.

TEST_F(ServiceManagerTest, workerMain_goToSleep) {
    // Workers were already created when the test initialized.  Initially
    // the (first) worker should not go to sleep (time appears to
    // stand still for it, because we aren't calling dispatch->poll).
    Worker* worker = manager->idleThreads[0];
    transport.outputLog.clear();
    usleep(20000);
    EXPECT_EQ(Worker::POLLING, worker->state.load());

    // Update dispatch->currentTime. When the worker sees this it should
    // go to sleep.
    context.dispatch->currentTime = Cycles::rdtsc();
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (worker->state.load() == Worker::SLEEPING) {
            break;
        }
    }
    EXPECT_EQ(Worker::SLEEPING, worker->state.load());

    // Make sure that the worker can be woken up.
    EXPECT_EQ(static_cast<Transport::ServerRpc*>(NULL), worker->rpc);
    manager->handleRpc(new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4"));
    manager.destroy();
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
}

TEST_F(ServiceManagerTest, workerMain_futexError) {
    // Get rid of the original manager (it has too many threads, which
    // would confuse this test).
    manager.destroy();
    TestLog::reset();

    // Create a new manager, whose service has only 1 thread.
    ServiceManager manager2(context);
    MockService service2(1);
    manager2.addService(service2, BACKUP_SERVICE);
    Worker* worker = manager2.idleThreads[0];

    sys.futexWaitErrno = EPERM;
    // Wait for the worker to go to sleep, then make sure it logged
    // an error message.
    usleep(20000);
    context.dispatch->currentTime = Cycles::rdtsc();
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (worker->state.load() == Worker::SLEEPING) {
            break;
        }
    }
    EXPECT_EQ("workerMain: futexWait failed in ServiceManager::workerMain: "
                "Operation not permitted", TestLog::get());
}

TEST_F(ServiceManagerTest, workerMain_exit) {
    manager.destroy();
    EXPECT_EQ("workerMain: exiting | workerMain: exiting | "
            "workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, Worker_exit) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10000 3 4");
    manager->handleRpc(rpc);
    manager->busyThreads[0]->exit();
    EXPECT_EQ("workerMain: exiting", TestLog::get());
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);

    // Try another exit just to make sure it's itempotent.
    manager->busyThreads[0]->exit();
}

TEST_F(ServiceManagerTest, Worker_handoff_dontCallFutex) {
    TestLog::Enable _;
    // Set futex to return an error, so we can make sure it doesn't
    // get called in the normal case.
    sys.futexWakeErrno = EPERM;
    manager->handleRpc(
            new MockTransport::MockServerRpc(&transport, "0x10000 99"));
    EXPECT_EQ("", TestLog::get());
    waitUntilDone(1);
    EXPECT_EQ("rpc: 0x10000 99", service.log);

    // Reset error so that the ServiceManager destructor can work
    // correctly.
    sys.futexWakeErrno = 0;
}

TEST_F(ServiceManagerTest, Worker_handoff_callFutex) {
    // Wait for all the workers to go to sleep.
    const char *message = "workers didn't go to sleep";
    for (int i = 0; i < 1000; i++) {
        if ((manager->idleThreads[0]->state.load() == Worker::SLEEPING)
            && (manager->idleThreads[1]->state.load() == Worker::SLEEPING)
            && (manager->idleThreads[2]->state.load() == Worker::SLEEPING)) {
            message = "";
            break;
        }
        usleep(1000);
        context.dispatch->poll();
    }
    EXPECT_STREQ("", message);

    // Issue an RPC and make sure it completes.
    transport.outputLog.clear();
    manager->handleRpc(
            new MockTransport::MockServerRpc(&transport, "0x10000 99"));
    waitUntilDone(1);
    manager->poll();
    EXPECT_EQ("serverReply: 0x10001 100", transport.outputLog);
}

// The following tests both the constructor and the clientSend method
// for WorkerSession.
TEST_F(ServiceManagerTest, WorkerSession) {
    MockTransport transport(context);
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    MockTransport::sessionDeleteCount = 0;

    Transport::Session* wrappedSession = new ServiceManager::WorkerSession(
            context, transport.getSession());

    // Make sure that clientSend gets passed down to the underlying session.
    wrappedSession->clientSend(&request, &reply);
    EXPECT_STREQ("clientSend: abcdefg/0", transport.outputLog.c_str());
    EXPECT_EQ(0U, MockTransport::sessionDeleteCount);

    // Make sure that sessions get cleaned up properly.
    delete wrappedSession;
    EXPECT_EQ(1U, MockTransport::sessionDeleteCount);
}

// The next test makes sure that clientSend synchronizes properly with the
// dispatch thread.

void serviceManagerTestWorker(Context* context,
        Transport::SessionRef session) {
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    session->clientSend(&request, &reply);
}

TEST_F(ServiceManagerTest, WorkerSession_SyncWithDispatchThread) {
    Context context(true);
    TestLog::Enable logSilencer;

    MockTransport transport(context);
    Transport::SessionRef wrappedSession = new ServiceManager::WorkerSession(
            context, transport.getSession());
    std::thread child(serviceManagerTestWorker, &context,
            wrappedSession);

    // Make sure the child hangs in clientSend until we invoke the dispatcher.
    usleep(1000);
    EXPECT_STREQ("", transport.outputLog.c_str());
    for (int i = 0; i < 1000; i++) {
        context.dispatch->poll();
        if (transport.outputLog.size() > 0) {
            break;
        }
        usleep(1000);
    }
    EXPECT_STREQ("clientSend: abcdefg/0", transport.outputLog.c_str());
    child.join();
}

TEST_F(ServiceManagerTest, WorkerSession_abort) {
    MockTransport transport(context);
    Buffer request;
    Buffer reply;
    request.fillFromString("abcdefg");
    MockTransport::sessionDeleteCount = 0;

    Transport::Session* wrappedSession = new ServiceManager::WorkerSession(
            context, transport.getSession());

    wrappedSession->abort("test message");
    EXPECT_STREQ("abort: test message", transport.outputLog.c_str());
}

TEST_F(ServiceManagerTest, WorkerSession_cancelRequest) {
    MockTransport transport(context);
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("abcdefg");
    MockTransport::sessionDeleteCount = 0;
    wrapper.testSend(transport.getSession());
    wrapper.cancel();
    EXPECT_STREQ("sendRequest: abcdefg/0 | cancel",
            transport.outputLog.c_str());
}

TEST_F(ServiceManagerTest, WorkerSession_sendRequest) {
    MockTransport transport(context);
    RpcWrapper wrapper(4);
    wrapper.request.fillFromString("abcdefg");
    MockTransport::sessionDeleteCount = 0;
    wrapper.testSend(transport.getSession());
    EXPECT_STREQ("sendRequest: abcdefg/0", transport.outputLog.c_str());
}


} // namespace RAMCloud
