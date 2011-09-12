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
    Tub<ServiceManager> manager;
    MockTransport transport;
    MockService service;
    TestLog::Enable logEnabler;
    Syscall *savedSyscall;
    MockSyscall sys;

    ServiceManagerTest() : manager(), transport(), service(),
            logEnabler(), savedSyscall(NULL), sys()
    {
        delete dispatch;
        dispatch = new Dispatch;
        manager.construct();
        manager->addService(service, RpcServiceType(2));
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
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);

    // Wait for the request to be processed for (but don't wait forever).
    for (int i = 0; i < 1000; i++) {
        dispatch->poll();
        if (!transport.outputLog.empty())
            break;
        usleep(1000);
    }
    EXPECT_EQ("rpc: 0x20000 3 4", service.log);
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);
}

TEST_F(ServiceManagerTest, constructor) {
    MockService mock;
    ServiceManager manager1;
    ServiceManager manager2;
    manager2.addService(mock, RpcServiceType(2));
    EXPECT_EQ(0, manager1.serviceCount);
    EXPECT_EQ(1, manager2.serviceCount);
    EXPECT_TRUE(manager2.services[2]);
}

TEST_F(ServiceManagerTest, destructor_clearServiceManager) {
    manager.destroy();
    EXPECT_TRUE(serviceManager != NULL);
    delete serviceManager;
    EXPECT_TRUE(serviceManager == NULL);
    ServiceManager::init();
}

TEST_F(ServiceManagerTest, destructor_cleanupThreads) {
    TestLog::Enable _;

    // Start 2 requests in parallel, but don't let them finish yet.
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc1);
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 5");
    manager->handleRpc(rpc2);
    usleep(1000);
    EXPECT_EQ(2U, manager->busyThreads.size());
    EXPECT_EQ("", TestLog::get());

    // Allow the requests to finish, but destroy the ServiceManager before
    // they have been moved back to the idle list.
    service.gate = 3;
    manager.destroy();
    EXPECT_EQ("workerMain: exiting | workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, addService) {
    MockService mock;
    ServiceManager manager1;
    EXPECT_EQ(0, manager1.serviceCount);
    EXPECT_FALSE(manager1.services[2]);
    manager1.addService(mock, RpcServiceType(2));
    EXPECT_EQ(1, manager1.serviceCount);
    EXPECT_TRUE(manager1.services[2]);
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
            &transport, "0x20000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x20000 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x20000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x20000 4");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(0U, manager->services[2]->waitingRpcs.size());
    manager->handleRpc(rpc4);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(1U, manager->services[2]->waitingRpcs.size());
}

TEST_F(ServiceManagerTest, handleRpc_createNewThreadsIfNeeded) {
    // Start 2 RPCs concurrently, which will require thread creation.
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x20000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x20000 2");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(2U, manager->idleThreads.size());

    // Start 2 more RPCs concurrently, and make sure they use the existing
    // workers.
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x20000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x20000 4");
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    EXPECT_EQ(0U, manager->idleThreads.size());
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(2U, manager->idleThreads.size());
}

TEST_F(ServiceManagerTest, idle) {
    EXPECT_TRUE(manager->idle());
    // Start one RPC.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
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
            &transport, "0x20000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x20000 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x20000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x20000 4");
    MockTransport::MockServerRpc* rpc5 = new MockTransport::MockServerRpc(
            &transport, "0x20000 5");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    manager->handleRpc(rpc5);
    EXPECT_EQ(2U, manager->services[2]->waitingRpcs.size());

    // Allow 2 of the requests to complete, and make sure that the remaining
    // 2 start service.
    service.gate = 1;
    waitUntilDone(1);
    service.gate = 2;
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(0U, manager->services[2]->waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x20001 3 | serverReply: 0x20001 2",
            transport.outputLog);

    // Allow the request in slot 0 of busyThreads to complete.
    transport.outputLog.clear();
    service.gate = 5;
    waitUntilDone(1);
    manager->poll();
    EXPECT_EQ("serverReply: 0x20001 6", transport.outputLog);
    EXPECT_EQ("0x20000 3", TestUtil::toString(
              &manager->busyThreads[0]->rpc->requestPayload));

    // Allow the remaining requests to complete.
    transport.outputLog.clear();
    service.gate = 0;
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ("serverReply: 0x20001 5 | serverReply: 0x20001 4",
            transport.outputLog);
}
TEST_F(ServiceManagerTest, poll_postprocessing) {
    // This test makes sure that the POSTPROCESSING state is handled
    // correctly (along with the subsequent POLLING state).
    service.gate = -1;
    service.sendReply = true;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    waitUntilDone(1);

    // At this point the state of the worker should be POSTPROCESSING.
    manager->poll();
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);
    EXPECT_EQ(0U, manager->idleThreads.size());

    // Now allow the worker to finish.
    service.gate = 0;
    for (int i = 0; i < 1000; i++) {
        manager->poll();
        if (!manager->idleThreads.empty())
            break;
        usleep(1000);
    }
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);
    EXPECT_EQ(1U, manager->idleThreads.size());
}

// No tests for waitForRpc: this method is only used in tests.

TEST_F(ServiceManagerTest, workerMain_goToSleep) {
    // Issue an RPC request to create a worker thread.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    waitUntilDone(1);
    manager->poll();
    Worker* worker = manager->idleThreads[0];
    transport.outputLog.clear();

    // Initially the worker should not go to sleep (time appears to
    // stand still for it, because we aren't calling dispatch->poll).
    usleep(20000);
    EXPECT_EQ(Worker::POLLING, worker->state.load());

    // Update dispatch->currentTime. When the worker sees this it should
    // go to sleep.
    dispatch->currentTime = Cycles::rdtsc();
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
            &transport, "0x20000 3 4"));
    manager.destroy();
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);
}
TEST_F(ServiceManagerTest, workerMain_futexError) {
    // Issue an RPC request to create a worker thread.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    waitUntilDone(1);
    manager->poll();
    Worker* worker = manager->idleThreads[0];

    sys.futexWaitErrno = EPERM;
    // Wait for the worker to go to sleep, then make sure it logged
    // an error message.
    usleep(20000);
    dispatch->currentTime = Cycles::rdtsc();
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
    // Issue an RPC request to create a worker thread.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    manager.destroy();
    EXPECT_EQ("workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, Worker_exit) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    manager->busyThreads[0]->exit();
    EXPECT_EQ("workerMain: exiting", TestLog::get());
    EXPECT_EQ("serverReply: 0x20001 4 5", transport.outputLog);

    // Try another exit just to make sure it's itempotent.
    manager->busyThreads[0]->exit();
}

TEST_F(ServiceManagerTest, Worker_handoff_dontCallFutex) {
    TestLog::Enable _;
    // Set futex to return an error, so we can make sure it doesn't
    // get called in the normal case.
    sys.futexWakeErrno = EPERM;
    manager->handleRpc(
            new MockTransport::MockServerRpc(&transport, "0x20000 99"));
    EXPECT_EQ("", TestLog::get());
    waitUntilDone(1);
    EXPECT_EQ("rpc: 0x20000 99", service.log);

    // Reset error so that the ServiceManager destructor can work
    // correctly.
    sys.futexWakeErrno = 0;
}

TEST_F(ServiceManagerTest, Worker_handoff_callFutex) {
    // Issue an RPC request to create a worker thread; wait for it
    // to go to sleep.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x20000 3 4");
    manager->handleRpc(rpc);
    const char *message = "worker didn't go to sleep";
    for (int i = 0; i < 1000; i++) {
        if (!manager->idleThreads.empty()
                && (manager->idleThreads[0]->state.load() ==
                Worker::SLEEPING)) {
            message = "";
            break;
        }
        usleep(1000);
        dispatch->poll();
    }
    EXPECT_STREQ("", message);

    // Issue the second RPC and make sure it completes.
    transport.outputLog.clear();
    manager->handleRpc(
            new MockTransport::MockServerRpc(&transport, "0x20000 99"));
    waitUntilDone(1);
    manager-> poll();
    EXPECT_EQ("serverReply: 0x20001 100", transport.outputLog);
}

} // namespace RAMCloud
