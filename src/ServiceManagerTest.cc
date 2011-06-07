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
    Tub<ServiceManager> serviceManager;
    MockTransport transport;
    MockService service;
    TestLog::Enable logEnabler;
    Syscall *savedSyscall;
    MockSyscall sys;

    ServiceManagerTest()
        : serviceManager(), transport(), service(), logEnabler(),
          savedSyscall(NULL), sys()
    {
        delete dispatch;
        dispatch = new Dispatch;
        serviceManager.construct(&service);
        savedSyscall = ServiceManager::sys;
        ServiceManager::sys = &sys;
    }

    ~ServiceManagerTest() {
        // Must explicitly destroy the service manager (to ensure that the
        // worker thread exits); if we used implicit destruction, it's possible
        // that some other objects such as MockService might get destroyed while
        // the worker thread is still using them.
        serviceManager.destroy();
        ServiceManager::sys = savedSyscall;
    }

    // Give the current RPC a chance to complete (but give up after a while).
    void
    waitUntilDone()
    {
        for (int i = 0; i < 1000; i++) {
            if (serviceManager->worker.state.load() !=
                    Worker::WORKING) {
                break;
            }
            usleep(1000);
        }
    }
    DISALLOW_COPY_AND_ASSIGN(ServiceManagerTest);
};

TEST_F(ServiceManagerTest, sanityCheck) {
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);

    // Wait for the request to be processed for (but don't wait forever).
    for (int i = 0; i < 1000; i++) {
        dispatch->poll();
        if (!service.log.empty())
            break;
        usleep(1000);
    }
    EXPECT_EQ("rpc: 3 4", service.log);
    EXPECT_EQ("serverReply: 4 5", transport.outputLog);
}

TEST_F(ServiceManagerTest, destructor) {
    TestLog::Enable _;
    ServiceManager* manager1 = new ServiceManager(NULL);
    ServiceManager* manager2 = new ServiceManager(NULL);
    EXPECT_EQ(manager2, ServiceManager::serviceManager);
    delete manager1;
    EXPECT_EQ(manager2, ServiceManager::serviceManager);
    delete manager2;
    EXPECT_EQ(static_cast<ServiceManager*>(NULL),
              ServiceManager::serviceManager);

    // Make sure the workers exited.
    EXPECT_EQ("workerMain: exiting | workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, handleRpc) {
    // First RPC should get passed to worker immediately.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);
    EXPECT_EQ(0U, serviceManager->waitingRpcs.size());

    // Worker hasn't finished first RPC, so second RPC gets queued.
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "5 6");
    ServiceManager::handleRpc(rpc2);
    EXPECT_EQ(1U, serviceManager->waitingRpcs.size());

    // Just for fun, try a third RPC.
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "7 8 9");
    ServiceManager::handleRpc(rpc3);
    EXPECT_EQ(2U, serviceManager->waitingRpcs.size());

    // Let all of the RPCs finish.
    serviceManager.destroy();
    EXPECT_EQ("serverReply: 4 5 | serverReply: 6 7 | serverReply: 8 9 10",
            transport.outputLog);
}

TEST_F(ServiceManagerTest, idle) {
    EXPECT_TRUE(ServiceManager::idle());
    // Start one RPC.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);
    EXPECT_FALSE(ServiceManager::idle());

    // Wait for it to finish.
    waitUntilDone();
    serviceManager->poll();
    EXPECT_TRUE(ServiceManager::idle());
}

TEST_F(ServiceManagerTest, pollInvokeBasics) {
    // First call: nothing to do.
    serviceManager->poll();

    // Second call: one RPC has finished, another needs to be
    // scheduled.
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "5 6");
    ServiceManager::handleRpc(rpc2);
    waitUntilDone();
    serviceManager->poll();
    EXPECT_EQ("serverReply: 4 5", transport.outputLog);
    transport.outputLog.clear();

    // Third call: cleanup after the second RPC, but no new RPC to schedule.
    waitUntilDone();
    serviceManager->poll();
    EXPECT_EQ("serverReply: 6 7", transport.outputLog);
    EXPECT_EQ(static_cast<Transport::ServerRpc*>(NULL),
              serviceManager->worker.rpc);
}
TEST_F(ServiceManagerTest, pollInvokePostprocessing) {
    // This test make sure that the POSTPROCESSING state is handled
    // correctly (along with the subsequent POLLING state).  Exit
    // the worker thread so we can manipulate the worker's state without
    // interference.
    serviceManager->worker.exit();
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);

    // Worker has "working": simulate a call to sendReply.
    serviceManager->worker.state.store(
            Worker::POSTPROCESSING);
    serviceManager->poll();
    EXPECT_EQ("serverReply: ", transport.outputLog);

    // Now fake a finish for the RPC.
    serviceManager->worker.state.store(Worker::POLLING);
    serviceManager->poll();
    EXPECT_EQ("serverReply: ", transport.outputLog);
}

// No tests for waitForRpc: this method is only used in tests.

TEST_F(ServiceManagerTest, workerMainGoToSleep) {
    // Initially the worker should not go to sleep (time appears to
    // stand still for it, because we aren't calling dispatch->poll).
    usleep(1000);
    EXPECT_EQ(Worker::POLLING,
              serviceManager->worker.state.load());

    // Update dispatch->currentTime. When the worker sees this it should
    // go to sleep.
    dispatch->currentTime = rdtsc();
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (serviceManager->worker.state.load() ==
                Worker::SLEEPING) {
            break;
        }
    }
    EXPECT_EQ(Worker::SLEEPING,
              serviceManager->worker.state.load());

    // Make sure that the worker can be woken up.
    EXPECT_EQ(static_cast<Transport::ServerRpc*>(NULL),
              serviceManager->worker.rpc);
    ServiceManager::handleRpc(new MockTransport::MockServerRpc(
            &transport, "3 4"));
    serviceManager.destroy();
    EXPECT_EQ("serverReply: 4 5", transport.outputLog);
}

TEST_F(ServiceManagerTest, workerMainFutexError) {
    sys.futexWaitErrno = EPERM;
    // Wait for the worker to go to sleep, then make sure it logged
    // an error message.
    usleep(1000);
    dispatch->currentTime = rdtsc();
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (serviceManager->worker.state.load() ==
                Worker::SLEEPING) {
            break;
        }
    }
    EXPECT_EQ("workerMain: futexWait failed in ServiceManager::workerMain: "
                "Operation not permitted", TestLog::get());
}

TEST_F(ServiceManagerTest, workerMainExit) {
    serviceManager.destroy();
    EXPECT_EQ("workerMain: exiting", TestLog::get());
}

TEST_F(ServiceManagerTest, workerExit) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "3 4");
    ServiceManager::handleRpc(rpc);
    serviceManager->worker.exit();
    EXPECT_EQ("workerMain: exiting", TestLog::get());
    EXPECT_EQ("serverReply: 4 5", transport.outputLog);

    // Try another exit just to make sure it's itempotent.
    serviceManager->worker.exit();
}

TEST_F(ServiceManagerTest, workerHandoffDontCallFutex) {
    // Set futex to return an error, so we can make sure it doesn't
    // get called in the normal case.
    sys.futexWakeErrno = EPERM;
    serviceManager->worker.handoff(
            new MockTransport::MockServerRpc(&transport, "99"));
    EXPECT_EQ("", TestLog::get());
    waitUntilDone();
    EXPECT_EQ("rpc: 99", service.log);

    // Reset error so that the ServiceManager destructor can work
    // correctly.
    sys.futexWakeErrno = 0;
}

TEST_F(ServiceManagerTest, workerHandoffCallFutex) {
    // Set futex to return an error, so we can make sure it was called
    // (and also check error handling).
    sys.futexWakeErrno = EPERM;
    serviceManager->worker.state = Worker::SLEEPING;
    serviceManager->worker.handoff(
            new MockTransport::MockServerRpc(&transport, "99"));
    EXPECT_EQ("handoff: futexWake failed in Worker"
            "::handoff: Operation not permitted", TestLog::get());
}

} // namespace RAMCloud
