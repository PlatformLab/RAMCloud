/* Copyright (c) 2011-2015 Stanford University
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
#include "RpcLevel.h"
#include "Tub.h"
#include "WorkerManager.h"

namespace RAMCloud {

    extern int nextId, workersAlive;

class WorkerManagerTest : public ::testing::Test {
  public:
    Context context;
    Tub<WorkerManager> manager;
    MockTransport transport;
    MockService service;
    TestLog::Enable logEnabler;
    Syscall *savedSyscall;
    MockSyscall sys;

    WorkerManagerTest()
        : context()
        , manager()
        , transport(&context)
        , service()
        , logEnabler()
        , savedSyscall(NULL)
        , sys()
    {
        static uint8_t levels[] = {0, 1, 2, 0, 1, 2};
        manager.construct(&context, 2);
        context.services[WireFormat::BACKUP_SERVICE] = &service;
        savedSyscall = WorkerManager::sys;
        WorkerManager::sys = &sys;
        RpcLevel::levelsPtr = levels;
        RpcLevel::savedMaxLevel = 2;
    }

    ~WorkerManagerTest() {
        service.gate = 0;
        // Must explicitly destroy the service manager (to ensure that the
        // worker thread exits); if we used implicit destruction, it's possible
        // that some other objects such as MockService might get destroyed while
        // the worker thread is still using them.
        manager.destroy();
        WorkerManager::sys = savedSyscall;
        RpcLevel::savedMaxLevel = -1;
        RpcLevel::levelsPtr = RpcLevel::levels;
    }

    // Wait for a given number of the currently-executing RPCs (i.e. those
    // actually assigned to a worker) to complete, but give up if this
    // takes too long.
    void
    waitUntilDone(int count)
    {
        int completed;
        for (int i = 0; i < 1000; i++) {
            completed = 0;
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
        EXPECT_EQ(count, completed);
    }
    DISALLOW_COPY_AND_ASSIGN(WorkerManagerTest);
};

TEST_F(WorkerManagerTest, sanityCheck) {
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

TEST_F(WorkerManagerTest, constructor) {
    EXPECT_EQ(4U, manager->idleThreads.size());
    EXPECT_EQ(3U, manager->levels.size());
    WorkerManager manager1(&context, 7);
    EXPECT_EQ(9U, manager1.idleThreads.size());
}

TEST_F(WorkerManagerTest, destructor_cleanupThreads) {
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

    // Allow the requests to finish, but destroy the WorkerManager before
    // they have been moved back to the idle list.
    service.gate = 3;
    manager.destroy();
    EXPECT_EQ("workerMain: exiting | workerMain: exiting | "
            "workerMain: exiting | workerMain: exiting", TestLog::get());
}

TEST_F(WorkerManagerTest, handleRpc_noHeader) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "");
    manager->handleRpc(rpc);
    EXPECT_EQ("handleRpc: Incoming RPC contains no header (message length 0)",
            TestLog::get());
    EXPECT_STREQ("STATUS_MESSAGE_TOO_SHORT",
            statusToSymbol(transport.status));
}

TEST_F(WorkerManagerTest, handleRpc_badOpcode) {
    TestLog::Enable _;
    MockTransport::MockServerRpc* rpc = new MockTransport::MockServerRpc(
            &transport, "0x10100");
    manager->handleRpc(rpc);
    EXPECT_EQ("handleRpc: Incoming RPC contained unknown opcode 256",
            TestLog::get());
    EXPECT_STREQ("STATUS_UNIMPLEMENTED_REQUEST",
            statusToSymbol(transport.status));
}

TEST_F(WorkerManagerTest, handleRpc_deferRpc) {
    // Create 2 RPCs that can be scheduled.
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10002 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10002 2");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    EXPECT_EQ(2U, manager->busyThreads.size());
    EXPECT_EQ(2, manager->levels[2].requestsRunning);

    // We're now past the maxCores limit, but this RPC gets scheduled
    // because it has a low level that isn't currently executing an RPC.
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3");
    manager->handleRpc(rpc3);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(1, manager->levels[0].requestsRunning);

    // The next RPC doesn't get scheduled because there's already an
    // RPC executing with a lower level.
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10001 4");
    manager->handleRpc(rpc4);
    EXPECT_EQ(3U, manager->busyThreads.size());
    EXPECT_EQ(1U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
}

TEST_F(WorkerManagerTest, handleRpc_handoffToWorker) {
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 2");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    waitUntilDone(2);
    manager->poll();
    EXPECT_EQ(4U, manager->idleThreads.size());
}

TEST_F(WorkerManagerTest, idle) {
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

TEST_F(WorkerManagerTest, poll_scheduleWaitingRpcs) {
    // Start 2 RPCs concurrently, with 2 more waiting.
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10000 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10000 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10001 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10002 4");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    EXPECT_EQ(2, manager->levels[0].requestsRunning);
    EXPECT_EQ(1U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ(1U, manager->levels[2].waitingRpcs.size());

    // Allow the original requests to complete, and make sure that the
    // remaining 2 start service in the right order (e.g., the level
    // for rpc2 is determined by the opcode 0 and the levels variable).
    service.gate = 1;
    waitUntilDone(1);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(1, manager->levels[0].requestsRunning);
    EXPECT_EQ(1, manager->levels[1].requestsRunning);
    EXPECT_EQ(0U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10001 2", transport.outputLog);
    EXPECT_EQ(1, manager->rpcsWaiting);
    service.gate = 2;
    waitUntilDone(1);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(0, manager->levels[0].requestsRunning);
    EXPECT_EQ(1, manager->levels[2].requestsRunning);
    EXPECT_EQ(0U, manager->levels[2].waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10001 2 | serverReply: 0x10001 3",
            transport.outputLog);

    // Allow the remaining requests to complete.
    transport.outputLog.clear();
    service.gate = 0;
    waitUntilDone(2);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(0, manager->levels[2].requestsRunning);
    EXPECT_EQ("serverReply: 0x10003 5 | serverReply: 0x10002 4",
            transport.outputLog);

    // There should be nothing left to do now.
    EXPECT_EQ(0, manager->poll());
}

TEST_F(WorkerManagerTest, poll_avoidDeadlock) {
    // This test ensures that we keep starting low-level threads even
    // if we're above the core limit.
    // Start 2 RPCs at level 2 then 1 at level 0
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10002 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10002 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10001 4");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    EXPECT_EQ(1, manager->levels[0].requestsRunning);
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(2, manager->levels[2].requestsRunning);
    EXPECT_EQ(1U, manager->levels[1].waitingRpcs.size());

    // Allow rpc3 (level 0) to complete, and make sure rpc4 (level 1) starts.
    service.gate = 3;
    waitUntilDone(1);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(0, manager->levels[0].requestsRunning);
    EXPECT_EQ(1, manager->levels[1].requestsRunning);
    EXPECT_EQ(0U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10001 4", transport.outputLog);
    EXPECT_EQ(0, manager->rpcsWaiting);

    // Allow the remaining requests to complete.
    transport.outputLog.clear();
    service.gate = 0;
    waitUntilDone(3);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(0, manager->levels[2].requestsRunning);
    EXPECT_EQ("serverReply: 0x10002 5 | serverReply: 0x10003 3 | "
            "serverReply: 0x10003 2",
            transport.outputLog);

    // There should be nothing left to do now.
    EXPECT_EQ(0, manager->poll());
}

TEST_F(WorkerManagerTest, poll_coreLimit) {
    // Don't start a new RPC if we are already over the core limit and
    // the new RPC isn't lower level than other running RPCs.
    // Start 2 RPCs at level 2 then 1 at level 0
    service.gate = -1;
    MockTransport::MockServerRpc* rpc1 = new MockTransport::MockServerRpc(
            &transport, "0x10002 1");
    MockTransport::MockServerRpc* rpc2 = new MockTransport::MockServerRpc(
            &transport, "0x10002 2");
    MockTransport::MockServerRpc* rpc3 = new MockTransport::MockServerRpc(
            &transport, "0x10000 3");
    MockTransport::MockServerRpc* rpc4 = new MockTransport::MockServerRpc(
            &transport, "0x10001 4");
    manager->handleRpc(rpc1);
    manager->handleRpc(rpc2);
    manager->handleRpc(rpc3);
    manager->handleRpc(rpc4);
    EXPECT_EQ(1, manager->levels[0].requestsRunning);
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(2, manager->levels[2].requestsRunning);
    EXPECT_EQ(1U, manager->levels[1].waitingRpcs.size());

    // Allow rpc1 (level 2) to complete, and make sure rpc4 (level 1)
    // doesn't start.
    service.gate = 1;
    waitUntilDone(1);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(1, manager->levels[0].requestsRunning);
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(1U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10003 2", transport.outputLog);
    EXPECT_EQ(1, manager->rpcsWaiting);

    // Finish rpc2 (level 2): now rpc4 should be able to start.
    transport.outputLog.clear();
    service.gate = 2;
    waitUntilDone(1);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(1, manager->levels[0].requestsRunning);
    EXPECT_EQ(1, manager->levels[1].requestsRunning);
    EXPECT_EQ(0U, manager->levels[1].waitingRpcs.size());
    EXPECT_EQ("serverReply: 0x10003 3", transport.outputLog);
    EXPECT_EQ(0, manager->rpcsWaiting);

    // Allow the remaining requests to complete.
    transport.outputLog.clear();
    service.gate = 0;
    waitUntilDone(2);
    EXPECT_EQ(1, manager->poll());
    EXPECT_EQ(0, manager->levels[1].requestsRunning);
    EXPECT_EQ(0, manager->levels[2].requestsRunning);
    EXPECT_EQ("serverReply: 0x10002 5 | serverReply: 0x10001 4",
            transport.outputLog);

    // There should be nothing left to do now.
    EXPECT_EQ(0, manager->poll());
}

TEST_F(WorkerManagerTest, poll_postprocessing) {
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
    EXPECT_EQ(3U, manager->idleThreads.size());

    // Now allow the worker to finish.
    service.gate = 0;
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        manager->poll();
        if (manager->idleThreads.size() == 4)
            break;
        usleep(1000);
    }
    EXPECT_EQ("serverReply: 0x10001 4 5", transport.outputLog);
    EXPECT_EQ(4U, manager->idleThreads.size());
}

// No tests for waitForRpc: this method is only used in tests.

TEST_F(WorkerManagerTest, workerMain_goToSleep) {
    // Workers were already created when the test initialized.  Initially
    // the (first) worker should not go to sleep (time appears to
    // stand still for it, because we stop the TSC clock).
    Cycles::mockTscValue = Cycles::rdtsc();
    Worker* worker = manager->idleThreads[0];
    transport.outputLog.clear();
    usleep(20000);
    EXPECT_EQ(Worker::POLLING, worker->state.load());

    // Restart the clock. When the worker sees this it should sleep.
    Cycles::mockTscValue = 0;
    // See "Timing-Dependent Tests" in designNotes.
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

TEST_F(WorkerManagerTest, workerMain_futexError) {
    // Get rid of the original manager (it has too many threads, which
    // would confuse this test).
    manager.destroy();
    TestLog::reset();

    // Create a new manager with only 1 worker thread.
    RpcLevel::savedMaxLevel = 0;
    WorkerManager manager2(&context, 1);
    EXPECT_EQ(1U, manager2.idleThreads.size());
    MockService service2(1);
    Worker* worker = manager2.idleThreads[0];

    sys.futexWaitErrno = EPERM;
    // Wait for the worker to go to sleep, then make sure it logged
    // an error message.
    usleep(20000);
    context.dispatch->currentTime = Cycles::rdtsc();
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        usleep(100);
        if (worker->state.load() == Worker::SLEEPING) {
            break;
        }
    }
    EXPECT_EQ("workerMain: futexWait failed in WorkerManager::workerMain: "
                "Operation not permitted", TestLog::get());
}

TEST_F(WorkerManagerTest, workerMain_exit) {
    manager.destroy();
    EXPECT_EQ("workerMain: exiting | workerMain: exiting | "
            "workerMain: exiting | workerMain: exiting", TestLog::get());
}

TEST_F(WorkerManagerTest, Worker_exit) {
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

TEST_F(WorkerManagerTest, Worker_handoff_dontCallFutex) {
    TestLog::Enable _;
    // Set futex to return an error, so we can make sure it doesn't
    // get called in the normal case.
    sys.futexWakeErrno = EPERM;
    manager->handleRpc(
            new MockTransport::MockServerRpc(&transport, "0x10000 99"));
    EXPECT_EQ("", TestLog::get());
    waitUntilDone(1);
    EXPECT_EQ("rpc: 0x10000 99", service.log);

    // Reset error so that the WorkerManager destructor can work
    // correctly.
    sys.futexWakeErrno = 0;
}

TEST_F(WorkerManagerTest, Worker_handoff_callFutex) {
    // Wait for all the workers to go to sleep.
    // See "Timing-Dependent Tests" in designNotes.
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

} // namespace RAMCloud
