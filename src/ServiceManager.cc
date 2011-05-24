/* Copyright (c) 2011 Stanford University
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

#include "BenchUtil.h"
#include "ServiceManager.h"

namespace RAMCloud {
/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* ServiceManager::sys = &defaultSyscall;

ServiceManager* ServiceManager::serviceManager = NULL;

// Pick a polling period that's much longer than typical RPC round-trip
// times (so the worker thread doesn't go to sleep in an ongoing conversation
// with a single client).
int ServiceManager::pollMicros = 100;

// The following constant is used to signal a worker thread that
// it should exit.
#define WORKER_EXIT reinterpret_cast<Transport::ServerRpc*>(1)

/**
 * Construct a ServiceManager.
 * 
 * \param service
 *      Service that will execute all incoming RPCs.  Eventually this class
 *      needs to support multiple services of different types.  In test mode
 *      this is NULL; see documentation for #service member for details.
 */
ServiceManager::ServiceManager(Service* service)
    : service(service), worker(service), waitingRpcs()
{
    serviceManager = this;
    worker.thread.construct(workerMain, &worker);
}

/**
 * Destroy a ServiceManager.  This method is most commonly invoked during
 * testing (ServiceManagers rarely, if ever, get deleted in normal operation).
 */
ServiceManager::~ServiceManager()
{
    if (serviceManager == this) {
        serviceManager = NULL;
    }
    worker.exit();
}

/**
 * Transports invoke this method when an incoming RPC is complete and
 * ready for processing.  This method will arrange for the RPC (eventually)
 * to be serviced, and will invoke its #sendReply method once the RPC
 * has been serviced.
 *
 * \param rpc
 *      RPC object containing a fully-formed request that is ready for
 *      service.
 */
void
ServiceManager::handleRpc(Transport::ServerRpc* rpc)
{
    if (!serviceManager->worker.idle
            || (serviceManager->service == NULL)) {
        // Worker thread is busy: save this RPC for execution later.
        serviceManager->waitingRpcs.push(rpc);
    } else {
        // The worker thread is available: hand off the RPC.
        serviceManager->worker.handoff(rpc);
    }
}

/**
 * This method is invoked by Dispatch during its polling loop.  It checks
 * for completion of outstanding RPCs.
 */
void
ServiceManager::poll()
{
    if (worker.idle) {
        // Quick shortcut for the common case.
        return;
    }
    int state = worker.state.load();
    if (state == Worker::WORKING) {
        return;
    }

    // The worker is either post-processing or idle; in either case, if
    // there is an RPC that we haven't yet responded to, respond now.
    if (worker.rpc != NULL) {
        worker.rpc->sendReply();
        worker.rpc = NULL;
    }

    // If the worker is idle and there are waiting RPCs, start the next one.
    if (state != Worker::POSTPROCESSING) {
        worker.idle = true;
        if (!waitingRpcs.empty()) {
            worker.handoff(waitingRpcs.front());
            waitingRpcs.pop();
        }
    }
}

/**
 * Wait for an RPC request to arrive, but give up if it takes too long.
 * This method is intended only for testing (it assumes that a NULL
 * service was specified in the constructor).
 *
 * \param timeoutSeconds
 *      If a request doesn't arrive within this many seconds, return NULL.
 *
 * \result
 *      The incoming RPC request, or NULL if nothing arrived within the time
 *      limit.
 */
Transport::ServerRpc*
ServiceManager::waitForRpc(double timeoutSeconds) {
    uint64_t start = rdtsc();
    while (true) {
        if (!waitingRpcs.empty()) {
            Transport::ServerRpc* result = waitingRpcs.front();
            waitingRpcs.pop();
            return result;
        }
        if (cyclesToSeconds(rdtsc() - start) > timeoutSeconds) {
            return NULL;
        }
        dispatch->poll();
    }
}

/**
 * This is the top-level method for worker threads.  It repeatedly waits for
 * an RPC to be assigned to it, then executes that RPC and communicates its
 * completion back to the dispatch thread.
 *
 * \param worker
 *      Pointer to information used to communicate between the worker thread
 *      and the dispatch thread.
 */
void
ServiceManager::workerMain(Worker* worker)
{
    uint64_t pollCycles = nanosecondsToCycles(1000*pollMicros);
    while (true) {
        uint64_t stopPollingTime = dispatch->currentTime + pollCycles;

        // Wait for ServiceManager to supply us with some work to do.
        while (worker->state.load() != Worker::WORKING) {
            if (dispatch->currentTime >= stopPollingTime) {
                // It's been a long time since we've had any work to do; go
                // to sleep so we don't waste any more CPU cycles.  Tricky
                // race condition: the dispatch thread could change the state
                // to WORKING just before we change it to SLEEPING, so use an
                // atomic op and only change to SLEEPING if the current value
                // is POLLING.
                int expected = Worker::POLLING;
                if (worker->state.compare_exchange_weak(expected,
                        Worker::SLEEPING)) {
                    if (sys->futexWait(reinterpret_cast<int*>(&worker->state),
                            Worker::SLEEPING) == -1) {
                        // EWOULDBLOCK means that someone already changed
                        // worker->state, so we didn't block; this is benign.
                        if (errno != EWOULDBLOCK) {
                            LOG(ERROR, "futexWait failed in ServiceManager::"
                                    "workerMain: %s", strerror(errno));
                        }
                    }
                }
            }
            // Empty loop body.
        }
        if (worker->rpc == WORKER_EXIT)
            break;

        Service::Rpc rpc(worker, worker->rpc->recvPayload,
                worker->rpc->replyPayload);
        worker->service->handleRpc(rpc);

        // Pass the RPC back to ServiceManager for completion.
        worker->state.store(Worker::POLLING);
    }
    TEST_LOG("exiting");
}

/**
 * Force this worker's thread to exit (and don't return until it has exited).
 * This method is only used during testing and ServiceManager destruction.
 * This method should be invoked only in the dispatch thread.
 */
void
Worker::exit()
{
    assert(dispatch->isDispatchThread());
    if (exited) {
        // Worker already exited; nothing to do.  This should only happen
        // during tests.
        return;
    }

    // Wait for the worker thread to finish handling any RPCs already
    // queued for it.
    while (!idle) {
        dispatch->poll();
    }

    // Tell the worker thread to exit, and wait for it to actually exit
    // (don't want it referencing the Worker structure anymore, since
    // it could go away).
    handoff(WORKER_EXIT);
    thread->join();
    rpc = NULL;
    idle = true;
    exited = true;
}

/**
 * This method is invoked by the dispatch thread to pass an RPC to an idle
 * worker.  It should only be invoked when the worker is idle (i.e. #rpc is
 * NULL).
 *
 * \param newRpc
 *      RPC object containing a fully-formed request that is ready for
 *      service.
 */
void
Worker::handoff(Transport::ServerRpc* newRpc)
{
    assert(rpc == NULL);
    assert(idle);
    rpc = newRpc;
    int prevState = state.exchange(WORKING);
    if (prevState == SLEEPING) {
        // The worker got tired of polling and went to sleep, so we
        // have to do extra work to wake it up.
        if (ServiceManager::sys->futexWake(reinterpret_cast<int*>(&state), 1)
                == -1) {
            LOG(ERROR,
                    "futexWake failed in Worker::handoff: %s",
                    strerror(errno));
        }
    }
    idle = false;
}

/**
 * Tell the dispatch thread that this worker has finished processing its RPC,
 * so it is safe to start sending the reply.  This method should only be
 * invoked in the worker thread.
 */
void
Worker::sendReply()
{
    state.store(POSTPROCESSING);
}

} // namespace RAMCloud
