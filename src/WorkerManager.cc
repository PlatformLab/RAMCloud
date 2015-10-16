/* Copyright (c) 2011-2015 Stanford University
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

#include <new>
#include "BitOps.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Fence.h"
#include "Initialize.h"
#include "PerfStats.h"
#include "RawMetrics.h"
#include "RpcLevel.h"
#include "ShortMacros.h"
#include "ServerRpcPool.h"
#include "TimeTrace.h"
#include "TimeTraceUtil.h"
#include "WireFormat.h"
#include "WorkerManager.h"

// If the following line is uncommented, trace records will be generated that
// allow service times to be computed for all RPCs.
// WARNING: These extra logging calls may (read: will likely) make the system
// unstable. The additional file IO on the dispatch thread will cause service
// gaps that prevent servers from responding to pings quickly enough to prevent
// eviction from the cluster.
// #define LOG_RPCS 1

// Uncomment following line (or specifying -D SMTT on the make command line)
// to enable a bunch of time tracing in this module.
// #define SMTT 1

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
Syscall* WorkerManager::sys = &defaultSyscall;

// Length of time that a worker will actively poll for new work before it puts
// itself to sleep. This period should be much longer than typical RPC
// round-trip times so the worker thread doesn't go to sleep in an ongoing
// conversation with a single client.  It must also be much longer than the
// time it takes to wake up the thread once it has gone to sleep (as of
// September 2011 this time appears to be as much as 50 microseconds).
int WorkerManager::pollMicros = 10000;
// The following constant is used to signal a worker thread that
// it should exit.
#define WORKER_EXIT reinterpret_cast<Transport::ServerRpc*>(1)

/**
 * Construct a WorkerManager.
 *
 * \param context
 *      Overall information about this server.
 * \param maxCores
 *      This class will try to ensure that the number of running worker
 *      threads doesn't exceed this value. However, in order to prevent
 *      deadlocks, it may occasionally be necessary to go beyond this
 *      limit.
 */
WorkerManager::WorkerManager(Context* context, uint32_t maxCores)
    : Dispatch::Poller(context->dispatch, "WorkerManager")
    , context(context)
    , levels()
    , busyThreads()
    , idleThreads()
    , maxCores(maxCores)
    , rpcsWaiting(0)
    , testingSaveRpcs(0)
    , testRpcs()
{
    levels.resize(RpcLevel::maxLevel() + 1);

    // Create all the worker threads. We create enough threads to
    // execute maxCores RPCs in parallel, *plus* one thread for each
    // RPC level not already in use. This is sufficient to prevent
    // distributed deadlock over worker threads.
    //
    // Note: we create threads here rather than waiting until the thread
    // is needed, because thread creation can be quite slow on Linux
    // (> 250ms sometimes, see RAM-343) and a long stall in actually
    // scheduling a thread can cause timeouts.

    for (int i = maxCores + RpcLevel::maxLevel(); i > 0; i--) {
        Worker* worker = new Worker(context);
        worker->thread.construct(workerMain, worker);
        idleThreads.push_back(worker);
    }
}

/**
 * Destroy a WorkerManager.  This method is most commonly invoked during
 * testing (WorkerManagers rarely, if ever, get deleted in normal operation).
 */
WorkerManager::~WorkerManager()
{
    Dispatch* dispatch = context->dispatch;
    assert(dispatch->isDispatchThread());
    while (!busyThreads.empty()) {
        dispatch->poll();
    }
    foreach (Worker* worker, idleThreads) {
        worker->exit();
        delete worker;
    }
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
WorkerManager::handleRpc(Transport::ServerRpc* rpc)
{
    // Find the service for this RPC.
    const WireFormat::RequestCommon* header;
    header = rpc->requestPayload.getStart<WireFormat::RequestCommon>();
    if ((header == NULL) || (header->opcode >= WireFormat::ILLEGAL_RPC_TYPE)) {
#if TESTING
        if (testingSaveRpcs) {
            // Special case for testing.
            testRpcs.push(rpc);
            return;
        }
#endif
        if (header == NULL) {
            LOG(WARNING, "Incoming RPC contains no header (message length %d)",
                    rpc->requestPayload.size());
            Service::prepareErrorResponse(&rpc->replyPayload,
                    STATUS_MESSAGE_TOO_SHORT);
        } else {
            LOG(WARNING, "Incoming RPC contained unknown opcode %d",
                    header->opcode);
            Service::prepareErrorResponse(&rpc->replyPayload,
                    STATUS_UNIMPLEMENTED_REQUEST);
        }
        rpc->sendReply();
        return;
    }
    int level = RpcLevel::getLevel(WireFormat::Opcode(header->opcode));
#ifdef LOG_RPCS
    LOG(NOTICE, "Received %s RPC at %lu with %u bytes",
            WireFormat::opcodeSymbol(header->opcode),
            reinterpret_cast<uint64_t>(rpc),
            rpc->requestPayload.size());
#endif

    // See if we should start executing this request. Once we reach our
    // desired concurrency limit, only start a new request if its level
    // is lower than that of any other running request. This ensures that
    // we will always have enough threads to execute one request at
    // each level, and this prevents distributed deadlock (deadlock could
    // occur if all of the servers use up all of their threads on high-level
    // requests, then those requests invoke lower-level RPCs to other
    // servers, but none of the servers have threads to execute those
    // lower-level requests).
    if (busyThreads.size() >= maxCores) {
        for (int i = level; i >= 0; i--) {
            if (levels[i].requestsRunning > 0) {
                // Can't run this request right now.
                levels[level].waitingRpcs.push(rpc);
                rpcsWaiting++;
                return;
            }
        }
    }

    // Temporary code to test how much faster things would be without threads.
#if 0
    if ((header->opcode == WireFormat::READ) &&
            (header->service == WireFormat::MASTER_SERVICE)) {
        Service::Rpc serviceRpc(NULL, &rpc->requestPayload, &rpc->replyPayload);
        services[WireFormat::MASTER_SERVICE]->service.handleRpc(&serviceRpc);
        rpc->sendReply();
        return;
    }
#endif

    levels[level].requestsRunning++;

    // Hand off the RPC to a worker thread.
    assert(!idleThreads.empty());
    Worker* worker = idleThreads.back();
    idleThreads.pop_back();
    worker->opcode = WireFormat::Opcode(header->opcode);
    worker->level = level;
    worker->handoff(rpc);
    worker->busyIndex = downCast<int>(busyThreads.size());
    busyThreads.push_back(worker);
}

/**
 * Returns true if there are currently no RPCs being serviced, false
 * if at least one RPC is currently being executed by a worker.  If true
 * is returned, it also means that any changes to memory made by any
 * worker threads will be visible to the caller.
 */
bool
WorkerManager::idle()
{
    return busyThreads.empty();
}

/**
 * This method is invoked by Dispatch during its polling loop.  It checks
 * for completion of outstanding RPCs.
 */
int
WorkerManager::poll()
{
    int foundWork = 0;

    // Each iteration of the following loop checks the status of one active
    // worker. The order of iteration is crucial, since it allows us to
    // remove a worker from busyThreads in the middle of the loop without
    // interfering with the remaining iterations.
    for (int i = downCast<int>(busyThreads.size()) - 1; i >= 0; i--) {
        Worker* worker = busyThreads[i];
        assert(worker->busyIndex == i);
        int state = worker->state.load();
        if (state == Worker::WORKING) {
            continue;
        }
        foundWork = 1;
        Fence::enter();

        // The worker is either post-processing or idle; in either case,
        // there may be an RPC that we have to respond to. Save the RPC
        // information for now.
        Transport::ServerRpc* rpc = worker->rpc;
        worker->rpc = NULL;

        // Highest priority: if there are pending requests that are waiting
        // for workers, hand off a new request to this worker ASAP.
        bool startedNewRpc = false;
        if (state != Worker::POSTPROCESSING) {
            levels[worker->level].requestsRunning--;
            if (rpcsWaiting) {
                // Start an RPC with the lowest level (this is most efficient,
                // since it's more likely that there are other servers with
                // resources tied up waiting for this RPC).
                //
                // In addition, we must observe the core limits, which means
                // we don't start another RPC unless we have spare cores, or
                // unless the RPC we would start is at a level lower than any
                // other running RPC.
                for (int i = 0; ; i++) {
                    Level* level = &levels[i];
                    if ((level->requestsRunning != 0) &&
                            // Note: we haven't yet removed the current
                            // thread from busyThreads, so the number of
                            // running workers is one less than
                            // busyThreads.size().
                            (busyThreads.size() > maxCores)) {
                        // Can't start another RPC without exceeding core
                        // limits.
                        break;
                    }
                    if (level->waitingRpcs.empty()) {
                        continue;
                    }
                    rpcsWaiting--;
                    level->requestsRunning++;
                    worker->level = i;
                    worker->handoff(level->waitingRpcs.front());
                    level->waitingRpcs.pop();
                    startedNewRpc = true;
                    break;
                }
            }
        }

        // Now send the response, if any.
        if (rpc != NULL) {
#ifdef LOG_RPCS
            LOG(NOTICE, "Sending reply for %s at %lu with %u bytes",
                    WireFormat::opcodeSymbol(&rpc->requestPayload),
                    reinterpret_cast<uint64_t>(rpc),
                    rpc->replyPayload.size());
#endif
            rpc->sendReply();
#ifdef SMTT
            context->timeTrace->record(
                    TimeTraceUtil::statusMsg(worker->threadId,
                    worker->opcode, TimeTraceUtil::RequestStatus::REPLY_SENT));
#endif

        }

        // If the worker is idle, remove it from busyThreads (fill its
        // slot with the worker in the last slot).
        if (!startedNewRpc && (state != Worker::POSTPROCESSING)) {
            if (worker != busyThreads.back()) {
                busyThreads[worker->busyIndex] = busyThreads.back();
                busyThreads[worker->busyIndex]->busyIndex =
                        worker->busyIndex;
            }
            busyThreads.pop_back();
            worker->busyIndex = -1;
            idleThreads.push_back(worker);
        }
    }
    return foundWork;
}

/**
 * Wait for an RPC request to appear in the testRpcs queue, but give up if
 * it takes too long.  This method is intended only for testing (it only
 * works when there are no registered services).
 *
 * \param timeoutSeconds
 *      If a request doesn't arrive within this many seconds, return NULL.
 *
 * \result
 *      The incoming RPC request, or NULL if nothing arrived within the time
 *      limit.
 */
Transport::ServerRpc*
WorkerManager::waitForRpc(double timeoutSeconds) {
    uint64_t start = Cycles::rdtsc();
    while (true) {
        if (!testRpcs.empty()) {
            Transport::ServerRpc* result = testRpcs.front();
            testRpcs.pop();
            return result;
        }
        if (Cycles::toSeconds(Cycles::rdtsc() - start) > timeoutSeconds) {
            return NULL;
        }
        context->dispatch->poll();
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
WorkerManager::workerMain(Worker* worker)
{
    PerfStats::registerStats(&PerfStats::threadStats);

    // Cycles::rdtsc time that's updated continuously when this thread is idle.
    // Used to keep track of how much time this thread spends doing useful
    // work.
    uint64_t lastIdle = Cycles::rdtsc();

    try {
        uint64_t pollCycles = Cycles::fromNanoseconds(1000*pollMicros);
        while (true) {
            uint64_t stopPollingTime = lastIdle + pollCycles;

            // Wait for WorkerManager to supply us with some work to do.
            while (worker->state.load() != Worker::WORKING) {
                if (lastIdle >= stopPollingTime) {
#ifdef SMTT
                    worker->context->timeTrace->record(
                            TimeTraceUtil::statusMsg(worker->threadId,
                            worker->opcode,
                            TimeTraceUtil::RequestStatus::WORKER_SLEEP));
#endif

                    // It's been a long time since we've had any work to do; go
                    // to sleep so we don't waste any more CPU cycles.  Tricky
                    // race condition: the dispatch thread could change the
                    // state to WORKING just before we change it to SLEEPING,
                    // so use an atomic op and only change to SLEEPING if the
                    // current value is POLLING.
                    int expected = Worker::POLLING;
                    if (worker->state.compareExchange(expected,
                                                      Worker::SLEEPING)) {
                        if (sys->futexWait(
                                reinterpret_cast<int*>(&worker->state),
                                Worker::SLEEPING) == -1) {
                            // EWOULDBLOCK means that someone already changed
                            // worker->state, so we didn't block; this is
                            // benign.
                            if (errno != EWOULDBLOCK) {
                                LOG(ERROR, "futexWait failed in "
                                           "WorkerManager::workerMain: %s",
                                    strerror(errno));
                            }
                        }
                    }
                }
                lastIdle = Cycles::rdtsc();
            }
            Fence::enter();
            if (worker->rpc == WORKER_EXIT)
                break;

#ifdef SMTT
            worker->context->timeTrace->record(
                    TimeTraceUtil::statusMsg(worker->threadId,
                    worker->opcode,
                    TimeTraceUtil::RequestStatus::WORKER_START));
#endif

            worker->rpc->epoch = ServerRpcPool<>::getCurrentEpoch();
            Service::Rpc rpc(worker, &worker->rpc->requestPayload,
                    &worker->rpc->replyPayload);
            Service::handleRpc(worker->context, &rpc);

            // Pass the RPC back to the dispatch thread for completion.
            Fence::leave();
#ifdef SMTT
            worker->context->timeTrace->record(
                    TimeTraceUtil::statusMsg(worker->threadId,
                    worker->opcode,
                    TimeTraceUtil::RequestStatus::WORKER_DONE));
#endif
            worker->state.store(Worker::POLLING);

            // Update performance statistics.
            uint64_t current = Cycles::rdtsc();
            PerfStats::threadStats.workerActiveCycles += (current - lastIdle);
            lastIdle = current;
        }
        TEST_LOG("exiting");
    } catch (std::exception& e) {
        LOG(ERROR, "worker: %s", e.what());
        throw; // will likely call std::terminate()
    } catch (...) {
        LOG(ERROR, "worker");
        throw; // will likely call std::terminate()
    }
}

/**
 * Force this worker's thread to exit (and don't return until it has exited).
 * This method is only used during testing and WorkerManager destruction.
 * This method should be invoked only in the dispatch thread.
 */
void
Worker::exit()
{
    Dispatch* dispatch = context->dispatch;
    assert(dispatch->isDispatchThread());
    if (exited) {
        // Worker already exited; nothing to do.  This should only happen
        // during tests.
        return;
    }

    // Wait for the worker thread to finish handling any RPCs already
    // queued for it.
    while (busyIndex >= 0) {
        dispatch->poll();
    }

    // Tell the worker thread to exit, and wait for it to actually exit
    // (don't want it referencing the Worker structure anymore, since
    // it could go away).
    handoff(WORKER_EXIT);
    thread->join();
    rpc = NULL;
    exited = true;
}

/**
 * This method is invoked by the dispatch thread to pass an RPC to an idle
 * worker.  It should only be invoked when the worker is idle (i.e. #rpc is
 * NULL).
 *
 * \param newRpc
 *      RPC object containing a fully-formed request that is ready for
 *      service. May have special distinguished value WORKER_EXIT, which
 *      indicates that the worker thread should exit.
 */
void
Worker::handoff(Transport::ServerRpc* newRpc)
{
    assert(rpc == NULL);
    rpc = newRpc;
    Fence::leave();
#ifdef SMTT
    if (rpc != WORKER_EXIT) {
        context->timeTrace->record(
                TimeTraceUtil::statusMsg(threadId,
                opcode, TimeTraceUtil::RequestStatus::HANDOFF));
    }
#endif
    int prevState = state.exchange(WORKING);
    if (prevState == SLEEPING) {
        // The worker got tired of polling and went to sleep, so we
        // have to do extra work to wake it up.
        if (WorkerManager::sys->futexWake(reinterpret_cast<int*>(&state), 1)
                == -1) {
            LOG(ERROR,
                    "futexWake failed in Worker::handoff: %s",
                    strerror(errno));
            // We should probably do something else here, such as panicking
            // or unwinding the RPC.  As of 6/2011 it isn't clear what the
            // right action is, so things just get left in limbo.
        }
    }
}

/**
 * Tell the dispatch thread that this worker has finished processing its RPC,
 * so it is safe to start sending the reply.  This method should only be
 * invoked in the worker thread.
 */
void
Worker::sendReply()
{
    Fence::leave();
    state.store(POSTPROCESSING);
#ifdef SMTTXX
    context->timeTrace->record(
            TimeTraceUtil::statusMsg(threadId,
            opcode, TimeTraceUtil::RequestStatus::POST_PROCESSING));
#endif
}

} // namespace RAMCloud
