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

#ifndef RAMCLOUD_WORKERMANAGER_H
#define RAMCLOUD_WORKERMANAGER_H

#include <queue>

#include "Dispatch.h"
#include "Service.h"
#include "Transport.h"
#include "WireFormat.h"
#include "ThreadId.h"
#include "TimeTrace.h"
#include "PerfStats.h"

namespace RAMCloud {

/**
 * This class manages a pool of worker threads that carry out RPCs for
 * RAMCloud services.  It also implements an asynchronous interface between
 * the dispatch thread (which manages all of the network connections for a
 * server and runs Transport code) and the worker threads.
 */
class WorkerManager : Dispatch::Poller {
  public:
    explicit WorkerManager(Context* context, uint32_t maxCores = 3);
    ~WorkerManager();

    void exitWorker();
    void handleRpc(Transport::ServerRpc* rpc);
    bool idle();
    static void init();
    int poll();
    void setServerId(ServerId serverId);
    Transport::ServerRpc* waitForRpc(double timeoutSeconds);

  PROTECTED:

    /// How many microseconds worker threads should remain in their polling
    /// loop waiting for work. If no new arrives during this period the
    /// worker thread will put itself to sleep, which releases its core but
    /// will result in additional delay for the next RPC while it wakes up.
    /// The value of this variable is typically not modified except during
    /// testing.
    static int pollMicros;

    /// Shared RAMCloud information.
    Context* context;

    // This class (along with the levels variable) stores information
    // for each of the levels defined by RpcLevel; if we run low on threads
    // for servicing RPCs, we queue RPCs according to their level.
    class Level {
      public:
        int requestsRunning;           /// The number of RPCs at this level
                                       /// that are currently executing.
        std::queue<Transport::ServerRpc*> waitingRpcs;
                                       /// Requests that cannot execute until
                                       /// a thread becomes available.
        explicit Level()
            : requestsRunning(0)
            , waitingRpcs()
        {}
    };
    std::vector<Level> levels;

    // Worker threads that are currently executing RPCs (no particular order).
    std::vector<Worker*> busyThreads;

    // Worker threads that are available to execute incoming RPCs.  Threads
    // are push_back'ed and pop_back'ed (the thread with highest index was
    // the last one to go idle, so it's most likely to be POLLING and thus
    // offer a fast wakeup).
    std::vector<Worker*> idleThreads;

    // Once the number of worker threads reaches this value, new RPCs will
    // only start executing if that is needed to provide a distributed
    // deadlock; other RPCs will wait until some threads finish.
    uint32_t maxCores;

    // Total number of RPCs (across all Levels) in waitingRpcs queues.
    int rpcsWaiting;

    // Nonzero means save incoming RPCs rather than executing them.
    // Intended for use in unit tests only.
    int testingSaveRpcs;

    // Used for testing: if testingSaveRpcs is set, incoming RPCs are
    // queued here, not sent to workers.
    std::queue<Transport::ServerRpc*> testRpcs;

    static void workerMain(Worker* worker);
    static Syscall *sys;

    friend class Worker;
    DISALLOW_COPY_AND_ASSIGN(WorkerManager);
};

/**
 * An object of this class describes a single worker thread and is used
 * for communication between the thread and the WorkerManager poller
 * running in the dispatch thread.  This structure is read-only to the
 * worker except for the #state field.  In principle this class definition
 * should be nested inside WorkerManager; however, we need to make forward
 * references to it, and C++ doesn't seem to permit forward references to
 * nested classes.
 */
class Worker {
  typedef RAMCloud::Perf::ReadThreadingCost_MetricSet
      ReadThreadingCost_MetricSet;
  public:
    void sendReply();

  PRIVATE:
    Context* context;                  /// Shared RAMCloud information.
    Tub<std::thread> thread;           /// Thread that executes this worker.
  public:
    int threadId;                      /// Identifier for this thread, assigned
                                       /// by the ThreadId class.
    WireFormat::Opcode opcode;         /// Opcode value from most recent RPC.
    int level;                         /// RpcLevel of most recent RPC.
    Transport::ServerRpc* rpc;         /// RPC being serviced by this worker.
                                       /// NULL means the last RPC given to
                                       /// the worker has been finished and a
                                       /// response sent (but the worker may
                                       /// still be in POSTPROCESSING state).
  PRIVATE:
    int busyIndex;                     /// Location of this worker in
                                       /// #busyThreads, or -1 if this worker
                                       /// is idle.
    Atomic<int> state;                 /// Shared variable used to pass RPCs
                                       /// between the dispatch thread and this
                                       /// worker.

    /// Values for #state:
    enum {
        /// Set by the worker thread to indicate that it has finished
        /// processing its current request and is in a polling loop waiting
        /// for more work to do.  From this state 2 things can happen:
        /// * dispatch thread can change state to WORKING
        /// * worker can change state to SLEEPING.
        /// Note: this is the only state where both threads may set a new
        /// state (it requires special care!).
        POLLING,

        /// Set by the dispatch thread to indicate that a new RPC is ready
        /// to be processed.  From this state the worker will change state
        /// to either POSTPROCESSING or POLLING.
        WORKING,

        /// Set by the worker thread if it invokes #sendReply on the RPC;
        /// means that the RPC response is ready to be returned to the client,
        /// but the worker is still busy so we can't give it anything else
        /// to do.  From the state the worker will eventually change the
        /// state to POLLING.
        POSTPROCESSING,

        /// Set by the worker thread to indicate that it has been waiting
        /// so long for new work that it put itself to sleep; the dispatch
        /// thread will need to wake it up the next time it has an RPC for the
        /// worker.  From the state the dispatch thread will eventually change
        /// state to WORKING.
        SLEEPING
    };
    bool exited;                       /// True means the worker is no longer
                                       /// running.

    explicit Worker(Context* context)
            : context(context)
            , thread()
            , threadId(ThreadId::get())
            , opcode(WireFormat::Opcode::ILLEGAL_RPC_TYPE)
            , level(0)
            , rpc(NULL)
            , busyIndex(-1)
            , state(POLLING)
            , exited(false),
            threadWork(&ReadThreadingCost_MetricSet::threadWork, false)
        {}
    void exit();
    void handoff(Transport::ServerRpc* rpc);

  public:
    ReadThreadingCost_MetricSet::Interval threadWork;

  private:
    friend class WorkerManager;
    DISALLOW_COPY_AND_ASSIGN(Worker);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_WORKERMANAGER_H
