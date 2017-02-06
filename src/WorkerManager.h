/* Copyright (c) 2011-2016 Stanford University
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

    void handleRpc(Transport::ServerRpc* rpc);
    bool idle();
    int poll();
    Transport::ServerRpc* waitForRpc(double timeoutSeconds);
    void workerMain(Transport::ServerRpc* serverRpc);

  PROTECTED:
  static inline void timeTrace(const char* format,
        uint32_t arg0 = 0, uint32_t arg1 = 0, uint32_t arg2 = 0,
        uint32_t arg3 = 0);

    /// How many microseconds worker threads should remain in their polling
    /// loop waiting for work. If no new arrives during this period the
    /// worker thread will put itself to sleep, which releases its core but
    /// will result in additional delay for the next RPC while it wakes up.
    /// The value of this variable is typically not modified except during
    /// testing.
    static int pollMicros;

    /// Shared RAMCloud information.
    Context* context;

    /// Requests that cannot execute until / a thread becomes available.
    std::queue<Transport::ServerRpc*> waitingRpcs;


    // Use this collection to track Rpcs that we have handed to a worker thread
    // but have not yet sent replies for.
    std::vector<Transport::ServerRpc*> outstandingRpcs;

    // Track the current number of Rpcs that have entered the system and not
    // yet exited.
    uint64_t numOutstandingRpcs;

    // Nonzero means save incoming RPCs rather than executing them.
    // Intended for use in unit tests only.
    int testingSaveRpcs;

    // Used for testing: if testingSaveRpcs is set, incoming RPCs are
    // queued here, not sent to workers.
    std::queue<Transport::ServerRpc*> testRpcs;

    static Syscall *sys;

    friend class Worker;
    DISALLOW_COPY_AND_ASSIGN(WorkerManager);
};

/**
 * An object of this class encapsulates the state needed by a worker thread
 * that handles a single Rpc on the server side.
 */
class Worker {
  typedef RAMCloud::Perf::ReadThreadingCost_MetricSet
      ReadThreadingCost_MetricSet;
  public:
    void sendReply();

  PRIVATE:
    Context* context;                  /// Shared RAMCloud information.

  public:
    bool replySent;                    /// Allow  worker thread
                                       /// to track whether it has already sent
                                       /// its reply and avoid sending
                                       /// duplicate replies.
    WireFormat::Opcode opcode;         /// Opcode value from most recent RPC.
    Transport::ServerRpc* rpc;         /// RPC being serviced by this worker.
                                       /// NULL means the last RPC given to
                                       /// the worker has been finished and a
                                       /// response sent (but the worker may
                                       /// still be in POSTPROCESSING state).
  PRIVATE:
    bool exited;                       /// True means the worker is no longer
                                       /// running.

    explicit Worker(Context* context, Transport::ServerRpc* rpc, WireFormat::Opcode opcode)
            : context(context)
            , replySent(false)
            , opcode(opcode)
            , rpc(rpc)
            , exited(false)
            , threadWork(&ReadThreadingCost_MetricSet::threadWork, false)
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
