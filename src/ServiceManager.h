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

#ifndef RAMCLOUD_SERVICEMANAGER_H
#define RAMCLOUD_SERVICEMANAGER_H

#include <cstdatomic>
#include <queue>

#include "atomicPatch.h"
#include "Dispatch.h"
#include "Service.h"
#include "Transport.h"

namespace RAMCloud {
// Forward reference (header files have circular dependencies):
class Service;

/**
    * An object of this class describes a single worker thread and is used
    * for communication between the thread and the ServiceManager poller
    * running in the dispatch thread.  This structure is read-only to the
    * worker except for the #state field.  In principle this class definition
    * should be nested inside ServiceManager; however, we need to make forward
    * references to it, and C++ doesn't seem to permit forward references to
    * nested classes.
    */
class Worker {
  public:
    void sendReply();

  PRIVATE:
    Service* service;                  /// All RPCs will be dispatched to
                                       /// this server for execution.
    Tub<boost::thread> thread;         /// Thread that executes this worker.
    Transport::ServerRpc* rpc;         /// RPC being serviced by this worker.
                                       /// NULL means the last RPC given to
                                       /// the worker has been finished and a
                                       /// response sent (but the worker may
                                       /// still be in POSTPROCESSING state).
    bool idle;                         /// True means we know the worker has
                                       /// finished processing the last RPC
                                       /// we gave it.  False means the worker
                                       /// may still be working on an RPC:
                                       /// check #state to be sure.
    atomic_int state;                  /// Shared variable used to pass RPCs
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

    explicit Worker(Service* service)
        : service(service), thread(), rpc(NULL), idle(true),
          state(POLLING), exited(false) {}
    ~Worker() {}
    void exit();
    void handoff(Transport::ServerRpc* rpc);

  private:
    friend class ServiceManager;
    friend class ServiceManagerTest;
    DISALLOW_COPY_AND_ASSIGN(Worker);
};

/**
 * This class manages a pool of worker threads that carry out RPCs for
 * RAMCloud services.  It also implements an asynchronous interface between
 * the dispatch thread (which manages all of the network connections for a
 * server and runs Transport code) and the worker threads.
 */
class ServiceManager : Dispatch::Poller {
  public:
    explicit ServiceManager(Service* service);
    ~ServiceManager();

    void exitWorker();
    static void handleRpc(Transport::ServerRpc* rpc);
    bool operator() ();
    Transport::ServerRpc* waitForRpc(double timeoutSeconds);

    /// How many microseconds worker threads should remain in their polling
    /// loop waiting for work. If no new arrives during this period the
    /// worker thread will put itself to sleep, which releases its core but
    /// will result in additional delay for the next RPC while it wakes up.
    /// The value of this variable is typically not modified except during
    /// testing.
    static int pollMicros;

  PROTECTED:
    static void workerMain(Worker* worker);

    // Singleton object, used by all calls to handleRpc and sendReply.
    static ServiceManager* serviceManager;

    // All RPCs will be dispatched to this service for execution.  NULL
    // means we are in a test mode where handlerRpc doesn't actually
    // dispatch RPCs; it simply queues them.
    Service* service;

    // Information about the (single, for now) worker thread.
    Worker worker;

    // Holds requests that arrive when the worker is busy.
    std::queue<Transport::ServerRpc*> waitingRpcs;

    static Syscall *sys;

    friend class Worker;
    friend class ServiceManagerTest;
    DISALLOW_COPY_AND_ASSIGN(ServiceManager);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_SERVICEMANAGER_H
