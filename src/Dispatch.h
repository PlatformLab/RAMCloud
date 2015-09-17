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

#ifndef RAMCLOUD_DISPATCH_H
#define RAMCLOUD_DISPATCH_H

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 5
#include <atomic>
#else
#include <cstdatomic>
#endif
#include <thread>

#include "Common.h"
#include "Atomic.h"
#include "ThreadId.h"
#include "Tub.h"
#include "SpinLock.h"
#include "Syscall.h"

namespace RAMCloud {

/**
 * The Dispatch class keeps track of interesting events such as open files
 * and timers, and arranges for particular methods to be invoked when the
 * events happen. The inner loop for the class is polling-based, and it
 * allows additional polling-based event handlers to be added.  This
 * implementation works in a multi-threaded environment, but with certain
 * restrictions:
 * - #poll should only be invoked in the thread that created the Dispatch
 *   object.  This thread is called the "dispatch thread".
 * - Other threads can invoke Dispatch methods, but they must hold a
 *   Dispatch::Lock object at the time of the invocation, in order to avoid
 *   synchronization problems.
 * - The Timer methods are thread-safe without acquiring Dispatch::Lock
 *   (use of Dispatch::Lock is now deprecated because it is slow, so we
 *   are gradually eliminating its use).
 */
class Dispatch {
  public:
    explicit Dispatch(bool hasDedicatedThread);
    ~Dispatch();

    /**
     * Returns true if this thread is the one in which the object was created
     * or if there is no thread which owns the dispatch
     * (see #hasDedicatedThread).
     */
    bool
    isDispatchThread()
    {
        return (!hasDedicatedThread || ownerId == ThreadId::get());
    }

    int poll();
    void run() __attribute__ ((noreturn));

    /// The return value from rdtsc at the beginning of the last call to
    /// #poll.  May be read from multiple threads, so must be volatile.
    /// This value is relatively accurate for any code running in a Dispatch
    /// handler (Timer, Poller, etc.), and it is much cheaper than calling
    /// rdtsc(). However, on clients, if no RPCs are invoked for a while then
    /// #currentTime may be out of date, since #poll is only invoked while
    /// waiting for RPCs to complete.
    volatile uint64_t currentTime;

    void startProfiler(const uint64_t totalElements);

    void stopProfiler();

    void dumpProfile(const char* fileName);

    /**
     * A Poller object is invoked once each time through the dispatcher's
     * inner polling loop.
     */
    class Poller {
      public:
        explicit Poller(Dispatch* dispatch, const string& pollerName);
        virtual ~Poller();

        /**
         * This method is defined by a subclass and invoked once by the
         * dispatcher during each pass through its inner polling loop.
         *
         * \return
         *      1 means that this poller did useful work during this call.
         *      0 means that the poller found no work to do.
         */
        virtual int poll() = 0;
      PRIVATE:
        /// The Dispatch object that owns this Poller.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

        /// Human-readable string name given to the poller to make it
        /// easy to identify for debugging. For most pollers just passing
        /// in the subclass name probably makes sense.
        string pollerName;

        /// Index of this Poller in Dispatch::pollers.  Allows deletion
        /// without having to scan all the entries in pollers. -1 means
        /// this poller isn't currently in Dispatch::pollers (happens
        /// after Dispatch::reset).
        int slot;
        friend class Dispatch;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };

    /**
     * Defines the kinds of events for which File handlers can be defined
     * (some combination of readable and writable).
     */
    enum FileEvent {
        READABLE = 1,
        WRITABLE = 2
    };

    /**
     * A File object is invoked whenever its associated fd is readable
     * and/or writable.
     */
    class File {
      public:
        explicit File(Dispatch* dispatch, int fd, int events = 0);
        virtual ~File();
        void setEvents(int events);

        /**
         * This method is defined by a subclass and invoked by the dispatcher
         * whenever an event associated with the object has occurred. If
         * the event still exists when this method returns (e.g., the file
         * is readable but the method did not read the data), then the method
         * will be invoked again. During the execution of this method events
         * for this object are disabled (calling Dispatch::poll will not cause
         * this method to be invoked).
         *
         * \param events
         *      Indicates whether the file is readable or writable or both
         *      (OR'ed combination of FileEvent values).
         */
        virtual void handleFileEvent(int events) = 0;

      PROTECTED:
        /// The Dispatch object that owns this File.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

        /// The file descriptor passed into the constructor.
        int fd;

        /// The events that are currently being watched for this file.
        int events;

        /// Indicates whether epoll_ctl(EPOLL_CTL_ADD) has been called.
        bool active;

        /// Non-zero means that handleFileEvent has been invoked but hasn't
        /// yet returned; the actual value is a (reasonably) unique identifier
        /// for this particular invocation.  Zero means handleFileEvent is not
        /// currently active.  This field is used to defer the effect of
        /// setEvents until after handleFileEvent returns.
        int invocationId;

      PRIVATE:
        friend class Dispatch;
        DISALLOW_COPY_AND_ASSIGN(File);
    };

    /**
     * A Timer object is invoked once when its time expires; it can be
     * restarted to provide multiple invocations.
     */
    class Timer {
      public:
        explicit Timer(Dispatch* dispatch);
        explicit Timer(Dispatch* dispatch, uint64_t cycles);
        virtual ~Timer();
        virtual void handleTimerEvent();
        bool isRunning();
        void start(uint64_t cycles);
        void stop();

      PROTECTED:
        void stopInternal(std::lock_guard<SpinLock>& lock);

        /// The Dispatch object that owns this Timer.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

      PRIVATE:

        /// If the timer is running it will be invoked as soon as #rdtsc
        /// returns a value greater or equal to this. This value is only
        /// valid if slot >= 0.
        uint64_t triggerTime;

        /// If >= 0 this timer is running, and the value contains the
        /// index of this Timer in Dispatch::timers. <0 means this
        /// timer is not currently running, and isn't in Dispatch::timers.
        /// Among other things, this value allows a timer to be deleted
        /// without having to scan all the entries in timers.
        int slot;

        friend class Dispatch;
        DISALLOW_COPY_AND_ASSIGN(Timer);
    };

    /**
     * Lock objects are used to synchronize between the dispatch thread and
     * other threads.  As long as a Lock object exists the following guarantees
     * are in effect: either (a) the thread is the dispatch thread or (b) no
     * other non-dispatch thread has a Lock object and the dispatch thread is
     * in an idle state waiting for the Lock to be destroyed.  Although Locks
     * are intended primarily for use in non-dispatch threads, they can also be
     * used in the dispatch hread (e.g., if you can't tell which thread will
     * run a particular piece of code). Locks may not be used recursively: a
     * single thread can only create a single Lock object at a time.
     */
    class Lock {
      public:
        explicit Lock(Dispatch* dispatch);
        ~Lock();
      PRIVATE:
        /// The Dispatch object associated with this Lock.
        Dispatch* dispatch;

        /// Used to lock Dispatch::mutex, but only if the Lock object
        /// is constructed in a thread other than the dispatch thread
        /// (no mutual exclusion is needed if the Lock is created in
        /// the dispatch thread).
        Tub<std::lock_guard<SpinLock>> lock;
        DISALLOW_COPY_AND_ASSIGN(Lock);
    };

  PRIVATE:
    static void epollThreadMain(Dispatch* owner);
    static bool fdIsReady(int fd);
    void cleanProfiler();

    // Keeps track of all of the pollers currently defined.  We don't
    // use an intrusive list here because it isn't reentrant: we need
    // to add/remove elements while the dispatcher is traversing the list.
    std::vector<Poller*> pollers;

    // Keeps track of of all of the file handlers currently defined.
    // Maps from a file descriptor to the corresponding File, or NULL
    // if none.
    std::vector<File*> files;

    // The file descriptor used for epoll.
    int epollFd;

    // We start a separate thread to invoke epoll kernel calls, so the
    // main polling loop is not delayed by kernel calls.  This thread
    // is only used when there are active Files.
    Tub<std::thread> epollThread;

    // Read and write descriptors for a pipe.  The epoll thread always has
    // the read fd for this pipe in its active set; writing data to the pipe
    // causes the epoll thread to exit.  These file descriptors are only
    // valid if #epollThread is non-null.
    int exitPipeFds[2];

    // Used for communication between the epoll thread and #poll: the
    // epoll thread stores a fd here when it becomes ready, and #poll
    // resets this back to -1 once it has retrieved the fd.
    volatile int readyFd;

    // Also used for communication between epoll thread and #poll:
    // before setting readyFd the epoll thread stores information here
    // about which events fired for readyFd (OR'ed combination of
    // FileEvent values).
    volatile int readyEvents;

    // Used to assign a (nearly) unique identifier to each invocation
    // of a File.
    int fileInvocationSerial;

    // Protects all accesses to timers and write accesses to
    // earliestTriggerTime.
    SpinLock timerMutex;

    // Keeps track of all of the timers that are currently active.  We
    // don't use an intrusive list here because it isn't reentrant: we need
    // to add/remove elements while the dispatcher is traversing the list.
    std::vector<Timer*> timers;

    // Optimization for timers: no timer will trigger sooner than this time
    // (measured in cycles).
    uint64_t earliestTriggerTime;

    // Unique identifier (as returned by ThreadId::get) for the thread that
    // created this object.
    int ownerId;

    // Used to make sure that only one thread at a time attempts to lock
    // the dispatcher.
    SpinLock mutex;

    // Insert extra space in order to ensure that fields below this are on a
    // different cache line from mutex (saves 30ns in creating and deleting a
    // Lock as of 6/2011).
    char pad[CACHE_LINE_SIZE];

    // Nonzero means there is a (non-dispatch) thread trying to lock the
    // dispatcher.
    Atomic<int> lockNeeded;

    // Nonzero means the dispatch thread is locked.
    Atomic<int> locked;

    /**
     * True if there is a thread which owns this dispatch (this is
     * true on RAMCloud servers).
     *
     * On clients this is false which means clients must guarantee that only
     * one thread interacts with the Dispatch at a time (synchronization
     * is disabled in the Dispatch).
     */
    bool hasDedicatedThread;

    /**
     * Threshold (in cycles) used to print warnings when the poller loop is taking
     * an unusually long time. 
     */
    uint64_t slowPollerCycles;

    // Dispatch profile data will be taken only if
    // this flag is true.
    bool profilerFlag;

    // Determines size of the circular array that keeps dispatch's
    // profile data.
    uint64_t totalElements;

    // Pointer to the circular array that keeps dispatch's
    // profile data.
    uint64_t *pollingTimes;

    // Keeps track of a rotating index in the circular array
    // pollingTimes. The index before this index is always the
    // last valid data point that's been taken.
    uint64_t nextInd;

    static Syscall *sys;

    friend class Poller;
    friend class File;
    friend class Timer;
    DISALLOW_COPY_AND_ASSIGN(Dispatch);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
