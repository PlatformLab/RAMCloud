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

#ifndef RAMCLOUD_DISPATCH_H
#define RAMCLOUD_DISPATCH_H

#include <cstdatomic>
#include <boost/thread.hpp>
#include <boost/thread/locks.hpp>

#include "Common.h"
#include "AtomicInt.h"
#include "ThreadId.h"
#include "Tub.h"
#include "SpinLock.h"
#include "Syscall.h"

namespace RAMCloud {

/// Singleton Dispatch object that is normally used for all operations,
/// and defaults in many cases.
class Dispatch;
extern Dispatch* dispatch;

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
 */
class Dispatch {
  public:
    Dispatch();
    ~Dispatch();
    static void init();

    /**
     * Returns true if this thread is the one in which the object was created.
     */
    bool
    isDispatchThread()
    {
        return (ownerId == ThreadId::get());
    }

    void poll();

    /// The return value from rdtsc at the beginning of the last call to
    /// #poll.  May be read from multiple threads, so must be volatile.
    /// This value is relatively accurate for any code running in a Dispatch
    /// handler (Timer, Poller, etc.), and it is much cheaper than calling
    /// rdtsc(). However, on clients, if no RPCs are invoked for a while then
    /// #currentTime may be out of date, since #poll is only invoked while
    /// waiting for RPCs to complete.
    volatile uint64_t currentTime;

    /**
     * A Poller object is invoked once each time through the dispatcher's
     * inner polling loop.
     */
    class Poller {
      public:
        explicit Poller(Dispatch* dispatch = RAMCloud::dispatch);
        virtual ~Poller();

        /**
         * This method is defined by a subclass and invoked once by the
         * dispatcher during each pass through its inner polling loop.
         *
         * \return
         *      True means that something interesting happened during this
         *      call. False means that there was nothing for this particular
         *      poller to do.
         */
        virtual void poll() = 0;
      PRIVATE:
        /// The Dispatch object that owns this Poller.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

        /// Index of this Poller in Dispatch::pollers.  Allows deletion
        /// without having to scan all the entries in pollers. -1 means
        /// this poller isn't currently in Dispatch::pollers (happens
        /// after Dispatch::reset).
        int slot;
        friend class Dispatch;
        friend class DispatchTest;
        DISALLOW_COPY_AND_ASSIGN(Poller);
    };

    /**
     * Defines the kinds of events for which File handlers can be defined
     * (some combination of readable and writable).
     */
    enum FileEvent {NONE, READABLE, WRITABLE, READABLE_OR_WRITABLE};

    /**
     * A File object is invoked whenever its associated fd is readable
     * and/or writable.
     */
    class File {
      public:
        explicit File(int fd, FileEvent event = NONE,
                Dispatch* dispatch = RAMCloud::dispatch);
        virtual ~File();
        void setEvent(FileEvent event);

        /**
         * This method is defined by a subclass and invoked by the dispatcher
         * whenever the event associated with the object has occurred. If
         * the event still exists when this method returns (e.g., the file
         * is readable but the method did not read the data), then the method
         * will be invoked again. During the execution of this method the
         * event is disabled (calling Dispatch::poll will not cause this
         * method to be invoked).
         */
        virtual void handleFileEvent() = 0;

      PROTECTED:
        /// The Dispatch object that owns this File.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

        /// The file descriptor passed into the constructor.
        int fd;

        /// The events that are currently being watched for this file.
        FileEvent event;

        /// Indicates whether epoll_ctl(EPOLL_CTL_ADD) has been called.
        bool active;

        /// Non-zero means that handleFileEvent has been invoked but hasn't
        /// yet returned; the actual value is a (reasonably) unique identifier
        /// for this particular invocation.  Zero means handleFileEvent is not
        /// currently active.  This field is used to defer the effect of
        /// setEvent until after handleFileEvent returns.
        int invocationId;

      PRIVATE:
        friend class Dispatch;
        friend class DispatchTest;
        DISALLOW_COPY_AND_ASSIGN(File);
    };

    /**
     * A Timer object is invoked once when its time expires; it can be
     * restarted to provide multiple invocations.
     */
    class Timer {
      public:
        explicit Timer(Dispatch* dispatch = RAMCloud::dispatch);
        explicit Timer(uint64_t cycles,
                Dispatch* dispatch = RAMCloud::dispatch);
        virtual ~Timer();
        bool isRunning();
        void start(uint64_t cycles);
        void stop();

        /**
         * This method is defined by a subclass and invoked when the
         * timer expires.
         */
        virtual void handleTimerEvent() = 0;
      PRIVATE:
        /// The Dispatch object that owns this Timer.  NULL means the
        /// Dispatch has been deleted.
        Dispatch* owner;

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
        friend class DispatchTest;
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
        explicit Lock(Dispatch* dispatch = RAMCloud::dispatch);
        ~Lock();
      PRIVATE:
        /// The Dispatch object associated with this Lock.
        Dispatch* dispatch;

        /// Used to lock Dispatch::mutex, but only if the Lock object
        /// is constructed in a thread other than the dispatch thread
        /// (no mutual exclusion is needed if the Lock is created in
        /// the dispatch thread).
        Tub<boost::lock_guard<SpinLock>> lock;
        DISALLOW_COPY_AND_ASSIGN(Lock);
    };

  PRIVATE:
    static void epollThreadMain(Dispatch* dispatch);

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
    Tub<boost::thread> epollThread;

    // Read and write descriptors for a pipe.  The epoll thread always has
    // the read fd for this pipe in its active set; writing data to the pipe
    // causes the epoll thread to exit.  These file descriptors are only
    // valid if #epollThread is non-null.
    int exitPipeFds[2];

    // Used for communication between the epoll thread and #poll: the
    // epoll thread stores a fd here when it becomes ready, and #poll
    // resets this back to -1 once it has retrieved the fd.
    volatile int readyFd;

    // Used to assign a (nearly) unique identifier to each invocation
    // of a File.
    int fileInvocationSerial;

    // Keeps track of all of the timers currently defined.  We don't
    // use an intrusive list here because it isn't reentrant: we need
    // to add/remove elements while the dispatcher is traversing the list.
    // BUG?? This list currently contains only the active timers.
    std::vector<Timer*> timers;

    // Optimization for timers: no timer will trigger sooner than this time
    // (measured in cycles).
    uint64_t earliestTriggerTime;

    // Unique identifier (as returned by ThreadId::get) for the thread that
    // created this object.
    uint64_t ownerId;

    // Used to make sure that only one thread at a time attempts to lock
    // the dispatcher.
    SpinLock mutex;

    // Insert extra space in order to ensure that fields below this are on a
    // different cache line from mutex (saves 30ns in creating and deleting a
    // Lock as of 6/2011).
    char pad[CACHE_LINE_SIZE];

    // Nonzero means there is a (non-dispatch) thread trying to lock the
    // dispatcher.
    AtomicInt lockNeeded;

    // Nonzero means the dispatch thread is locked.
    AtomicInt locked;

    static Syscall *sys;

    friend class Poller;
    friend class File;
    friend class Timer;
    friend class DispatchTest;
    DISALLOW_COPY_AND_ASSIGN(Dispatch);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
