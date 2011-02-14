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

#include <boost/thread.hpp>
#include "Common.h"
#include "Tub.h"
#include "Syscall.h"

namespace RAMCloud {

/**
 * The Dispatch class keeps track of interesting events such as open files
 * and timers, and arranges for particular methods to be invoked when the
 * events happen. The inner loop for the class is polling-based, and it
 * allows additional polling-based event handlers to be added.  The current
 * implementation does not support concurrent invocations from multiple
 * threads; for example:
 * - It is not safe for #poll to be invoked simultaneously in multiple threads.
 * - But, once #poll has invoked a handler it is safe to invoke #poll again
 *   in that thread or in another thread.
 * - It is not safe to create a new Dispatch::File while #poll is "active".
 * - But, if #poll has invoked a handler it is no longer active so it is safe
 *   for that handler, or another thread, to create/delete Dispatch::Files.
 */
class Dispatch {
  public:
    static bool poll();
    static void handleEvent();
    static void reset();

    /// The return value from rdtsc at the beginning of the last call to
    /// #poll.

    static uint64_t currentTime;

    /**
     * A Poller object is invoked once each time through the dispatcher's
     * inner polling loop.
     */
    class Poller {
      public:
        explicit Poller();
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
        virtual bool operator() () = 0;
      private:

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
        explicit File(int fd, FileEvent event = NONE);
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
        virtual void operator() () = 0;

      protected:
        /// The file descriptor passed into the constructor.
        int fd;

        /// The events that are currently being watched for this file.
        FileEvent event;

        /// Indicates whether epoll_ctl(EPOLL_CTL_ADD) has been called.
        bool active;

        /// Non-zero means that operator() has been invoked but hasn't
        /// yet returned; the actual value is a (reasonably) unique identifier
        /// for this particular invocation.  Zero means operator() is not
        /// currently active.  This field is used to defer the effect of
        /// setEvent until after operator() returns.
        int invocationId;

      private:
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
        Timer();
        explicit Timer(uint64_t cycles);
        virtual ~Timer();
        bool isRunning();
        void startCycles(uint64_t cycles);
        void startMicros(uint64_t micros);
        void startMillis(uint64_t ms);
        void startSeconds(uint64_t seconds);
        void stop();

        /**
         * This method is defined by a subclass and invoked when the
         * timer expires.
         */
        virtual void operator() () = 0;
      private:

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

  private:
    static void epollThreadMain();

    // Keeps track of all of the pollers currently defined.  We don't
    // use an intrusive list here because it isn't reentrant: we need
    // to add/remove elements while the dispatcher is traversing the list.
    static std::vector<Poller*> pollers;

    // Keeps track of of all of the file handlers currently defined.
    // Maps from a file descriptor to the corresponding File, or NULL
    // if none.
    static std::vector<File*> files;

    // The file descriptor used for epoll.
    static int epollFd;

    // We start a separate thread to invoke epoll kernel calls, so the
    // main polling loop is not delayed by kernel calls.  This thread
    // is only used when there are active Files.
    static Tub<boost::thread> epollThread;

    // Read and write descriptors for a pipe.  The epoll thread always has
    // the read fd for this pipe in its active set; writing data to the pipe
    // causes the epoll thread to exit.  These file descriptors are only
    // valid if #epollThread is non-null.
    static int exitPipeFds[2];

    // Used for communication between the epoll thread and #poll: the
    // epoll thread stores a fd here when it becomes ready, and #poll
    // resets this back to -1 once it has retrieved the fd.
    static volatile int readyFd;

    // Used to assign a (nearly) unique identifier to each invocation
    // of a File.
    static int fileInvocationSerial;

    // If #readyFd is already in use when the epoll thread wants to notify,
    // the epoll thread waits on this mutex, which is unlocked by #poll.
    static boost::mutex epollMutex;

    // Keeps track of all of the timers currently defined.  We don't
    // use an intrusive list here because it isn't reentrant: we need
    // to add/remove elements while the dispatcher is traversing the list.
    // List of all timers that are currently defined.
    static std::vector<Timer*> timers;

    // Optimization for timers: no timer will trigger sooner than this time
    // (measured in cycles).
    static uint64_t earliestTriggerTime;

    // The Dispatch class contains only static methods, so hide the constructor.
    Dispatch();

    static Syscall *sys;

    friend class Poller;
    friend class File;
    friend class Timer;
    friend class DispatchTest;
    DISALLOW_COPY_AND_ASSIGN(Dispatch);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
