/* Copyright (c) 2013 Stanford University
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

#ifndef RAMCLOUD_WORKERTIMER_H
#define RAMCLOUD_WORKERTIMER_H

#include <thread>
#include "BoostIntrusive.h"
#include "Dispatch.h"

namespace RAMCloud {

/**
 * This class implements a timer mechanism, which can be used to schedule
 * the execution of code in the future. WorkerTimers are different from the
 * timers provided by Dispatch in that WorkerTimers run in a separate worker
 * thread (whereas Dispatch timers run in the dispatch thread). This makes
 * WorkerTimer less efficient (since each timer event requires the worker
 * thread to wake up), but avoids disrupting the Dispatch thread for timer
 * events that take a long time to process.
 *
 * Each WorkerTimer object represents a single timer; the client implements
 * the handleTimerEvent method, which is called when the timer fires.
 *
 * This class is thread-safe (it can be invoked by competing threads
 * simultaneously).
 */
class WorkerTimer {
  PUBLIC:
    explicit WorkerTimer(Dispatch* dispatch);
    explicit WorkerTimer(Dispatch* dispatch, uint64_t cycles);
    virtual ~WorkerTimer();
    virtual void handleTimerEvent();
    bool isRunning();
    void start(uint64_t cycles);
    void stop();

  PRIVATE:
    class Manager;
    typedef std::unique_lock<std::mutex> Lock;
    void stopInternal(Lock& lock);

    /// Manager that controls this timer.
    Manager* manager;

    /// If the timer is running it will be invoked as soon as #rdtsc
    /// returns a value greater or equal to this. This value is only
    /// valid if slot >= 0.
    uint64_t triggerTime;

    /// Indicates whether or not this timer is currently running.
    bool active;

    /// The following variables make it safe for the destructor to be invoked
    /// even if the handler is running or about to run (in general, it's hard
    /// to avoid situations like this, since the handler runs asynchronously
    /// with other threads that might destroy the object). If this happens,
    /// the destructor must wait until the handler has finished running:
    /// handlerRunning indicates whether handleTimerEvent has been called (or
    /// is about to be called), and handlerFinished gets notified whenever
    /// handlerRunning is set to false.
    bool handlerRunning;
    std::condition_variable handlerFinished;

    /// We don't want anyone (such as the handler) to restart the timer
    /// once the destructor has been invoked. This boolean indicates
    /// that starts should be ignored.
    bool destroyed;

    /// Used to link WorkerTimers together in Manager::activeTimers.
    IntrusiveListHook links;

    /**
     * The following class is used internally to manage WorkerTimers.
     * Each instance of this object corresponds to a particular Dispatch
     * object, and manages all of the WorkerTimers associated with that
     * Dispatch object. This object is a Dispatch::Timer, which uses the
     * Dispatch mechanism to wait for the earliest WorkerTimer for this
     * manager to reach its trigger time.
     */
    class Manager: public Dispatch::Timer {
      PUBLIC:
        explicit Manager(Dispatch* dispatch, Lock& lock);
        ~Manager();
        virtual void handleTimerEvent();

        /// The dispatcher to use for low-level timer notifications.
        Dispatch* dispatch;

        /// Count of WorkerTimer associated with this manager; used to
        /// delete the manager object when there are no longer any timers
        /// for it.
        int timerCount;

        /// The following rdtsc time value must be no larger than the
        /// smallest trigger time for any active WorkerTimer associated
        /// with this Manager.
        uint64_t earliestTriggerTime;

        /// Keeps track of all of the timers that are currently running (i.e.
        /// start has been called, but the timer hasn't actually fired).
        INTRUSIVE_LIST_TYPEDEF(WorkerTimer, links) TimerList;
        TimerList activeTimers;

        /// Used to link Managers together in WorkerTimer::managers.
        IntrusiveListHook links;

        friend class WorkerTimer;

        DISALLOW_COPY_AND_ASSIGN(Manager);
    };

    /*------------------------------------------------------
     * Static variables and methods:
     *------------------------------------------------------
     */

    /// Monitor-style lock: acquired by all externally visible methods,
    /// assumed by all internal methods to be held.
    static std::mutex mutex;

    /// Used by WorkerTimer::handleTimerEvent to wake up workerThread.
    static std::condition_variable timerExpired;

    /// Holds all managers currently in existence.
    INTRUSIVE_LIST_TYPEDEF(Manager, links) ManagerList;
    static ManagerList managers;

    /// When a WorkerTimer fires, it is executed in this thread.
    static Tub<std::thread> workerThread;

    /// For testing: incremented each time workerThreadMain completes
    /// doing a chunk of work ("completion" means either it waited on
    /// timerExpired or it exited).
    static int workerThreadProgressCount;

    static Manager* findManager(Dispatch* dispatch, Lock& lock);
    static WorkerTimer* findTriggeredTimer(Lock& lock);
    static void workerThreadMain();

    DISALLOW_COPY_AND_ASSIGN(WorkerTimer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_WORKERTIMER_H

