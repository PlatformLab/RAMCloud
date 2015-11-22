/* Copyright (c) 2013-2015 Stanford University
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

#include <condition_variable>
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

    /// The following variables allow us to detect that the timer's
    /// handler is running; if so, we must wait for it to finish running
    /// when stopping or destroying the timer. handlerRunning indicates
    /// whether handleTimerEvent has been called (or is about to be called),
    /// and handlerFinished gets notified whenever handlerRunning is set
    /// to false.
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
        void checkTimers(Lock& lock);
        virtual void handleTimerEvent();
        static void workerThreadMain(Manager* manager);

        /// The dispatcher to use for low-level timer notifications.
        Dispatch* dispatch;

        /// Count of WorkerTimers associated with this manager; used to
        /// delete the manager object when there are no longer any timers
        /// for it. A value < 0 means this object is being destroyed.
        int timerCount;

        /// The following rdtsc time value must be no larger than the
        /// smallest trigger time for any active WorkerTimer associated
        /// with this Manager.
        uint64_t earliestTriggerTime;

        /// WorkerTimers associated with this Manager will execute in this
        /// thread.
        Tub<std::thread> workerThread;

        /// workerThread waits on this when it has nothing to do.
        std::condition_variable waitingForWork;

        /// Keeps track of all of the timers that are currently running (i.e.
        /// start has been called, but the timer hasn't actually fired).
        /// The most recently added timer is at the back of the list.
        INTRUSIVE_LIST_TYPEDEF(WorkerTimer, links) TimerList;
        TimerList activeTimers;

        /// The epoch when a WorkerTimer handler starts. Manager::checkTimers
        /// will set this value automatically. This number can be used later
        /// to prevent any WorkerTimer's timerEventHandler dereference a pointer
        /// in log unsafely.
        /// If no handler is running, this value is reset back to ~0UL.
        uint64_t epoch;

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
    /// assumed by all internal methods to be held, used for all condition
    /// variables.
    static std::mutex mutex;

    /// Holds all managers currently in existence.
    INTRUSIVE_LIST_TYPEDEF(Manager, links) ManagerList;
    static ManagerList managers;

    /// For testing: incremented each time workerThreadMain completes
    /// doing a chunk of work ("completion" means either it waited on
    /// timerExpired or it exited).
    static int workerThreadProgressCount;

    /// How long (in milliseconds) WorkerTimer::stopInternal will wait
    /// for a handler to complete before printing a warning message.
    /// If zero, use default value; should be nonzero only for unit tests
    static int stopWarningMs;

    static Manager* findManager(Dispatch* dispatch, Lock& lock);

  PUBLIC:
    static uint64_t getEarliestOutstandingEpoch();

    DISALLOW_COPY_AND_ASSIGN(WorkerTimer);
};

} // namespace RAMCloud

#endif // RAMCLOUD_WORKERTIMER_H

