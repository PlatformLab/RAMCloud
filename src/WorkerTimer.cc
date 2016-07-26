/* Copyright (c) 2013-2016 Stanford University
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

#include <chrono>

#include "Cycles.h"
#include "Logger.h"
#include "LogProtector.h"
#include "ShortMacros.h"
#include "Unlock.h"
#include "WorkerTimer.h"

namespace RAMCloud {
std::mutex WorkerTimer::mutex;
WorkerTimer::ManagerList WorkerTimer::managers;
int WorkerTimer::workerThreadProgressCount = 0;
int WorkerTimer::stopWarningMs = 0;
bool WorkerTimer::disableTimerHandlers = false;

/**
 * Construct a WorkerTimer but do not start it: it will not fire until #start
 * is invoked.
 *
 * \param dispatch
 *      Dispatch object that will manage this timer.
 */
WorkerTimer::WorkerTimer(Dispatch* dispatch)
    : manager(NULL)
    , triggerTime(0)
    , startTime(0)
    , active(false)
    , handlerRunning(false)
    , handlerFinished()
    , destroyed(false)
{
    Lock lock(mutex);
    manager = findManager(dispatch, lock);
    manager->timerCount++;
}

/**
 * Destructor for WorkerTimers.
 */
WorkerTimer::~WorkerTimer()
{
    Lock lock(mutex);
    destroyed = true;
    stopInternal(lock);
    manager->timerCount--;
    if (manager->timerCount == 0) {
        delete manager;
    }
}

/**
 * This method is overridden by a subclass and invoked when the
 * timer expires.
 */
void
WorkerTimer::handleTimerEvent()
{
    // Empty default is useful for some tests.
}

/**
 * Returns true if the timer is currently running, false if it isn't.
 */
bool
WorkerTimer::isRunning()
{
    return active;
}

/**
 * Start this timer running.
 *
 * \param rdtscTime
 *      The timer will trigger when #Cycles::rdtsc() returns a value at least
 *      this large.  If the timer was already running, the old trigger time is
 *      forgotten.
 */
void
WorkerTimer::start(uint64_t rdtscTime)
{
    Lock _(mutex);
    if (destroyed) {
        // The destructor has been invoked, so we don't want to do
        // anything that could cause the handler to be invoked again.
        return;
    }
    // Drop the existing entries first (see WorkerTimer::Manager::activeTimers
    // and WorkerTimer::Manager::runnableTimers).
    if (active) {
        manager->activeTimers.erase(this);
        manager->runnableTimers.erase(this);
    }
    triggerTime = rdtscTime;
    startTime = Cycles::rdtsc();
    manager->activeTimers.insert(this);
    if (triggerTime < manager->earliestTriggerTime) {
        manager->earliestTriggerTime = triggerTime;
        manager->start(manager->earliestTriggerTime);
    }
    active = true;
}

/**
 * Stop this timer, if it was running. After this call, the timer will
 * not currently be running, and will not trigger until #start is invoked
 * again.
 */
void
WorkerTimer::stop()
{
    Lock lock(mutex);
    stopInternal(lock);
}

/**
 * This method does most of the work of "stop". It is separated so that
 * it can be invoked by existing methods that already have acquired the
 * monitor lock.
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 */
void WorkerTimer::stopInternal(Lock& lock)
{
    // If the handler is currently running, wait for it to complete.
    // This design choice is a mixed bag. It fixes RAM-762 (we won't
    // delete a timer out from underneath a running handler), but it
    // can result in deadlock if a handler tries to delete itself
    // (e.g. see RAM-806). In order to detect deadlocks, print a warning
    // message if the handler doesn't complete for a long time.
    while (handlerRunning) {
        TEST_LOG("waiting for handler");
        std::chrono::milliseconds timeout(
                stopWarningMs ? stopWarningMs : 10000);
        if (handlerFinished.wait_until(lock, std::chrono::system_clock::now()
                + timeout) == std::cv_status::timeout) {
            LOG(WARNING, "WorkerTimer stalled waiting for handler to "
                    "complete; perhaps destructor was invoked from handler?");
        }
    }
    if (active) {
        manager->activeTimers.erase(this);
        manager->runnableTimers.erase(this);
        active = false;
        if (manager->activeTimers.empty()) {
            manager->earliestTriggerTime = ~0lu;
            manager->stop();
        }
    }
}

/**
 * Give all runnable WorkerTimers a chance to execute; don't return
 * until all of them have run. This is intended for use by unit
 * tests.
 */
void
WorkerTimer::sync()
{
    Lock lock(mutex);
    for (ManagerList::iterator it(managers.begin());
            it != managers.end(); it++) {
        Manager& manager = *it;
        while (manager.checkTimerCount > 0) {
            manager.checkTimersDone.wait(lock);
        }
        manager.checkTimers(lock);
    }
}

/**
 * Construct a Manager object.
 *
 * \param dispatch
 *      Dispatcher that will invoke our timer handler.
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 */

WorkerTimer::Manager::Manager(Dispatch* dispatch, Lock& lock)
    : Dispatch::Timer(dispatch)
    , dispatch(dispatch)
    , timerCount(0)
    , earliestTriggerTime(~0ul)
    , workerThread()
    , waitingForWork()
    , checkTimerCount(0)
    , checkTimersDone()
    , activeTimers()
    , runnableTimers()
    , logProtectorActivity()
    , links()
{
    managers.push_back(*this);
    workerThread.construct(workerThreadMain, this);
}

/**
 * Destroy a Manager object. Caller must have acquired WorkerTimer::mutex.
 */

WorkerTimer::Manager::~Manager()
{
    erase(managers, *this);

    // Shut down the worker thread. All we need to do is wake it up:
    // it will see that there are no more managers and then exit.
    timerCount = -1;
    waitingForWork.notify_one();
    {
        // Must release lock while waiting for thread to exit; otherwise
        // it can't run.
        Unlock<std::mutex> unlock(mutex);
        workerThread->join();
    }
    workerThread.destroy();
}

/**
 * This method is invoked by the dispatcher when one of the active
 * timers for this manager has reached its trigger time.
 */
void
WorkerTimer::Manager::handleTimerEvent()
{
    // This code has gone back and forth on whether to lock WorkerTimer::mutex
    // here. Initially it locked, but that caused the deadlock with the
    // dispatch thread (WorkerTimer::mutex held when restarting this
    // Dispatch::Timer, which required the dispatch lock, which the dispatch
    // thread can't release because it is trying to lock WorkerTimer::mutex
    // here). So, we removed the lock. But, this caused notifications on
    // waitingForWork to be lost if they happened before workerThreadMain
    // executed its wait. So, now we're back to locking again. This appears to
    // be safe because Dispatch::Timer::start now uses a mutex rather than
    // the dispatch lock.
    Lock lock(mutex);
    waitingForWork.notify_one();
}

/**
 * Locate the Manager object for a particular dispatcher, and create a
 * new one if needed.
 *
 * \param dispatch
 *      This is the dispatch for which the caller would like to have a
 *      Manager object.
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 */
WorkerTimer::Manager*
WorkerTimer::findManager(Dispatch* dispatch, Lock& lock)
{
    // See if there already exists a manager for this dispatcher.
    for (ManagerList::iterator it(managers.begin());
            it != managers.end(); it++) {
        Manager& manager = *it;
        if (manager.dispatch == dispatch) {
            return &manager;
        }
    }

    // No manager for this dispatcher; create a new one.
    Manager* manager = new Manager(dispatch, lock);
    return manager;
}

/**
 * This method does most of the work for the worker thread. It checks
 * to see if any WorkerTimers are ready to execute and, if so, it
 * runs all of the ones that are ready. In addition, it restarts the
 * Dispatch::Timer for this manager if there are WorkerTimers still
 * waiting. It only returns when there are no more timers to run.
 *
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 */
void WorkerTimer::Manager::checkTimers(Lock& lock)
{
    checkTimerCount++;
    while (1) {
        WorkerTimer* ready = NULL;
        uint64_t now = Cycles::rdtsc();

        // Don't run if handlers are disabled (used for unit testing)
        if (disableTimerHandlers) {
            start(earliestTriggerTime);
            break;
        }

        // Scan the list of active timers to find all timers that are ready
        // to run and pick the runnable timer that has been waiting the longest
        // (this prevents starvation).
        Manager::ActiveTimerList::iterator it = activeTimers.begin();
        while (it != activeTimers.end() && (*it)->triggerTime <= now) {
            runnableTimers.insert(*it);
            it = activeTimers.erase(it);
        }
        if (!runnableTimers.empty()) {
            // Pull the runnable timer that has been waiting the longest
            ready = *runnableTimers.begin();
            // Remove the timer from the list (it can reschedule itself if
            // it wants).
            runnableTimers.erase(runnableTimers.begin());
        } else {
            // If there are no runnable timers reschedule the manager to run
            // again when the next timer is ready (if any).
            earliestTriggerTime = ~0lu;
            if (!activeTimers.empty()) {
                earliestTriggerTime = (*activeTimers.begin())->triggerTime;
                start(earliestTriggerTime);
            }
            break;
        }

        ready->active = false;
        ready->handlerRunning = true;

        // Release the monitor lock while the timer handler runs; otherwise,
        // WorkerTimer::handleTimerEvent might hang on the lock for a long
        // time, effectively blocking the dispatch thread.
        {
            Unlock<std::mutex> unlock(mutex);
            LogProtector::Guard logGuard(logProtectorActivity);
            ready->handleTimerEvent();
        }
        ready->handlerRunning = false;
        ready->handlerFinished.notify_one();
    }
    checkTimerCount--;
    checkTimersDone.notify_all();
}

/**
 * This function is the main program for worker threads, which
 * executes the handlers for WorkerTimers associated with a single
 * Dispatch.
 */
void WorkerTimer::Manager::workerThreadMain(Manager* manager)
try {
    Lock lock(mutex);

    while (true) {
        workerThreadProgressCount++;
        manager->checkTimers(lock);

        // The following check must occur *after* calling checkTimers, since
        // checkTimers may release the lock, which could allow timerCount
        // to change. Note: if timerCount was already negative, then
        // checkTimers won't find any work to do.
        if (manager->timerCount < 0) {
            // There are no longer any Manager objects; this is our cue
            // to exit the thread.
            TEST_LOG("exiting");
            return;
        }
        manager->waitingForWork.wait(lock);
    }
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in workerThreadMain: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in workerThreadMain.");
    throw;
}

} // namespace RAMCloud
