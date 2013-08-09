/* Copyright (c) 2013 Stanford University
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

#include "Cycles.h"
#include "Logger.h"
#include "ShortMacros.h"
#include "Unlock.h"
#include "WorkerTimer.h"

namespace RAMCloud {
std::mutex WorkerTimer::mutex;
std::condition_variable WorkerTimer::timerExpired;
WorkerTimer::ManagerList WorkerTimer::managers;
Tub<std::thread> WorkerTimer::workerThread;
int WorkerTimer::workerThreadProgressCount = 0;

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
    , active(false)
    , handlerRunning(false)
    , handlerFinished()
    , destroyed(false)
    , links()
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
    if (active) {
        stopInternal(lock);
    }
    while (handlerRunning) {
        TEST_LOG("waiting for handler");
        handlerFinished.wait(lock);
    }
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
bool WorkerTimer::isRunning()
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
void WorkerTimer::start(uint64_t rdtscTime)
{
    Lock _(mutex);
    if (destroyed) {
        // The destructor has been invoked, so we don't want to do
        // anything that could cause the handler to be invoked again.
        return;
    }
    triggerTime = rdtscTime;
    if (!active) {
        manager->activeTimers.push_back(*this);
    }
    if (triggerTime < manager->earliestTriggerTime) {
        Dispatch::Lock dispatchLock(manager->dispatch);
        manager->earliestTriggerTime = triggerTime;
        manager->start(manager->earliestTriggerTime);
    }
    active = true;
}

/**
 * Stop this timer, if it was running. After this call, the timer won't
 * trigger until #start is invoked again.
 */
void WorkerTimer::stop()
{
    Lock lock(mutex);
    if (active) {
        stopInternal(lock);
    }
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
    TEST_LOG("stopping WorkerTimer");
    erase(manager->activeTimers, *this);
    active = false;
    if (manager->activeTimers.empty()) {
        Dispatch::Lock dispatchLock(manager->dispatch);
        manager->earliestTriggerTime = ~0lu;
        manager->stop();
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
    , activeTimers()
    , links()
{
    managers.push_back(*this);

    // Start the worker thread, if it doesn't already exist.
    if (!workerThread) {
        workerThread.construct(workerThreadMain);
    }
}

/**
 * Destroy a Manager object. Caller must have acquired WorkerTimer::mutex.
 */

WorkerTimer::Manager::~Manager()
{
    erase(managers, *this);
    if (managers.empty()) {
        // Shut down the worker thread. All we need to do is wake it up:
        // it will see that there are no more managers and then exit.
        timerExpired.notify_one();
        {
            // Must release lock while waiting for thread to exit; otherwise
            // it can't run.
            Unlock<std::mutex> unlock(mutex);
            workerThread->join();
        }
        workerThread.destroy();
    }
}

/**
 * This method is invoked by the dispatcher when one of the active
 * timers for this manager has reached its trigger time.
 */
void
WorkerTimer::Manager::handleTimerEvent()
{
    // Just wake up the worker thread.
    Lock _(WorkerTimer::mutex);
    WorkerTimer::timerExpired.notify_one();
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
 * This function does most of the work of WorkerThreadMain. It is
 * separated into its own method for ease of testing. This method
 * searches the active timers to see if any have triggered. If so,
 * it returns the first one that has triggered (and also resets the
 * state of that timer to reflect the fact that it is no longer running).
 * This method does not actually execute the handler.
 *
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 * 
 * \return
 *      The return value is a pointer to a timer whose handler should be
 *      invoked, or NULL if there are no such timers.
 */
WorkerTimer* WorkerTimer::findTriggeredTimer(Lock& lock)
{
    uint64_t currentTime = Cycles::rdtsc();
    Manager* manager;
    WorkerTimer* timer;

    // Search all of the Manager objects (typically there will be
    // only one) to see if there is a runnable WorkerTimer.
    for (ManagerList::iterator managerIterator(managers.begin());
            managerIterator != managers.end(); managerIterator++) {
        manager = &(*managerIterator);
        for (Manager::TimerList::iterator
                timerIterator(manager->activeTimers.begin());
                timerIterator != manager->activeTimers.end();
                timerIterator++) {
            timer = &(*timerIterator);
            if (timer->triggerTime <= currentTime) {
                goto foundTriggered;
            }
        }
    }

    // No timer has triggered yet.
    return NULL;

    // A timer expired. Deactivate it and recompute the timer for its Manager.
    foundTriggered:
    erase(manager->activeTimers, *timer);
    timer->active = false;
    manager->earliestTriggerTime = ~0ul;
    for (Manager::TimerList::iterator
            timerIterator(manager->activeTimers.begin());
            timerIterator != manager->activeTimers.end();
            timerIterator++) {
        if (timerIterator->triggerTime < manager->earliestTriggerTime) {
            manager->earliestTriggerTime = timerIterator->triggerTime;
        }
    }
    if (manager->earliestTriggerTime != ~0ul) {
        Dispatch::Lock dispatchLock(manager->dispatch);
        manager->start(manager->earliestTriggerTime);
    }
    return timer;
}

/**
 * This function is the main program for the worker thread, which
 * executes the handlers for WorkerTimers.
 */
void WorkerTimer::workerThreadMain()
try {
    Lock lock(mutex);

    // Each iteration through the following loop runs one WorkerTimer,
    // if one has triggered, or else it waits on a condition variable for
    // the next timer to complete.
    while (true) {
        if (managers.empty()) {
            // There are no longer any Manager objects; this is our cue
            // to exit the thread.
            TEST_LOG("exiting");
            workerThreadProgressCount++;
            return;
        }

        WorkerTimer* timer = findTriggeredTimer(lock);
        if (timer != NULL) {
            // Release the monitor lock while the timer handler runs; otherwise,
            // WorkerTimer::handleTimerEvent might hang on the lock for a long
            // time, effectively blocking the dispatch thread.
            timer->handlerRunning = true;
            {
                Unlock<std::mutex> unlock(mutex);
                timer->handleTimerEvent();
            }
            timer->handlerRunning = false;
            timer->handlerFinished.notify_one();
        } else {
            workerThreadProgressCount++;
            timerExpired.wait(lock);
        }
    }
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in workerThreadMain: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in workerThreadMain.");
    throw;
}

} // namespace RAMCloud
