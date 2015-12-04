/* Copyright (c) 2013-2015 Stanford University
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
#include "ShortMacros.h"
#include "Unlock.h"
#include "WorkerTimer.h"
#include "ServerRpcPool.h"

namespace RAMCloud {
std::mutex WorkerTimer::mutex;
WorkerTimer::ManagerList WorkerTimer::managers;
int WorkerTimer::workerThreadProgressCount = 0;
int WorkerTimer::stopWarningMs = 0;

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
void WorkerTimer::stop()
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
    // message if the handler doesn't complete quickly.
    while (handlerRunning) {
        TEST_LOG("waiting for handler");
        std::chrono::milliseconds timeout(stopWarningMs ? stopWarningMs : 1000);
        if (handlerFinished.wait_until(lock, std::chrono::system_clock::now()
                + timeout) == std::cv_status::timeout) {
            LOG(WARNING, "WorkerTimer stalled waiting for handler to "
                    "complete; perhaps destructor was invoked from handler?");
        }
    }
    if (active) {
        erase(manager->activeTimers, *this);
        active = false;
        if (manager->activeTimers.empty()) {
            manager->earliestTriggerTime = ~0lu;
            manager->stop();
        }
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
    , activeTimers()
    , epoch(~0)
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
    // There used to be a WorkerTimer::mutex here, but it is not necessary to
    // hold a lock and it causes deadlock between dispatch thread and the
    // WorkerTimer thread.  Just wake up the worker thread.
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
 * Obtain the earliest (lowest value) epoch of any outstanding WorkerTimer in
 * the system. If there are no ongoing timer handlers, then the return value
 * is -1 (i.e. the largest 64-bit unsigned integer).
 */
uint64_t
WorkerTimer::getEarliestOutstandingEpoch()
{
    Lock _(mutex);
    uint64_t earliest = ~0;

    for (ManagerList::iterator it(managers.begin());
            it != managers.end(); it++) {
        earliest = std::min(it->epoch, earliest);
    }
    return earliest;
}

/**
 * This method does most of the work for the worker thread. It checks
 * to see if any WorkerTimers are ready to execute and, if so, it
 * runs one of them. In addition, it restarts the Dispatch::Timer
 * for this manager if there are WorkerTimers still waiting.
 *
 * \param lock
 *      Used to ensure that caller has acquired WorkerTimer::mutex.
 *      Not actually used by the method.
 */
void WorkerTimer::Manager::checkTimers(Lock& lock)
{
    // Scan the list of active timers to (a) find a timer that's ready to run
    // (if there is one) and (b) recompute earliestTriggerTime.
    WorkerTimer* ready = NULL;
    uint64_t newEarliest = ~0lu;
    uint64_t now = Cycles::rdtsc();
    for (Manager::TimerList::iterator
            timerIterator(activeTimers.begin());
            timerIterator != activeTimers.end();
            timerIterator++) {
        // Note: if multiple WorkerTimers are ready, run the one that
        // has been on the queue the longest (this prevents starvation).)
        if ((timerIterator->triggerTime <= now)
                && (ready == NULL)) {
            ready = &(*timerIterator);
        } else {
            if (timerIterator->triggerTime < newEarliest) {
                newEarliest = timerIterator->triggerTime;
            }
        }
    }
    earliestTriggerTime = newEarliest;

    if (ready != NULL) {
        // Remove the timer from the list (it can reschedule itself if
        // it wants).
        erase(activeTimers, *ready);
        ready->active = false;
        ready->handlerRunning = true;
        epoch = ServerRpcPool<>::getCurrentEpoch();

        // Release the monitor lock while the timer handler runs; otherwise,
        // WorkerTimer::handleTimerEvent might hang on the lock for a long
        // time, effectively blocking the dispatch thread.
        {
            Unlock<std::mutex> unlock(mutex);
            ready->handleTimerEvent();
        }
        epoch = ~0lu;
        ready->handlerRunning = false;
        ready->handlerFinished.notify_one();
    }

    if (!activeTimers.empty()) {
        start(earliestTriggerTime);
    }
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
