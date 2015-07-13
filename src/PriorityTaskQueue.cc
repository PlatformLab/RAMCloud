/* Copyright (c) 2012 Stanford University
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

#include "PriorityTaskQueue.h"
#include "Logger.h"
#include "PerfStats.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- PriorityTask ---

/**
 * Create a PriorityTask which will be run by #taskQueue if scheduled.
 *
 * \param taskQueue
 *      PriorityTaskQueue which will execute performTask().
 */
PriorityTask::PriorityTask(PriorityTaskQueue& taskQueue)
    : taskQueue(taskQueue)
    , entry()
{
}

/**
 * Virtual destructor; cancels the task if already scheduled.
 * performTask() may get invoked while subclasses are in the middle of
 * being destroyed. It is probably a REALLY good idea to avoid this rather
 * than rely on the deschedule() here in the superclass destructor, so
 * subclasses should deschedule at the start of their own destructors to
 * ensure no future calls to performTask get through (though one could
 * be executing already).
 */
PriorityTask::~PriorityTask()
{
    deschedule();
}

/**
 * Return true if this PriorityTask will be executed by #taskQueue.
 * This call is safe even during concurrent operations on the underlying
 * PriorityTaskQueue.
 */
bool
PriorityTask::isScheduled() const
{
    return entry;
}

/**
 * Enqueue this PriorityTask for execution by #taskQueue. This call is safe
 * even during concurrent operations on the underlying PriorityTaskQueue.
 *
 * Just before taskQueue executes this task, isScheduled() is reset to
 * false and this task will not be executed on subsequent passes made
 * by #taskQueue unless schedule() is called again. It is perfectly
 * legal to call schedule() during a call to performTask(), which indicates
 * that the task should be run again in the future by the #taskQueue.
 * Calling schedule() when isScheduled() == true has no effect unless the
 * supplied priority is higher than the priority the task was already
 * scheduled at. In that case the task is rescheduled with higher priority
 * which will guarantee that it will execute at least as early as it
 * would have formerly.
 */
void
PriorityTask::schedule(Priority priority)
{
    taskQueue.schedule(this, priority);
}

/**
 * Remove this task from the #taskQueue; performTask() will not be called
 * again until after schedule() is called. It is safe to call deschedule()
 * during performTask(). Callers should be aware that deschedule() can operate
 * concurrently with performTask() and may yield to lost calls to performTask()
 * without proper synchronization.
 */
void
PriorityTask::deschedule()
{
    taskQueue.deschedule(this);
}

// --- PriorityTaskQueue ---

/**
 * Create an PriorityTaskQueue. Tasks don't start execution until one of
 * the three methods for driving the queue is employed. See PriorityTaskQueue
 * documentation.
 */
PriorityTaskQueue::PriorityTaskQueue()
    : mutex()
    , changes()
    , thread()
    , running(true)
    , tasks(PriorityTaskQueue::entryLessThan)
    , entryPool()
    , enqueuedCount()
    , doneCount()
{
}

/**
 * Halts outstanding tasks and destroys the thread used by this instance to
 * perform tasks in the background.
 */
PriorityTaskQueue::~PriorityTaskQueue()
{
    halt();
    Lock lock(mutex);
    while (!tasks.empty()) {
        PriorityQueueEntry* entry = tasks.top();
        tasks.pop();
        deschedule(lock, entry->task);
        entryPool.destroy(entry);
    }
}

/**
 * If tasks are ready for execution then execute one and return, otherwise
 * a no-op.
 *
 * Just before this PriorityTaskQueue executes this task, task->isScheduled()
 * is reset to false and this task will not be executed on subsequent calls
 * unless Task::schedule() is called again.  It is perfectly
 * legal to call Task::schedule() during a call to performTask(), which
 * indicates that the task should be run again in the future by this
 * PriorityTaskQueue.
 *
 * PriorityTaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe. Keep in mind that
 * having multiple threads calling performTask() (and/or
 * performTasksUntilHalt()) means any shared state between tasks will have
 * to have synchronized access.
 */
void
PriorityTaskQueue::performTask()
{
    PriorityTask* task = getNextTask(false);
    if (!task)
        return;
    task->performTask();
    ++doneCount;
    changes.notify_all();
}

/**
 * Perform all tasks that are ready for execution as they are scheduled,
 * and sleep waiting for calls to schedule(). This method returns after
 * halt() is called once it has finished performing the task it is
 * currently running (if any).
 *
 * Just before this PriorityTaskQueue executes this task, task->isScheduled()
 * is * reset to false and this task will not be executed on subsequent calls
 * unless Task::schedule() is called again.  It is perfectly
 * legal to call Task::schedule() during a call to performTask(), which
 * indicates that the task should be run again in the future by this
 * PriorityTaskQueue.
 *
 * PriorityTaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe. Keep in mind that
 * having multiple threads calling performTask() (and/or
 * performTasksUntilHalt()) means any shared state between tasks will have
 * to have synchronized access.
 */
void
PriorityTaskQueue::performTasksUntilHalt()
{
    while (PriorityTask* task = getNextTask(true)) {
        task->performTask();
        ++doneCount;
        changes.notify_all();
    }
}

/**
 * Waits for new tasks and dispatches them one-at-a-time.
 * Exits when halt() is called; may wait for a single task to complete before
 * returning.
 *
 * PriorityTaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe. Keep in mind that
 * having multiple threads calling performTask() (and/or
 * performTasksUntilHalt()) means any shared state between tasks will have
 * to have synchronized access.
 */
void
PriorityTaskQueue::main()
try {
    PerfStats::registerStats(&PerfStats::threadStats);
    performTasksUntilHalt();
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in PriorityTaskQueue: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in PriorityTaskQueue.");
    throw;
}

/**
 * Notify any executing calls to performTaskUntilHalt() that they should
 * exit as soon as they finish executing any currently executing task, if
 * any. This includes the thread created by start() if it was called.
 */
void
PriorityTaskQueue::halt()
{
    Lock lock(mutex);
    running = false;
    changes.notify_all();
    lock.unlock();
    if (thread)
        thread->join();
    thread.destroy();
}

/**
 * Start performing enqueued tasks in the background.
 * Calling start() on an instance that is already started has no effect.
 */
void
PriorityTaskQueue::start()
{
    Lock _(mutex);
    if (thread)
        return;
    running = true;
    thread.construct(&PriorityTaskQueue::main, this);
}

/**
 * Enqueue a task that should be performed asynchronously.
 * Tasks will be performed first according to priority order and then
 * in-order of enqueuing within a single priority.
 * Only called by PriorityTask::schedule().
 *
 * \param task
 *      Asynchronous job to be executed by the PriorityTaskQueue in the future.
 * \param priority
 *      LOW, NORMAL, HIGH. All scheduled tasks of higher priority run before
 *      those with a lower priority.
 */
void
PriorityTaskQueue::schedule(PriorityTask* task, Priority priority)
{
    Lock lock(mutex);
    if (task->isScheduled()) {
        if (task->entry->priority < priority) {
            deschedule(lock, task);
        } else {
            return;
        }
    }
    PriorityQueueEntry* entry =
        entryPool.construct(priority, ++enqueuedCount, task);
    tasks.push(entry);
    task->entry = entry;
    changes.notify_all();
}

/**
 * Remove this task from the #taskQueue; performTask() will not be called
 * again until after schedule() is called. It is safe to call deschedule()
 * during performTask(). Callers should be aware that deschedule() can operate
 * concurrently with performTask() and may yield to lost calls to performTask()
 * without proper synchronization.
 *
 * \param task
 *      Asynchronous job that should not be executed by the PriorityTaskQueue
 *      in the future. No effect if NULL.
 */
void
PriorityTaskQueue::deschedule(PriorityTask* task)
{
    Lock lock(mutex);
    deschedule(lock, task);
}

// Lock-free version of deschedule(). Used by schedule() and deschedule().
void
PriorityTaskQueue::deschedule(Lock& lock, PriorityTask* task)
{
    if (!task || !task->entry)
        return;
    task->entry->task = NULL;
    task->entry = NULL;
    ++doneCount;
}

/**
 * Wait until tasks have completed; NOTICE this call DOES NOT block out new
 * operations and is prone to being starved out. Used primarily by high-level
 * RAMCloud system tests to ensure all started backup operations have made it
 * to storage.
 */
void
PriorityTaskQueue::quiesce()
{
    Lock lock(mutex);
    while (running && !tasks.empty() && enqueuedCount != doneCount)
        changes.wait(lock);
}

// - private -

/**
 * Return the next task from the task queue if one is scheduled.
 *
 * \param sleepIfIdle
 *      If true then put the thread to sleep when no tasks are scheduled.
 *      Otherwise, if false then return NULL immediately if no tasks are
 *      scheduled.
 * \return
 *      Return the next task from the task queue if one is scheduled.
 *      If not and \a sleepIfIdle is false, then return NULL. Otherwise,
 *      put the thread to sleep waiting for the next task to be scheduled.
 *      Returns NULL when \a sleepIfIdle is true iff halt() was called.
 */
PriorityTask*
PriorityTaskQueue::getNextTask(bool sleepIfIdle)
{
    Lock lock(mutex);
    PriorityTask* task = NULL;
    while (!task) {
        while (true) {
            if (!running)
                return NULL;
            if (!sleepIfIdle || !tasks.empty())
                break;
            changes.wait(lock);
        }
        if (tasks.empty())
            return NULL;
        PriorityQueueEntry* entry = tasks.top();
        task = entry->task;
        if (task)
            task->entry = NULL;
        tasks.pop();
        entryPool.destroy(entry);
    }
    return task;
}

/**
 * Returns true if \a left should be done LATER THAN \a right.
 * Based on the priority of the tasks and then using the order the tasks
 * were enqueued to break ties.
 */
bool
PriorityTaskQueue::entryLessThan(const PriorityQueueEntry* left,
                                 const PriorityQueueEntry* right)
{
    if (!left && !right)
        return false;
    else if (!left)
        return true;
    else if (!right)
        return false;
    else if (left->priority == right->priority)
        return left->enqueuedTimestamp > right->enqueuedTimestamp;
    else
        return left->priority < right->priority;
}

} // namespace RAMCloud
