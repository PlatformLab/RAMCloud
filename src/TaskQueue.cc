/* Copyright (c) 2011-2012 Stanford University
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

#include "ShortMacros.h"
#include "TaskQueue.h"
#include "TestLog.h"

namespace RAMCloud {

// --- Task ---

/**
 * Create a Task which will be run by #taskQueue if scheduled.
 *
 * \param taskQueue
 *      TaskQueue which will execute performTask().
 */
Task::Task(TaskQueue& taskQueue)
    : taskQueue(taskQueue)
    , scheduled(false)
{
}

/**
 * Virtual destructor; does nothing.
 * Subclasses of Task will often want to ensure that an outstanding task
 * completes before it is destroyed (otherwise it remains in the TaskQueue's
 * queue and will result in undefined behavior on the next iteration); they can
 * easily do so since they have access to the undestructed state belonging to
 * the subclass.  This destructor cannot performTask() until scheduled == false
 * here, it's too late because the virtual destructors of our subclasses have
 * already been called.
 */
Task::~Task()
{
}

/**
 * Return true if this Task will be executed by #taskQueue. This call is safe
 * even during concurrent operations on the underlying TaskQueue.
 */
bool
Task::isScheduled()
{
    return scheduled;
}

/**
 * Enqueue this Task for execution by #taskQueue. This call is safe
 * even during concurrent operations on the underlying TaskQueue.
 *
 * Just before taskQueue executes this task, isScheduled() is reset to
 * false and this task will not be executed on subsequent passes made
 * by #taskQueue unless schedule() is called again.  It is perfectly
 * legal to call schedule() during a call to performTask(), which indicates
 * that the task should be run again in the future by the #taskQueue.
 * Calling schedule() when isScheduled() == true has no effect.
 *
 * Importantly, creators of tasks must take care to ensure that a task is not
 * scheduled when it is destroyed, otherwise the taskQueue will exhibit
 * undefined behavior when it attempts to execute this (destroyed) task.
 */
void
Task::schedule()
{
    taskQueue.schedule(this);
}

// --- TaskQueue ---

/// Create a TaskQueue.
TaskQueue::TaskQueue()
    : mutex()
    , taskAdded()
    , running(true)
    , tasks()
{
}

TaskQueue::~TaskQueue()
{
}

/// Returns true if no tasks are waiting to run.
bool
TaskQueue::isIdle()
{
    Lock _(mutex);
    return tasks.size() == 0;
}

/// Returns number of tasks waiting to run.
size_t
TaskQueue::outstandingTasks()
{
    Lock _(mutex);
    return tasks.size();
}

/**
 * If tasks are ready for execution then execute one and return, otherwise
 * a no-op.
 *
 * Just before this TaskQueue executes this task, task->isScheduled() is
 * reset to false and this task will not be executed on subsequent calls
 * unless Task::schedule() is called again.  It is perfectly
 * legal to call Task::schedule() during a call to performTask(), which
 * indicates that the task should be run again in the future by this
 * TaskQueue.
 *
 * TaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe. Keep in mind that
 * having multiple threads calling performTask() (and/or
 * performTasksUntilHalt()) means any shared state between tasks will have
 * to have synchronized access.
 *
 * \return
 *      True if a task was performed, false if no task was performed.
 */
bool
TaskQueue::performTask()
{
    Task* task = getNextTask(false);
    if (!task)
        return false;
    task->performTask();
    return true;
}

/**
 * Perform all tasks that are ready for execution as they are scheduled,
 * and sleep waiting for calls to schedule(). This method returns after
 * halt() is called once it has finished performing the task it is
 * currently running (if any).
 *
 * Just before this TaskQueue executes this task, task->isScheduled() is
 * reset to false and this task will not be executed on subsequent calls
 * unless Task::schedule() is called again.  It is perfectly
 * legal to call Task::schedule() during a call to performTask(), which
 * indicates that the task should be run again in the future by this
 * TaskQueue.
 *
 * TaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe. Keep in mind that
 * having multiple threads calling performTask() (and/or
 * performTasksUntilHalt()) means any shared state between tasks will have
 * to have synchronized access.
 */
void
TaskQueue::performTasksUntilHalt()
{
    while (true) {
        Task* task = getNextTask(true);
        if (!task)
            return;
        task->performTask();
    }
}

/**
 * Notify any executing calls to performTaskUntilHalt() that they should
 * exit as soon as they finish executing any currently executing task, if
 * any.
 */
void
TaskQueue::halt()
{
    Lock _(mutex);
    running = false;
    taskAdded.notify_one();
}

// -- private --

/**
 * Queue \a task for execution on future calls to performTask (or
 * performTasksUntilHalt()).
 * Only called by Task::schedule().
 * TaskQueue is thread-safe so simultaneous calls to schedule(),
 * performTask(), and performTasksUntilHalt() are safe.
 *
 * \param task
 *      Asynchronous job to be executed by the TaskQueue in the future.
 */
void
TaskQueue::schedule(Task* task)
{
    Lock _(mutex);
    if (task->scheduled)
        return;
    task->scheduled = true;
    tasks.push(task);
    taskAdded.notify_one();
    TEST_LOG("scheduled");
}

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
Task*
TaskQueue::getNextTask(bool sleepIfIdle)
{
    Lock lock(mutex);
    while (true) {
        if (!running)
            return NULL;
        if (!sleepIfIdle || !tasks.empty())
            break;
        taskAdded.wait(lock);
    }
    if (tasks.empty())
        return NULL;
    Task* task = tasks.front();
    tasks.pop();
    task->scheduled = false;
    return task;
}

} // namespace RAMCloud
