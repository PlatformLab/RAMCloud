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

#include "TaskManager.h"

namespace RAMCloud {

// --- Task ---

/**
 * Create a Task which will be run by #taskManager if scheduled.
 *
 * \param taskManager
 *      TaskManager which will execute #performTask().
 */ Task::Task(TaskManager& taskManager)
    : taskManager(taskManager)
    , scheduled(false)
{
}

/// Virtual destructor; does nothing.
Task::~Task()
{
    // We cannot wait until scheduled == false here, it's too late
    // because the virtual destructors of our subclasses have already
    // been called.
    assert(!scheduled);
}

/**
 * Return true if this Task will be executed by #taskManager
 * (that is, #performTask() will be called the next time
 * taskManager.proceed() is called).
 */
bool
Task::isScheduled()
{
    return scheduled;
}

/**
 * Ensure this Task will be executed (once) by #taskManager (that is,
 * #performTask() will be called the next time taskManager.proceed() is
 * called).
 *
 * Just before taskManager executes this task, #isScheduled() is reset to
 * false and this task will not be executed on subsequent passes made
 * by #taskManager unless #schedule() is called again.  It is perfectly
 * legal to call #schedule() during a call to #performTask(), which indicates
 * that the task should be run again on the next pass of the #taskManager.
 * Calling #schedule() when #isScheduled() == true has no effect.
 *
 * Importantly, creators of tasks must take care to ensure that a task is not
 * scheduled when it is destroyed, otherwise future calls to
 * taskManager.proceed() will result in undefined behavior.
 */
void
Task::schedule()
{
    taskManager.schedule(this);
}

// --- TaskManager ---

/// Create a TaskManager.
TaskManager::TaskManager()
    : tasks()
{
}

TaskManager::~TaskManager()
{
    assert(tasks.size() == 0);
}

/// Returns true if no tasks are waiting to run on the next call to proceed().
bool
TaskManager::isIdle()
{
    return tasks.size() == 0;
}

/**
 * Execute all tasks scheduled since the last call to #proceed() (by
 * calling their performTask() virtual method).
 *
 * Just before taskManager executes this task, task->isScheduled() is reset to
 * false and this task will not be executed on subsequent #proceed() calls
 * unless Task::schedule() is called again.  It is perfectly
 * legal to call Task::schedule() during a call to #performTask(), which
 * indicates that the task should be run again on the next call to #proceed().
 */
void
TaskManager::proceed()
{
    size_t numTasks = tasks.size();
    for (size_t i = 0; i < numTasks; ++i) {
        assert(!tasks.empty());
        Task* task = tasks.front();
        tasks.pop();
        task->scheduled = false;
        task->performTask();
    }
}

// -- private --

/**
 * Queue #task for execution on the next call to #proceed().
 * Only called by Task::schedule().
 *
 * \param task
 *      Asynchronous job to be executed on the next call to #proceed().
 */
void
TaskManager::schedule(Task* task)
{
    if (task->isScheduled())
        return;
    task->scheduled = true;
    tasks.push(task);
}

} // namespace RAMCloud
