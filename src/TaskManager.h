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

#ifndef RAMCLOUD_TASKMANAGER_H
#define RAMCLOUD_TASKMANAGER_H

#include <queue>

#include "Common.h"

namespace RAMCloud {

class TaskManager;      // forward-declaration

/**
 * Abstract class which represents some work that can be queued up and executed
 * at a later time.  This makes it easy to quickly create asynchronous jobs
 * which are periodically checked for completeness out of a performance
 * sensitive context.
 *
 * Users subclass Task and provide an implementation for performTask()
 * specific to the deferred work they want done.  Each task is associated with
 * a TaskManager which eventually performs it whenever the task is scheduled
 * (see schedule()).
 *
 * Importantly, creators of tasks must take care to ensure that a task is not
 * scheduled when it is destroyed, otherwise future calls to
 * taskManager.proceed() will result in undefined behavior.
 */
class Task {
  PUBLIC:
    explicit Task(TaskManager& taskManager);
    virtual ~Task();

    /**
     * Pure virtual method implemented by subclasses; its execution
     * is deferred to a later time perform work asynchronously.
     * See schedule() and TaskManager::proceed().
     */
    virtual void performTask() = 0;

    bool isScheduled();
    void schedule();

  PRIVATE:
    /// Executes this Task when it isScheduled() on taskManager.proceed().
    TaskManager& taskManager;

    /// True if performTask() will be run on the next taskManager.proceed().
    bool scheduled;

    friend class TaskManager;
};

/**
 * Queues up tasks and exceutes them at a later time.  This makes it easy to
 * quickly schedule asynchronous jobs which are periodically checked for
 * completeness out of a performance sensitive context.
 * See Task for details on how to create tasks and releated gotchas.
 */
class TaskManager {
  PUBLIC:
    TaskManager();
    ~TaskManager();
    bool isIdle();
    void proceed();

  PRIVATE:
    void schedule(Task* task);

    /**
     * Points to tasks which should be executed on the next call to proceed().
     * Provides FIFO order for task scheduling.
     */
    std::queue<Task*> tasks;

    friend class Task;
};

} // namespace RAMCloud

#endif
