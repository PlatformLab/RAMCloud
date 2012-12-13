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

#ifndef RAMCLOUD_TASKQUEUE_H
#define RAMCLOUD_TASKQUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>

#include "Common.h"

namespace RAMCloud {

class TaskQueue;      // forward-declaration

/**
 * Abstract class which represents some work that can be queued up and executed
 * at a later time.  This makes it easy to quickly create asynchronous jobs
 * which are periodically checked for completeness out of a performance
 * sensitive context.
 *
 * Users subclass Task and provide an implementation for performTask()
 * specific to the deferred work they want done.  Each task is associated with
 * a TaskQueue which eventually performs it whenever the task is scheduled
 * (see schedule()).
 *
 * Importantly, creators of tasks must take care to ensure that a task is not
 * scheduled when it is destroyed, otherwise the taskQueue will exhibit
 * undefined behavior when it attempts to execute this (destroyed) task.
 */
class Task {
  PUBLIC:
    explicit Task(TaskQueue& taskQueue);
    virtual ~Task();

    /**
     * Pure virtual method implemented by subclasses; its execution
     * is deferred to a later time perform work asynchronously.
     * See schedule() and TaskQueue::performTask().
     */
    virtual void performTask() = 0;

    bool isScheduled();
    void schedule();

  PROTECTED:
    /// Executes this Task when it isScheduled() on taskQueue.performTask().
    TaskQueue& taskQueue;

  PRIVATE:
    /// True if performTask() will be run on the next taskQueue.performTask().
    bool scheduled;

    friend class TaskQueue;
};

/**
 * Queues up tasks and executes them at a later time.  This makes it easy to
 * quickly schedule asynchronous jobs which are periodically checked for
 * completeness out of a performance sensitive context.
 * See Task for details on how to create tasks and related gotchas.
 */
class TaskQueue {
  PUBLIC:
    TaskQueue();
    ~TaskQueue();
    bool isIdle();
    size_t outstandingTasks();
    bool performTask();
    void performTasksUntilHalt();
    void halt();

  PRIVATE:
    void schedule(Task* task);
    Task* getNextTask(bool sleepIfIdle);

    /**
     * Protects all modifications to #tasks and #running; used to allow
     * safe concurrent calls to schedule(), performTask(), and
     * performTasksUntilHalt().
     */
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * Waited on during performTasksUntilHalt() if there are no tasks to run.
     * Notified on schedule() or halt().
     */
    std::condition_variable taskAdded;

    /**
     * Used to tell performTasksUntilHalt() to return after the completion of
     * any currently running task.
     */
    bool running;

    /**
     * Points to tasks which should be executed.
     * Provides FIFO order for task scheduling.
     */
    std::queue<Task*> tasks;

    friend class Task;
};

} // namespace RAMCloud

#endif
