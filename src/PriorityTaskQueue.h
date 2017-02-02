/* Copyright (c) 2012-2017 Stanford University
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

#ifndef RAMCLOUD_PRIORITYTASKQUEUE_H
#define RAMCLOUD_PRIORITYTASKQUEUE_H

#include <mutex>
#include <thread>
#include <condition_variable>
#include <deque>
#include <queue>

#include "Common.h"
#include "ObjectPool.h"
#include "Tub.h"

namespace RAMCloud {

class PriorityTaskQueue;

/**
 * Abstract class which represents some work that can be queued up and executed
 * at a later time according to a priority ordering. This makes it easy to
 * quickly create asynchronous jobs which are periodically checked for
 * completeness out of a performance sensitive context. Tasks from higher
 * priorities are executed before those with lower priorities; tasks are FIFO
 * within each priority level. Users that don't care about task priorities
 * should consider using Task and TaskQueue instead.
 *
 * Users subclass PriorityTask and provide an implementation for performTask()
 * specific to the deferred work they want done. Each task is associated with
 * a PriorityTaskQueue which eventually performs it whenever the task is
 * scheduled (see schedule()).
 *
 * If a task is destroyed while it is scheduled it will be descheduled.
 */
class PriorityTask {
  PUBLIC:
    explicit PriorityTask(PriorityTaskQueue& taskQueue);
    virtual ~PriorityTask();

    enum Priority { LOW, NORMAL, HIGH };

    /**
     * Pure virtual method implemented by subclasses; its execution
     * is deferred to a later time perform work asynchronously.
     * See schedule() and PriorityTaskQueue::performTask().
     * This empty implementation may get invoked when an instance of a subclass
     * is being destructed. It is probably a REALLY good idea to avoid this, so
     * subclasses should deschedule at the start of their own destructors to
     * ensure no future calls to performTask get through (though one could
     * be executing already).
     */
    virtual void performTask() {}

    bool isScheduled() const;
    void schedule(Priority priority);
    void deschedule();

  PROTECTED:
    /// Executes this task when it isScheduled() on taskQueue.performTask().
    PriorityTaskQueue* taskQueue;

  PRIVATE:
    /**
     * Entry in a PriorityTaskQueue which provides the fields for ordering
     * a scheduled task in the queue. This is mostly an implementation detail
     * of how tasks are descheduled from PriorityTaskQueue due to
     * std::priority_queue not supporting removal. See the task field within
     * for more information.
     */
    struct PriorityQueueEntry {
        PriorityQueueEntry(Priority priority,
                           uint64_t enqueuedTimestamp,
                           PriorityTask* task)
            : priority(priority)
            , enqueuedTimestamp(enqueuedTimestamp)
            , task(task)
        {}

        /// Priority of the task in the queue.
        Priority priority;

        /**
         * Logical time when the entry was added to the queue. Used to order
         * tasks within a single priority.
         */
        uint64_t enqueuedTimestamp;

        /**
         * Task associated with this entry in the priority queue. Used to
         * "fake" removes from the priority queue after a task has already
         * been scheduled. PriorityTaskQueue skips queue entries that have
         * NULL task pointers, so clearing this pointer will cancel the
         * task while retaining the heap invariants (since the fields used
         * for ordering remain intact). Only PriorityTaskQueue uses/modifies
         * this; it ensures if task is cleared that PriorityTask::entry is
         * also cleared so the task can be rescheduled at a new position
         * in the queue.
         */
        PriorityTask* task;
        DISALLOW_COPY_AND_ASSIGN(PriorityQueueEntry);
    };

    /**
     * Set if performTask() will be run on the next taskQueue.performTask().
     * Used by PriorityTaskQueue  to remove or move a task in the queue.
     * See PriorityQueueEntry.
     */
    PriorityQueueEntry* entry;

    friend class PriorityTaskQueue;
    DISALLOW_COPY_AND_ASSIGN(PriorityTask);
};

/**
 * Queues up tasks and executes them at a later time according to a priority
 * ordering. This makes it easy to quickly schedule asynchronous jobs which are
 * periodically checked for completeness out of a performance sensitive
 * context. Tasks from higher priorities are executed before those with lower
 * priorities; tasks are FIFO within each priority level. See PriorityTask for
 * details on how to create tasks and related gotchas.
 * Users that don't care about task priorities should consider using Task
 * and TaskQueue instead.
 *
 * Users of PriorityTaskQueue have three choices how/when tasks get executed:
 * 1) Calling start() creates a thread which executes tasks until halt() is
 *    called. Whenever the queue is empty the thread will sleep waiting for
 *    new tasks.
 * 2) Calling performTasksUntilHalt() blocks the calling thread until halt()
 *    is called. Just as above, whenever there are no tasks to perform the
 *    thread will be put to sleep.
 * 3) Calling performTask() manually each time the user is ready to perform
 *    the next task. performTask() will never block. If no task is ready
 *    it will return immediately. This allows the most flexibility. It allows
 *    tasks to be cooperatively scheduled with other work on the same thread
 *    or for tasks to be executed concurrently by multiple threads.
 */
class PriorityTaskQueue {
  PUBLIC:
    PriorityTaskQueue();
    ~PriorityTaskQueue();

    void performTask();
    void performTasksUntilHalt();
    void main();
    void start();
    void halt();

    void quiesce();

  PRIVATE:
    typedef std::unique_lock<std::mutex> Lock;
    typedef PriorityTask::Priority Priority;

    void schedule(PriorityTask* task, Priority priority);
    void deschedule(PriorityTask* task);
    void deschedule(Lock& lock, PriorityTask* task);

    PriorityTask* getNextTask(bool sleepIfIdle);
    typedef PriorityTask::PriorityQueueEntry PriorityQueueEntry;
    static bool entryLessThan(const PriorityQueueEntry* left,
                              const PriorityQueueEntry* right);

    /// Protects all members for thread-safety.
    std::mutex mutex;

    /**
     * Used to wait on new work, completion of existing work, and exit
     * conditions.
     * Notified in three cases:
     * 1) A PriorityTask has been enqueued.
     * 2) A PriorityTask has finished.
     * 3) #running becomes false.
     */
    std::condition_variable changes;

    /**
     * If start() is call, drives main() waiting for new tasks and performing
     * them one-at-a-time. Exits if halt() is called.
     */
    Tub<std::thread> thread;

    /// If false exit (from performTasksUntilHalt()) on the next task pop.
    bool running;

    typedef std::priority_queue<PriorityQueueEntry*,
                                std::deque<PriorityQueueEntry*>,
                                bool (*)(const PriorityQueueEntry*,
                                         const PriorityQueueEntry*)> Queue;
    /**
     * Contains PriorityQueueEntry pointers which have been enqueued for IO,
     * which indirectly point to PriorityTasks.
     * Orders tasks by the parameters in the PriorityQueueEntry; first by
     * priority and then by the order in which they were enqueued.
     * Only manipulated by schedule() and getNextTask().
     */
    Queue tasks;

    /**
     * Recycles PriorityQueueEntries since once must be allocated/dellocated
     * each time a task is scheduled/rescheduled or completes.
     */
    ObjectPool<PriorityQueueEntry> entryPool;

    /**
     * Counts tasks that have been enqueued.
     * Has two uses:
     * Allows quiesce to ensure no tasks are enqueued or underway.
     * And, acts as a logical timestamp to give total order to tasks within the
     * same priority (see PriorityQueueEntry::enqueuedTimestamp).
     */
    uint64_t enqueuedCount;

    /**
     * Counts tasks which have completed.
     * Used by quiesce to determine when all work enqueued is completed.
     */
    uint64_t doneCount;

    friend class PriorityTask;
    DISALLOW_COPY_AND_ASSIGN(PriorityTaskQueue);
};

} // namespace RAMCloud

#endif
