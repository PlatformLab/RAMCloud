/* Copyright (c) 2010-2015 Stanford University
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

#ifndef RAMCLOUD_PARALLELRUN_H
#define RAMCLOUD_PARALLELRUN_H

#include "Common.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A Concept used in #parallelRun.
 */
struct AsynchronousTaskConcept {
    /**
     * Return true if calling #wait() would make progress on this task without
     * blocking. This should return false if #isDone() returns true.
     */
    bool isReady();
    /**
     * Return true if there is no more work to be done on this task.
     */
    bool isDone();
    /**
     * Start a new chunk of work, to be later completed in #wait().
     */
    void send();
    /**
     * Wait for a chunk of work previously started in #send() to complete.
     * Note that, after calling #wait(), #isDone() may still return false. If
     * this is the case, #wait() will be called again at some later time.
     */
    void wait();
};

/**
 * Execute asynchronous tasks in parallel until they complete.
 * This is useful for broadcasting RPCs, etc.  This should be
 * used only in worker threads (not in the dispatch thread).
 */
template <typename T>
class ParallelRun {
  PUBLIC:
    /**
     * Kick off the first round of tasks to execute. Calls to proceed()
     * reap completed tasks and start new tasks.
     *
     * \param tasks
     *      An array of \a numTasks entries in length of objects having the
     *      interface documented in #AsynchronousTaskConcept. Any empty
     *      entries will be skipped.
     * \param numTasks
     *      The number of entries in the \a tasks array.
     * \param maxOutstanding
     *      The maximum number of task to run in parallel with each other.
     */
    ParallelRun(Tub<T>* tasks, size_t numTasks, size_t maxOutstanding)
        : tasks(tasks)
        , numTasks(numTasks)
        , maxOutstanding(maxOutstanding)
        , currentOutstanding(0)
        , firstNotIssued(0)
        , firstNotDone(0)
    {
        assert(maxOutstanding > 0 || numTasks == 0);

        // Start off first round of tasks
        for (uint32_t i = 0;
                (i < numTasks) && (currentOutstanding < maxOutstanding); ++i) {
            auto& task = tasks[i];
            if (task) {
                task->send();
                ++currentOutstanding;
            }
            ++firstNotIssued;
        }
    }

    /**
     * Reap any tasks which were started earlier and have now finished, and
     * kick off new tasks.
     *
     * \return
     *      True if all tasks have been completed, false otherwise.
     */
    bool proceed() {
        for (uint32_t i = firstNotDone; i < firstNotIssued; ++i) {
            auto& task = tasks[i];
            if (!task || task->isDone()) { // completed already
                if (firstNotDone == i)
                    ++firstNotDone;
                continue;
            }
            if (!task->isReady()) // not started or reply hasn't arrived
                continue;
            task->wait();
            if (!task->isDone())
                continue;
            --currentOutstanding;
            if (firstNotDone == i)
                ++firstNotDone;
        }

        while ((firstNotIssued < numTasks) &&
                (currentOutstanding < maxOutstanding)) {
            Tub<T>& taskToIssue = tasks[firstNotIssued];
            if (taskToIssue) {
                taskToIssue->send();
                ++currentOutstanding;
            }
            ++firstNotIssued;
        }

        return firstNotDone == numTasks;
    }

    /// Return true if all tasks have been completed, false otherwise.
    bool isDone() {
        return firstNotDone == numTasks;
    }

    /// Block until all tasks have been completed.
    void wait() {
        while (!isDone())
            proceed();
    }

    /**
     * An array of #numTasks entries in length of objects having the
     * interface documented in #AsynchronousTaskConcept.
     */
    Tub<T>* tasks;

    /// The number of entries in the #tasks array.
    size_t numTasks;

    /// The maximum number of tasks to run in parallel with each other.
    const size_t maxOutstanding;

  PRIVATE:
    /// Number of tasks currently running.
    size_t currentOutstanding;

    /// The first element of #tasks has not yet had start() called.
    uint32_t firstNotIssued;

    /// The first element of #tasks is not yet done.
    uint32_t firstNotDone;

    DISALLOW_COPY_AND_ASSIGN(ParallelRun);
};

/**
 * Execute asynchronous tasks in parallel until they complete.
 * This is useful for broadcasting RPCs, etc.  This method should be invoked
 * only in worker threads (not in the dispatch thread).
 * \param tasks
 *      An array of \a numTasks entries in length of objects having the
 *      interface documented in #AsynchronousTaskConcept.
 * \param numTasks
 *      The number of entries in the \a tasks array.
 * \param maxOutstanding
 *      The maximum number of task to run in parallel with each other.
 */
template<typename T>
void
parallelRun(Tub<T>* tasks, size_t numTasks, size_t maxOutstanding)
{
    ParallelRun<T>(tasks, numTasks, maxOutstanding).wait();
}

} // end RAMCloud

#endif  // RAMCLOUD_PARALLELRUN_H
