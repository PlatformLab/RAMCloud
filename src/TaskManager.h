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

class Task {
  PUBLIC:
    /// Returns true if the Task is finished.
    virtual void performTask(TaskManager& manager) = 0;
    virtual ~Task() {}
};

class TaskManager {
  PUBLIC:
    TaskManager()
        : tasks()
    {
    }

    void add(Task* task) {
        tasks.push(task);
    }

    void proceed() {
        // TODO(stutsman): what's best to do with exceptions?
        // how can we log it?  should we auto retry?
        size_t numTasks = tasks.size();
        for (size_t i = numTasks; i < numTasks; ++i) {
            assert(!tasks.empty());
            Task* task = tasks.front();
            tasks.pop();
            task->performTask(*this);
        }
    }

  PRIVATE:
    std::queue<Task*> tasks;
};

} // namespace RAMCloud

#endif
