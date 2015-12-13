/* Copyright (c) 2015 Stanford University
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

#include "ClientTransactionManager.h"
#include "ClientTransactionTask.h"

namespace RAMCloud {

/**
 * Constructor for the client transaction manager.
 */
ClientTransactionManager::ClientTransactionManager()
    : taskList()
{}

/**
 * Called when the manager has been scheduled to do some work.  This method
 * will perform some incremental work and reschedule itself if more work needs
 * to be done.
 */
void
ClientTransactionManager::poll()
{
    auto it = taskList.begin();
    while (it != taskList.end()) {
        ClientTransactionTask* task = it->get();
        if (task->isReady()) {
            // Remove the task once it is done.
            it = taskList.erase(it);
        } else {
            task->performTask();
            it++;
        }
    }
}

/**
 * Add a transaction task that needs to be run to the manager.  The transaction
 * commit protocol is considered started once it has been added to this the
 * transaction manager.
 *
 * \param taskPtr
 *      Reference to a shared pointer to a task that needs to be run.
 */
void
ClientTransactionManager::startTransactionTask(
        std::shared_ptr<ClientTransactionTask>& taskPtr)
{
    taskList.push_back(taskPtr);
}


} // end RAMCloud
