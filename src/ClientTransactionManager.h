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

#ifndef RAMCLOUD_CLIENTTRANSACTIONMANAGER_H
#define RAMCLOUD_CLIENTTRANSACTIONMANAGER_H

#include <list>
#include <memory>

#include "Common.h"

namespace RAMCloud {

class ClientTransactionTask;
class RamCloud;

/**
 * The ClientTransactionManager drives a collection of ClientTransactionTasks
 * until they have run their commit protocol to completion.
 *
 * In typical usage, a Transaction will construct a ClientTransactionTask as
 * part of setting up a client transaction.  When the Transaction wishes to
 * commit, it will add the task to the manager for it to be run.  Even if the
 * Transaction is destructed before the task is finished (this may happen if the
 * client wants to commit the transaction asynchronously), the manager will
 * still hold on to the task and run it until completion.
 *
 * The ClientTransactionManager is driven in calls to Transaction::commit.
 */
class ClientTransactionManager {
  PUBLIC:
    ClientTransactionManager();
    void poll();
    void startTransactionTask(std::shared_ptr<ClientTransactionTask>& taskPtr);

  PRIVATE:
    std::list< std::shared_ptr<ClientTransactionTask> > taskList;

    DISALLOW_COPY_AND_ASSIGN(ClientTransactionManager);
};

} // end RAMCloud

#endif  /* RAMCLOUD_CLIENTTRANSACTIONMANAGER_H */
