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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "ClientTransactionManager.h"
#include "MockCluster.h"
#include "ClientTransactionTask.h"

namespace RAMCloud {

class ClientTransactionManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    RamCloud ramcloud;
    ClientTransactionManager txManager;
    std::shared_ptr<ClientTransactionTask> taskPtr;
    std::shared_ptr<ClientTransactionTask> taskPtrOther;

    ClientTransactionManagerTest()
        : logEnabler()
        , context()
        , cluster(&context, "mock:host=coordinator")
        , ramcloud(&context, "mock:host=coordinator")
        , txManager()
        , taskPtr(new ClientTransactionTask(&ramcloud))
        , taskPtrOther(new ClientTransactionTask(&ramcloud))
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
    }

    DISALLOW_COPY_AND_ASSIGN(ClientTransactionManagerTest);
};

TEST_F(ClientTransactionManagerTest, poll) {
    txManager.taskList.push_back(taskPtr);
    txManager.taskList.push_back(taskPtrOther);
    taskPtrOther.get()->state = ClientTransactionTask::DONE;

    EXPECT_EQ(ClientTransactionTask::INIT, taskPtr.get()->state);
    EXPECT_EQ(ClientTransactionTask::DONE, taskPtrOther.get()->state);
    EXPECT_EQ(2U, txManager.taskList.size());

    txManager.poll();

    EXPECT_EQ(ClientTransactionTask::DONE, taskPtr.get()->state);
    EXPECT_EQ(ClientTransactionTask::DONE, taskPtrOther.get()->state);
    EXPECT_EQ(1U, txManager.taskList.size());
}

TEST_F(ClientTransactionManagerTest, startTransactionTask) {
    EXPECT_EQ(0U, txManager.taskList.size());
    txManager.startTransactionTask(taskPtr);
    EXPECT_EQ(1U, txManager.taskList.size());
}

} // end RAMCloud
