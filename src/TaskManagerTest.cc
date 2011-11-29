/* Copyright (c) 2009-2011 Stanford University
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

#include "TestUtil.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "CoordinatorService.h"
#include "BackupClient.h"
#include "BackupManager.h"
#include "BindTransport.h"
#include "ShortMacros.h"

namespace RAMCloud {

struct TaskManagerTest : public ::testing::Test {
    struct MockTask : Task {
        explicit MockTask(TaskManager& taskManager)
            : Task(taskManager)
            , count(0)
        {
        }

        void
        performTask()
        {
            ++count;
        }

        int count;
    };

    struct ReschedulingMockTask : Task {
        explicit ReschedulingMockTask(TaskManager& taskManager)
            : Task(taskManager)
            , count(0)
        {
        }

        void
        performTask()
        {
            ++count;
            if (count == 1)
                schedule();
        }

        int count;
    };

    TaskManager taskManager;
    MockTask task1;
    MockTask task2;

    TaskManagerTest()
        : taskManager()
        , task1(taskManager)
        , task2(taskManager)
    {
    }
};

TEST_F(TaskManagerTest, proceedEmpty)
{
    taskManager.proceed();
}

TEST_F(TaskManagerTest, proceed)
{
    const int times = 3;
    for (int i = 0; i < times; ++i) {
        task1.schedule();
        task2.schedule();
        taskManager.proceed();
        EXPECT_FALSE(task1.isScheduled());
        EXPECT_FALSE(task2.isScheduled());
    }
    EXPECT_EQ(3, task1.count);
    EXPECT_EQ(3, task2.count);
}

TEST_F(TaskManagerTest, schedule)
{
    task1.schedule();
    ASSERT_TRUE(task1.isScheduled());
    task1.schedule(); // check to make sure double schedules don't happen
    ASSERT_EQ(1u, taskManager.tasks.size());
    EXPECT_EQ(&task1, taskManager.tasks.front());
    taskManager.proceed(); // clear out task queue
}

TEST_F(TaskManagerTest, scheduleNested)
{
    ReschedulingMockTask task(taskManager);
    task.schedule();
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskManager.tasks.size());
    EXPECT_EQ(&task, taskManager.tasks.front());
    taskManager.proceed();
    EXPECT_EQ(1, task.count);
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskManager.tasks.size());
    EXPECT_EQ(&task, taskManager.tasks.front());
    taskManager.proceed(); // clear out task queue
    EXPECT_EQ(2, task.count);
}

} // namespace RAMCloud
