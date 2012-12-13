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

#include <functional>

#include "TestUtil.h"
#include "Common.h"
#include "ShortMacros.h"
#include "TaskQueue.h"

namespace RAMCloud {

struct TaskQueueTest : public ::testing::Test {
    struct MockTask : Task {
        explicit MockTask(TaskQueue& taskQueue)
            : Task(taskQueue)
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
        explicit ReschedulingMockTask(TaskQueue& taskQueue)
            : Task(taskQueue)
            , count(0)
        {
        }

        void
        performTask()
        {
            ++count;
            if (count == 1)
                schedule();
            if (count == 2)
                taskQueue.halt();
        }

        int count;
    };

    TaskQueue taskQueue;
    MockTask task1;
    MockTask task2;

    TaskQueueTest()
        : taskQueue()
        , task1(taskQueue)
        , task2(taskQueue)
    {
    }
};

TEST_F(TaskQueueTest, performTaskEmpty)
{
    EXPECT_FALSE(taskQueue.performTask());
}

TEST_F(TaskQueueTest, performTask)
{
    const int times = 3;
    for (int i = 0; i < times; ++i) {
        task1.schedule();
        task2.schedule();
        EXPECT_TRUE(taskQueue.performTask());
        EXPECT_FALSE(task1.isScheduled());
        EXPECT_TRUE(task2.isScheduled());
        EXPECT_TRUE(taskQueue.performTask());
        EXPECT_FALSE(task2.isScheduled());
        EXPECT_FALSE(taskQueue.performTask());
    }
    EXPECT_EQ(3, task1.count);
    EXPECT_EQ(3, task2.count);
}

namespace {
void runTaskQueue(TaskQueue& taskQueue) {
    taskQueue.performTasksUntilHalt();
}
}

TEST_F(TaskQueueTest, performTasksUntilHalt)
{
    std::thread thread(&runTaskQueue, std::ref(taskQueue));
    ReschedulingMockTask task(taskQueue);
    task.schedule();
    thread.join();
    EXPECT_FALSE(task.isScheduled());
    EXPECT_EQ(2, task.count);
}

TEST_F(TaskQueueTest, halt)
{
    std::thread thread(&runTaskQueue, std::ref(taskQueue));
    taskQueue.halt();
    thread.join();
}

TEST_F(TaskQueueTest, schedule)
{
    task1.schedule();
    ASSERT_TRUE(task1.isScheduled());
    task1.schedule(); // check to make sure double schedules don't happen
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(&task1, taskQueue.tasks.front());
    taskQueue.performTask(); // clear out task queue
}

TEST_F(TaskQueueTest, scheduleNested)
{
    ReschedulingMockTask task(taskQueue);
    task.schedule();
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(&task, taskQueue.tasks.front());
    taskQueue.performTask();
    EXPECT_EQ(1, task.count);
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(&task, taskQueue.tasks.front());
    taskQueue.performTask(); // clear out task queue
    EXPECT_EQ(2, task.count);
}

TEST_F(TaskQueueTest, getNextTask)
{
    EXPECT_EQ(static_cast<Task*>(NULL), taskQueue.getNextTask(false));
    task1.schedule();
    EXPECT_EQ(&task1, taskQueue.getNextTask(false));
    task1.schedule();
    EXPECT_EQ(&task1, taskQueue.getNextTask(true));
    taskQueue.halt();
    EXPECT_EQ(static_cast<Task*>(NULL), taskQueue.getNextTask(true));
}

} // namespace RAMCloud
