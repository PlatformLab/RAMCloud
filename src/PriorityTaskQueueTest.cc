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

#include "TestUtil.h"
#include "PriorityTaskQueue.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

struct PriorityTaskQueueTest : public ::testing::Test {
    typedef PriorityTask::PriorityQueueEntry QEntry;

    struct MockTask : PriorityTask {
        explicit MockTask(PriorityTaskQueue& taskQueue)
            : PriorityTask(taskQueue)
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

    struct ReschedulingMockTask : PriorityTask {
        explicit ReschedulingMockTask(PriorityTaskQueue& taskQueue)
            : PriorityTask(taskQueue)
            , count(0)
        {
        }

        void
        performTask()
        {
            ++count;
            if (count == 1)
                schedule(LOW);
            if (count == 2)
                taskQueue->halt();
        }

        int count;
    };

    PriorityTaskQueue taskQueue;
    MockTask task1;
    MockTask task2;

    PriorityTaskQueueTest()
        : taskQueue()
        , task1(taskQueue)
        , task2(taskQueue)
    {
    }
};

TEST_F(PriorityTaskQueueTest, destructor)
{
    Tub<MockTask> task1;
    {
        PriorityTaskQueue taskQueue;
        task1.construct(taskQueue);
        task1->schedule(PriorityTask::LOW);
        EXPECT_TRUE(task1->isScheduled());
    }
    EXPECT_FALSE(task1->isScheduled());
}

TEST_F(PriorityTaskQueueTest, performTaskEmpty)
{
    taskQueue.performTask();
}

TEST_F(PriorityTaskQueueTest, performTask)
{
    const int times = 3;
    for (int i = 0; i < times; ++i) {
        task1.schedule(PriorityTask::LOW);
        task2.schedule(PriorityTask::HIGH);
        taskQueue.performTask();
        EXPECT_TRUE(task1.isScheduled());
        EXPECT_FALSE(task2.isScheduled());
        taskQueue.performTask();
        EXPECT_FALSE(task1.isScheduled());
    }
    EXPECT_EQ(6lu, taskQueue.enqueuedCount);
    EXPECT_EQ(6lu, taskQueue.doneCount);
    EXPECT_EQ(3, task1.count);
    EXPECT_EQ(3, task2.count);
}

namespace {
void runPriorityTaskQueue(PriorityTaskQueue& taskQueue) {
    taskQueue.performTasksUntilHalt();
}
}

TEST_F(PriorityTaskQueueTest, performTasksUntilHalt)
{
    std::thread thread(&runPriorityTaskQueue, std::ref(taskQueue));
    ReschedulingMockTask task(taskQueue);
    task.schedule(PriorityTask::LOW);
    thread.join();
    EXPECT_FALSE(task.isScheduled());
    EXPECT_EQ(2lu, taskQueue.enqueuedCount);
    EXPECT_EQ(2lu, taskQueue.doneCount);
    EXPECT_EQ(2, task.count);
}

TEST_F(PriorityTaskQueueTest, halt)
{
    std::thread thread(&runPriorityTaskQueue, std::ref(taskQueue));
    taskQueue.halt();
    thread.join();
}

TEST_F(PriorityTaskQueueTest, startHalt)
{
    taskQueue.start();
    taskQueue.halt();
}

TEST_F(PriorityTaskQueueTest, schedule)
{
    task1.schedule(PriorityTask::LOW);
    ASSERT_TRUE(task1.isScheduled());
    task2.schedule(PriorityTask::NORMAL);
    ASSERT_TRUE(task2.isScheduled());
    EXPECT_EQ(2u, taskQueue.enqueuedCount);
    EXPECT_EQ(0u, taskQueue.doneCount);

    // Double schedule at same priority has no effect.
    task1.schedule(PriorityTask::LOW);
    ASSERT_EQ(2u, taskQueue.tasks.size());
    EXPECT_EQ(&task2, taskQueue.tasks.top()->task);
    // Double schedule at higher priority re-enqueues the task with the
    // higher priority. Though there are two entries in the priority queue
    // only the earliest one has a valid task pointer. The only queue entry
    // at the lower priority should have been abandoned.
    task1.schedule(PriorityTask::HIGH);
    EXPECT_EQ(1u, taskQueue.doneCount);
    ASSERT_EQ(3u, taskQueue.tasks.size());
    EXPECT_EQ(&task1, taskQueue.tasks.top()->task);

    taskQueue.performTask();
    ASSERT_EQ(2u, taskQueue.tasks.size());
    EXPECT_EQ(3u, taskQueue.enqueuedCount);
    EXPECT_EQ(2u, taskQueue.doneCount);
    EXPECT_EQ(&task2, taskQueue.tasks.top()->task);

    taskQueue.performTask();
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(3u, taskQueue.enqueuedCount);
    EXPECT_EQ(3u, taskQueue.doneCount);
    EXPECT_FALSE(taskQueue.tasks.top()->task);

    taskQueue.performTask();
    EXPECT_EQ(3u, taskQueue.enqueuedCount);
    EXPECT_EQ(3u, taskQueue.doneCount);
}

TEST_F(PriorityTaskQueueTest, scheduleNested)
{
    ReschedulingMockTask task(taskQueue);
    task.schedule(PriorityTask::LOW);
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(&task, taskQueue.tasks.top()->task);
    taskQueue.performTask();
    EXPECT_EQ(1, task.count);
    ASSERT_TRUE(task.isScheduled());
    ASSERT_EQ(1u, taskQueue.tasks.size());
    EXPECT_EQ(&task, taskQueue.tasks.top()->task);
    taskQueue.performTask(); // clear out task queue
    EXPECT_EQ(2, task.count);
}

TEST_F(PriorityTaskQueueTest, deschedule)
{
    task1.schedule(PriorityTask::LOW);
    task1.deschedule();
    EXPECT_FALSE(task1.entry);
    EXPECT_FALSE(taskQueue.tasks.top()->task);
    EXPECT_EQ(1lu, taskQueue.doneCount);

    task1.deschedule();
    EXPECT_FALSE(task1.entry);
    EXPECT_FALSE(taskQueue.tasks.top()->task);
    EXPECT_EQ(1lu, taskQueue.doneCount);

    taskQueue.deschedule(NULL);
    EXPECT_FALSE(taskQueue.tasks.top()->task);
    EXPECT_EQ(1lu, taskQueue.doneCount);
}

TEST_F(PriorityTaskQueueTest, quiesce) {
    task1.schedule(PriorityTask::NORMAL);

    taskQueue.start();
    taskQueue.quiesce();

    EXPECT_EQ(1, task1.count);
}

TEST_F(PriorityTaskQueueTest, getNextTask)
{
    EXPECT_FALSE(taskQueue.getNextTask(false));
    task1.schedule(PriorityTask::LOW);
    EXPECT_EQ(&task1, taskQueue.getNextTask(false));
    task1.schedule(PriorityTask::LOW);
    EXPECT_EQ(&task1, taskQueue.getNextTask(true));
    taskQueue.halt();
    EXPECT_FALSE(taskQueue.getNextTask(true));
}

TEST_F(PriorityTaskQueueTest, getNextTaskPriorities) {
    PriorityTask::Priority priorities[] = {
        PriorityTask::LOW,
        PriorityTask::HIGH,
        PriorityTask::LOW,
        PriorityTask::LOW,
        PriorityTask::NORMAL,
        PriorityTask::HIGH,
        PriorityTask::NORMAL,
        PriorityTask::LOW,
    };

    const size_t count = arrayLength(priorities);
    std::deque<MockTask> tasks;
    for (size_t i = 0; i < count; ++i) {
        tasks.emplace_back(taskQueue);
        tasks.back().schedule(priorities[i]);
    }

    int expectedOrder[] = {1, 5, 4, 6, 0, 2, 3, 7};
    ASSERT_EQ(count, arrayLength(expectedOrder));
    for (size_t i = 0; i < count; ++i) {
        PriorityTask* task = taskQueue.getNextTask(false);
        ASSERT_TRUE(task);
        EXPECT_EQ(&tasks[expectedOrder[i]], task);
    }
}

TEST_F(PriorityTaskQueueTest, entryLessThan) {
    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(NULL, NULL));

    QEntry left(PriorityTask::NORMAL, 0, NULL);
    QEntry right(PriorityTask::NORMAL, 0, NULL);

    EXPECT_TRUE(PriorityTaskQueue::entryLessThan(NULL, &right));
    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(&right, NULL));

    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(&left, &right));
    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(&right, &left));

    right.enqueuedTimestamp = 1;
    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(&left, &right));
    EXPECT_TRUE(PriorityTaskQueue::entryLessThan(&right, &left));

    left.priority = PriorityTask::LOW;
    EXPECT_TRUE(PriorityTaskQueue::entryLessThan(&left, &right));
    EXPECT_FALSE(PriorityTaskQueue::entryLessThan(&right, &left));
}

} // namespace RAMCloud
