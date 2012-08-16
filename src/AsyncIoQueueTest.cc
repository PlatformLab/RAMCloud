/* Copyright (c) 2012 Stanford University
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
#include "AsyncIoQueue.h"
#include "ShortMacros.h"
#include "StringUtil.h"

namespace RAMCloud {

namespace {
struct MockSyscall : public Syscall {
    MockSyscall()
        : Syscall()
        , nextReturn()
    {}
    ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
        TEST_LOG("%d %p %lu %lu", fd, buf, count, offset);
        if (nextReturn) {
            auto r = nextReturn;
            nextReturn = 0;
            return r;
        }
        return count;
    }
    ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
        TEST_LOG("%d %p %lu %lu", fd, buf, count, offset);
        if (nextReturn) {
            auto r = nextReturn;
            nextReturn = 0;
            return r;
        }
        return count;
    }
    ssize_t nextReturn;
};
}

struct AsyncIoQueueTest : public ::testing::Test {
    AsyncIoQueue aioQueue;
    MockSyscall sys;
    SyscallGuard sysGuard;
    void* buffer;

    AsyncIoQueueTest()
        : aioQueue()
        , sys()
        , sysGuard(&AsyncIoQueue::sys, &sys)
        , buffer(reinterpret_cast<void*>(0x12345))
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
    }

    DISALLOW_COPY_AND_ASSIGN(AsyncIoQueueTest);
};

TEST_F(AsyncIoQueueTest, main) {
    AsyncIo ios[] = {
        {AsyncIo::READ, AsyncIo::LOW, 0, buffer, 10, 20},
        {AsyncIo::WRITE, AsyncIo::HIGH, 1, buffer, 11, 21},
    };
    for (size_t i = 0; i < arrayLength(ios); ++i)
        aioQueue.enqueueIo(&ios[i]);
    TestLog::Enable _;
    aioQueue.start();
    while (true) {
        AsyncIoQueue::Lock(aioQueue.mutex);
        if (aioQueue.queue.empty())
            break;
    }
    aioQueue.halt();
    EXPECT_EQ("pwrite: 1 0x12345 11 21 | pread: 0 0x12345 10 20",
              TestLog::get());
}

TEST_F(AsyncIoQueueTest, enqueueIo) {
    AsyncIo ios[] = {
        {AsyncIo::READ, AsyncIo::LOW, 0, buffer, 10, 20},
        {AsyncIo::WRITE, AsyncIo::HIGH, 1, buffer, 11, 21},
    };
    for (size_t i = 0; i < arrayLength(ios); ++i)
        aioQueue.enqueueIo(&ios[i]);
    EXPECT_EQ(2lu, aioQueue.queue.size());
    EXPECT_EQ(1lu, ios[0].enqueuedTimestamp);
    EXPECT_EQ(2lu, ios[1].enqueuedTimestamp);
    EXPECT_EQ(&ios[1], aioQueue.queue.top());
    aioQueue.queue.pop();
    EXPECT_EQ(&ios[0], aioQueue.queue.top());
    for (size_t i = 0; i < arrayLength(ios); ++i)
        ios[i].done = true;

    EXPECT_EQ(2lu, aioQueue.enqueuedCount);
}

TEST_F(AsyncIoQueueTest, quiesce) {
    AsyncIo io{AsyncIo::READ, AsyncIo::LOW, 0, buffer, 10, 20};
    aioQueue.enqueueIo(&io);

    aioQueue.start();
    aioQueue.quiesce();

    EXPECT_EQ("", TestLog::get());
}

TEST_F(AsyncIoQueueTest, popNextIo) {
    AsyncIo ios[] = {
        {AsyncIo::READ, AsyncIo::LOW, 0, buffer, 10, 20},
        {AsyncIo::READ, AsyncIo::HIGH, 1, buffer, 11, 21},
        {AsyncIo::READ, AsyncIo::LOW, 2, buffer, 12, 22},
        {AsyncIo::READ, AsyncIo::LOW, 3, buffer, 13, 23},
        {AsyncIo::READ, AsyncIo::NORMAL, 4, buffer, 14, 24},
        {AsyncIo::READ, AsyncIo::HIGH, 5, buffer, 15, 25},
        {AsyncIo::READ, AsyncIo::NORMAL, 6, buffer, 16, 26},
        {AsyncIo::READ, AsyncIo::LOW, 7, buffer, 17, 27},
    };

    aioQueue.running = true;
    for (size_t i = 0; i < arrayLength(ios); ++i)
        aioQueue.enqueueIo(&ios[i]);
    
    int expectedFds[] = {1, 5, 4, 6, 0, 2, 3, 7};
    ASSERT_EQ(arrayLength(ios), arrayLength(expectedFds));
    for (size_t i = 0; i < arrayLength(ios); ++i) {
        AsyncIo* io = aioQueue.popNextIo();
        ASSERT_TRUE(io);
        EXPECT_EQ(expectedFds[i], io->fd);
    }

    aioQueue.running = false;
    aioQueue.enqueueIo(&ios[0]);
    EXPECT_FALSE(aioQueue.popNextIo());

    for (size_t i = 0; i < arrayLength(ios); ++i)
        ios[i].done = true;
}

TEST_F(AsyncIoQueueTest, performIo) {
    Tub<AsyncIo> io;
    io.construct(AsyncIo::READ, AsyncIo::LOW, 3, buffer, 4, 5);
    TestLog::Enable _;
    aioQueue.performIo(io.get());
    EXPECT_TRUE(io->isDone());
    EXPECT_EQ("pread: 3 0x12345 4 5", TestLog::get());

    io.construct(AsyncIo::WRITE, AsyncIo::NORMAL, 6, buffer, 7, 8);
    TestLog::reset();
    aioQueue.performIo(io.get());
    EXPECT_TRUE(io->isDone());
    EXPECT_EQ("pwrite: 6 0x12345 7 8", TestLog::get());

    EXPECT_EQ(2lu, aioQueue.doneCount);

    io.construct(AsyncIo::WRITE, AsyncIo::HIGH, 9, buffer, 10, 11);
    sys.nextReturn = -1;
    TestLog::reset();
    EXPECT_THROW(aioQueue.performIo(io.get()),
                 FatalError);
    EXPECT_FALSE(io->isDone());
    EXPECT_TRUE(
        StringUtil::startsWith(TestLog::get(),
            "pwrite: 9 0x12345 10 11 | "
            "performIo: Failure performing asynchronous IO:"));
    io->done = true; // Prevent IO from blocking.
}

TEST_F(AsyncIoQueueTest, asyncIoPointerLess) {
    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(NULL, NULL));

    AsyncIo left(AsyncIo::READ, AsyncIo::NORMAL, 3, buffer, 4, 5);
    AsyncIo right(AsyncIo::READ, AsyncIo::NORMAL, 3, buffer, 4, 5);

    EXPECT_TRUE(AsyncIoQueue::asyncIoPointerLess(NULL, &right));
    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(&right, NULL));

    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(&left, &right));
    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(&right, &left));

    right.enqueuedTimestamp = 1;
    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(&left, &right));
    EXPECT_TRUE(AsyncIoQueue::asyncIoPointerLess(&right, &left));

    left.priority = AsyncIo::LOW;
    EXPECT_TRUE(AsyncIoQueue::asyncIoPointerLess(&left, &right));
    EXPECT_FALSE(AsyncIoQueue::asyncIoPointerLess(&right, &left));

    left.done = true;
    right.done = true;
}

} // namespace RAMCloud
