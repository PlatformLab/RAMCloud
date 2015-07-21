/* Copyright (c) 2011-2014 Stanford University
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
#include "SpinLock.h"

namespace RAMCloud {

TEST(SpinLockTest, basics) {
    SpinLock lock("Lord Lockington");
    EXPECT_EQ("Lord Lockington", lock.name);
    EXPECT_EQ(0U, lock.acquisitions);
    EXPECT_EQ(0U, lock.contendedAcquisitions);
    EXPECT_EQ(0U, lock.contendedTicks);

    EXPECT_EQ(0, lock.mutex.load());
    lock.lock();
    EXPECT_EQ(1, lock.mutex.load());
    EXPECT_FALSE(lock.try_lock());
    EXPECT_EQ(1, lock.mutex.load());
    lock.unlock();
    EXPECT_EQ(0, lock.mutex.load());
    EXPECT_TRUE(lock.try_lock());
    EXPECT_EQ(1, lock.mutex.load());

    EXPECT_EQ(1U, lock.acquisitions);
    EXPECT_EQ(0U, lock.contendedAcquisitions);
    EXPECT_EQ(0U, lock.contendedTicks);
}

TEST(SpinLockTest, constructorDestructor_maintainLockTable) {
    Tub<SpinLock> lock1, lock2, lock3;
    int startCount = SpinLock::numLocks();
    lock1.construct("lock1");
    EXPECT_EQ(1, SpinLock::numLocks() - startCount);
    lock2.construct("lock2");
    lock3.construct("lock3");
    EXPECT_EQ(3, SpinLock::numLocks() - startCount);
    lock2.destroy();
    EXPECT_EQ(2, SpinLock::numLocks() - startCount);
    lock1.destroy();
    EXPECT_EQ(1, SpinLock::numLocks() - startCount);
    lock3.destroy();
    EXPECT_EQ(0, SpinLock::numLocks() - startCount);
}

// Helper function that runs in a separate thread for the following test.
static void blockingChild(SpinLock* lock, volatile bool* done)
{
    lock->lock();
    *done = true;
}

TEST(SpinLockTest, threadBlocks) {
    SpinLock lock;
    volatile bool done;
    lock.lock();

    // Make sure that the child thread waits for the lock to become
    // available.
    std::thread thread(blockingChild, &lock, &done);
    usleep(1000);
    EXPECT_FALSE(done);

    // Make sure that the child thread eventually completes once we
    // release the lock.
    // See "Timing-Dependent Tests" in designNotes.
    lock.unlock();
    for (int i = 0; !done && i < 1000; i++) {
        usleep(100);
    }
    EXPECT_TRUE(done);
    thread.join();
}

// Helper function that runs in a separate thread for the following test.
static void contentionChild(SpinLock* lock, volatile bool* ready,
                            volatile int* value)
{
    while (!*ready) {
        // Wait for all of the threads to get started to ensure that
        // there is contention for the lock.
    }
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        lock->lock();
        (*value)++;
        lock->unlock();
    }
}

TEST(SpinLockTest, contention) {
    // Create several threads contenting for a SpinLock to control access
    //  to acritical section that increments a variable, and make sure that
    // none of the increments get lost.
    SpinLock lock;
    volatile int value = 0;
    volatile bool ready = false;
    // Start child threads.
    std::thread thread1(contentionChild, &lock, &ready, &value);
    std::thread thread2(contentionChild, &lock, &ready, &value);
    usleep(1000);
    ready = true;
    contentionChild(&lock, &ready, &value);
    thread1.join();
    thread2.join();
    EXPECT_EQ(3000, value);
    EXPECT_EQ(3000U, lock.acquisitions);
    EXPECT_GT(lock.contendedAcquisitions, 0U);
    EXPECT_LT(lock.contendedAcquisitions, lock.acquisitions);
    EXPECT_GT(lock.contendedTicks, 0U);
}

TEST(SpinLockTest, setName) {
    SpinLock lock;
    EXPECT_EQ("unnamed", lock.name);
    lock.setName("John Paul Jones");
    EXPECT_EQ("John Paul Jones", lock.name);
}

TEST(SpinLockTest, getStatistics) {
    SpinLock lock1("Jimmy Page");
    SpinLock lock2("John Bonham");
    SpinLock lock3("Robert Plant");

    lock1.acquisitions = 2;
    lock1.contendedAcquisitions = 1;
    lock1.contendedTicks = Cycles::fromNanoseconds(10000);

    lock2.acquisitions = 51;
    lock2.contendedAcquisitions = 12;
    lock2.contendedTicks = Cycles::fromNanoseconds(5000);

    ProtoBuf::SpinLockStatistics stats;
    SpinLock::getStatistics(&stats);

    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "locks { name: \"Jimmy Page\" acquisitions: 2 "
        "contended_acquisitions: 1 contended_nsec: 10000 } ",
        stats.ShortDebugString()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "locks { name: \"John Bonham\" acquisitions: 51 "
        "contended_acquisitions: 12 contended_nsec: 5000 } ",
        stats.ShortDebugString()));
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "locks { name: \"Robert Plant\" acquisitions: 0 "
        "contended_acquisitions: 0 contended_nsec: 0 }",
        stats.ShortDebugString()));
}

}  // namespace RAMCloud
