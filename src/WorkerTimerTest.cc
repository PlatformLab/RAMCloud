/* Copyright (c) 2013 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "Logger.h"
#include "ShortMacros.h"
#include "WorkerTimer.h"

namespace RAMCloud {
class WorkerTimerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Dispatch dispatch;
    int progressCount;

    WorkerTimerTest()
        : logEnabler()
        , dispatch(false)
        , progressCount(0)
    {
        WorkerTimer::workerThreadProgressCount = 0;
        Cycles::mockTscValue = 100;
    }

    ~WorkerTimerTest()
    {
        Cycles::mockTscValue = 0;
    }

    /**
     * This method waits for the value of workerThreadProgressCount to
     * change: this indicates that the worker thread is woken up since
     * the last time this method was invoked (or since the test started),
     * done all the work that it could, and then either gone back to sleep
     * or exited.
     */
    void waitForWorkerProgress()
    {
        for (int i = 0; i < 1000; i++) {
            if (WorkerTimer::workerThreadProgressCount != progressCount) {
                progressCount = WorkerTimer::workerThreadProgressCount;
                return;
            }
            usleep(1000);
        }
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(WorkerTimerTest);
};


// The following class is used for testing: it generates a log message
// identifying this timer whenever it is invoked.
class DummyWorkerTimer : public WorkerTimer {
  public:
    explicit DummyWorkerTimer(const char *name, Dispatch* dispatch)
        : WorkerTimer(dispatch)
        , myName(name)
        , sleepMicroseconds(0)
        , restartTime(0)
    { }
    void handleTimerEvent() {
        TEST_LOG("WorkerTimer %s invoked", myName);
        if (sleepMicroseconds != 0) {
            usleep(sleepMicroseconds);
        }
        if (restartTime != 0) {
            start(restartTime);
            restartTime = 0;
            if (isRunning()) {
                TEST_LOG("restarted");
            }
        }
    }
    const char *myName;

    // If non-zero, then handler will delayed for this long before
    // returning.
    int sleepMicroseconds;

    // If non-zero, then handler will restart the timer with this
    // trigger time.
    uint64_t restartTime;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyWorkerTimer);
};

TEST_F(WorkerTimerTest, sanityCheck) {
    DummyWorkerTimer timer("xyzzy", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer.start(10000);
    Cycles::mockTscValue = 9000;
    dispatch.poll();
    EXPECT_EQ("", TestLog::get());
    Cycles::mockTscValue = 10001;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer xyzzy invoked", TestLog::get());
}

TEST_F(WorkerTimerTest, constructor) {
    WorkerTimer timer(&dispatch);
    EXPECT_EQ(1u, WorkerTimer::managers.size());
    EXPECT_EQ(1, timer.manager->timerCount);
}

TEST_F(WorkerTimerTest, destructor) {
    Tub<WorkerTimer> timer1, timer2;
    timer1.construct(&dispatch);
    timer2.construct(&dispatch);
    EXPECT_EQ(2, timer2->manager->timerCount);
    timer1.destroy();
    // Wait a while to see if the worker thread does anything (it shouldn't).
    usleep(10000);
    EXPECT_EQ(1, timer2->manager->timerCount);
    EXPECT_EQ("", TestLog::get());
    timer2.destroy();
    waitForWorkerProgress();
    EXPECT_EQ(0u, WorkerTimer::managers.size());
    EXPECT_EQ("workerThreadMain: exiting", TestLog::get());
}
TEST_F(WorkerTimerTest, destructor_stopTimer) {
    WorkerTimer timer1(&dispatch);
    Tub<WorkerTimer> timer2;
    timer2.construct(&dispatch);
    timer2->start(1000);
    timer2.destroy();
    EXPECT_EQ(1, timer1.manager->timerCount);
    EXPECT_EQ(0u, timer1.manager->activeTimers.size());
}
// Helper function for the following test and a few others: used to invoke
// Dispatch::poll in a separate thread.
static void testPoll(Dispatch* dispatch) {
    dispatch->poll();
}
TEST_F(WorkerTimerTest, destructor_waitForHandlerToFinish) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 0;              // We need to measure real time!
    uint64_t start = Cycles::rdtsc();
    Cycles::mockTscValue = 2000;
    std::thread thread(testPoll, &dispatch);
    usleep(5000);
    timer.destroy();
    Cycles::mockTscValue = 0;              // We need to measure real time!
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsed, .01);
    thread.join();
}

TEST_F(WorkerTimerTest, start) {
    WorkerTimer timer(&dispatch);
    EXPECT_FALSE(timer.isRunning());
    timer.start(1000);
    EXPECT_TRUE(timer.isRunning());
    timer.stop();
    EXPECT_FALSE(timer.isRunning());
}
TEST_F(WorkerTimerTest, start_dontStartOnceDestructorInvoked) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("t1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    Cycles::mockTscValue = 2000;

    // First, make sure that the timer can successfully restart itself.
    timer->restartTime = 1000;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_TRUE(TestUtil::matchesPosixRegex("restarted", TestLog::get()));

    // Now, make sure it doesn't restart itself if the destructor
    // has been invoked.
    TestLog::reset();               // Thread startup.
    timer->start(1500);
    timer->restartTime = 1500;
    timer->sleepMicroseconds = 10000;
    std::thread thread(testPoll, &dispatch);
    usleep(5000);
    timer.destroy();
    EXPECT_TRUE(TestUtil::doesNotMatchPosixRegex("restarted", TestLog::get()));
    thread.join();
}

TEST_F(WorkerTimerTest, start_addToActiveList) {
    WorkerTimer timer(&dispatch);
    EXPECT_EQ(0u, timer.manager->activeTimers.size());
    timer.start(1000);
    EXPECT_EQ(1u, timer.manager->activeTimers.size());
    // Don't add again if already present.
    timer.start(2000);
    EXPECT_EQ(1u, timer.manager->activeTimers.size());
}
TEST_F(WorkerTimerTest, start_startDispatchTimer) {
    WorkerTimer timer(&dispatch);
    timer.manager->earliestTriggerTime = 1000;
    timer.start(1000);
    EXPECT_FALSE(timer.manager->isRunning());
    timer.start(999);
    EXPECT_TRUE(timer.manager->isRunning());
    EXPECT_EQ(999u, timer.manager->earliestTriggerTime);
}

TEST_F(WorkerTimerTest, stop) {
    WorkerTimer timer(&dispatch);
    timer.start(1000);
    timer.stop();
    EXPECT_EQ("stopInternal: stopping WorkerTimer", TestLog::get());
    TestLog::reset();
    timer.stop();
    EXPECT_EQ("", TestLog::get());
}
TEST_F(WorkerTimerTest, stopInternal) {
    WorkerTimer timer1(&dispatch);
    WorkerTimer timer2(&dispatch);
    timer1.start(1000);
    timer2.start(2000);
    EXPECT_EQ(2u, timer1.manager->activeTimers.size());
    EXPECT_TRUE(timer1.manager->isRunning());
    timer1.stop();
    EXPECT_EQ(1u, timer1.manager->activeTimers.size());
    EXPECT_TRUE(timer1.manager->isRunning());
    timer2.stop();
    EXPECT_EQ(0u, timer1.manager->activeTimers.size());
    EXPECT_FALSE(timer1.manager->isRunning());
}

TEST_F(WorkerTimerTest, Manager_constructor) {
    WorkerTimer timer1(&dispatch);
    waitForWorkerProgress();
    EXPECT_EQ(1, WorkerTimer::workerThreadProgressCount);

    // Make sure only one worker thread is started, no matter
    // how many managers.
    Dispatch dispatch2(false);
    WorkerTimer timer2(&dispatch2);
    // Wait a while and see if a new worker starts up (it better not).
    usleep(10000);
    EXPECT_EQ(1, WorkerTimer::workerThreadProgressCount);
    EXPECT_EQ(2u, WorkerTimer::managers.size());
}

TEST_F(WorkerTimerTest, Manager_destructor) {
    Tub<WorkerTimer> timer1, timer2;
    Dispatch dispatch2(false);
    timer1.construct(&dispatch);
    timer2.construct(&dispatch2);
    timer2.destroy();
    EXPECT_EQ(1u, WorkerTimer::managers.size());
    EXPECT_EQ("", TestLog::get());
    timer1.destroy();
    EXPECT_EQ(0u, WorkerTimer::managers.size());
    EXPECT_EQ("workerThreadMain: exiting", TestLog::get());
}

TEST_F(WorkerTimerTest, Manager_handleTimerEvent) {
    DummyWorkerTimer timer("xyzzy", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer.start(10000);
    Cycles::mockTscValue = 20000;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer xyzzy invoked", TestLog::get());
}

TEST_F(WorkerTimerTest, findManager) {
    WorkerTimer timer1(&dispatch);
    EXPECT_EQ(1u, WorkerTimer::managers.size());
    Dispatch dispatch2(false);
    Dispatch dispatch3(false);
    WorkerTimer timer2(&dispatch2);
    EXPECT_EQ(2u, WorkerTimer::managers.size());
    WorkerTimer timer3(&dispatch);
    EXPECT_EQ(2u, WorkerTimer::managers.size());
    WorkerTimer timer4(&dispatch);
    EXPECT_EQ(2u, WorkerTimer::managers.size());
    WorkerTimer timer5(&dispatch3);
    EXPECT_EQ(3u, WorkerTimer::managers.size());
}

TEST_F(WorkerTimerTest, findTriggeredTimer) {
    WorkerTimer timer1(&dispatch);
    Dispatch dispatch2(false);
    WorkerTimer timer2(&dispatch2);
    WorkerTimer timer3(&dispatch2);
    WorkerTimer timer4(&dispatch2);
    timer1.start(1000);
    timer2.start(600);
    timer3.start(500);
    timer4.start(1100);

    // First test: nothing has triggered.
    WorkerTimer::Lock lock(WorkerTimer::mutex);
    Cycles::mockTscValue = 400;
    WorkerTimer* triggered = WorkerTimer::findTriggeredTimer(lock);
    EXPECT_TRUE(triggered == NULL);

    // Second test: timer2 and timer3 have both triggered, but timer2
    // will be discovered first.
    Cycles::mockTscValue = 700;
    triggered = WorkerTimer::findTriggeredTimer(lock);
    EXPECT_TRUE(triggered == &timer2);
    EXPECT_EQ(2u, timer2.manager->activeTimers.size());
    EXPECT_EQ(500lu, timer2.manager->earliestTriggerTime);

    // Third test: all timers have triggered, but timer1 will be
    // discovered first.
    Cycles::mockTscValue = 1500;
    triggered = WorkerTimer::findTriggeredTimer(lock);
    EXPECT_TRUE(triggered == &timer1);
    EXPECT_EQ(0u, timer1.manager->activeTimers.size());
    EXPECT_EQ(~0lu, timer1.manager->earliestTriggerTime);
}
TEST_F(WorkerTimerTest, findTriggeredTimer_restartDispatchTimer) {
    DummyWorkerTimer timer1("t1", &dispatch);
    DummyWorkerTimer timer2("t2", &dispatch);
    DummyWorkerTimer timer3("t3", &dispatch);
    timer1.start(500);
    timer2.start(600);
    timer3.start(700);
    waitForWorkerProgress();               // Thread startup.

    Cycles::mockTscValue = 500;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer t1 invoked", TestLog::get());
    EXPECT_TRUE(timer1.manager->isRunning());

    TestLog::reset();
    Cycles::mockTscValue = 600;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer t2 invoked", TestLog::get());
    EXPECT_TRUE(timer1.manager->isRunning());

    TestLog::reset();
    Cycles::mockTscValue = 700;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer t3 invoked", TestLog::get());
    EXPECT_FALSE(timer1.manager->isRunning());
}

TEST_F(WorkerTimerTest, workerThreadMain_exit) {
    Tub<DummyWorkerTimer> timer1;
    timer1.construct("t1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer1.destroy();
    waitForWorkerProgress();
    EXPECT_EQ("workerThreadMain: exiting", TestLog::get());
}
TEST_F(WorkerTimerTest, workerThreadMain_invokeHandler) {
    DummyWorkerTimer timer1("t1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer1.start(1000);
    Cycles::mockTscValue = 2000;
    dispatch.poll();
    waitForWorkerProgress();
    EXPECT_EQ("handleTimerEvent: WorkerTimer t1 invoked", TestLog::get());
}
TEST_F(WorkerTimerTest, workerThreadMain_setHandlerRunningAndSignalFinished) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 0;              // We need to measure real time!
    uint64_t start = Cycles::rdtsc();
    Cycles::mockTscValue = 2000;
    std::thread thread(testPoll, &dispatch);
    usleep(5000);
    timer.destroy();
    Cycles::mockTscValue = 0;              // We need to measure real time!
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsed, .01);
    thread.join();
}


}  // namespace RAMCloud
