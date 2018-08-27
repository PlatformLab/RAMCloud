/* Copyright (c) 2013-2016 Stanford University
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
#include "LogProtector.h"
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
        WorkerTimer::stopWarningMs = 0;
    }

    /**
     * This method waits for the value of workerThreadProgressCount to
     * change: this indicates that the worker thread has woken up since
     * the last time this method was invoked (or since the test started),
     * done all the work that it could, and then either gone back to sleep
     * or exited.
     */
    void waitForWorkerProgress()
    {
        // See "Timing-Dependent Tests" in designNotes.
        for (int i = 0; i < 1000; i++) {
            if (WorkerTimer::workerThreadProgressCount != progressCount) {
                progressCount = WorkerTimer::workerThreadProgressCount;
                return;
            }
            usleep(1000);
        }
        EXPECT_NE(progressCount, WorkerTimer::workerThreadProgressCount);
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
        , logString()
        , restartTime(0)
    { }
    void handleTimerEvent() {
        TEST_LOG("WorkerTimer %s invoked", myName);
        if (sleepMicroseconds != 0) {
            usleep(sleepMicroseconds);
        }
        if (!logString.empty()) {
            for (int i = 0; i < 1000; i++) {
                if (TestLog::get().find(logString) != string::npos) {
                    break;
                }
                usleep(1000);
            }
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

    // If non-empty, then handler won't return until TestLog contains
    // this string (or  until a long time elapses).
    string logString;

    // If non-zero, then handler will restart the timer with this
    // trigger time.
    uint64_t restartTime;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyWorkerTimer);
};

// Helper function that invokes Dispatch::poll in a separate thread.
// Used in a few tests.
static void testPoll(Dispatch* dispatch) {
    dispatch->poll();
}

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
    TestUtil::waitForLog();
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
    TestLog::reset();
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
    // Wait until the TestLog becomes non-empty, sicne the above line does not
    // guarantee that the TestLog will have already been written, the worker
    // making progress is a necessary but not sufficient condition for the
    // thread to have reached the TestLog line.
    TestUtil::waitForLog(NULL);
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
    // Remove then add again if already present.
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
    EXPECT_FALSE(timer.isRunning());
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
TEST_F(WorkerTimerTest, stopInternal_waitForHandlerToFinish) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 0;              // We need to measure real time!
    uint64_t start = Cycles::rdtsc();
    Cycles::mockTscValue = 2000;
    std::thread thread(testPoll, &dispatch);
    // Wait for the handler to start executing (see "Timing-Dependent Tests"
    // in designNotes).
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(timer->handlerRunning);
    timer->stop();
    Cycles::mockTscValue = 0;              // We need to measure real time!
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsed, .01);
    EXPECT_EQ("handleTimerEvent: WorkerTimer timer1 invoked | "
            "stopInternal: waiting for handler", TestLog::get());
    thread.join();
}
TEST_F(WorkerTimerTest, stopInternal_handlerDoesntFinishQuickly) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    timer->logString = "WorkerTimer stalled waiting for handler";
    WorkerTimer::stopWarningMs = 1;
    waitForWorkerProgress();               // Thread startup.
    timer->start(0);
    std::thread thread(testPoll, &dispatch);
    // Wait for the handler to start executing (see "Timing-Dependent Tests"
    // in designNotes).
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(timer->handlerRunning);
    TestLog::reset();
    timer->stop();
    EXPECT_EQ("stopInternal: waiting for handler | "
            "stopInternal: WorkerTimer stalled waiting for handler to "
            "complete; perhaps destructor was invoked from handler? | "
            "stopInternal: waiting for handler", TestLog::get());
    thread.join();
}

TEST_F(WorkerTimerTest, sync) {
    Dispatch dispatch2(false);
    DummyWorkerTimer timer1("timer1", &dispatch);
    DummyWorkerTimer timer2("timer2", &dispatch);
    DummyWorkerTimer timer3("timer3", &dispatch2);
    timer1.start(0);
    timer2.start(0);
    timer3.start(0);
    WorkerTimer::sync();
    EXPECT_TRUE(TestUtil::contains(TestLog::get(), "timer1 invoked"));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(), "timer2 invoked"));
    EXPECT_TRUE(TestUtil::contains(TestLog::get(), "timer3 invoked"));
}

TEST_F(WorkerTimerTest, Manager_constructor) {
    WorkerTimer timer1(&dispatch);
    waitForWorkerProgress();
    EXPECT_EQ(1, WorkerTimer::workerThreadProgressCount);
    EXPECT_EQ(1lu, WorkerTimer::managers.size());
}

TEST_F(WorkerTimerTest, Manager_destructor) {
    Tub<WorkerTimer> timer1, timer2;
    timer1.construct(&dispatch);
    timer2.construct(&dispatch);
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
    TestUtil::waitForLog(NULL);
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

TEST_F(WorkerTimerTest, checkTimers_basics) {
    DummyWorkerTimer timer1("timer1", &dispatch);
    DummyWorkerTimer timer2("timer2", &dispatch);
    DummyWorkerTimer timer3("timer3", &dispatch);
    DummyWorkerTimer timer4("timer4", &dispatch);
    waitForWorkerProgress();               // Thread startup

    // Stagger the start times so they get sorted
    Cycles::mockTscValue = 100;
    timer1.start(1000);
    Cycles::mockTscValue = 101;
    timer2.start(600);
    Cycles::mockTscValue = 102;
    timer3.start(1100);
    Cycles::mockTscValue = 103;
    timer4.start(500);
    WorkerTimer::Lock lock(WorkerTimer::mutex);

    // First test: nothing has triggered.
    Cycles::mockTscValue = 400;
    timer1.manager->stop();
    timer1.manager->checkTimers(lock);
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(4u, timer1.manager->activeTimers.size());
    EXPECT_EQ(500lu, timer1.manager->earliestTriggerTime);
    EXPECT_TRUE(timer1.manager->isRunning());

    // Second test: timer2 and timer4 have both triggered; timer2
    // should run first.
    Cycles::mockTscValue = 700;
    timer1.manager->stop();
    timer1.manager->checkTimers(lock);
    EXPECT_EQ("handleTimerEvent: WorkerTimer timer2 invoked | "
            "handleTimerEvent: WorkerTimer timer4 invoked", TestLog::get());
    EXPECT_EQ(2u, timer1.manager->activeTimers.size());
    EXPECT_EQ(1000lu, timer1.manager->earliestTriggerTime);
    EXPECT_FALSE(timer2.active);
    EXPECT_TRUE(timer1.manager->isRunning());

    // Third test: run remaining timers.
    Cycles::mockTscValue = 1200;
    TestLog::reset();
    timer1.manager->stop();
    timer1.manager->checkTimers(lock);
    EXPECT_EQ("handleTimerEvent: WorkerTimer timer1 invoked | "
            "handleTimerEvent: WorkerTimer timer3 invoked", TestLog::get());
    EXPECT_EQ(0u, timer1.manager->activeTimers.size());
    EXPECT_EQ(~0lu, timer1.manager->earliestTriggerTime);
    EXPECT_FALSE(timer1.manager->isRunning());
}

TEST_F(WorkerTimerTest, checkTimers_setHandlerRunningAndSignalFinished) {
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 0;              // We need to measure real time!
    uint64_t start = Cycles::rdtsc();
    Cycles::mockTscValue = 2000;
    std::thread thread(testPoll, &dispatch);

    // Wait for the handler to start executing (see "Timing-Dependent Tests"
    // in designNotes).
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(timer->handlerRunning);
    timer.destroy();
    Cycles::mockTscValue = 0;              // We need to measure real time!
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsed, .01);
    thread.join();
}

TEST_F(WorkerTimerTest, checkTimers_logProtector) {
    LogProtector::currentSystemEpoch = 24;
    Tub<DummyWorkerTimer> timer;
    timer.construct("timer1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 0;              // We need to measure real time!
    uint64_t start = Cycles::rdtsc();
    Cycles::mockTscValue = 2000;
    EXPECT_EQ(~0UL, LogProtector::getEarliestOutstandingEpoch(~0));
    std::thread thread(testPoll, &dispatch);

    // Wait for the handler to start executing (see "Timing-Dependent Tests"
    // in designNotes).
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(timer->handlerRunning);
    EXPECT_EQ(24U, timer->manager->logProtectorActivity.epoch);
    EXPECT_EQ(~0, timer->manager->logProtectorActivity.activityMask);
    EXPECT_EQ(24U, LogProtector::getEarliestOutstandingEpoch(~0));
    timer.destroy();
    EXPECT_EQ(~0UL, LogProtector::getEarliestOutstandingEpoch(~0));
    Cycles::mockTscValue = 0;              // We need to measure real time!
    double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
    EXPECT_GE(elapsed, .01);
    thread.join();
}

TEST_F(WorkerTimerTest, workerThreadMain_exit) {
    Tub<DummyWorkerTimer> timer1;
    timer1.construct("t1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer1.destroy();
    waitForWorkerProgress();
    TestUtil::waitForLog(NULL);
    EXPECT_EQ("workerThreadMain: exiting", TestLog::get());
}

TEST_F(WorkerTimerTest, workerThreadMain_invokeHandler) {
    DummyWorkerTimer timer1("t1", &dispatch);
    waitForWorkerProgress();               // Thread startup.
    timer1.start(1000);
    Cycles::mockTscValue = 2000;
    dispatch.poll();
    waitForWorkerProgress();
    TestUtil::waitForLog(NULL);
    EXPECT_EQ("handleTimerEvent: WorkerTimer t1 invoked", TestLog::get());
}

}  // namespace RAMCloud
