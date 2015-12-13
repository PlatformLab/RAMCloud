/* Copyright (c) 2011-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
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
#include "Cycles.h"
#include "Dispatch.h"
#include "MockSyscall.h"
#include "TransportManager.h"
#include "WorkerManager.h"

namespace RAMCloud {
static string *localLog;

// The following class is used for testing: it generates a log message
// identifying this poller whenever it is invoked.
class DummyPoller : public Dispatch::Poller {
  public:
    DummyPoller(const char *name, int callsUntilTrue, Dispatch *dispatch)
        : Dispatch::Poller(dispatch, "DummyPoller"), myName(name),
        callsUntilTrue(callsUntilTrue), pollersToDelete(),
        returnValue(1) { }
    int poll() {
        bool deleteThis = false;
        if (localLog->length() != 0) {
            localLog->append("; ");
        }
        localLog->append(format("poller %s invoked", myName));
        for (uint32_t i = 0; i < pollersToDelete.size(); i++) {
            if (pollersToDelete[i] != this) {
                delete pollersToDelete[i];
            } else {
                // We're supposed to delete this object, which is fine except
                // we can't do it now because we're about to access more fields
                // in it.  Wait until the end of the method.
                deleteThis = true;
            }
        }
        pollersToDelete.clear();
        if (callsUntilTrue > 0) {
            callsUntilTrue--;
        }
        if (deleteThis) {
            delete this;
        }
        return returnValue;
    }
    // Arrange to delete a given poller the next time this poller is
    // invoked (used for testing reentrancy).
    void deleteWhenInvoked(Poller* poller) {
        pollersToDelete.push_back(poller);
    }
    const char *myName;
    int callsUntilTrue;
    std::vector<Poller*> pollersToDelete;
    int returnValue;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyPoller);
};

// The following class is used for testing: it increments a counter each
// time it is invoked.
class CountPoller : public Dispatch::Poller {
  public:
    explicit CountPoller(Dispatch* dispatch)
            : Dispatch::Poller(dispatch, "CountPoller"), count(0) { }
    int poll() {
        count++;
        return 1;
    }
    volatile int count;
  private:
    DISALLOW_COPY_AND_ASSIGN(CountPoller);
};

// The following class is used for testing: it generates a log message
// identifying this timer whenever it is invoked.
class DummyTimer : public Dispatch::Timer {
  public:
    explicit DummyTimer(const char *name, Dispatch* dispatch)
            : Dispatch::Timer(dispatch), myName(name), timersToDelete() { }
    DummyTimer(const char *name, uint64_t cycles, Dispatch* dispatch)
            : Dispatch::Timer(dispatch, cycles), myName(name),
            timersToDelete() { }
    void handleTimerEvent() {
        bool deleteThis = false;
        if (localLog->length() != 0) {
            localLog->append("; ");
        }
        localLog->append(format("timer %s invoked", myName));
        for (uint32_t i = 0; i < timersToDelete.size(); i++) {
            if (timersToDelete[i] != this) {
                delete timersToDelete[i];
            } else {
                // We're supposed to delete this object, which is fine except
                // we can't do it now because we're about to access more fields
                // in it.  Wait until the end of the method.
                deleteThis = true;
            }
        }
        timersToDelete.clear();
        if (deleteThis) {
            delete this;
        }
    }
    // Arrange to delete a given timer the next time this timer is
    // invoked (used for testing reentrancy).
    void deleteWhenInvoked(Timer* timer) {
        timersToDelete.push_back(timer);
    }
    const char *myName;
    std::vector<Timer*> timersToDelete;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyTimer);
};

// The following class is used for testing Files.  It logs when it
// is triggered and (optionally) reads incoming data.
class DummyFile : public Dispatch::File {
  public:
    explicit DummyFile(const char *name, bool readData, int fd,
            int events, Dispatch* dispatch)
            : Dispatch::File(dispatch, fd, events), myName(name),
            readData(readData), lastInvocationId(0), deleteThis(false),
            eventInfo() { }
    explicit DummyFile(const char *name, bool readData, int fd,
            Dispatch* dispatch)
            : Dispatch::File(dispatch, fd, 0),
            myName(name), readData(readData), lastInvocationId(0),
            deleteThis(false), eventInfo() { }
    void handleFileEvent(int events) {
        char buffer[11];
        if (localLog->length() != 0) {
            localLog->append("; ");
        }
        eventInfo.clear();
        if (events & Dispatch::FileEvent::READABLE) {
            eventInfo.append("READABLE");
        }
        if (events & Dispatch::FileEvent::WRITABLE) {
            if (eventInfo.size() > 0) {
                eventInfo.append("|");
            }
            eventInfo.append("WRITABLE");
        }
        if (readData) {
            size_t count = read(fd, buffer, sizeof(buffer) - 1);
            buffer[count] = 0;
            localLog->append(format("file %s invoked, read '%s'", myName,
                    buffer));
        } else {
            localLog->append(format("file %s invoked", myName));
        }
        lastInvocationId = invocationId;
        if (deleteThis) {
            close(fd);
            delete this;
        }
    }
    const char *myName;
    bool readData;
    int lastInvocationId;
    bool deleteThis;
    string eventInfo;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyFile);
};

class DispatchTest : public ::testing::Test {
  public:
    Dispatch dispatch;
    string exceptionMessage;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable* logEnabler;
    int pipeFds[2];

    DispatchTest()
        : dispatch(false)
        , exceptionMessage()
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler(NULL)
    {
        exceptionMessage = "no exception";
        if (!localLog) {
            localLog = new string;
        }
        localLog->clear();
        dispatch.currentTime = 100;
        sys = new MockSyscall();
        savedSyscall = Dispatch::sys;
        Dispatch::sys = sys;
        logEnabler = new TestLog::Enable();
        EXPECT_EQ(0, pipe(pipeFds));
    }

    ~DispatchTest() {
        close(pipeFds[0]);
        close(pipeFds[1]);
        Dispatch::sys = savedSyscall;
        delete sys;
        sys = NULL;
        Cycles::mockTscValue = 0;
    }

    // Calls dispatch.poll repeatedly until either a log entry is
    // generated or a given amount of time has elapsed.  Return the result
    // from the last call to dispatch.poll.
    int waitForPollSuccess(double timeoutSeconds) {
        uint64_t start = Cycles::rdtsc();
        int result = 99;
        while (localLog->size() == 0) {
            usleep(100);
            result = dispatch.poll();
            if (Cycles::toSeconds(Cycles::rdtsc() - start) > timeoutSeconds)
                return result;
        }
        return result;
    }

    // Waits for a file to become ready, but gives up after a given
    // elapsed time.
    void waitForReadyFd(double timeoutSeconds) {
        uint64_t start = Cycles::rdtsc();
        while (dispatch.readyFd < 0) {
            usleep(1000);
            if (Cycles::toSeconds(Cycles::rdtsc() - start) > timeoutSeconds)
                return;
        }
    }

    // Utility method: create a DummyFile for a particular file descriptor
    // and see if it gets invoked.
    string checkReady(int fd, int events, const char* desired) {
        // Flush any stale events.
        for (int i = 0; i < 100; i++) dispatch.poll();
        DummyFile f("f1", false, fd, events, &dispatch);

        // See "Timing-Dependent Tests" in designNotes.
        string result;
        for (int i = 0; i < 1000; i++) {
            usleep(1000);
            dispatch.poll();
            result = f.eventInfo;
            if (result.compare(desired) == 0) {
                break;
            }
        }
        return result;
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(DispatchTest);
};

TEST_F(DispatchTest, constructor) {
    EXPECT_TRUE(dispatch.isDispatchThread());
}

TEST_F(DispatchTest, destructor) {
    Dispatch *td = new Dispatch(false);
    DummyPoller* p1 = new DummyPoller("p1", 0, td);
    DummyPoller* p2 = new DummyPoller("p2", 0, td);
    DummyTimer* t1 = new DummyTimer("t1", 100, td);
    DummyTimer* t2 = new DummyTimer("t2", 200, td);
    int fds[2];
    EXPECT_EQ(0, pipe(fds));
    DummyFile* f1 = new DummyFile("f1", false, fds[0],
            Dispatch::FileEvent::READABLE, td);
    EXPECT_EQ("", TestLog::get());
    delete td;
    td = NULL;
    EXPECT_EQ("epollThreadMain: done", TestLog::get());
    close(fds[0]);
    close(fds[1]);
    EXPECT_EQ(-1, p1->slot);
    EXPECT_TRUE(p1->owner == NULL);
    EXPECT_EQ(-1, p2->slot);
    EXPECT_TRUE(p2->owner == NULL);
    EXPECT_EQ(-1, t1->slot);
    EXPECT_TRUE(t1->owner == NULL);
    EXPECT_EQ(-1, t2->slot);
    EXPECT_TRUE(t2->owner == NULL);
    EXPECT_EQ(0, f1->active);
    EXPECT_EQ(0, f1->events);
    delete p1;
    delete p2;
    delete t1;
    delete t2;
    delete f1;
}

// Helper function that runs in a separate thread for the following test.
static void lockTestThread(Dispatch* dispatch, volatile int* flag,
        CountPoller** poller) {
    dispatch->hasDedicatedThread = true;
    dispatch->ownerId = ThreadId::get();
    *poller = new CountPoller(dispatch);
    *flag = 1;
    while (*flag == 1)
        dispatch->poll();
    delete *poller;
    dispatch->hasDedicatedThread = false;
}

TEST_F(DispatchTest, poll_locking) {
    Tub<Dispatch::Lock> lock;
    // The following Dispatch is created by the child thread and polled
    // from there.
    CountPoller* counter = NULL;
    volatile int flag = 0;
    std::thread thread(lockTestThread, &dispatch, &flag, &counter);

    // Wait for the child thread to start up and enter its polling loop.
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        if ((counter != NULL) && (counter->count >= 10))
            break;
        usleep(100);
    }
    EXPECT_EQ(1, flag);
    EXPECT_NE(counter->count, 0);

    // Create a lock and make sure that the dispatcher stops (e.g. make
    // sure that pollers aren't being invoked).
    lock.construct(&dispatch);
    EXPECT_EQ(1, dispatch.lockNeeded.load());
    EXPECT_EQ(1, dispatch.locked.load());
    int oldCount = counter->count;
    usleep(1000);
    EXPECT_EQ(0, counter->count - oldCount);

    // Delete the lock and make sure that the dispatcher starts working
    // again.
    lock.destroy();
    for (int i = 0; (counter->count == oldCount) && (i < 1000); i++) {
        usleep(100);
    }
    EXPECT_EQ(0, dispatch.locked.load());
    EXPECT_EQ(0, dispatch.lockNeeded.load());
    EXPECT_GT(counter->count, oldCount);

    flag = 0;
    thread.join();
}

TEST_F(DispatchTest, poll_fileHandling) {
    DummyFile *f = new DummyFile("f1", true, pipeFds[0],
            Dispatch::FileEvent::READABLE, &dispatch);
    dispatch.fileInvocationSerial = -2;

    // No event on file.
    usleep(5000);
    EXPECT_EQ(0, dispatch.poll());
    EXPECT_EQ("", *localLog);
    write(pipeFds[1], "0123456789abcdefghijklmnop", 26);

    // File ready.
    EXPECT_EQ(1, waitForPollSuccess(1.0));
    EXPECT_EQ("file f1 invoked, read '0123456789'", *localLog);
    EXPECT_EQ(-1, f->lastInvocationId);
    localLog->clear();

    // File is still ready; make sure event re-enabled.
    EXPECT_EQ(1, waitForPollSuccess(1.0));
    EXPECT_EQ("file f1 invoked, read 'abcdefghij'", *localLog);
    EXPECT_EQ(1, f->lastInvocationId);
    localLog->clear();
    delete f;

    // File still ready, but object has been deleted.
    EXPECT_EQ(0, dispatch.poll());
    EXPECT_EQ("", *localLog);
}

TEST_F(DispatchTest, poll_fileDeletedDuringInvocation) {
    int fds[2];
    pipe(fds);
    DummyFile *f = new DummyFile("f1", false, fds[1],
            Dispatch::FileEvent::WRITABLE, &dispatch);
    f->deleteThis = true;
    dispatch.fileInvocationSerial = 400;
    EXPECT_EQ(1, waitForPollSuccess(1.0));
    EXPECT_EQ("file f1 invoked", *localLog);
    // If poll tried to reenable the event it would have thrown an
    // exception since the handler also closed the file descriptor.
    // Just to double-check, wait a moment and make sure the
    // fd doesn't appear in readyFd.
    usleep(5000);
    EXPECT_EQ(-1, dispatch.readyFd);
    close(fds[0]);
}

TEST_F(DispatchTest, poll_dontEvenCheckTimers) {
    DummyTimer t1("t1", &dispatch);
    t1.start(150);
    Cycles::mockTscValue = 200;
    dispatch.earliestTriggerTime = 201;
    EXPECT_EQ(0, dispatch.poll());
    EXPECT_EQ("", *localLog);
    dispatch.earliestTriggerTime = 0;
    EXPECT_EQ(1, dispatch.poll());
    EXPECT_EQ("timer t1 invoked", *localLog);
}

TEST_F(DispatchTest, poll_triggerTimers) {
    DummyTimer t1("t1", &dispatch), t2("t2", &dispatch);
    DummyTimer t3("t3", &dispatch), t4("t4", &dispatch);
    t1.start(150);
    t2.start(160);
    t3.start(180);
    t4.start(170);
    Cycles::mockTscValue = 175;
    EXPECT_EQ(3, dispatch.poll());
    EXPECT_EQ("timer t4 invoked; timer t2 invoked; "
                "timer t1 invoked", *localLog);
    EXPECT_EQ(180UL, dispatch.earliestTriggerTime);
}

TEST_F(DispatchTest, poll_callEachTimerOnlyOnce) {
    // The timer below will reschedule itself the first few times
    // it's invoked.
    class LoopTimer : public DummyTimer {
      public:
        explicit LoopTimer(const char *name, Dispatch* dispatch)
            : DummyTimer(name, dispatch), dispatch(dispatch), count(3) { }
        void handleTimerEvent() {
            count--;
            if (count != 0) {
                start(dispatch->currentTime);
            }
            DummyTimer::handleTimerEvent();
        }
        Dispatch* dispatch;
        int count;
        DISALLOW_COPY_AND_ASSIGN(LoopTimer);
    };
    DummyTimer t1("t1", &dispatch);
    LoopTimer t2("t2", &dispatch);
    t1.start(150);
    t2.start(160);
    Cycles::mockTscValue = 175;
    dispatch.poll();

    // t2 had better be invoked only once, even though it rescheduled
    // itself and is actually runnable.
    EXPECT_EQ("timer t2 invoked; timer t1 invoked", *localLog);
    localLog->clear();
    dispatch.poll();
    EXPECT_EQ("timer t2 invoked", *localLog);
}

TEST_F(DispatchTest, poll_handlerDeletesTimers) {
    // If one timer deletes others, make sure that the loop index doesn't
    // overflow the length of the "timers" vector.
    DummyTimer t1("t1", &dispatch), t4("t4", &dispatch);
    DummyTimer* t2 = new DummyTimer("t2", &dispatch);
    DummyTimer* t3 = new DummyTimer("t3", &dispatch);
    t1.start(150);
    t2->start(160);
    t3->start(180);
    t4.start(170);
    t4.deleteWhenInvoked(t2);
    t4.deleteWhenInvoked(t3);
    Cycles::mockTscValue = 175;
    dispatch.poll();
    EXPECT_EQ("timer t4 invoked; timer t1 invoked", *localLog);
}

// No tests for Dispatch::run: it doesn't return, so can't test (it's
// pretty simple anyway).

// Helper function that runs in a separate thread for the following test.
static void checkDispatchThread(Dispatch* dispatch, bool* result) {
    *result = dispatch->isDispatchThread();
}
TEST_F(DispatchTest, isDispatchThread) {
    dispatch.hasDedicatedThread = true;
    EXPECT_TRUE(dispatch.isDispatchThread());
    bool childResult = true;
    std::thread(checkDispatchThread, &dispatch, &childResult).join();
    EXPECT_FALSE(childResult);
}

// The following test exercises most of the functionality related to
// pollers (creation, deletion, invocation).
TEST_F(DispatchTest, Poller_basics) {
    DummyPoller p1("p1", 1000, &dispatch);
    dispatch.poll();
    EXPECT_EQ("poller p1 invoked", *localLog);
    {
        DummyPoller p2("p2", 1000, &dispatch);
        DummyPoller p3("p3", 0, &dispatch);
        localLog->clear();
        EXPECT_EQ(3, dispatch.poll());
        EXPECT_EQ("poller p1 invoked; poller p2 invoked; "
                "poller p3 invoked", *localLog);
    }
    localLog->clear();
    EXPECT_EQ(1, dispatch.poll());
    EXPECT_EQ("poller p1 invoked", *localLog);
}

TEST_F(DispatchTest, Poller_destructor_updateSlot) {
    DummyPoller* p1 = new DummyPoller("p1", 0, &dispatch);
    DummyPoller* p2 = new DummyPoller("p2", 0, &dispatch);
    DummyPoller p3("p3", 0, &dispatch);
    EXPECT_EQ(2, p3.slot);
    delete p2;
    EXPECT_EQ(1, p3.slot);
    delete p1;
    EXPECT_EQ(0, p3.slot);
}

TEST_F(DispatchTest, Poller_reentrant) {
    // Make sure everything still works if a poller gets deleted
    // in the middle of invoking pollers.
    DummyPoller p1("p1", 0, &dispatch);
    DummyPoller *p2 = new DummyPoller("p2", 0, &dispatch);
    p2->deleteWhenInvoked(p2);
    p2->deleteWhenInvoked(new DummyPoller("p3", 0, &dispatch));
    p2->deleteWhenInvoked(new DummyPoller("p4", 0, &dispatch));
    EXPECT_EQ(2, dispatch.poll());
    EXPECT_EQ("poller p1 invoked; poller p2 invoked",
            *localLog);
}

TEST_F(DispatchTest, File_constructor_errorInEpollCreate) {
    sys->epollCreateErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], &dispatch);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("epoll_create failed in Dispatch: "
            "Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_errorCreatingExitPipe) {
    sys->pipeErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], &dispatch);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("Dispatch couldn't create exit pipe for "
            "epoll thread: Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_errorInEpollCtl) {
    sys->epollCtlErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], &dispatch);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("Dispatch couldn't set epoll event for "
            "exit pipe: Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_createPollingThread) {
    EXPECT_FALSE(dispatch.epollThread);
    DummyFile f1("f1", false, pipeFds[0], &dispatch);
    EXPECT_TRUE(dispatch.epollThread);
}

TEST_F(DispatchTest, File_constructor_growFileTable) {
    uint32_t fd = 100;
    if (fd < dispatch.files.size()) {
        fd = downCast<uint32_t>(dispatch.files.size()) + 10;
    }
    DummyFile f1("f1", false, fd, &dispatch);
    EXPECT_EQ(2*fd, dispatch.files.size());
}

TEST_F(DispatchTest, File_constructor_twoHandlersForSameFd) {
    try {
        DummyFile f1("f1", false, pipeFds[0], &dispatch);
        DummyFile f2("f2", false, pipeFds[0], &dispatch);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("can't have more than 1 Dispatch::File "
            "for a file descriptor", exceptionMessage);
}

TEST_F(DispatchTest, File_destructor_disableEvent) {
    DummyFile* f1 = new DummyFile("f1", true, pipeFds[0],
            Dispatch::FileEvent::READABLE, &dispatch);

    // First make sure that the handler responds to data written
    // to the pipe.
    EXPECT_EQ(1, write(pipeFds[1], "x", 1));
    waitForPollSuccess(1.0);
    EXPECT_EQ("file f1 invoked, read 'x'", *localLog);

    // Now delete the handler, and make sure that data in the pipe
    // is ignored.
    delete f1;
    usleep(5000);
    EXPECT_EQ(-1, dispatch.readyFd);
    EXPECT_EQ(1, write(pipeFds[1], "y", 1));
    usleep(5000);
    EXPECT_EQ(-1, dispatch.readyFd);
}

TEST_F(DispatchTest, File_checkInvocationId) {
    DummyFile f("f", false, 22, &dispatch);
    f.invocationId = 99;
    try {
        sys->epollCtlErrno = EPERM;
        f.setEvents(Dispatch::FileEvent::READABLE);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("no exception", exceptionMessage);
    EXPECT_EQ(Dispatch::FileEvent::READABLE, f.events);
}

TEST_F(DispatchTest, File_setEvents_variousEvents) {
    // Create a pipe that is ready for reading but not writing, and
    // make sure all the correct events fire.
    int readable[2];
    EXPECT_EQ(0, pipe(readable));
    // Fill the pipe to the point where writes would block.
    EXPECT_EQ(0, fcntl(readable[1], F_SETFL, O_NONBLOCK));
    for (int i = 0; ; i++) {
        char buffer[1000] = "abcdefg";
        ssize_t count = write(readable[1], buffer, 1000);
        if (count < 0) {
            EXPECT_EQ(EAGAIN, errno);
            break;
        }
        EXPECT_LT(i, 100);
    }
    EXPECT_EQ("", checkReady(readable[0], 0, ""));
    EXPECT_EQ("READABLE", checkReady(readable[0],
        Dispatch::FileEvent::READABLE, "READABLE"));
    EXPECT_EQ("READABLE", checkReady(readable[0],
        Dispatch::FileEvent::READABLE | Dispatch::FileEvent::WRITABLE,
        "READABLE"));
    EXPECT_EQ("", checkReady(readable[1], 0, ""));
    EXPECT_EQ("", checkReady(readable[1],
        Dispatch::FileEvent::WRITABLE, ""));
    EXPECT_EQ("", checkReady(readable[1],
        Dispatch::FileEvent::READABLE | Dispatch::FileEvent::WRITABLE, ""));
    close(readable[0]);
    close(readable[1]);

    // Now, create a pipe that is ready for writing but not reading,
    // and make sure all the correct events fire.
    int writable[2];
    EXPECT_EQ(0, pipe(writable));
    EXPECT_EQ("", checkReady(writable[0], 0, ""));
    EXPECT_EQ("", checkReady(writable[0],
        Dispatch::FileEvent::READABLE, ""));
    EXPECT_EQ("", checkReady(writable[0],
        Dispatch::FileEvent::READABLE | Dispatch::FileEvent::WRITABLE, ""));
    EXPECT_EQ("", checkReady(writable[1], 0, ""));
    EXPECT_EQ("WRITABLE", checkReady(writable[1],
        Dispatch::FileEvent::WRITABLE, "WRITABLE"));
    EXPECT_EQ("WRITABLE", checkReady(writable[1],
        Dispatch::FileEvent::READABLE | Dispatch::FileEvent::WRITABLE,
        "WRITABLE"));
    close(writable[0]);
    close(writable[1]);
}

TEST_F(DispatchTest, File_setEvents_errorInEpollCtl) {
    DummyFile f("f", false, 22, &dispatch);
    try {
        sys->epollCtlErrno = EPERM;
        f.setEvents(Dispatch::FileEvent::READABLE);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("Dispatch couldn't set epoll event for fd "
            "22: Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, epollThreadMain_errorsInEpollWait) {
    epoll_event event;
    sys->epollWaitCount = 0;
    sys->epollWaitEvents = &event;
    sys->epollWaitErrno = EPERM;
    Dispatch::epollThreadMain(&dispatch);
    EXPECT_EQ("epollThreadMain: epoll_wait returned no "
            "events in Dispatch::epollThread | epollThreadMain: "
            "epoll_wait failed in Dispatch::epollThread: Operation "
            "not permitted", TestLog::get());
}

TEST_F(DispatchTest, epollThreadMain_exitIgnoringFd) {
    sys->write(pipeFds[1], "blah", 4);
    DummyFile f1("f1", false, pipeFds[0],
            Dispatch::FileEvent::READABLE, &dispatch);

    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        if (dispatch.readyFd != -1) {
            break;
        }
        usleep(1000);
    }
    EXPECT_EQ("", TestLog::get());
    EXPECT_NE(-1, dispatch.readyFd);
    sys->write(dispatch.exitPipeFds[1], "x", 1);

    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        if (TestLog::get().size() > 0) {
            break;
        }
        usleep(1000);
    }
    EXPECT_EQ("epollThreadMain: done", TestLog::get());
}

TEST_F(DispatchTest, fdIsReady) {
    int fds[2];
    EXPECT_EQ(0, pipe(fds));
    EXPECT_FALSE(Dispatch::fdIsReady(fds[0]));
    EXPECT_EQ(1, write(fds[1], "x", 1));
    close(fds[1]);
    EXPECT_TRUE(Dispatch::fdIsReady(fds[0]));
    close(fds[0]);
}

static void epollThreadWrapper(Dispatch* dispatch) {
    Dispatch::epollThreadMain(dispatch);
    *localLog = "epoll thread finished";
}

TEST_F(DispatchTest, epollThreadMain_signalEventsAndExit) {
    // This unit test tests several things:
    // * Several files becoming ready simultaneously
    // * Using readyFd and readyEvents to synchronize with the poll loop.
    // * Exiting when fd -1 is seen.
    epoll_event events[3];
    events[0].data.fd = 43;
    events[0].events = EPOLLOUT;
    events[1].data.fd = 19;
    events[1].events = EPOLLIN|EPOLLOUT;
    events[2].data.fd = -1;
    sys->epollWaitEvents = events;
    sys->epollWaitCount = 3;

    // Start up the polling thread; it will signal the first ready file.
    dispatch.readyFd = -1;
    std::thread(epollThreadWrapper, &dispatch).detach();
    waitForReadyFd(1.0);
    EXPECT_EQ(43, dispatch.readyFd);
    EXPECT_EQ(Dispatch::FileEvent::WRITABLE, dispatch.readyEvents);

    // The polling thread should already be waiting on readyFd,
    // so clearing it should cause another fd to appear immediately.
    dispatch.readyFd = -1;
    waitForReadyFd(1.0);
    EXPECT_EQ(19, dispatch.readyFd);
    EXPECT_EQ(Dispatch::FileEvent::READABLE|Dispatch::FileEvent::WRITABLE,
            dispatch.readyEvents);

    // Let the polling thread see the next ready file, which should
    // cause it to exit.
    dispatch.readyFd = -1;
    usleep(5000);
    EXPECT_EQ(-1, dispatch.readyFd);
    EXPECT_EQ("epoll thread finished", *localLog);
}

TEST_F(DispatchTest, Timer_constructorDestructor) {
    DummyTimer* t1 = new DummyTimer("t1", &dispatch);
    DummyTimer* t2 = new DummyTimer("t2", 100, &dispatch);
    EXPECT_EQ(1U, dispatch.timers.size());
    EXPECT_EQ(-1, t1->slot);
    EXPECT_EQ(0, t2->slot);
    EXPECT_EQ(100UL, t2->triggerTime);
    delete t1;
    delete t2;
    EXPECT_EQ(0U, dispatch.timers.size());
}

// Make sure that a timer can safely be deleted from a timer
// handler.
TEST_F(DispatchTest, Timer_reentrant) {
    DummyTimer t1("t1", 500, &dispatch);
    DummyTimer* t2 = new DummyTimer("t2", 100, &dispatch);
    t2->deleteWhenInvoked(t2);
    t2->deleteWhenInvoked(new DummyTimer("t3", &dispatch));
    t2->deleteWhenInvoked(new DummyTimer("t4", &dispatch));
    Cycles::mockTscValue = 300;
    dispatch.poll();
    EXPECT_EQ("timer t2 invoked", *localLog);
    EXPECT_EQ(1U, dispatch.timers.size());
}

TEST_F(DispatchTest, Timer_isRunning) {
    DummyTimer t1("t1", &dispatch);
    EXPECT_FALSE(t1.isRunning());
    t1.start(200);
    EXPECT_TRUE(t1.isRunning());
    t1.stop();
    EXPECT_FALSE(t1.isRunning());
}

TEST_F(DispatchTest, Timer_start) {
    DummyTimer t1("t1", &dispatch);
    DummyTimer t2("t2", &dispatch);
    dispatch.earliestTriggerTime = 200;
    t1.start(210);
    EXPECT_EQ(210UL, t1.triggerTime);
    EXPECT_EQ(0, t1.slot);
    EXPECT_EQ(200UL, dispatch.earliestTriggerTime);
    t2.start(190);
    EXPECT_EQ(190UL, dispatch.earliestTriggerTime);
    EXPECT_EQ(1, t2.slot);
    t1.start(300);
    EXPECT_EQ(300UL, t1.triggerTime);
    EXPECT_EQ(0, t1.slot);
}

TEST_F(DispatchTest, Timer_start_dispatchDeleted) {
    Tub<Dispatch> dispatch;
    dispatch.construct(false);
    DummyTimer t1("t1", dispatch.get());
    t1.start(1000);
    dispatch.destroy();
    t1.start(2000);
    EXPECT_EQ(1000UL, t1.triggerTime);
}

TEST_F(DispatchTest, Timer_stop) {
    DummyTimer t1("t1", 100, &dispatch);
    DummyTimer t2("t2", 100, &dispatch);
    DummyTimer t3("t3", 100, &dispatch);
    EXPECT_EQ(0, t1.slot);
    t1.stop();
    EXPECT_EQ(-1, t1.slot);
    EXPECT_EQ(2U, dispatch.timers.size());
    EXPECT_EQ(0, t3.slot);
    t1.stop();
    EXPECT_EQ(-1, t1.slot);
    EXPECT_EQ(2U, dispatch.timers.size());
}

TEST_F(DispatchTest, Lock_inDispatchThread) {
    // Creating a lock shouldn't stop the Dispatcher from polling.
    DummyPoller p1("p1", 1000, &dispatch);
    Dispatch::Lock lock(&dispatch);
    dispatch.poll();
    EXPECT_EQ("poller p1 invoked", *localLog);
}

// The test case poll_locking has already tested the
// functionality of locking from a non-dispatch thread.


// Helper function for the following test; creates a dispatch object
// in a separate thread.
static void testRecursionThread(Dispatch** dispatch) {
    Dispatch actual(true);
    *dispatch = &actual;
    while (*dispatch != NULL) {
        // Wait for the main thread to finish using this object.
        actual.poll();
    }
}
TEST_F(DispatchTest, Lock_recursiveLocks) {
    Dispatch *dispatch = NULL;
    std::thread thread(testRecursionThread, &dispatch);
    // See "Timing-Dependent Tests" in designNotes.
    for (int i = 0; i < 1000; i++) {
        if (dispatch != NULL)
            break;
        usleep(1000);
    }
    EXPECT_TRUE(dispatch != NULL);
    Tub<Dispatch::Lock> lock1, lock2, lock3;
    lock1.construct(dispatch);
    EXPECT_EQ(1, dispatch->lockNeeded.load());
    lock2.construct(dispatch);
    lock3.construct(dispatch);
    EXPECT_EQ(1, dispatch->lockNeeded.load());
    lock3.destroy();
    lock2.destroy();
    EXPECT_EQ(1, dispatch->lockNeeded.load());
    lock1.destroy();
    EXPECT_EQ(0, dispatch->lockNeeded.load());
    dispatch = NULL;
    thread.join();
}

}  // namespace RAMCloud
