/* Copyright (c) 2011 Stanford University
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
#include "BenchUtil.h"
#include "Dispatch.h"
#include "MockSyscall.h"

namespace RAMCloud {
static string *localLog;

// The following class is used for testing: it generates a log message
// identifying this poller whenever it is invoked.
class DummyPoller : public Dispatch::Poller {
  public:
    DummyPoller(const char *name, int callsUntilTrue, Dispatch *dispatch)
        : Dispatch::Poller(dispatch), myName(name),
        callsUntilTrue(callsUntilTrue), pollersToDelete() { }
    void poll() {
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
    }
    // Arrange to delete a given poller the next time this poller is
    // invoked (used for testing reentrancy).
    void deleteWhenInvoked(Poller* poller) {
        pollersToDelete.push_back(poller);
    }
    const char *myName;
    int callsUntilTrue;
    std::vector<Poller*> pollersToDelete;
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyPoller);
};

// The following class is used for testing: it increments a counter each
// time it is invoked.
class CountPoller : public Dispatch::Poller {
  public:
    explicit CountPoller(Dispatch* dispatch)
            : Dispatch::Poller(dispatch), count(0) { }
    void poll() {
        count++;
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
            : Dispatch::Timer(cycles, dispatch), myName(name),
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
            Dispatch::FileEvent event, Dispatch* dispatch)
            : Dispatch::File(fd, event, dispatch), myName(name),
            readData(readData), lastInvocationId(0), deleteThis(false) { }
    explicit DummyFile(const char *name, bool readData, int fd,
            Dispatch* dispatch)
            : Dispatch::File(fd, Dispatch::FileEvent::NONE, dispatch),
            myName(name), readData(readData), lastInvocationId(0),
            deleteThis(false) { }
    void handleFileEvent() {
        char buffer[11];
        if (localLog->length() != 0) {
            localLog->append("; ");
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
  private:
    DISALLOW_COPY_AND_ASSIGN(DummyFile);
};

class DispatchTest : public ::testing::Test {
  public:
    string exceptionMessage;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable* logEnabler;
    int pipeFds[2];
    Dispatch *td;

    DispatchTest()
        : exceptionMessage()
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler(NULL)
        , td(NULL)
    {
        exceptionMessage = "no exception";
        if (!localLog) {
            localLog = new string;
        }
        localLog->clear();
        td = new Dispatch;
        td->currentTime = 100;
        sys = new MockSyscall();
        savedSyscall = Dispatch::sys;
        Dispatch::sys = sys;
        logEnabler = new TestLog::Enable();
        EXPECT_EQ(0, pipe(pipeFds));

        // Stop the official dispatcher so it can't interfere with the tests.
        delete RAMCloud::dispatch;
        RAMCloud::dispatch = NULL;
    }

    ~DispatchTest() {
        close(pipeFds[0]);
        close(pipeFds[1]);
        delete td;
        Dispatch::sys = savedSyscall;
        delete sys;
        sys = NULL;
        mockTSCValue = 0;

        // Restart the main dispatcher.
        RAMCloud::dispatch = new Dispatch;
    }

    // Calls td->poll repeatedly until either a log entry is
    // generated or a given amount of time has elapsed.
    void waitForPollSuccess(double timeoutSeconds) {
        uint64_t start = rdtsc();
        while (localLog->size() == 0) {
            usleep(100);
            td->poll();
            if (cyclesToSeconds(rdtsc() - start) > timeoutSeconds)
                return;
        }
    }

    // Waits for a file to become ready, but gives up after a given
    // elapsed time.
    void waitForReadyFd(double timeoutSeconds) {
        uint64_t start = rdtsc();
        while (td->readyFd < 0) {
            usleep(1000);
            if (cyclesToSeconds(rdtsc() - start) > timeoutSeconds)
                return;
        }
    }

    // Utility method: create a DummyFile for a particular file descriptor
    // and see if it gets invoked.
    bool checkReady(int fd, Dispatch::FileEvent event) {
        // Flush any stale events.
        for (int i = 0; i < 100; i++) td->poll();
        localLog->clear();
        DummyFile f("f1", false, fd, event, td);
        usleep(5000);
        td->poll();
        return localLog->size() > 0;
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(DispatchTest);
};

TEST_F(DispatchTest, constructor) {
    EXPECT_TRUE(td->isDispatchThread());
}

TEST_F(DispatchTest, destructor) {
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
    EXPECT_EQ(0, f1->event);
    delete p1;
    delete p2;
    delete t1;
    delete t2;
    delete f1;
}

// Helper function that runs in a separate thread for the following test.
static void lockTestThread(Dispatch** d, volatile int* flag,
        CountPoller** poller) {
    *d = new Dispatch();
    *poller = new CountPoller(*d);
    *flag = 1;
    while (*flag == 1)
        (*d)->poll();
    delete *poller;
    delete *d;
}

TEST_F(DispatchTest, poll_locking) {
    Tub<Dispatch::Lock> lock;
    // The following Dispatch is created by the child thread and polled
    // from there.
    Dispatch* dispatch;
    CountPoller* counter = NULL;
    volatile int flag = 0;
    boost::thread thread(lockTestThread, &dispatch, &flag, &counter);

    // Wait for the child thread to start up and enter its polling loop.
    for (int i = 0; i < 1000; i++) {
        if ((counter != NULL) && (counter->count >= 10))
            break;
        usleep(100);
    }
    EXPECT_EQ(1, flag);
    EXPECT_NE(counter->count, 0);

    // Create a lock and make sure that the dispatcher stops (e.g. make
    // sure that pollers aren't being invoked).
    lock.construct(dispatch);
    EXPECT_EQ(1, dispatch->lockNeeded.load());
    EXPECT_EQ(1, dispatch->locked.load());
    int oldCount = counter->count;
    usleep(1000);
    EXPECT_EQ(0, counter->count - oldCount);

    // Delete the lock and make sure that the dispatcher starts working
    // again.
    lock.destroy();
    for (int i = 0; (counter->count == oldCount) && (i < 1000); i++) {
        usleep(100);
    }
    EXPECT_EQ(0, dispatch->locked.load());
    EXPECT_EQ(0, dispatch->lockNeeded.load());
    EXPECT_GT(counter->count, oldCount);

    flag = 0;
    thread.join();
}

TEST_F(DispatchTest, poll_fileHandling) {
    DummyFile *f = new DummyFile("f1", true, pipeFds[0],
            Dispatch::FileEvent::READABLE, td);
    td->fileInvocationSerial = -2;

    // No event on file.
    usleep(5000);
    td->poll();
    EXPECT_EQ("", *localLog);
    write(pipeFds[1], "0123456789abcdefghijklmnop", 26);

    // File ready.
    waitForPollSuccess(1.0);
    EXPECT_EQ("file f1 invoked, read '0123456789'", *localLog);
    EXPECT_EQ(-1, f->lastInvocationId);
    localLog->clear();

    // File is still ready; make sure event re-enabled.
    waitForPollSuccess(1.0);
    EXPECT_EQ("file f1 invoked, read 'abcdefghij'", *localLog);
    EXPECT_EQ(1, f->lastInvocationId);
    localLog->clear();
    delete f;

    // File still ready, but object has been deleted.
    td->poll();
    EXPECT_EQ("", *localLog);
}

TEST_F(DispatchTest, poll_fileDeletedDuringInvocation) {
    int fds[2];
    pipe(fds);
    DummyFile *f = new DummyFile("f1", false, fds[1],
            Dispatch::FileEvent::WRITABLE, td);
    f->deleteThis = true;
    td->fileInvocationSerial = 400;
    waitForPollSuccess(1.0);
    EXPECT_EQ("file f1 invoked", *localLog);
    // If poll tried to reenable the event it would have thrown an
    // exception since the handler also closed the file descriptor.
    // Just to double-check, wait a moment and make sure the
    // fd doesn't appear in readyFd.
    usleep(5000);
    EXPECT_EQ(-1, td->readyFd);
    close(fds[0]);
}

TEST_F(DispatchTest, poll_dontEvenCheckTimers) {
    DummyTimer t1("t1", td);
    t1.start(150);
    mockTSCValue = 200;
    td->earliestTriggerTime = 201;
    td->poll();
    EXPECT_EQ("", *localLog);
    td->earliestTriggerTime = 0;
    td->poll();
    EXPECT_EQ("timer t1 invoked", *localLog);
}

TEST_F(DispatchTest, poll_triggerTimers) {
    DummyTimer t1("t1", td), t2("t2", td), t3("t3", td), t4("t4", td);
    t1.start(150);
    t2.start(160);
    t3.start(180);
    t4.start(170);
    mockTSCValue = 175;
    td->poll();
    EXPECT_EQ("timer t1 invoked; timer t4 invoked; "
                "timer t2 invoked", *localLog);
    EXPECT_EQ(180UL, td->earliestTriggerTime);
}


// Helper function that runs in a separate thread for the following test.
static void checkDispatchThread(Dispatch* td, bool* result) {
    *result = td->isDispatchThread();
}
TEST_F(DispatchTest, isDispatchThread) {
    EXPECT_TRUE(td->isDispatchThread());
    bool childResult = true;
    boost::thread(checkDispatchThread, td, &childResult).join();
    EXPECT_FALSE(childResult);
}

// The following test exercises most of the functionality related to
// pollers (creation, deletion, invocation).
TEST_F(DispatchTest, Poller_basics) {
    DummyPoller p1("p1", 1000, td);
    td->poll();
    EXPECT_EQ("poller p1 invoked", *localLog);
    {
        DummyPoller p2("p2", 1000, td);
        DummyPoller p3("p3", 0, td);
        localLog->clear();
        td->poll();
        EXPECT_EQ("poller p1 invoked; poller p2 invoked; "
                "poller p3 invoked", *localLog);
    }
    localLog->clear();
    td->poll();
    EXPECT_EQ("poller p1 invoked", *localLog);
}

TEST_F(DispatchTest, Poller_destructor_updateSlot) {
    DummyPoller* p1 = new DummyPoller("p1", 0, td);
    DummyPoller* p2 = new DummyPoller("p2", 0, td);
    DummyPoller p3("p3", 0, td);
    EXPECT_EQ(2, p3.slot);
    delete p2;
    EXPECT_EQ(1, p3.slot);
    delete p1;
    EXPECT_EQ(0, p3.slot);
}

TEST_F(DispatchTest, Poller_reentrant) {
    // Make sure everything still works if a poller gets deleted
    // in the middle of invoking pollers.
    DummyPoller p1("p1", 0, td);
    DummyPoller *p2 = new DummyPoller("p2", 0, td);
    p2->deleteWhenInvoked(p2);
    p2->deleteWhenInvoked(new DummyPoller("p3", 0, td));
    p2->deleteWhenInvoked(new DummyPoller("p4", 0, td));
    td->poll();
    EXPECT_EQ("poller p1 invoked; poller p2 invoked",
            *localLog);
}

TEST_F(DispatchTest, File_constructor_errorInEpollCreate) {
    sys->epollCreateErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], td);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("epoll_create failed in Dispatch: "
            "Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_errorCreatingExitPipe) {
    sys->pipeErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], td);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("Dispatch couldn't create exit pipe for "
            "epoll thread: Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_errorInEpollCtl) {
    sys->epollCtlErrno = EPERM;
    try {
        DummyFile f1("f1", false, pipeFds[0], td);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("Dispatch couldn't set epoll event for "
            "exit pipe: Operation not permitted", exceptionMessage);
}

TEST_F(DispatchTest, File_constructor_createPollingThread) {
    EXPECT_FALSE(td->epollThread);
    DummyFile f1("f1", false, pipeFds[0], td);
    EXPECT_TRUE(td->epollThread);
}

TEST_F(DispatchTest, File_constructor_growFileTable) {
    uint32_t fd = 100;
    if (fd < td->files.size()) {
        fd = downCast<uint32_t>(td->files.size()) + 10;
    }
    DummyFile f1("f1", false, fd, td);
    EXPECT_EQ(2*fd, td->files.size());
}

TEST_F(DispatchTest, File_constructor_twoHandlersForSameFd) {
    try {
        DummyFile f1("f1", false, pipeFds[0], td);
        DummyFile f2("f2", false, pipeFds[0], td);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("can't have more than 1 Dispatch::File "
            "for a file descriptor", exceptionMessage);
}

TEST_F(DispatchTest, File_destructor_disableEvent) {
    DummyFile* f1 = new DummyFile("f1", true, pipeFds[0],
            Dispatch::FileEvent::READABLE, td);

    // First make sure that the handler responds to data written
    // to the pipe.
    EXPECT_EQ(1, write(pipeFds[1], "x", 1));
    waitForPollSuccess(1.0);
    EXPECT_EQ("file f1 invoked, read 'x'", *localLog);

    // Now delete the handler, and make sure that data in the pipe
    // is ignored.
    delete f1;
    usleep(5000);
    EXPECT_EQ(-1, td->readyFd);
    EXPECT_EQ(1, write(pipeFds[1], "y", 1));
    usleep(5000);
    EXPECT_EQ(-1, td->readyFd);
}

TEST_F(DispatchTest, File_checkInvocationId) {
    DummyFile f("f", false, 22, td);
    f.invocationId = 99;
    try {
        sys->epollCtlErrno = EPERM;
        f.setEvent(Dispatch::FileEvent::READABLE);
    } catch (FatalError& e) {
        exceptionMessage = e.message;
    }
    EXPECT_EQ("no exception", exceptionMessage);
    EXPECT_EQ(Dispatch::FileEvent::READABLE, f.event);
}

TEST_F(DispatchTest, File_setEvent_variousEvents) {
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
    EXPECT_FALSE(checkReady(readable[0],
        Dispatch::FileEvent::NONE));
    EXPECT_TRUE(checkReady(readable[0],
        Dispatch::FileEvent::READABLE));
    EXPECT_TRUE(checkReady(readable[0],
        Dispatch::FileEvent::READABLE_OR_WRITABLE));
    EXPECT_FALSE(checkReady(readable[1],
        Dispatch::FileEvent::NONE));
    EXPECT_FALSE(checkReady(readable[1],
        Dispatch::FileEvent::WRITABLE));
    EXPECT_FALSE(checkReady(readable[1],
        Dispatch::FileEvent::READABLE_OR_WRITABLE));
    close(readable[0]);
    close(readable[1]);

    // Now, create a pipe that is ready for writing but not reading,
    // and make sure all the correct events fire.
    int writable[2];
    EXPECT_EQ(0, pipe(writable));
    EXPECT_FALSE(checkReady(writable[0],
        Dispatch::FileEvent::NONE));
    EXPECT_FALSE(checkReady(writable[0],
        Dispatch::FileEvent::READABLE));
    EXPECT_FALSE(checkReady(writable[0],
        Dispatch::FileEvent::READABLE_OR_WRITABLE));
    EXPECT_FALSE(checkReady(writable[1],
        Dispatch::FileEvent::NONE));
    EXPECT_TRUE(checkReady(writable[1],
        Dispatch::FileEvent::WRITABLE));
    EXPECT_TRUE(checkReady(writable[1],
        Dispatch::FileEvent::READABLE_OR_WRITABLE));
    close(writable[0]);
    close(writable[1]);
}

TEST_F(DispatchTest, File_setEvent_errorInEpollCtl) {
    DummyFile f("f", false, 22, td);
    try {
        sys->epollCtlErrno = EPERM;
        f.setEvent(Dispatch::FileEvent::READABLE);
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
    Dispatch::epollThreadMain(td);
    EXPECT_EQ("epollThreadMain: epoll_wait returned no "
            "events in Dispatch::epollThread | epollThreadMain: "
            "epoll_wait failed in Dispatch::epollThread: Operation "
            "not permitted", TestLog::get());
}

static void epollThreadWrapper(Dispatch* dispatch) {
    Dispatch::epollThreadMain(dispatch);
    *localLog = "epoll thread finished";
}

TEST_F(DispatchTest, epollThreadMain_signalEventsAndExit) {
    // This unit test tests several things:
    // * Several files becoming ready simultaneously
    // * Using readyFd to synchronize with the poll loop.
    // * Exiting when fd -1 is seen.
    epoll_event events[3];
    events[0].data.fd = 43;
    events[1].data.fd = 19;
    events[2].data.fd = -1;
    sys->epollWaitEvents = events;
    sys->epollWaitCount = 3;

    // Start up the polling thread; it will signal the first ready file.
    td->readyFd = -1;
    boost::thread(epollThreadWrapper, td).detach();
    waitForReadyFd(1.0);
    EXPECT_EQ(43, td->readyFd);

    // The polling thread should already be waiting on readyFd,
    // so clearing it should cause another fd to appear immediately.
    td->readyFd = -1;
    waitForReadyFd(1.0);
    EXPECT_EQ(19, td->readyFd);

    // Let the polling thread see the next ready file, which should
    // cause it to exit.
    td->readyFd = -1;
    usleep(5000);
    EXPECT_EQ(-1, td->readyFd);
    EXPECT_EQ("epoll thread finished", *localLog);
}

TEST_F(DispatchTest, Timer_constructorDestructor) {
    DummyTimer* t1 = new DummyTimer("t1", td);
    DummyTimer* t2 = new DummyTimer("t2", 100, td);
    EXPECT_EQ(1U, td->timers.size());
    EXPECT_EQ(-1, t1->slot);
    EXPECT_EQ(0, t2->slot);
    EXPECT_EQ(100UL, t2->triggerTime);
    delete t1;
    delete t2;
    EXPECT_EQ(0U, td->timers.size());
}

// Make sure that a timer can safely be deleted from a timer
// handler.
TEST_F(DispatchTest, Timer_reentrant) {
    DummyTimer t1("t1", 500, td);
    DummyTimer* t2 = new DummyTimer("t2", 100, td);
    t2->deleteWhenInvoked(t2);
    t2->deleteWhenInvoked(new DummyTimer("t3", td));
    t2->deleteWhenInvoked(new DummyTimer("t4", td));
    mockTSCValue = 300;
    td->poll();
    CPPUNIT_ASSERT_EQUAL("timer t2 invoked", *localLog);
    CPPUNIT_ASSERT_EQUAL(1U, td->timers.size());
}

TEST_F(DispatchTest, Timer_isRunning) {
    DummyTimer t1("t1", td);
    EXPECT_FALSE(t1.isRunning());
    t1.start(200);
    EXPECT_TRUE(t1.isRunning());
    t1.stop();
    EXPECT_FALSE(t1.isRunning());
}

TEST_F(DispatchTest, Timer_start) {
    DummyTimer t1("t1", td);
    DummyTimer t2("t2", td);
    td->earliestTriggerTime = 200;
    t1.start(210);
    EXPECT_EQ(210UL, t1.triggerTime);
    EXPECT_EQ(0, t1.slot);
    EXPECT_EQ(200UL, td->earliestTriggerTime);
    t2.start(190);
    EXPECT_EQ(190UL, td->earliestTriggerTime);
    EXPECT_EQ(1, t2.slot);
    t1.start(300);
    EXPECT_EQ(300UL, t1.triggerTime);
    EXPECT_EQ(0, t1.slot);
}

TEST_F(DispatchTest, Timer_stop) {
    DummyTimer t1("t1", 100, td);
    DummyTimer t2("t2", 100, td);
    DummyTimer t3("t3", 100, td);
    EXPECT_EQ(0, t1.slot);
    t1.stop();
    EXPECT_EQ(-1, t1.slot);
    EXPECT_EQ(2U, td->timers.size());
    t1.stop();
    EXPECT_EQ(-1, t1.slot);
    EXPECT_EQ(2U, td->timers.size());
}

TEST_F(DispatchTest, Lock_inDispatchThread) {
    // Creating a lock shouldn't stop the Dispatcher from polling.
    DummyPoller p1("p1", 1000, td);
    Dispatch::Lock lock(td);
    td->poll();
    EXPECT_EQ("poller p1 invoked", *localLog);
}

// The test case test_poll_locking has already tested the
// functionality of locking from a non-dispatch thread.

}  // namespace RAMCloud
