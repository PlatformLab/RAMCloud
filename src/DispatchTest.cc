/* Copyright (c) 2010 Stanford University
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
    DummyPoller(const char *name, int callsUntilTrue)
        : Dispatch::Poller(), myName(name), callsUntilTrue(callsUntilTrue),
        pollersToDelete() { }
    bool operator() () {
        bool deleteThis = false;
        bool result = true;
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
            result = false;
        }
        if (deleteThis) {
            delete this;
        }
        return result;
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
    CountPoller() : Dispatch::Poller(), count(0) { }
    bool operator() () {
        count++;
        return true;
    }
    volatile int count;
  private:
    DISALLOW_COPY_AND_ASSIGN(CountPoller);
};

// The following class is used for testing: it generates a log message
// identifying this timer whenever it is invoked.
class DummyTimer : public Dispatch::Timer {
  public:
    explicit DummyTimer(const char *name)
            : Dispatch::Timer(), myName(name), timersToDelete() { }
    DummyTimer(const char *name, uint64_t cycles)
            : Dispatch::Timer(cycles), myName(name), timersToDelete() { }
    void operator() () {
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
            Dispatch::FileEvent event = Dispatch::FileEvent::NONE)
            : Dispatch::File(fd, event), myName(name), readData(readData),
            lastInvocationId(0), deleteThis(false) { }
    void operator() () {
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

class DispatchTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(DispatchTest);
    CPPUNIT_TEST(test_Poller_basics);
    CPPUNIT_TEST(test_Poller_destructor_updateSlot);
    CPPUNIT_TEST(test_Poller_reentrant);
    CPPUNIT_TEST(test_poll_locking);
    CPPUNIT_TEST(test_poll_fileHandling);
    CPPUNIT_TEST(test_poll_fileDeletedDuringInvocation);
    CPPUNIT_TEST(test_poll_dontEvenCheckTimers);
    CPPUNIT_TEST(test_poll_triggerTimers);
    CPPUNIT_TEST(test_handleEvent);
    CPPUNIT_TEST(test_reset);
    CPPUNIT_TEST(test_setDispatchThread);
    CPPUNIT_TEST(test_File_constructor_errorInEpollCreate);
    CPPUNIT_TEST(test_File_constructor_errorCreatingExitPipe);
    CPPUNIT_TEST(test_File_constructor_errorInEpollCtl);
    CPPUNIT_TEST(test_File_constructor_createPollingThread);
    CPPUNIT_TEST(test_File_constructor_growFileTable);
    CPPUNIT_TEST(test_File_constructor_twoHandlersForSameFd);
    CPPUNIT_TEST(test_File_destructor_disableEvent);
    CPPUNIT_TEST(test_File_checkInvocationId);
    CPPUNIT_TEST(test_File_setEvent_variousEvents);
    CPPUNIT_TEST(test_File_setEvent_errorInEpollCtl);
    CPPUNIT_TEST(test_epollThreadMain_errorsInEpollWait);
    CPPUNIT_TEST(test_epollThreadMain_signalEventsAndExit);
    CPPUNIT_TEST(test_Timer_constructorDestructor);
    CPPUNIT_TEST(test_Timer_reentrant);
    CPPUNIT_TEST(test_isRunning);
    CPPUNIT_TEST(test_Timer_startCycles);
    CPPUNIT_TEST(test_Timer_startMicros);
    CPPUNIT_TEST(test_Timer_startMillis);
    CPPUNIT_TEST(test_Timer_startSeconds);
    CPPUNIT_TEST(test_Timer_stop);
    CPPUNIT_TEST(test_Lock_inDispatchThread);
    CPPUNIT_TEST_SUITE_END();

  public:
    string exceptionMessage;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable* logEnabler;
    int pipeFds[2];

    DispatchTest()
        : exceptionMessage()
        , sys(NULL)
        , savedSyscall(NULL)
        , logEnabler(NULL)
    { }

    void setUp() {
        exceptionMessage = "no exception";
        if (!localLog) {
            localLog = new string;
        }
        localLog->clear();
        Dispatch::reset();
        Dispatch::setDispatchThread();
        Dispatch::currentTime = 0;
        sys = new MockSyscall();
        savedSyscall = Dispatch::sys;
        Dispatch::sys = sys;
        logEnabler = new TestLog::Enable();
        CPPUNIT_ASSERT_EQUAL(0, pipe(pipeFds));
    }

    void tearDown() {
        delete sys;
        sys = NULL;
        Dispatch::sys = savedSyscall;
        close(pipeFds[0]);
        close(pipeFds[1]);
        Dispatch::reset();
        Dispatch::setDispatchThread();
    }

    // Calls Dispatch::poll repeatedly until either it returns true
    // or a given amount of time has elapsed.  Returns "ok" if
    // Dispatch::poll actually did something, "no dispatch activity"
    // if nothing happened
    const char* waitForPollSuccess(double timeoutSeconds) {
        uint64_t start = rdtsc();
        while (!Dispatch::poll()) {
            usleep(1000);
            if (cyclesToSeconds(rdtsc() - start) > timeoutSeconds)
                return "no dispatch activity";
        }
        return "ok";
    }

    // Waits for a file to become ready, but gives up after a given
    // elapsed time.
    void waitForReadyFd(double timeoutSeconds) {
        uint64_t start = rdtsc();
        while (Dispatch::readyFd < 0) {
            usleep(1000);
            if (cyclesToSeconds(rdtsc() - start) > timeoutSeconds)
                return;
        }
    }

    static string log;

    // The following test exercises most of the functionality related to
    // pollers (creation, deletion, invocation).
    void test_Poller_basics() {
        DummyPoller p1("p1", 1000);
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("poller p1 invoked", *localLog);
        {
            DummyPoller p2("p2", 1000);
            DummyPoller p3("p3", 0);
            localLog->clear();
            CPPUNIT_ASSERT_EQUAL(true, Dispatch::poll());
            CPPUNIT_ASSERT_EQUAL("poller p1 invoked; poller p2 invoked; "
                    "poller p3 invoked", *localLog);
        }
        localLog->clear();
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("poller p1 invoked", *localLog);
    }

    void test_Poller_destructor_updateSlot() {
        DummyPoller* p1 = new DummyPoller("p1", 0);
        DummyPoller* p2 = new DummyPoller("p2", 0);
        DummyPoller p3("p3", 0);
        CPPUNIT_ASSERT_EQUAL(2, p3.slot);
        delete p2;
        CPPUNIT_ASSERT_EQUAL(1, p3.slot);
        delete p1;
        CPPUNIT_ASSERT_EQUAL(0, p3.slot);
    }

    void test_Poller_reentrant() {
        DummyPoller p1("p1", 0);
        DummyPoller *p2 = new DummyPoller("p2", 0);
        p2->deleteWhenInvoked(p2);
        p2->deleteWhenInvoked(new DummyPoller("p3", 0));
        p2->deleteWhenInvoked(new DummyPoller("p4", 0));
        Dispatch::poll();
        CPPUNIT_ASSERT_EQUAL("poller p1 invoked; poller p2 invoked",
                *localLog);
    }
    

    // Helper function that runs in a separate thread for the following test.
    static void lockTestThread() {
        Dispatch::setDispatchThread();
        while (Dispatch::isDispatchThread())
            Dispatch::poll();
    }

    void test_poll_locking() {
        Tub<Dispatch::Lock> lock;
        CountPoller* counter = new CountPoller();
        boost::thread thread(lockTestThread);

        // Wait for the child thread to start up and enter its polling loop.
        for (int i = 0; (counter->count == 0) && (i < 1000); i++) {
            usleep(100);
        }
        CPPUNIT_ASSERT(counter->count != 0);

        // Create a lock and make sure that the dispatcher stops (e.g. make
        // sure that pollers aren't being invoked).
        lock.construct();
        CPPUNIT_ASSERT_EQUAL(1, Dispatch::lockNeeded.load());
        CPPUNIT_ASSERT_EQUAL(1, Dispatch::locked.load());
        int oldCount = counter->count;
        usleep(1000);
        CPPUNIT_ASSERT_EQUAL(0, counter->count - oldCount);

        // Delete the lock and make sure that the dispatcher starts working
        // again.
        lock.destroy();
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::locked.load());
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::lockNeeded.load());
        for (int i = 0; (counter->count == oldCount) && (i < 1000); i++) {
            usleep(100);
        }
        CPPUNIT_ASSERT(counter->count > oldCount);

        Dispatch::setDispatchThread();
        delete counter;
        thread.join();
    }

    void test_poll_fileHandling() {
        DummyFile *f = new DummyFile("f1", true, pipeFds[0],
                Dispatch::FileEvent::READABLE);
        Dispatch::fileInvocationSerial = -2;

        // No event on file.
        usleep(5000);
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("", *localLog);
        write(pipeFds[1], "0123456789abcdefghijklmnop", 26);

        // File ready.
        CPPUNIT_ASSERT_EQUAL("ok", waitForPollSuccess(1.0));
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read '0123456789'", *localLog);
        CPPUNIT_ASSERT_EQUAL(-1, f->lastInvocationId);
        localLog->clear();

        // File is still ready; make sure event re-enabled.
        CPPUNIT_ASSERT_EQUAL("ok", waitForPollSuccess(1.0));
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read 'abcdefghij'", *localLog);
        CPPUNIT_ASSERT_EQUAL(1, f->lastInvocationId);
        localLog->clear();
        delete f;

        // File still ready, but object has been deleted.
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("", *localLog);
    }

    void test_poll_fileDeletedDuringInvocation() {
        int fds[2];
        pipe(fds);
        DummyFile *f = new DummyFile("f1", false, fds[1],
                Dispatch::FileEvent::WRITABLE);
        f->deleteThis = true;
        Dispatch::fileInvocationSerial = 400;
        CPPUNIT_ASSERT_EQUAL("ok", waitForPollSuccess(1.0));
        // If poll tried to reenable the event it would have thrown an
        // exception since the handler also closed the file descriptor.
        // Just to double-check, wait a moment and make sure the
        // fd doesn't appear in readyFd.
        usleep(5000);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
        close(fds[0]);
    }

    void test_poll_dontEvenCheckTimers() {
        DummyTimer t1("t1");
        t1.startCycles(50);
        mockTSCValue = 100;
        Dispatch::earliestTriggerTime = 101;
        Dispatch::poll();
        CPPUNIT_ASSERT_EQUAL("", *localLog);
        Dispatch::earliestTriggerTime = 0;
        Dispatch::poll();
        CPPUNIT_ASSERT_EQUAL("timer t1 invoked", *localLog);
    }

    void test_poll_triggerTimers() {
        DummyTimer t1("t1"), t2("t2"), t3("t3"), t4("t4");
        t1.startCycles(50);
        t2.startCycles(60);
        t3.startCycles(80);
        t4.startCycles(70);
        mockTSCValue = 75;
        CPPUNIT_ASSERT_EQUAL(true, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("timer t1 invoked; timer t4 invoked; "
                    "timer t2 invoked", *localLog);
        CPPUNIT_ASSERT_EQUAL(80, Dispatch::earliestTriggerTime);
    }

    void test_handleEvent() {
        DummyPoller p1("p1", 2);
        Dispatch::handleEvent();
        CPPUNIT_ASSERT_EQUAL("poller p1 invoked; poller p1 invoked; "
                "poller p1 invoked", *localLog);
    }

    void test_reset() {
        DummyPoller* p1 = new DummyPoller("p1", 0);
        DummyPoller* p2 = new DummyPoller("p2", 0);
        DummyTimer* t1 = new DummyTimer("t1", 100);
        DummyTimer* t2 = new DummyTimer("t2", 200);
        int fds[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(fds));
        DummyFile* f1 = new DummyFile("f1", false, fds[0],
                Dispatch::FileEvent::READABLE);
        Dispatch::reset();
        Dispatch::setDispatchThread();
        close(fds[0]);
        close(fds[1]);
        CPPUNIT_ASSERT_EQUAL(-1, p1->slot);
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::pollers.size());
        CPPUNIT_ASSERT_EQUAL(-1, t2->slot);
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::timers.size());
        CPPUNIT_ASSERT_EQUAL(0, f1->active);
        CPPUNIT_ASSERT_EQUAL(0, f1->event);
        CPPUNIT_ASSERT_EQUAL(NULL, Dispatch::files[f1->fd]);
        delete p1;
        delete p2;
        delete t1;
        delete t2;
        delete f1;
        Dispatch::reset();
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::isDispatchThread());
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::epoch);
    }

    // Helper function that runs in a separate thread for the following test.

    static void childThread(bool* result) {
        *result = Dispatch::isDispatchThread();
        Dispatch::setDispatchThread();
    }

    void test_setDispatchThread() {
        bool isDispatchThread;
        Dispatch::reset();
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::isDispatchThread());
        Dispatch::setDispatchThread();
        CPPUNIT_ASSERT_EQUAL(true, Dispatch::isDispatchThread());
        boost::thread thread(childThread, &isDispatchThread);
        thread.join();
        CPPUNIT_ASSERT_EQUAL(false, isDispatchThread);
        CPPUNIT_ASSERT_EQUAL(2, Dispatch::epoch);
    }

    void test_File_constructor_errorInEpollCreate() {
        sys->epollCreateErrno = EPERM;
        try {
            DummyFile f1("f1", false, pipeFds[0]);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("epoll_create failed in Dispatch: "
                "Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_errorCreatingExitPipe() {
        sys->pipeErrno = EPERM;
        try {
            DummyFile f1("f1", false, pipeFds[0]);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Dispatch couldn't create exit pipe for "
                "epoll thread: Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_errorInEpollCtl() {
        sys->epollCtlErrno = EPERM;
        try {
            DummyFile f1("f1", false, pipeFds[0]);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Dispatch couldn't set epoll event for "
                "exit pipe: Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_createPollingThread() {
        CPPUNIT_ASSERT(!Dispatch::epollThread);
        DummyFile f1("f1", false, pipeFds[0]);
        CPPUNIT_ASSERT(Dispatch::epollThread);
    }

    void test_File_constructor_growFileTable() {
        uint32_t fd = 100;
        if (fd < Dispatch::files.size()) {
            fd = downCast<uint32_t>(Dispatch::files.size()) + 10;
        }
        DummyFile f1("f1", false, fd);
        CPPUNIT_ASSERT_EQUAL(2*fd, Dispatch::files.size());
    }

    void test_File_constructor_twoHandlersForSameFd() {
        try {
            DummyFile f1("f1", false, pipeFds[0]);
            DummyFile f2("f2", false, pipeFds[0]);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("can't have more than 1 Dispatch::File "
                "for a file descriptor", exceptionMessage);
    }

    void test_File_destructor_disableEvent() {
        DummyFile* f1 = new DummyFile("f1", true, pipeFds[0],
                Dispatch::FileEvent::READABLE);

        // First make sure that the handler responds to data written
        // to the pipe.
        CPPUNIT_ASSERT_EQUAL(1, write(pipeFds[1], "x", 1));
        Dispatch::handleEvent();
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read 'x'", *localLog);

        // Now delete the handler, and make sure that data in the pipe
        // is ignored.
        delete f1;
        usleep(5000);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
        CPPUNIT_ASSERT_EQUAL(1, write(pipeFds[1], "y", 1));
        usleep(5000);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
    }

    void test_File_checkInvocationId() {
        DummyFile f("f", false, 22);
        f.invocationId = 99;
        try {
            sys->epollCtlErrno = EPERM;
            f.setEvent(Dispatch::FileEvent::READABLE);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("no exception", exceptionMessage);
        CPPUNIT_ASSERT_EQUAL(Dispatch::FileEvent::READABLE, f.event);
    }

    // Utility method: create a DummyFile for a particular file descriptor
    // and see if it gets invoked.
    bool checkReady(int fd, Dispatch::FileEvent event) {
        // Flush any stale events.
        while (Dispatch::poll()) {
            /* Empty loop body */
        }
        localLog->clear();
        DummyFile f("f1", false, fd, event);
        usleep(5000);
        while (Dispatch::poll()) {
            /* Empty loop body */
        }
        return localLog->size() > 0;
    }

    void test_File_setEvent_variousEvents() {
        // Create a pipe that is ready for reading but not writing, and
        // make sure all the correct events fire.
        int readable[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(readable));
        // Fill the pipe to the point where writes would block.
        CPPUNIT_ASSERT_EQUAL(0, fcntl(readable[1], F_SETFL, O_NONBLOCK));
        for (int i = 0; ; i++) {
            char buffer[1000] = "abcdefg";
            ssize_t count = write(readable[1], buffer, 1000);
            if (count < 0) {
                CPPUNIT_ASSERT_EQUAL(EAGAIN, errno);
                break;
            }
            CPPUNIT_ASSERT(i < 100);
        }
        CPPUNIT_ASSERT_EQUAL(false, checkReady(readable[0],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(true, checkReady(readable[0],
            Dispatch::FileEvent::READABLE));
        CPPUNIT_ASSERT_EQUAL(true, checkReady(readable[0],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(readable[1],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(readable[1],
            Dispatch::FileEvent::WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(readable[1],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        close(readable[0]);
        close(readable[1]);

        // Now, create a pipe that is ready for writing but not reading,
        // and make sure all the correct events fire.
        int writable[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(writable));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(writable[0],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(writable[0],
            Dispatch::FileEvent::READABLE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(writable[0],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, checkReady(writable[1],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(true, checkReady(writable[1],
            Dispatch::FileEvent::WRITABLE));
        CPPUNIT_ASSERT_EQUAL(true, checkReady(writable[1],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        close(writable[0]);
        close(writable[1]);
    }

    void test_File_setEvent_errorInEpollCtl() {
        DummyFile f("f", false, 22);
        try {
            sys->epollCtlErrno = EPERM;
            f.setEvent(Dispatch::FileEvent::READABLE);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Dispatch couldn't set epoll event for fd "
                "22: Operation not permitted", exceptionMessage);
    }

    void test_epollThreadMain_errorsInEpollWait() {
        epoll_event event;
        sys->epollWaitCount = 0;
        sys->epollWaitEvents = &event;
        sys->epollWaitErrno = EPERM;
        Dispatch::epollThreadMain();
        CPPUNIT_ASSERT_EQUAL("epollThreadMain: epoll_wait returned no "
                "events in Dispatch::epollThread | epollThreadMain: "
                "epoll_wait failed in Dispatch::epollThread: Operation "
                "not permitted", TestLog::get());
    }

    static void epollThreadWrapper() {
        Dispatch::epollThreadMain();
        *localLog = "epoll thread finished";
    }

    void test_epollThreadMain_signalEventsAndExit() {
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
        Dispatch::readyFd = -1;
        boost::thread(epollThreadWrapper).detach();
        waitForReadyFd(1.0);
        CPPUNIT_ASSERT_EQUAL(43, Dispatch::readyFd);

        // The polling thread should already be waiting on readyFd,
        // so clearing it should cause another fd to appear immediately.
        Dispatch::readyFd = -1;
        waitForReadyFd(1.0);
        CPPUNIT_ASSERT_EQUAL(19, Dispatch::readyFd);

        // Let the polling thread see the next ready file, which should
        // cause it to exit.
        Dispatch::readyFd = -1;
        usleep(5000);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
        CPPUNIT_ASSERT_EQUAL("epoll thread finished", *localLog);
    }

    void test_Timer_constructorDestructor() {
        DummyTimer* t1 = new DummyTimer("t1");
        DummyTimer* t2 = new DummyTimer("t2", 100);
        CPPUNIT_ASSERT_EQUAL(1, Dispatch::timers.size());
        CPPUNIT_ASSERT_EQUAL(-1, t1->slot);
        CPPUNIT_ASSERT_EQUAL(0, t2->slot);
        CPPUNIT_ASSERT_EQUAL(100, t2->triggerTime);
        delete t1;
        delete t2;
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::timers.size());
    }

    // Make sure that a timer can safely be deleted from a timer
    // handler.
    void test_Timer_reentrant() {
        DummyTimer t1("t1", 500);
        DummyTimer* t2 = new DummyTimer("t2", 100);
        t2->deleteWhenInvoked(t2);
        t2->deleteWhenInvoked(new DummyTimer("t3"));
        t2->deleteWhenInvoked(new DummyTimer("t4"));
        mockTSCValue = 200;
        Dispatch::poll();
        CPPUNIT_ASSERT_EQUAL("timer t2 invoked", *localLog);
        CPPUNIT_ASSERT_EQUAL(1, Dispatch::timers.size());
    }

    void test_isRunning() {
        DummyTimer t1("t1");
        CPPUNIT_ASSERT_EQUAL(0, t1.isRunning());
        t1.startCycles(100);
        CPPUNIT_ASSERT_EQUAL(1, t1.isRunning());
        t1.stop();
        CPPUNIT_ASSERT_EQUAL(0, t1.isRunning());
    }

    void test_Timer_startCycles() {
        DummyTimer t1("t1");
        Dispatch::earliestTriggerTime = 100;
        t1.startCycles(110);
        CPPUNIT_ASSERT_EQUAL(110, t1.triggerTime);
        CPPUNIT_ASSERT_EQUAL(0, t1.slot);
        CPPUNIT_ASSERT_EQUAL(100, Dispatch::earliestTriggerTime);
        t1.startCycles(90);
        CPPUNIT_ASSERT_EQUAL(90, Dispatch::earliestTriggerTime);
    }

    void test_Timer_startMicros() {
        cyclesPerSec = 2000000000;
        DummyTimer t1("t1");
        t1.startMicros(15);
        CPPUNIT_ASSERT_EQUAL(30000, t1.triggerTime);
    }

    void test_Timer_startMillis() {
        cyclesPerSec = 2000000000;
        DummyTimer t1("t1");
        t1.startMillis(6);
        CPPUNIT_ASSERT_EQUAL(12000000, t1.triggerTime);
    }

    void test_Timer_startSeconds() {
        cyclesPerSec = 2000000000;
        DummyTimer t1("t1");
        t1.startSeconds(3);
        CPPUNIT_ASSERT_EQUAL(6000000000, t1.triggerTime);
    }

    void test_Timer_stop() {
        DummyTimer t1("t1", 100);
        DummyTimer t2("t2", 100);
        DummyTimer t3("t3", 100);
        CPPUNIT_ASSERT_EQUAL(0, t1.slot);
        t1.stop();
        CPPUNIT_ASSERT_EQUAL(-1, t1.slot);
        CPPUNIT_ASSERT_EQUAL(2, Dispatch::timers.size());
        t1.stop();
        CPPUNIT_ASSERT_EQUAL(-1, t1.slot);
        CPPUNIT_ASSERT_EQUAL(2, Dispatch::timers.size());
    }

    void test_Lock_inDispatchThread() {
        // Creating a lock shouldn't stop the Dispatcher from polling.
        DummyPoller p1("p1", 1000);
        Dispatch::Lock lock;
        Dispatch::poll();
        CPPUNIT_ASSERT_EQUAL("poller p1 invoked", *localLog);
    }

    // No need to test the case of locking from a non-dispatch thread: this is
    // already handled by the test case test_poll_locking.

  private:
    DISALLOW_COPY_AND_ASSIGN(DispatchTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(DispatchTest);

}  // namespace RAMCloud
