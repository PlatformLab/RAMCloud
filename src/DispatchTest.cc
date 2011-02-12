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
            int count = read(fd, buffer, sizeof(buffer) - 1);
            CPPUNIT_ASSERT(count > 0);
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
    CPPUNIT_TEST(test_poll_fileHandling);
    CPPUNIT_TEST(test_poll_fileDeletedDuringInvocation);
    CPPUNIT_TEST(test_poll_dontEvenCheckTimers);
    CPPUNIT_TEST(test_poll_triggerTimers);
    CPPUNIT_TEST(test_handleEvent);
    CPPUNIT_TEST(test_reset);
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
    CPPUNIT_TEST_SUITE_END();

  public:
    string exceptionMessage;
    MockSyscall* sys;
    Syscall *savedSyscall;
    TestLog::Enable* logEnabler;

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
        Dispatch::currentTime = 0;
        sys = new MockSyscall();
        savedSyscall = Dispatch::sys;
        Dispatch::sys = sys;
        logEnabler = new TestLog::Enable();
    }

    void tearDown() {
        delete sys;
        sys = NULL;
        Dispatch::sys = savedSyscall;
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

    void test_poll_fileHandling() {
        int fds[2];
        pipe(fds);
        DummyFile *f = new DummyFile("f1", true, fds[0],
                Dispatch::FileEvent::READABLE);
        Dispatch::fileInvocationSerial = -2;
        sleepMs(5);

        // No event on file.
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("", *localLog);
        write(fds[1], "0123456789abcdefghijklmnop", 26);
        sleepMs(5);

        // File ready.
        CPPUNIT_ASSERT_EQUAL(true, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read '0123456789'", *localLog);
        CPPUNIT_ASSERT_EQUAL(-1, f->lastInvocationId);
        localLog->clear();
        sleepMs(5);

        // File is still ready; make sure event re-enabled.
        CPPUNIT_ASSERT_EQUAL(true, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read 'abcdefghij'", *localLog);
        CPPUNIT_ASSERT_EQUAL(1, f->lastInvocationId);
        localLog->clear();
        delete f;

        // File still ready, but object has been deleted.
        CPPUNIT_ASSERT_EQUAL(false, Dispatch::poll());
        CPPUNIT_ASSERT_EQUAL("", *localLog);
        close(fds[0]);
        close(fds[1]);
    }

    void test_poll_fileDeletedDuringInvocation() {
        int fds[2];
        pipe(fds);
        DummyFile *f = new DummyFile("f1", false, fds[1],
                Dispatch::FileEvent::WRITABLE);
        f->deleteThis = true;
        Dispatch::fileInvocationSerial = 400;
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(true, Dispatch::poll());
        // If poll tried to reenable the event it would have thrown an
        // exception since the handler also closed the file descriptor.
        // Just to double-check, wait a moment and make sure the
        // fd doesn't appear in readyFd.
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
        // The following check is technically unsafe (since f has been
        // deleted); it can be removed if it causes complaints from program
        // checkers such as valgrind.
        CPPUNIT_ASSERT_EQUAL(401, f->invocationId);
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
        DummyPoller p1("p1", 0), p2("p2", 0);
        DummyTimer t1("t1", 100), t2("t2", 200);
        int fds[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(fds));
        DummyFile f1("f1", false, fds[0], Dispatch::FileEvent::READABLE);
        Dispatch::reset();
        close(fds[0]);
        close(fds[1]);
        CPPUNIT_ASSERT_EQUAL(-1, p1.slot);
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::pollers.size());
        CPPUNIT_ASSERT_EQUAL(-1, t2.slot);
        CPPUNIT_ASSERT_EQUAL(0, Dispatch::timers.size());
        CPPUNIT_ASSERT_EQUAL(0, f1.active);
        CPPUNIT_ASSERT_EQUAL(0, f1.event);
        CPPUNIT_ASSERT_EQUAL(NULL, Dispatch::files[f1.fd]);
    }

    void test_File_constructor_errorInEpollCreate() {
        sys->epollCreateErrno = EPERM;
        try {
            DummyFile f1("f1", false, 0);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("epoll_create failed in Dispatch: "
                "Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_errorCreatingExitPipe() {
        sys->pipeErrno = EPERM;
        try {
            DummyFile f1("f1", false, 0);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Dispatch couldn't create exit pipe for "
                "epoll thread: Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_errorInEpollCtl() {
        sys->epollCtlErrno = EPERM;
        try {
            DummyFile f1("f1", false, 0);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("Dispatch couldn't set epoll event for "
                "exit pipe: Operation not permitted", exceptionMessage);
    }

    void test_File_constructor_createPollingThread() {
        CPPUNIT_ASSERT_EQUAL(NULL, Dispatch::epollThread);
        DummyFile f1("f1", false, 0);
        CPPUNIT_ASSERT(Dispatch::epollThread != NULL);
    }

    void test_File_constructor_growFileTable() {
        uint32_t fd = 100;
        if (fd < Dispatch::files.size()) {
            fd = Dispatch::files.size() + 10;
        }
        DummyFile f1("f1", false, fd);
        CPPUNIT_ASSERT_EQUAL(2*fd, Dispatch::files.size());
    }

    void test_File_constructor_twoHandlersForSameFd() {
        try {
            DummyFile f1("f1", false, 0);
            DummyFile f2("f2", false, 0);
        } catch (FatalError& e) {
            exceptionMessage = e.message;
        }
        CPPUNIT_ASSERT_EQUAL("can't have more than 1 Dispatch::File "
                "for a file descriptor", exceptionMessage);
    }

    void test_File_destructor_disableEvent() {
        int fds[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(fds));
        DummyFile* f1 = new DummyFile("f1", true, fds[0],
                Dispatch::FileEvent::READABLE);

        // First make sure that the handler responds to data written
        // to the pipe.
        CPPUNIT_ASSERT_EQUAL(1, write(fds[1], "x", 1));
        Dispatch::handleEvent();
        CPPUNIT_ASSERT_EQUAL("file f1 invoked, read 'x'", *localLog);

        // Now delete the handler, and make sure that data in the pipe
        // is ignored.
        delete f1;
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
        CPPUNIT_ASSERT_EQUAL(1, write(fds[1], "y", 1));
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);
    }

    bool testReady(int fd, Dispatch::FileEvent event) {
        // Flush any stale events.
        while (Dispatch::poll()) {
            /* Empty loop body */
        }
        localLog->clear();
        DummyFile f("f1", false, fd, event);
        sleepMs(5);
        while (Dispatch::poll()) {
            /* Empty loop body */
        }
        return localLog->size() > 0;
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
        CPPUNIT_ASSERT_EQUAL(false, testReady(readable[0],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(true, testReady(readable[0],
            Dispatch::FileEvent::READABLE));
        CPPUNIT_ASSERT_EQUAL(true, testReady(readable[0],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(readable[1],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(readable[1],
            Dispatch::FileEvent::WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(readable[1],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        close(readable[0]);
        close(readable[1]);

        // Now, create a pipe that is ready for writing but not reading,
        // and make sure all the correct events fire.
        int writable[2];
        CPPUNIT_ASSERT_EQUAL(0, pipe(writable));
        CPPUNIT_ASSERT_EQUAL(false, testReady(writable[0],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(writable[0],
            Dispatch::FileEvent::READABLE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(writable[0],
            Dispatch::FileEvent::READABLE_OR_WRITABLE));
        CPPUNIT_ASSERT_EQUAL(false, testReady(writable[1],
            Dispatch::FileEvent::NONE));
        CPPUNIT_ASSERT_EQUAL(true, testReady(writable[1],
            Dispatch::FileEvent::WRITABLE));
        CPPUNIT_ASSERT_EQUAL(true, testReady(writable[1],
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
        // * Using epollMutex to synchronize with the poll loop.
        // * Exiting when fd -1 is seen.
        epoll_event events[3];
        events[0].data.fd = 43;
        events[1].data.fd = 19;
        events[2].data.fd = -1;
        sys->epollWaitEvents = events;
        sys->epollWaitCount = 3;

        boost::mutex mutex;
        mutex.lock();
        sys->epollWaitMutex = &mutex;

        // Start up the polling thread; it will hang in epoll_wait.
        Dispatch::readyFd = -1;
        boost::thread(epollThreadWrapper).detach();
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);

        // Allow epoll_wait to complete; the polling thread should now
        // signal the first ready file.
        mutex.unlock();
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(43, Dispatch::readyFd);

        // The polling thread should now be waiting on epollMutex, so
        // clearing readyFd should have no impact.
        Dispatch::readyFd = -1;
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(-1, Dispatch::readyFd);

        // Unlock epollMutex so the polling thread can signal the next
        // ready file.
        Dispatch::epollMutex.unlock();
        sleepMs(5);
        CPPUNIT_ASSERT_EQUAL(19, Dispatch::readyFd);

        // Let the polling thread see the next ready file, which should
        // cause it to exit.
        Dispatch::readyFd = -1;
        Dispatch::epollMutex.unlock();
        sleepMs(5);
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

  private:
    DISALLOW_COPY_AND_ASSIGN(DispatchTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(DispatchTest);

}  // namespace RAMCloud
