/* Copyright (c) 2011-2013 Stanford University
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
#include "PortAlarm.h"
#include "Transport.h"

namespace RAMCloud {

// A Class pass to each unit test.
class PortAlarmTest : public ::testing::Test {
  public:
    static Context context;
    static PortAlarmTimer timer;

    PortAlarmTest()
    {
        // Allocate alarm port in the test.
    }

    ~PortAlarmTest()
    {
        Cycles::mockTscValue = 0;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(PortAlarmTest);
};
// Body of static instance.
Context PortAlarmTest::context;
PortAlarmTimer PortAlarmTest::timer(&context);

// The following class is used for testing.
//      port = new AlarmPort(&context,"TestPort1", &port);

class AlarmPort : public Transport::ServerPort {
  public:
    explicit AlarmPort(string name, AlarmPort** portPtr)
            : context(&PortAlarmTest::context)
            , alarm(&PortAlarmTest::timer, this)
            , portName(name)
            , portPtr(portPtr)
    {
        appendLog("AlarmPort: " + portName);
    }
    virtual void
    close()
    {
        appendLog("PortClosed: " + portName);
        *portPtr = NULL; // Record that this port is closed
        // alarm->stopPortTimer(); // Use delete for objects in the heap
        delete this;     // Suiside of this port with alarm
    }

    static void appendLog(string message)
    {
        if (log.length() != 0) {
            log.append(", ");
        }
        log.append(message);
    }

    const string getPortName() const
    {
        return portName;
    }

    Context* context;
    PortAlarm alarm;
    string portName;
    AlarmPort** portPtr; // pointer to this
    static string log;
    DISALLOW_COPY_AND_ASSIGN(AlarmPort);
};
// real body of static string AlarmPort::log
std::string AlarmPort::log;

TEST_F(PortAlarmTest, constructor_timeoutSet) {
    // Initial value is -1: PortTimer is disabled.
    EXPECT_EQ(-1, PortAlarmTest::timer.portTimeoutMs);

    // Set to Default
    timer.setPortTimeout(0);
    EXPECT_EQ(64000, timer.getPortTimeout()); // Testing getter.

    // Set to Specific value
    timer.setPortTimeout(50000);
    EXPECT_EQ(50000, timer.getPortTimeout()); // Testing getter.
}

TEST_F(PortAlarmTest, basics) {
    // Run a test that should produce a timeout several times, and make
    // sure that at least once a timeout occurs in a period that we'd
    // expect (scheduling glitches on the machine could cause timeouts
    // to occasionally take longer than this).
    TestLog::Enable _;
    // Set alarm timeout to 30ms
    timer.setPortTimeout(30);
    AlarmPort* port = new AlarmPort("TestPort1", &port);
    AlarmPort::log.clear();

    double elapsed = 0.0;
    // at least one timeout occur at 40ms
    double desired = .040;
    for (int i = 0; i < 10; i++) {
        port->alarm.startPortTimer();
        uint64_t start = Cycles::rdtsc();
        while (true) {
            elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
            if (elapsed > desired) {
                break; // watchdog timeout not occured in desired time.
            }
            context.dispatch->poll();
            if (port == NULL)
                break;
        }
        if (port == NULL) { // watchdog timeout occured, port deleted.
            EXPECT_EQ(
                "PortClosed: TestPort1", AlarmPort::log);
            break;
        }
    }
    EXPECT_GT(desired, elapsed);
    EXPECT_EQ(0U, timer.activeAlarms.size());
}

TEST_F(PortAlarmTest, requestArrived) {
    // Inherit 30ms for PortTimeout
    AlarmPort* port = new AlarmPort("TestPort1", &port);
    port->alarm.startPortTimer();
    EXPECT_EQ(port->alarm.idleMs, 0);
    port->alarm.idleMs = 80; // set to 80ms
    EXPECT_EQ(port->alarm.idleMs, 80);

    port->alarm.requestArrived();
    EXPECT_EQ(port->alarm.idleMs, 0);
    delete port;
}

TEST_F(PortAlarmTest, triple_alarm) {
    TestLog::Enable _;

    // Inherit 30ms for PortTimeout
    // 1st port
    AlarmPort* port1 = new AlarmPort("TestPort1", &port1);
    // 2nd port
    AlarmPort* port2 = new AlarmPort("TestPort2", &port2);
    // 3rd port
    AlarmPort* port3 = new AlarmPort("TestPort3", &port3);

    port1->alarm.startPortTimer();
    EXPECT_EQ(1U, timer.activeAlarms.size());
    port2->alarm.startPortTimer();
    EXPECT_EQ(2U, timer.activeAlarms.size());
    port3->alarm.startPortTimer();
    EXPECT_EQ(3U, timer.activeAlarms.size());

    // Let port1 timeout 10ms earlier..
    port1->alarm.idleMs = 10;
    port2->alarm.idleMs = 0;
    port3->alarm.idleMs = 0;

    // alarm.requestArrived(); // Restart Watchdog
    double elapsed = 0.0;
    double desired = .065; // at least all of them timeouts.
    for (int i = 0; i < 10; i++) {
        AlarmPort::log.clear();

        // reset elapsed time after each timer event orruerence.
        uint64_t start = Cycles::rdtsc();
        while (true) {
            elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
            if (elapsed > desired) {
                break; // watchdog timeout not occured in desired time.
            }
            context.dispatch->poll();

            // Any one of them timeout
            if (port1 == NULL || port2 == NULL || port3 == NULL)
                break;
        }
#define DONE (reinterpret_cast<AlarmPort *>(-1))
        switch (i) {
            case 0:
                EXPECT_TRUE(port1 == NULL && port2 && port3);
                port1 = DONE;
                EXPECT_TRUE(elapsed < 0.03); // in 30 - 10ms
                EXPECT_EQ(2U, timer.activeAlarms.size());
                EXPECT_EQ("PortClosed: TestPort1", AlarmPort::log);
                port3->alarm.requestArrived(); // restart port3 timer.
                break;
            case 1:
                EXPECT_TRUE(port1 == DONE && port2 == NULL && port3);
                port2 = DONE;
                EXPECT_EQ(1U, timer.activeAlarms.size());
                EXPECT_TRUE(elapsed > 0.005); // in 10ms
                EXPECT_TRUE(elapsed < 0.015);
                port3->alarm.requestArrived(); // restart port3 timer.
                EXPECT_EQ("PortClosed: TestPort2", AlarmPort::log);
                break;
            case 2:
                EXPECT_TRUE(port1 == DONE && port2 == DONE && port3 == NULL);
                port3 = DONE;

                EXPECT_EQ(0U, timer.activeAlarms.size());
                EXPECT_TRUE(elapsed < 0.035); // in 30ms
                EXPECT_TRUE(elapsed > 0.020);
                EXPECT_EQ("PortClosed: TestPort3", AlarmPort::log);
                break;
            case 3:
                EXPECT_TRUE(port1 == DONE && port2 == DONE && port3 == DONE);
                EXPECT_GT(elapsed, desired);
                EXPECT_EQ(0U, timer.activeAlarms.size());
                continue; // no timer should trigger anymore
            default:
                goto done;
                break;
        }
    }
 done:
    EXPECT_GT(elapsed, desired);

    // check log
    size_t curPos = 0; // Current Pos: given to getUntil()
    EXPECT_EQ("handleTimerEvent: Close server port TestPort1 after 35 ms"
              " (listening timeout) | "
              , TestLog::getUntil("handle", curPos, &curPos));
    EXPECT_EQ("handleTimerEvent: Close server port TestPort2 after 35 ms"
              " (listening timeout) | "
              , TestLog::getUntil("handle", curPos, &curPos));
    EXPECT_EQ("handleTimerEvent: Close server port TestPort3 after 35 ms"
              " (listening timeout)"
              , TestLog::getUntil("", curPos, &curPos)); // to the end
}

TEST_F(PortAlarmTest, destructor_cleanupPorts) {
    // Basically tested in basic test in close() call.
    Tub<PortAlarm> alarm;
    Transport::ServerPort port;
    PortAlarm* ap = alarm.construct(&timer, &port);
    ap->startPortTimer(); // Cleate alarm this entry in timer
    ap->startPortTimer(); // Appended to timer map only once.
    alarm.destroy(); // removes the entry from timer.

    EXPECT_EQ(0U, timer.activeAlarms.size());
}

TEST_F(PortAlarmTest, restart_portTimer) {
    AlarmPort* port1 = new AlarmPort("TestPort1", &port1);
    AlarmPort::log.clear();

    // Starts port1 timer
    port1->alarm.startPortTimer();
    EXPECT_EQ(1U, timer.activeAlarms.size());
    EXPECT_TRUE(port1->alarm.portTimerRunning);

    // Start another port timer
    AlarmPort* port2 = new AlarmPort("TestPort2", &port2);

    port2->alarm.startPortTimer();
    EXPECT_EQ(2U, timer.activeAlarms.size());
    EXPECT_TRUE(port2->alarm.portTimerRunning);

    double elapsed = 0.0;
    double desired = 0.013; // (s) = 13ms
    uint64_t start = Cycles::rdtsc();

    // wait 12ms, two poll events should occur.
    while (true) {
        elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
        if (elapsed > desired) {
            break;
        }
        context.dispatch->poll();
    }

    EXPECT_TRUE(port1); // watchdog timeout should not occur
    EXPECT_TRUE(port2); // watchdog timeout should not occur
    // waitingForRequestsMs is incremented every 5ms
    EXPECT_EQ(port1->alarm.idleMs, 10);
    EXPECT_EQ(port2->alarm.idleMs, 10);

    // Starts Port Timer Again: only the watchdog counter changes
    port1->alarm.startPortTimer();
    EXPECT_EQ(2U, timer.activeAlarms.size()); // should not change
    EXPECT_TRUE(port1->alarm.portTimerRunning);
    EXPECT_EQ(port1->alarm.idleMs, 0);
    // no change on alarm2
    EXPECT_EQ(port2->alarm.idleMs, 10);

    // Check the portname
    EXPECT_EQ(port1->portName, "TestPort1");
    EXPECT_EQ(port2->portName, "TestPort2");

    delete port1;
    delete port2;
    EXPECT_EQ(0U, timer.activeAlarms.size()); // all timer stopped.
}

// testing with AlarmPort instance allocated on the local stack.
// If close occurs, test breaks.
TEST_F(PortAlarmTest, handleTimerEvent_incrementResponseTime) {
    AlarmPort *ans1, *ans2;
    AlarmPort port1("TestPort1", &ans1);
    AlarmPort port2("TestPort2", &ans2);
    port1.alarm.startPortTimer();
    port2.alarm.startPortTimer();
    port2.alarm.idleMs = 5;

    timer.handleTimerEvent();
    EXPECT_EQ(5,  port1.alarm.idleMs);
    EXPECT_EQ(10, port2.alarm.idleMs);
    EXPECT_EQ(2U, timer.activeAlarms.size());
    port1.alarm.stopPortTimer();
    port2.alarm.stopPortTimer();
    EXPECT_EQ(0U, timer.activeAlarms.size()); // all timer stopped
}

TEST_F(PortAlarmTest, handleTimerEvent_restartTimer) {
    AlarmPort* port = new AlarmPort("TestPort1", &port);
    // set timeout to 20000 ms
    timer.setPortTimeout(20000);

    // If mockTscValue is defined, rdtsc() returns the value
    // in unit test by reading the cpu cycle counter.
    Cycles::mockTscValue = 1000;
    PortAlarmTest::timer.timerIntervalTicks = 100UL;
    // Update curretTime to mockTscValue
    timer.owner->currentTime = Cycles::mockTscValue;
    port->alarm.startPortTimer();

    EXPECT_EQ(0, port->alarm.idleMs);
    context.dispatch->poll();

    Cycles::mockTscValue = 2000;
    // context.dispatch->poll();
    context.dispatch->poll();
    EXPECT_TRUE(timer.isRunning()); // Inherited from Dispach
    EXPECT_EQ(5, port->alarm.idleMs);

    Cycles::mockTscValue = 3000;
    EXPECT_EQ(PortAlarmTest::timer.timerIntervalTicks, 100UL);
    port->alarm.stopPortTimer();
    context.dispatch->poll();
    EXPECT_FALSE(timer.isRunning());
}

}  // namespace RAMCloud
