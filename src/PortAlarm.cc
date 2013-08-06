/* Copyright (c) 2011-2012 Stanford University
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

#include "Buffer.h"
#include "Cycles.h"
#include "PortAlarm.h"

namespace RAMCloud {

/**
 * Constructor for PortAlarm objects.
 *      PortAlarm watchdog timer is not started at the instance 
 *      creation. startPortTimer() should be called to start watchdog.
 * \param timer
 *      Shared structure that will manage this alarm. Usually comes from
 *      a Context object.
 * \param port
 *      The listening port to monitor in this alarm.
 *      Each watchdog timer is reset by PortAlarm::requestArrived().
 * \param timeoutMs
 *      If this many 2 x milliseconds elapse in with no request arriving
 *      to the port, then the port will be closed.
 */
PortAlarm::PortAlarm(PortAlarmTimer* timer, Transport::ServerPort *port,
                     int timeoutMs)
        : timer(timer)
        , port(port)
        , timerIndex(0)
        , portTimerRunning(false)
        , waitingForRequestMs(0)
        , closeMs(timeoutMs*2)
{
    // Because of estimation errors in PortAlarmTimer, we need to enforce
    // a minimum threshold for closeMs.

    //    closeMs suposed to be greater than  SessionAlarm::pingMs
    //    a minimum threshold for these are
    //    pingMs  = 3*SessionAlarmTimer::TIMER_INTERVAL_MS;
    //    abortMs = 2*pingMs;

    int minimumCloseMs = 2*3*PortAlarmTimer::TIMER_INTERVAL_MS;
    if (closeMs < minimumCloseMs) {
        closeMs = minimumCloseMs;
    }
    // Needs to start the port timer explicitly
}

/**
 * Destructor for PortAlarm objects.
 */
PortAlarm::~PortAlarm()
{
    stopPortTimer();
}

/**
 * Starts Port Timer.
 * Port Timer is not started at a PortAlarm instance creation.
 **/
void
PortAlarm::startPortTimer()
{
    // Check if PortAlarm::timer is instanciated, so that
    // we can stop portAlarm with commenting out
    // its instanciation in the Context constructor.
    if (timer != NULL && !portTimerRunning) {
        // Append this alarm at the end of activeAlarms.
        timerIndex = timer->activeAlarms.size();
        timer->activeAlarms.push_back(this);
        if (timerIndex == 0) {
            // Before now there were no active alarms, so make sure the
            // timer is running.

            // Note: in some situations dispatch->currentTime may be stale
            // but this is should be OK; it will simply result in an extra
            // timer wakeup, resulting in waitingForResponseMs overestimating
            // by up to TIMER_INTERVAL_MS.  This approach saves the time of
            // reading the clock every time an RPC starts.
            timer->start(timer->owner->currentTime +
                    timer->timerIntervalTicks);
        }
        portTimerRunning = true;
    }
    // reset watchdog timer count.
    waitingForRequestMs = 0;
}

/**
 * Helper Function to stop Port Timer.
 **/
void
PortAlarm::stopPortTimer()
{
    if (timer != NULL &&  portTimerRunning) {
        assert(timerIndex < timer->activeAlarms.size());
        assert(timer->activeAlarms[timerIndex] == this);

        // Deleting the element at timerIndex by
        // copying the tail element to timerIndex and deleting
        // the tail element
        timer->activeAlarms[timerIndex] = timer->activeAlarms.back();
        timer->activeAlarms[timerIndex]->timerIndex = timerIndex;
        timer->activeAlarms.pop_back();

        // Since shutdown of all the port watchdog is rare case,
        // we can turn off timer when all the port is closed.
        if (timer->activeAlarms.size() == 0) {
            timer->stop();
        }
        portTimerRunning = false;
    }
}

/**
 * Restart Port Watch Dog when a request has arrived
 */
void
PortAlarm::requestArrived()
{
    // Disable the following assertion check,
    // since portAlarm is temporally disabled.
    // assert(portTimerRunning);
    waitingForRequestMs = 0;
}

/**
 * Constructor for PortAlarmTimer objects.
 */
PortAlarmTimer::PortAlarmTimer(Context* context)
    : Dispatch::Timer(context->dispatch)
    , context(context)
    , activeAlarms()
    , timerIntervalTicks(Cycles::fromNanoseconds(TIMER_INTERVAL_MS * 1000000))
{
}

/**
 * Destructor for PortAlarmTimer objects.
 */
PortAlarmTimer::~PortAlarmTimer()
{
    while (!activeAlarms.empty()) {
        activeAlarms[0]->stopPortTimer();
    }
}

/**
 * This method is invoked by the dispatcher every TIMER_INTERVAL_MS when
 * there are any active ports. It scans all of the active ports of server,
 * checking whether each port receives at least one client request
 * within closeMs timeout.
 * If it does not, it destory the port.
 *
 * If the ping check is running on the client, the port will not be
 * closed even with smaller closeMs.
 *
 * In some ports such as the port for establishing new connection,
 * however, ping is not runing on the client.
 * The closeMs timeout needs to be large enough for the ports.
 */

void
PortAlarmTimer::handleTimerEvent()
{
    foreach (PortAlarm* alarm, activeAlarms) {
        if (alarm->portTimerRunning)
            alarm->waitingForRequestMs += TIMER_INTERVAL_MS;
        // Port listening Timeout
        if (alarm->waitingForRequestMs  > alarm->closeMs) {
            // fprintf(stderr, "alarm->port->getPortName() =%08l\n",
            //       const_cast<char *>(alarm->port->getPortName().c_str()));
            RAMCLOUD_LOG(WARNING,
                "Close server port %s after %d ms "
                "(listening timeout)",
                alarm->port->getPortName().c_str(),
                alarm->waitingForRequestMs);
            // Delete the port resources in close() function
            alarm->port->close();   // close this port
            continue;
        }
    }

    if (!activeAlarms.empty()) {
        // Reschedule this timer.
        start(owner->currentTime + timerIntervalTicks);
    }
}

} // namespace RAMCloud
