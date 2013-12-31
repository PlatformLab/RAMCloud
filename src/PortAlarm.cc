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
 */
PortAlarm::PortAlarm(PortAlarmTimer* timer, Transport::ServerPort *port)
        : timer(timer)
        , port(port)
        , timerIndex(0)
        , portTimerRunning(false)
        , idleMs(0)
{
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
 * Starts Port Timer:
 * Port Timer is not started at a PortAlarm instance creation.
 * With delaying start-up of the port timer, we can avoid
 * closing the port before corresponding client thread starts.
 *
 * In addition, we can intentionally start and stop porttimer
 * with these method.
 *
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
    idleMs = 0;
}

/**
 * stopPortTimer:
 *   Called in destructor.
 *   We can start and stop port timer with these methods.
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
    // assert(portTimerRunning);
    idleMs = 0;
}

/**
 * Constructor for PortAlarmTimer objects.
 */
PortAlarmTimer::PortAlarmTimer(Context* context)
    : Dispatch::Timer(context->dispatch)
    , context(context)
    , activeAlarms()
    , timerIntervalTicks(Cycles::fromNanoseconds(TIMER_INTERVAL_MS * 1000000))
    , portTimeoutMs(-1)  // portTimer disabled.
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
 * Use a unique port timeout value for all transports.
 *
 * \param timeoutMs
 *      Timeout period (in ms) to pass to transports.
 */
void PortAlarmTimer::setPortTimeout(int32_t timeoutMs)
{
    this->portTimeoutMs = (timeoutMs == 0) ?
            DEFAULT_PORT_TIMEOUT_MS : timeoutMs;
    RAMCLOUD_LOG(NOTICE, "Set PortTimeout to %d (ms: -1 to disable.)",
            this->portTimeoutMs);
}

/**
 * Return current timeout value (ms) for all server ports.
 *
 */
int32_t PortAlarmTimer::getPortTimeout() const
{
    return portTimeoutMs;
}

/**
 * This method is invoked by the dispatcher every TIMER_INTERVAL_MS when
 * there are any active ports. It scans all of the active ports of server,
 * checking whether each port receives at least one client request
 * within PortAlarmTimer::portTimeoutMs.
 * If it does not, it destory the port.
 *
 * While client RPC request is outstanding, pingRPC will be sent at
 * sessionTimeout periodically. However, if client has nothing to send
 * no request is sent by client.
 *
 */
void
PortAlarmTimer::handleTimerEvent()
{
    if (portTimeoutMs < 0) {
        return; // portAlarm disabled
    }
    foreach (PortAlarm* alarm, activeAlarms) {
        if (alarm->portTimerRunning)
            alarm->idleMs += TIMER_INTERVAL_MS;
        // Port listening Timeout
        if (alarm->idleMs  > portTimeoutMs) {
            // fprintf(stderr, "alarm->port->getPortName() =%08l\n",
            //       const_cast<char *>(alarm->port->getPortName().c_str()));
            RAMCLOUD_LOG(WARNING,
                "Close server port %s after %d ms "
                "(listening timeout)",
                alarm->port->getPortName().c_str(),
                alarm->idleMs);
            // Delete the port resources in close() function
            alarm->port->close();   // close this port
        }
    }

    if (!activeAlarms.empty()) {
        // Reschedule this timer.
        start(owner->currentTime + timerIntervalTicks);
    }
}

} // namespace RAMCloud
