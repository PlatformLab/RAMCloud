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

#include "Buffer.h"
#include "Client.h"
#include "Cycles.h"
#include "SessionAlarm.h"

namespace RAMCloud {

/**
 * Constructor for SessionAlarm objects.
 * \param timer
 *      Shared structure that will manage this alarm.  Usually comes from
 *      Context::get().sessionAlarmTimer.
 * \param session
 *      The transport session to monitor in this alarm.  Any RPCs in this
 *      session should result in calls to rpcStarted and rpcFinished.
 * \param timeoutMs
 *      If this many milliseconds elapse in an RPC with no sign of life from
 *      from the server, then the session will be aborted.  After half this
 *      time has elapsed we will send a ping RPC to the server; as long as
 *      it responds to the pings there will be no abort.
 */
SessionAlarm::SessionAlarm(SessionAlarmTimer& timer,
        Transport::Session& session,
        int timeoutMs)
    : session(session)
    , timer(timer)
    , timerIndex(0)
    , outstandingRpcs(0)
    , waitingForResponseMs(0)
    , pingMs(timeoutMs/2)
    , abortMs(timeoutMs)
{
    // Because of estimation errors in SessionAlarmTimer, we need to enforce
    // a minimum threshold for pingMs.
    if (pingMs < 3*SessionAlarmTimer::TIMER_INTERVAL_MS) {
        pingMs = 3*SessionAlarmTimer::TIMER_INTERVAL_MS;
        abortMs = 2*pingMs;
    }
}

/**
 * Destructor for SessionAlarm objects.
 */
SessionAlarm::~SessionAlarm()
{
    while (outstandingRpcs > 0) {
        rpcFinished();
    }
}

/**
 * This method is invoked whenever an RPC is initiated on the session
 * associated with this alarm.  As long as there are outstanding RPCs
 * for the session, we will make sure that either (a) RPCs are completing
 * or (b) the server is capable of receiving and responding to ping
 * requests (which effectively makes (a) true).  If a long period of time
 * goes by without either of these conditions being satisfied, then the
 * abort method is invoked on the session.
 */
void
SessionAlarm::rpcStarted()
{
    outstandingRpcs++;
    if (outstandingRpcs == 1) {
        timerIndex = timer.activeAlarms.size();
        timer.activeAlarms.push_back(this);
        if (timerIndex == 0) {
            // Before now there were no active alarms, so make sure the
            // timer is running.

            // Note: in some situations dispatch->currentTime may be stale
            // but this is should be OK; it will simply result in an extra
            // timer wakeup, resulting in waitingForResponseMs overestimating
            // by up to TIMER_INTERVAL_MS.  This approach saves the time of
            // reading the clock every time an RPC starts.
            timer.start(timer.owner->currentTime +
                    timer.timerIntervalTicks);
        }
    }
}

/**
 * This method must be invoked whenever an RPC completes on the session
 * associated with this alarm.
 */
void
SessionAlarm::rpcFinished()
{
    outstandingRpcs--;
    if (outstandingRpcs == 0) {
        assert(timerIndex < timer.activeAlarms.size());
        assert(timer.activeAlarms[timerIndex] = this);

        timer.activeAlarms[timerIndex] = timer.activeAlarms.back();
        timer.activeAlarms[timerIndex]->timerIndex = timerIndex;
        timer.activeAlarms.pop_back();

        // Note: we don't turn off the timer here, even if there are no
        // active RPCs.  Just let the timer fire, and it will turn itself
        // off if there are still no active RPCs.  However, it's pretty
        // likely that more RPCs will start soon, in which case we might
        // as well save the overhead of stopping and restarting the timer.
    }
    waitingForResponseMs = 0;
}

/**
 * Constructor for SessionAlarmTimer objects.
 */
SessionAlarmTimer::SessionAlarmTimer(Dispatch& dispatch)
    : Dispatch::Timer(dispatch)
    , activeAlarms()
    , timerIntervalTicks(Cycles::fromNanoseconds(TIMER_INTERVAL_MS * 1000000))
    , pings()
{
}

/**
 * Destructor for SessionAlarmTimer objects.
 */
SessionAlarmTimer::~SessionAlarmTimer()
{
    for (PingMap::iterator it = pings.begin(); it != pings.end(); it++) {
        it->second.rpc->cancel();
        delete it->second.request;
        delete it->second.response;
    }
    while (!activeAlarms.empty()) {
        activeAlarms[0]->rpcFinished();
    }
}

/**
 * This method is invoked by the dispatcher every TIMER_INTERVAL_MS when
 * there are active RPCs.  It scans all of the active sessions, checking
 * for slow server responses and issuing pings if needed to make sure that
 * the servers are still alive.
 */
void
SessionAlarmTimer::handleTimerEvent()
{
    foreach (SessionAlarm* alarm, activeAlarms) {
        alarm->waitingForResponseMs += TIMER_INTERVAL_MS;
        if (alarm->waitingForResponseMs < alarm->pingMs)
            continue;
        if (alarm->waitingForResponseMs > alarm->abortMs) {
            alarm->session.abort(format("server at %s is not responding",
                    alarm->session.getServiceLocator().c_str()));
            continue;
        }
        if (pings.find(alarm) != pings.end()) {
            // We have already sent a ping RPC for this alarm; no need to
            // send another.
            continue;
        }

        // It's time to initiate a ping RPC to make sure the server is still
        // alive.
        Buffer* request = new Buffer;
        Buffer* response = new Buffer;
        PingRpc::Request& reqHdr(Client::allocHeader<PingRpc>(*request));
        reqHdr.nonce = 12345;
        Transport::ClientRpc* rpc = alarm->session.clientSend(request,
                response);
        pings[alarm] = {rpc, request, response};
        RAMCLOUD_LOG(NOTICE, "initiated ping request to %s",
                alarm->session.getServiceLocator().c_str());
    }

    // Clean up ping RPCs that completed successfully.
    for (PingMap::iterator it = pings.begin(); it != pings.end(); ) {
        PingMap::iterator current = it;
        it++;
        if (current->second.rpc->isReady()) {
            try {
                current->second.rpc->wait();
                RAMCLOUD_LOG(NOTICE, "received ping response from %s",
                        current->first->session.getServiceLocator().c_str());
            }
            catch (TransportException& e) {
                RAMCLOUD_LOG(ERROR, "%s", e.message.c_str());
            }
            delete current->second.request;
            delete current->second.response;
            pings.erase(current);
        }
    }

    if (!activeAlarms.empty()) {
        // Reschedule this timer.
        start(owner->currentTime + timerIntervalTicks);
    }
}

} // namespace RAMCloud
