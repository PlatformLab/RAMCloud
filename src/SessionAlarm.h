/* Copyright (c) 2011-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * This file defines classes that are used by several of the RAMCloud
 * transports to detect when RPCs are hung because of connection or server
 * problems.  In order for transport to use this file it must support
 * multiple concurrent RPCs on a single session (so we can send a ping
 * request at the same time that another request is outstanding).
 */

#ifndef RAMCLOUD_SESSIONALARM_H
#define RAMCLOUD_SESSIONALARM_H

#include <unordered_map>
#include "Dispatch.h"
#include "RpcWrapper.h"
#include "Transport.h"

namespace RAMCloud {
class SessionAlarmTimer;

/**
 * One SessionAlarm object is created in each transport session object; it
 * keeps track of outstanding RPCs on that session, detects when RPCs are
 * taking an abnormally long time to complete, and issues ping requests
 * when this happens to make sure that the server is up and responsive.
 * It's OK for RPCs to take arbitrarily long, as long as the server appears
 * responsive.
 */
class SessionAlarm {
  public:
    SessionAlarm(SessionAlarmTimer* timer, Transport::Session* session,
            int timeoutMs);
    ~SessionAlarm();
    void rpcStarted();
    void rpcFinished();

  PRIVATE:
    /// Session that this alarm is monitoring.
    Transport::Session* session;

    /// Used to detect failures in this session.
    SessionAlarmTimer* timer;

    /// The index of the entry within timer->alarms that points to us.
    /// Meaningless if outstandingRpcs == 0.
    size_t timerIndex;

    /// Counts the number of RPCs that have been issued on \c session
    /// but have not completed.
    int outstandingRpcs;

    /// An estimate of how many milliseconds have elapsed with
    /// outstandingRpcs > 0 and no RPC completions on \c session.  If
    /// this value gets large, we should check to make sure the server
    /// is still alive.  This could overestimate the actual waiting
    /// time by as much as TIMER_INTERVAL_MS; see note in
    /// SessionAlarm:rpcStarted.
    int waitingForResponseMs;

    /// When \c waitingForResponseMs reaches this value, send a ping
    /// to make sure the server is still alive.
    int pingMs;

    /// When \c waitingForResponseMs reaches this value, abort the
    /// session.
    int abortMs;

    friend class SessionAlarmTimer;
    DISALLOW_COPY_AND_ASSIGN(SessionAlarm);
};

/**
 * The following class manages a collection of SessionAlarms (typically those
 * from all transports in a Context).  It keeps track of all of the active
 * sessions and wakes up periodically to check for session failures.
 */
class SessionAlarmTimer: public Dispatch::Timer {
  public:
    explicit SessionAlarmTimer(Context* context);
    ~SessionAlarmTimer();
    void handleTimerEvent();

  PRIVATE:
    /// This class implements the ping RPCs that keep track of whether servers
    /// are alive. We can't use "normal" ping RPCs because we don't know the
    /// server id; all we have is an open session.
    class PingRpc : public RpcWrapper {
      public:
        PingRpc(Context* context, Transport::SessionRef session);
        ~PingRpc() {}
        bool succeeded();

      PRIVATE:
        Context* context;
        DISALLOW_COPY_AND_ASSIGN(PingRpc);
    };

    /// Shared RAMCloud information.
    Context* context;

    /// Holds all of the SessionAlarms with nonzero \c outstandingRpcs.
    /// The order of entries is irrelevant.
    std::vector<SessionAlarm*> activeAlarms;

    /// How frequently the timer should fire, in milliseconds.
    static const int TIMER_INTERVAL_MS = 5;

    /// Timer interval in ticks (computed from TIMER_INTERVAL_MS).
    uint64_t timerIntervalTicks;

    /// Keeps track of all outstanding ping RPCs and the alarms for which
    /// they were sent.
    typedef std::unordered_map<SessionAlarm*, PingRpc*> PingMap;
    PingMap pings;

    friend class SessionAlarm;
    DISALLOW_COPY_AND_ASSIGN(SessionAlarmTimer);
};

} // end RAMCloud

#endif  // RAMCLOUD_SESSIONALARM_H
