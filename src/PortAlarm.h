/* Copyright (c) 2011-2013 Stanford University
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
 *
 * This file also defines classes that are used by several of the RAMCloud
 * transports to detect when ports are hung because of connection or 
 * client problems.
 */

#ifndef RAMCLOUD_PORTALARM_H
#define RAMCLOUD_PORTALARM_H

#include <unordered_map>
#include "Dispatch.h"
#include "RpcWrapper.h"
#include "Transport.h"

namespace RAMCloud {
// Forward declaration
class PortAlarmTimer;

/**
 * A transport contains multiple ports, eg. QPs for InfRcTransport,
 * in order to listen  clients' requests.
 * One PortAlarm object keeps track of a port is alive,
 * if no activities are detected for long time, which goes beyond timeoutMs
 * (ms) it close the port.
 * This feature is useful for server side to destroy orphan side of
 * removed queue pair.
 */
class PortAlarm {
  public:
    PortAlarm(PortAlarmTimer* timer,
              Transport::ServerPort* port);
    ~PortAlarm();

    // Update timer when a request arrives to the port or a reply
    // is sent to the port, which means the listening port is alive.
    void requestArrived();
    void startPortTimer();
    void stopPortTimer();

  PRIVATE:
    /// Used to detect failures in this session.
    PortAlarmTimer* timer;

    /// ServerPort of this timer used for deleteing this port
    Transport::ServerPort* port;

    /// The index of the entry within timer->alarms that points to us.
    size_t timerIndex;

    /// Tell the timer for this portAlarm is running.
    bool portTimerRunning;

    /// Time (ms) passed after the final request arrival.
    int  idleMs;

    friend class PortAlarmTimer;
    DISALLOW_COPY_AND_ASSIGN(PortAlarm);
};

/**
 *  PortAlarmTimer class manages a collection of PortAlarms
 *  for liveness of the port (typically Queue Pairs with InfRc).
 *
 *  It updates the watchdog timer of each reception ports on server
 *  and check if a request arrives within timeoutMs.
 *  If the watchdog timeout is detected, it considers the corresponding
 *  client port is not active anymore and closes the server port.
 *
 */
class PortAlarmTimer : public Dispatch::Timer {
  public:
    explicit PortAlarmTimer(Context* context);
    ~PortAlarmTimer();
    void handleTimerEvent();
    void setPortTimeout(int32_t timeoutMs);
    int32_t getPortTimeout() const;

  PRIVATE:
    /// Shared RAMCloud information.
    Context* context;

    /// Holds all of the PortAlarms.
    /// The order of entries is irrelevant.
    std::vector<PortAlarm*> activeAlarms;

    /// How frequently the timer should fire, in milliseconds.
    static const int TIMER_INTERVAL_MS = 5;

    /// Timer interval in ticks (computed from TIMER_INTERVAL_MS).
    uint64_t timerIntervalTicks;

    /// Default value for port TIMEOUT (ms).
    static const int32_t DEFAULT_PORT_TIMEOUT_MS = 64000;

    /// PortTimeout (ms): Uniq for all transports to detect
    /// dead clients or listening port on servers.
    int32_t portTimeoutMs;

    friend class PortAlarm;
    DISALLOW_COPY_AND_ASSIGN(PortAlarmTimer);
};

} // end RAMCloud

#endif  // RAMCLOUD_PORTALARM_H
