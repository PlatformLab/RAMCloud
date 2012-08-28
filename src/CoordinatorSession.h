/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_COORDINATORSESSION_H
#define RAMCLOUD_COORDINATORSESSION_H

#include "TransportManager.h"

namespace RAMCloud {

/**
 * CoordinatorSession manages the session used to communicate with the
 * coordinator.  Its main job is to locate the coordinator, given a
 * description of which coordinator to use, and to locate a new coordinator
 * if the existing one crashes.
 */
class CoordinatorSession {
  public:
    explicit CoordinatorSession(Context* context);
    ~CoordinatorSession();

    void flush();
    string& getLocation();
    Transport::SessionRef getSession();
    void setLocation(const char* location);

  PROTECTED:
    /// Used in a monitor-style fashion for mutual exclusion.
    SpinLock mutex;

    /// Shared RAMCloud information.
    Context* context;

    /// Describes how to find the coordinator. Currently this is a
    /// service locator string.
    string coordinatorLocator;

    /// Used to communicate with the coordinator.  NULL means a session
    /// hasn't been opened yet, or it was flushed because of communication
    /// errors.
    Transport::SessionRef session;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorSession);
};

} // end RAMCloud

#endif  // RAMCLOUD_COORDINATORSESSION_H
