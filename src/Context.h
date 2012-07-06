/* Copyright (c) 2011 Facebook
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

#ifndef RAMCLOUD_CONTEXT_H
#define RAMCLOUD_CONTEXT_H

#include "Common.h"

namespace RAMCloud {

// forward declarations
class Logger;
class CoordinatorServerList;
class CoordinatorSession;
class Dispatch;
class MockContextMember;
class TransportManager;
class ServerList;
class ServiceManager;
class SessionAlarmTimer;

/**
 * Context is a container for global variables.
 *
 * Its main purpose is to allow multiple instances of these variables to
 * coexist in the same address space. This is useful for a variety of reasons,
 * for example:
 *  - Accessing RAMCloud from multiple threads in a multi-threaded client
 *    application without locking.
 *  - Running a simulator of RAMCloud in which multiple clients and servers
 *    share a single process.
 *  - Running unit tests in parallel in the same process.
 *
 * Context also defines an explicit order in which these variables are
 * constructed and destroyed. Without such an ordering, it's easy to run into
 * memory corruption problems (e.g., RAM-212).
 *
 * Expected usage: on client machines there will be one Context per RamCloud
 * object, which also means one Context per thread.  On server machines there
 * is a single Context object shared among all the threads.
 */
class Context {
  PUBLIC:
    explicit Context(bool hasDedicatedDispatchThread = false);
    ~Context();

    // Rationale:
    // - These are pointers to the heap to work around circular dependencies in
    //   header files.
    // - They are exposed publicly rather than via accessor methods for
    //   convenience in caller code.
    // - They are not managed by smart pointers (such as std::unique_ptr)
    //   because they need to be constructed and destroyed while inside this
    //   context (since later members may depend on earlier ones). That's
    //   pretty awkward to achieve with smart pointers in the face of
    //   exceptions.

    MockContextMember* mockContextMember1; ///< for testing purposes
    Dispatch* dispatch;
    MockContextMember* mockContextMember2; ///< for testing purposes
    TransportManager* transportManager;
    ServiceManager* serviceManager;
    SessionAlarmTimer* sessionAlarmTimer;
    CoordinatorSession* coordinatorSession;

    // Variables below this point are used only in servers.  They are
    // always NULL on clients.

    // The following variable is available on masters and backups, but
    // not coordinators.
    ServerList* serverList;

    // The following variable is available on coordinators, but not
    // masters and backups.
    CoordinatorServerList* coordinatorServerList;

  PRIVATE:
    void destroy();
    DISALLOW_COPY_AND_ASSIGN(Context);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
