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

// TODO(ankitak): Figure out a way to include logcabin classes without
// including here, using only forward declarations.
#include <Client/Client.h>

#include "Common.h"

namespace RAMCloud {

// forward declarations
class AbstractServerList;
class BackupService;
class CoordinatorServerList;
class CoordinatorService;
class CoordinatorSession;
class Dispatch;
class LogCabinHelper;
class Logger;
class MasterRecoveryManager;
class MasterService;
class MockContextMember;
class ServiceManager;
class SessionAlarmTimer;
class TransportManager;

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

    // Master-related information for this server. NULL if this process
    // is not running a RAMCloud master. Owned elsewhere; not freed by this
    // class.
    MasterService* masterService;

    // Backup-related information for this server. NULL if this process
    // is not running a RAMCloud backup. Owned elsewhere; not freed by this
    // class.
    BackupService* backupService;

    // Coordinator-related information for this server. NULL if this process
    // is not running a RAMCloud coordinator. Owned elsewhere; not freed by
    // this class.
    CoordinatorService* coordinatorService;

    // The following variable is available on all servers (masters, backups,
    // coordinator). It provides facilities that are common to both ServerList
    // and CoordinatorServerList. Owned elsewhere; not freed by this class.
    AbstractServerList* serverList;

    // On coordinators, the following variable refers to the same object as
    // \c serverList; it provides additional features used by coordinators to
    // manage cluster membership.  NULL except on coordinators.  Owned
    // elsewhere; not freed by this class.
    CoordinatorServerList* coordinatorServerList;

    // Handles all master recovery details on behalf of the coordinator.
    // NULL except on coordinators. Owned elsewhere;
    // not freed by this class.
    MasterRecoveryManager* recoveryManager;

    // Handle to the log interface provided by LogCabin.
    // NULL except on coordinators. Owned elsewhere;
    // not freed by this class.
    LogCabin::Client::Log* logCabinLog;

    // Handle to a helper class that provides higher level abstractions
    // to interact with LogCabin.
    // NULL except on coordinators. Owned elsewhere;
    // not freed by this class.
    LogCabinHelper* logCabinHelper;

    // EntryId of the last entry appended to log by an instance of
    // coordinator. This is used for safe appends, i.e., appends that are
    // conditional on last entry being appended by this entry, that helps
    // ensure leadership.
    // NULL except on coordinators. Owned elsewhere;
    // not freed by this class.
    // TODO(ankitak): This should not be here. I will have one per
    // instance of coordinator, while if it is here, then it will
    // be shared by all instances.
    LogCabin::Client::EntryId* expectedEntryId;

  PRIVATE:
    void destroy();
    DISALLOW_COPY_AND_ASSIGN(Context);
};

} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
