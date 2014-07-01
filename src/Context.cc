/* Copyright (c) 2011 Facebook
 * Copyright (c) 2011-2014 Stanford University
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

#include "Context.h"
#include "CoordinatorServerList.h"
#include "CoordinatorSession.h"
#include "Dispatch.h"
#include "ServiceManager.h"
#include "ShortMacros.h"
#include "SessionAlarm.h"
#include "PortAlarm.h"
#include "TableManager.h"
#include "TimeTrace.h"
#include "CacheTrace.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Used by the unit tests to signal to MockContextMember's constructor that it
 * should throw an exception.
 */
int mockContextMemberThrowException = 0;

/**
 * This is a member of the Context that is used for testing purposes only.
 */
class MockContextMember {
  PUBLIC:
    explicit MockContextMember(int id)
        : id(id)
    {
        TEST_LOG("%d", id);
        if (mockContextMemberThrowException == id) {
            mockContextMemberThrowException = 0;
            throw Exception(HERE, format(
                "Mock context member %d asked to throw exception", id).c_str());
        }
    }
    ~MockContextMember() {
        TEST_LOG("%d", id);
    }
    int id;
};

/**
 * Create a new context.
 * This should be called when creating a RamCloud instance and in the main
 * function of RAMCloud daemons.
 * \param hasDedicatedDispatchThread
 *      Argument passed on to Dispatch's constructor.
 */
Context::Context(bool hasDedicatedDispatchThread)
    : mockContextMember1(NULL)
    , dispatch(NULL)
    , mockContextMember2(NULL)
    , transportManager(NULL)
    , serviceManager(NULL)
    , sessionAlarmTimer(NULL)
    , portAlarmTimer(NULL)
    , coordinatorSession(NULL)
    , timeTrace(NULL)
    , cacheTrace(NULL)
    , externalStorage(NULL)
    , masterService(NULL)
    , backupService(NULL)
    , coordinatorService(NULL)
    , serverList(NULL)
    , coordinatorServerList(NULL)
    , tableManager(NULL)
    , recoveryManager(NULL)
{
    try {
#if TESTING
        mockContextMember1 = new MockContextMember(1);
#endif
        timeTrace = new TimeTrace();
        cacheTrace = new CacheTrace();
        dispatch = new Dispatch(hasDedicatedDispatchThread);
#if TESTING
        mockContextMember2 = new MockContextMember(2);
#endif
        transportManager = new TransportManager(this);
        serviceManager = new ServiceManager(this);
        sessionAlarmTimer = new SessionAlarmTimer(this);
        portAlarmTimer = new PortAlarmTimer(this);

        // Until we find the solution to prevent active ports
        // which have just nothing to send, we disable portAlarm
        // portAlarmTimer = new PortAlarmTimer(this);

        coordinatorSession = new CoordinatorSession(this);
    } catch (...) {
        destroy();
        throw;
    }
}

/**
 * Destructor; just calls #destroy().
 */
Context::~Context()
{
    destroy();
}

/**
 * A helper function that is essentially the destructor. This is also called by
 * the constructor if an exception is thrown, in which case some of the members
 * may not yet be constructed.
 */
void
Context::destroy()
{
    // The pointers are set to NULL here after they're deleted to make it
    // easier to catch bugs in which outer members try to access inner members.

    // Make sure to delete the members in the opposite order from their
    // construction.

    delete coordinatorSession;
    coordinatorSession = NULL;

    delete serviceManager;
    serviceManager = NULL;

    delete transportManager;
    transportManager = NULL;

#if TESTING
    delete mockContextMember2;
    mockContextMember2 = NULL;
#endif

    delete dispatch;
    dispatch = NULL;

    delete timeTrace;
    timeTrace = NULL;

    delete cacheTrace;
    cacheTrace = NULL;

#if TESTING
    delete mockContextMember1;
    mockContextMember1 = NULL;
#endif

    serverList = NULL;
    coordinatorServerList = NULL;

    tableManager = NULL;
}

} // namespace RAMCloud
