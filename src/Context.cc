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

#include "Context.h"
#include "Dispatch.h"
#include "ServiceManager.h"
#include "ShortMacros.h"
#include "SessionAlarm.h"
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
 * You will almost always want to enter the current thread into this new
 * context using a Context::Guard.
 * \param hasDedicatedDispatchThread
 *      Argument passed on to Dispatch's constructor.
 */
Context::Context(bool hasDedicatedDispatchThread)
    : logger(NULL)
    , mockContextMember1(NULL)
    , dispatch(NULL)
    , mockContextMember2(NULL)
    , transportManager(NULL)
    , serviceManager(NULL)
    , sessionAlarmTimer(NULL)
    , serverList(NULL)
{
    // The constructors of inner members may try to access the outer members.
    // Set the current context while running the constructors to allow this.
    Guard _(*this);

    try {
        logger = new Logger();
#if TESTING
        mockContextMember1 = new MockContextMember(1);
#endif
        dispatch = new Dispatch(hasDedicatedDispatchThread);
#if TESTING
        mockContextMember2 = new MockContextMember(2);
#endif
        transportManager = new TransportManager();
        serviceManager = new ServiceManager();
        sessionAlarmTimer = new SessionAlarmTimer(*dispatch);
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
    // The destructors of inner members may try to access the outer members.
    // Set the current context while running the destructors to allow this.
    Guard _(*this);

    // The pointers are set to NULL here after they're deleted to make it
    // easier to catch bugs in which outer members try to access inner members.

    // Make sure to delete the members in the opposite order from their
    // construction.

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

#if TESTING
    delete mockContextMember1;
    mockContextMember1 = NULL;
#endif

    delete logger;
    logger = NULL;
}

/**
 * This is a slower version of Context::get() that outputs a reasonable error
 * message if the context isn't set. Normally, Context::get() will just
 * segfault, but this version is called instead if TESTING is set.
 */
Context&
Context::friendlyGet() {
    if (currentContext == NULL) {
        DIE("Tried to call Context::get() but not currently running within a "
            "context. You need to set the context with ScopedContext in every "
            "main function (for threads too!) and every client-facing method.");
    }
    return *currentContext;
}

/**
 * The current Context that would be returned by Context::get(). This variable
 * is manipulated with Context::Guard instances.
 */
__thread Context* Context::currentContext = NULL;

} // namespace RAMCloud
