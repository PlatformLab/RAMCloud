/* Copyright (c) 2012 Stanford University
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

#include "CoordinatorSession.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for CoordinatorSession.
 */
CoordinatorSession::CoordinatorSession(Context& context)
    : mutex()
    , context(context)
    , coordinatorLocator()
    , session(NULL)
{}

/**
 * Destructor for CoordinatorSession.
 */
CoordinatorSession::~CoordinatorSession()
{}

/**
 * Close any existing coordinator session, so that a new session will be
 * opened in the next call to #getSession.
 */
void
CoordinatorSession::flush()
{
    TEST_LOG("flushing session");
    std::lock_guard<SpinLock> lock(mutex);
    session = NULL;
}

/**
 * Returns the location string currently being used to open coordinator
 * sessions (the #location argument to the most recent call to setLocation).
 * 
 */
string&
CoordinatorSession::getLocation()
{
    std::lock_guard<SpinLock> lock(mutex);
    return coordinatorLocator;
}

/**
 * Returns a session that can be used to communicate with the coordinator.
 * If there is not currently a session open, then a new session is opened.
 * If a session cannot be opened, then the message is logged and a
 * FailSession is returned.
 * 
 */
Transport::SessionRef
CoordinatorSession::getSession()
{
    std::lock_guard<SpinLock> lock(mutex);
    if (session != NULL)
        return session;

    // There is currently no session open; open one.
    if (coordinatorLocator.empty()) {
        throw FatalError(HERE,
                "CoordinatorSession::setLocation never invoked");
    }
    session = context.transportManager->openSession(
        coordinatorLocator.c_str());
    return session;
}

/**
 * This method is invoked to set or change the location of the coordinator.
 * Any previous coordinator session is discarded.
 *
 * \param location
 *      Describes which coordinator to use. Currently this is a service
 *      locator; eventually it will change to a description of how to
 *      find a coordinator through LogCabin.
 */
void
CoordinatorSession::setLocation(const char* location)
{
    std::lock_guard<SpinLock> lock(mutex);
    coordinatorLocator = location;
    session = NULL;
}

} // namespace RAMCloud
