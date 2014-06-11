/* Copyright (c) 2012-2013 Stanford University
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
#include "ExternalStorage.h"
#include "FailSession.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for CoordinatorSession.
 */
CoordinatorSession::CoordinatorSession(Context* context)
    : mutex()
    , context(context)
    , coordinatorLocator()
    , clusterName()
    , storage(NULL)
    , session(NULL)
{}

/**
 * Destructor for CoordinatorSession.
 */
CoordinatorSession::~CoordinatorSession()
{
    delete storage;
}

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

    // If an external storage system is available, lookup the current
    // coordinator there.
    string locator;
    if (storage != NULL) {
        Buffer value;
        if (!storage->get("coordinator", &value)) {
            LOG(WARNING, "Couldn't read coordinator object for cluster %s "
                    "from storage at '%s'",
                    clusterName.c_str(), coordinatorLocator.c_str());
            return FailSession::get();
        }
        locator.append(value.getStart<char>(), value.getTotalLength());
    } else {
        // This is an old-style "direct" locator.
        locator = coordinatorLocator;
    }
    session = context->transportManager->openSession(locator);
    if (session->getServiceLocator() != "fail:") {
        LOG(NOTICE, "Opened session with coordinator at %s", locator.c_str());
    }
    return session;
}

/**
 * This method is invoked to set or change the location of the coordinator.
 * Any previous coordinator session is discarded.
 *
 * \param location
 *      Describes how to locate the coordinator. For details, see the
 *      documentation for the same argument to the RamCloud constructor.
 *  \param clusterName
 *      Name of the current cluster; defaults to "main".
 */
void
CoordinatorSession::setLocation(const char* location, const char* clusterName)
{
    std::lock_guard<SpinLock> lock(mutex);
    coordinatorLocator = location;
    this->clusterName = clusterName;

    // See if we can connect to an external storage system (close any
    // existing connection).
    delete storage;
    storage = ExternalStorage::open(coordinatorLocator, context);
    if (storage != NULL) {
        string workspace("/ramcloud/");
        workspace.append(clusterName);
        workspace.append("/");
        storage->setWorkspace(workspace.c_str());
    }
    session = NULL;
}

} // namespace RAMCloud
