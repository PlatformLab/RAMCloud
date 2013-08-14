/* Copyright (c) 2013 Stanford University
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

#include <vector>
#include "Common.h"
#include "Cycles.h"
#include "Logger.h"
#include "ZooStorage.h"


namespace RAMCloud {

/**
 * Construct a ZooStorage object and connect to the server. This method
 * will not return until a connection has been successfully opened, so it
 * could block indefinitely if there are no active servers.
 *
 * \param serverInfo
 *      Describes where the ZooKeeper servers are running: comma-separated
 *      host:port pairs, such as "rc03:2109,rc04:2109".
 * \param dispatch
 *      Needed for scheduling lease renewal if we become leader. If
 *      becomeLeader is never going to be invoked, then this can be
 *      specified as NULL.
 */
ZooStorage::ZooStorage(string* serverInfo, Dispatch* dispatch)
    : serverInfo(*serverInfo)
    , zoo(NULL)
    , leader(false)
    , connectionRetryMs(0)
    , checkLeaderIntervalMs(0)
    , renewLeaseIntervalCycles(0)
    , renewLeaseRetryCycles(0)
    , leaderVersion(-1)
    , leaderObject()
    , leaderInfo()
    , leaseRenewer()
    , dispatch(dispatch)
{
    // Compute all of the time constants; see the comments near the
    // declarations for important information about the relationship
    // between these numbers.
    connectionRetryMs = 100;
    checkLeaderIntervalMs = 200;
    renewLeaseIntervalCycles = Cycles::fromSeconds(.100);
    renewLeaseRetryCycles = Cycles::fromSeconds(.010);

    open();
}

/**
 * Destructor for ZooStorage objects.
 */
ZooStorage::~ZooStorage()
{
    close();
}


// See documentation for ExternalStorage::becomeLeader.
void
ZooStorage::becomeLeader(const char* name, const string* leaderInfo)
{
    this->leaderObject = name;
    this->leaderInfo = *leaderInfo;
    while (1) {
        if (checkLeader()) {
            return;
        }
        usleep(checkLeaderIntervalMs*1000);
    }
}

// See documentation for ExternalStorage::get.
bool
ZooStorage::get(const char* name, Buffer* value)
{
    if (!leader) {
        throw NotLeaderException(HERE);
    }

    // Make an initial guess about how large the object is, in order to
    // provide a large enough buffer. If the guess is too small, use the
    // stat data to figure out exactly how much space is needed, then try
    // again.
    int32_t length = 1000;
    struct Stat stat;
    while (1) {
        value->reset();
        char* buffer = new(value, APPEND) char[length];
        int status = zoo_get(zoo, name, 0, buffer, &length, &stat);
        if (status != ZOK) {
            if (status == ZNONODE) {
                value->reset();
                return false;
            }
            handleError(status);
            continue;
        }
        // Warning: ZooKeeper returns -1 dataLength for empty nodes.
        if ((length != stat.dataLength) && (stat.dataLength != 0)) {
            // We didn't allocate a large enough buffer; try again.
            length = stat.dataLength;
            continue;
        }
        value->truncateEnd(value->getTotalLength() - length);
        return true;
    }
}

// See documentation for ExternalStorage::getChildren.
void
ZooStorage::getChildren(const char* name, vector<Object>* children)
{
    int status;
    children->resize(0);

    if (!leader) {
        throw NotLeaderException(HERE);
    }

    // First, retrieve the names of all of the children of the node.
    struct String_vector names;
    while ((status = zoo_get_children(zoo, name, 0, &names)) != ZOK) {
        if (status == ZNONODE) {
            return;
        }
        handleError(status);
    }

    // This buffer is used for retrieving object values. The initial size
    // is just a guess; we may have to reallocate a larger buffer to
    // accommodate large objects.
    char* buffer;
    uint32_t bufferSize = 1000;
    buffer = new char[bufferSize];
    try {
        int length;
        std::string childName(name);
        childName.append("/");
        size_t baseLength = childName.length();
        struct Stat stat;

        // Each iteration through the following loop retrieves the value
        // for one  object.
        for (int i = 0; i < names.count; i++) {
            // It may take a couple of attempts to read each child (e.g.
            // if we lose the ZooKeeper connection, or if we have to
            // grow the buffer). Each iteration through the following loop
            // makes one attempt.
            while (1) {
                childName.resize(baseLength);
                childName.append(names.data[i]);
                length = bufferSize;
                int status = zoo_get(zoo, childName.c_str(), 0,
                        static_cast<char*>(buffer), &length, &stat);
                if (status != ZOK) {
                    if (status == ZNONODE) {
                        // The object got deleted after we collected
                        // all of the child names; just ignore it.
                        break;
                    }
                    handleError(status);
                    continue;
                }
                // Warning: ZooKeeper returns -1 dataLength for empty nodes.
                if ((length != stat.dataLength) && (stat.dataLength != 0)) {
                    // We didn't allocate a large enough buffer; resize the
                    // buffer and try again.
                    bufferSize = stat.dataLength;
                    delete buffer;
                    buffer = new char[bufferSize];
                    continue;
                }
                size_t childNameLength = childName.length() + 1;
                char* nameCopy = static_cast<char*>(malloc(childNameLength));
                memcpy(nameCopy, childName.c_str(), childNameLength);
                char* value = NULL;
                if (stat.dataLength > 0) {
                    value = static_cast<char*>(malloc(stat.dataLength));
                    memcpy(value, buffer, stat.dataLength);
                }
                children->emplace_back(nameCopy, value, stat.dataLength);
                break;
            }
        }
    }
    catch (...) {
        deallocate_String_vector(&names);
        delete buffer;
        throw;
    }
    deallocate_String_vector(&names);
    delete buffer;
}

// See documentation for ExternalStorage::remove.
void
ZooStorage::remove(const char* name)
{
    if (!leader) {
        throw NotLeaderException(HERE);
    }

    while (1) {
        int status = zoo_delete(zoo, name, -1);
        if ((status == ZOK) || (status == ZNONODE)) {
            return;
        }
        if (status != ZNOTEMPTY) {
            handleError(status);
            continue;
        }

        // If we get here, it means that the node has children. Delete
        // all of the children, then try deleting the parent again.
        struct String_vector names;
        while ((status = zoo_get_children(zoo, name, 0, &names)) != ZOK) {
            handleError(status);
        }
        std::string child(name);
        child.append("/");
        size_t baseLength = child.length();
        for (int i = 0; i < names.count; i++) {
            child.resize(baseLength);
            child.append(names.data[i]);
            try {
                remove(child.c_str());
            }
            catch (...) {
                deallocate_String_vector(&names);
                throw;
            }
        }
        deallocate_String_vector(&names);
    }
}

// See documentation for ExternalStorage::update.
void
ZooStorage::set(Hint flavor, const char* name, const char* value,
        int valueLength)
{
    if (!leader) {
        throw NotLeaderException(HERE);
    }

    // The value != NULL check below is only needed to handle recursive
    // calls to create intermediate nodes.
    if ((valueLength < 0) && (value != NULL)) {
        valueLength = downCast<int>(strlen(value));
    }
    while (1) {
        int status;
        if (flavor == Hint::CREATE) {
            status = zoo_create(zoo, name, static_cast<const char*>(value),
                    valueLength, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
            if (status == ZOK) {
                return;
            }
            if (status == ZNONODE) {
                // The parent node doesn't exist; create it first.
                createParent(name);
                continue;
            } else if (status == ZNODEEXISTS) {
                // The hint was incorrect: we'll have to use a "set"
                // operation instead of "create".
                RAMCLOUD_LOG(WARNING, "CREATE hint for \"%s\" ZooKeeper "
                        "object was incorrect: object already exists", name);
                flavor = Hint::UPDATE;
                continue;
            }
        } else {
            status = zoo_set(zoo, name, value, valueLength, -1);
            if (status == ZOK) {
                return;
            }
            if (status == ZNONODE) {
                // The hint was incorrect: we'll have to use a "create"
                // operation instead of "set".
                RAMCLOUD_LOG(WARNING, "UPDATE hint for \"%s\" ZooKeeper "
                        "object was incorrect: object doesn't exist", name);
                flavor = Hint::CREATE;
                continue;
            }
        }
        handleError(status);
    }
}

// See documentation for ExternalStorage::setLeaderInfo.
void ZooStorage::setLeaderInfo(const string* leaderInfo)
{
    this->leaderInfo = *leaderInfo;
}

/**
 * This method makes a single attempt to become leader (it checks the
 * leader object, and if it doesn't exist, or its version hasn't changed
 * since the last call to this method, then it attempts to become leader).
 * This method returns after a single attempt, whether or not it acquired
 * leadership (this code is separate from becomeLeader in order to simplify
 * testing).
 *
 * \return
 *      True means we successfully acquired leadership; false means we didn't.
 */
bool
ZooStorage::checkLeader()
{
    char buffer[1000];
    struct Stat stat;

    int length = downCast<int>(sizeof(buffer));
    int status = zoo_get(zoo, leaderObject.c_str(), 0, buffer, &length, &stat);
    if (status == ZNONODE) {
        // Leader object doesn't yet exist; create it to claim leadership
        // (but someone else might get there first).
        status = zoo_create(zoo, leaderObject.c_str(), leaderInfo.c_str(),
                downCast<uint32_t>(leaderInfo.length()),
                &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
        if (status == ZOK) {
            leader = true;
            leaderVersion = 0;
            if (renewLeaseIntervalCycles != 0) {
                leaseRenewer.construct(this);
                leaseRenewer->start(Cycles::rdtsc() + renewLeaseIntervalCycles);
            }
            RAMCLOUD_LOG(NOTICE, "Became leader (no existing leader)");
            return true;
        } else if (status == ZNONODE) {
            // Parent node hasn't been created; create it, then return
            // and let caller try again.
            createParent(leaderObject.c_str());
        } else if (status != ZNODEEXISTS) {
            handleError(status);
        }
        return false;
    }
    if (status != ZOK) {
        handleError(status);
        return false;
    }
    if (stat.version != leaderVersion) {
        // Version number has changed; leader must still be alive.
        leaderVersion = stat.version;
        return false;
    }

    // Version number hasn't changed since the last time we looked.
    // Assume leader is dead and try to become leader by writing
    // the leader object. Make the write conditional on the version
    // number; this will allow us to detect if someone else has beaten
    // us to the punch and taken over leadership.
    status = zoo_set(zoo, leaderObject.c_str(), leaderInfo.c_str(),
            downCast<uint32_t>(leaderInfo.length()), leaderVersion);
    if (status == ZOK) {
        leader = true;
        leaderVersion++;
        if (renewLeaseIntervalCycles != 0) {
            leaseRenewer.construct(this);
            leaseRenewer->start(Cycles::rdtsc() + renewLeaseIntervalCycles);
        }
        RAMCLOUD_LOG(NOTICE, "Became leader (old leader info was \"%.*s\")",
                length, buffer);
        return true;
    }
    if (status != ZBADVERSION) {
        handleError(status);
    }
    return false;
}

/**
 * Close the ZooKeeper connection, if it is open.
 */
void
ZooStorage::close()
{
    if (zoo != NULL) {
        int status = zookeeper_close(zoo);
        if (status != ZOK) {
            RAMCLOUD_LOG(WARNING, "ZooKeeper close failed: %s", zerror(status));
        } else {
            RAMCLOUD_LOG(NOTICE, "ZooKeeper connection closed");
        }
        zoo = NULL;
    }
}

/**
 * This method is called when a create operation fails because the parent
 * of a desired node doesn't exist. This method creates the parent (and its
 * parent, recursively, if needed).
 *
 * \param childName
 *      Name of the node whose parent seems to be missing: NULL-terminated
 *      hierarchical path.
 */
void
ZooStorage::createParent(const char* childName)
{
    string parentName(childName);
    size_t lastSlash = parentName.rfind('/');
    if (lastSlash == string::npos) {
        RAMCLOUD_LOG(ERROR, "ZooKeeper node name \"%s\" contains no slashes",
                childName);
        throw FatalError(HERE);
    }
    parentName.resize(lastSlash);
    set(Hint::CREATE, parentName.c_str(), 0, -1);
}

/**
 * This method is invoked whenever a ZooKeeper operation returns an
 * error. It logs the error and reopens the connection, if needed.
 *
 * \param status
 *      An error status value returned by ZooKeeper.
 *
 * \throws FatalError
 *      Thrown if the error is due to misuse of ZooKeeper, as opposed to
 *      a problem with the server or the connection.
 */
void
ZooStorage::handleError(int status)
{
    if (((status > ZAPIERROR) && (status != ZBADARGUMENTS))
            || (status == ZSESSIONEXPIRED)) {
        // The problem appears to be related to ZooKeeper; reopening
        // the ZooKeeper connection may fix it.
        RAMCLOUD_LOG(WARNING, "ZooKeeper error (%s): reopening connection",
                zerror(status));
        close();
        open();
        return;
    }

    // Someone is using ZooKeeper incorrectly; throw an exception.
    RAMCLOUD_LOG(ERROR, "ZooKeeper API error: %s", zerror(status));
    throw FatalError(HERE);
}

/**
 * Open a ZooKeeper connection, if there isn't one already open. This
 * method will wait for a ZooKeeper server to come up, if needed.
 */
void
ZooStorage::open()
{
    if (zoo != NULL) {
        return;
    }
    while (1) {
        // The session timeout is set to a very large number (we don't
        // need it for leader management and a small value interferes
        // with debugging).
        zoo = zookeeper_init(serverInfo.c_str(), NULL, 1000000, 0, NULL, 0);
        if (zoo == NULL) {
            RAMCLOUD_LOG(WARNING, "Couldn't open ZooKeeper connection: %s",
                    strerror(errno));
            goto retry;
        }

        // zookeeper_init doesn't actually wait for session connection
        // to complete, but it isn't safe to issue ZooKeeper commands
        // until it does. The code below waits (a while, but nor forever)
        // for the session to really truly become completely open and
        // functional.
        for (int i = 1; i < 100; i++) {
            int state = zoo_state(zoo);
            if (state == ZOO_CONNECTED_STATE) {
                RAMCLOUD_LOG(WARNING, "ZooKeeper connection opened: %s",
                        serverInfo.c_str());
                return;
            }
            if ((state == ZOO_EXPIRED_SESSION_STATE) ||
                    (state == ZOO_AUTH_FAILED_STATE)) {
                RAMCLOUD_LOG(WARNING,
                        "ZooKeeper connection reached bad state (%s) "
                        "during open", stateString(state));
                close();
                goto retry;
            }
            usleep(10000);
        }
        RAMCLOUD_LOG(WARNING, "ZooKeeper connection didn't reach open "
                "state; retrying");
        close();

        retry:
        usleep(connectionRetryMs*1000);
    }
}

/**
 * Return a human-readable string describing a particular connection state.
 */
const char*
ZooStorage::stateString(int state)
{
    if (state == ZOO_CONNECTED_STATE) {
          return "connected";
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
          return "expired session";
    } else if (state == ZOO_AUTH_FAILED_STATE) {
          return "authentication failed";
    } else if (state == ZOO_CONNECTING_STATE) {
          return "connecting";
    } else if (state == ZOO_ASSOCIATING_STATE) {
          return "associating";
    } else if (state == 0) {
        // For some reason there is no symbolic value defined for this state.
        return "closed";
    } else if (state == 999) {
        // For some reason there is no symbolic value defined for this state.
        return "not connected";
    } else {
        return "unknown state";
    }
}

/**
 * Constructor for LeaseRenewer.
 *
 * \param zooStorage
 *      The ZooStorage object on whose behalf we are working. Info in
 *      this record gets accessed in the timer handler.
 */
ZooStorage::LeaseRenewer::LeaseRenewer(ZooStorage* zooStorage)
    : WorkerTimer(zooStorage->dispatch)
    , zooStorage(zooStorage)
{}

/**
 * This method is invoked when the leaseRenewer timer expires; its job is
 * to update the leader object in order to renew our lease as leader.
 */
void ZooStorage::LeaseRenewer::handleTimerEvent()
{
    int status = zoo_set(zooStorage->zoo, zooStorage->leaderObject.c_str(),
            zooStorage->leaderInfo.c_str(),
            downCast<uint32_t>(zooStorage->leaderInfo.length()),
            zooStorage->leaderVersion);
    if (status == ZOK) {
        zooStorage->leaderVersion++;
        start(Cycles::rdtsc() + zooStorage->renewLeaseIntervalCycles);
    } else if (status == ZBADVERSION) {
        // Someone else has modified the leader object; this means we
        // are no longer the leader. In this case, no need to restart the
        // timer.
        zooStorage->leader = false;
    } else {
        // Something went wrong (e.g. server crashed?). Perform standard error
        // handling, then retry with a shorter timer.
        zooStorage->handleError(status);
        start(Cycles::rdtsc() + zooStorage->renewLeaseRetryCycles);
    }
}

} // namespace RAMCloud
