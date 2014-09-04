/* Copyright (c) 2013 Stanford University
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

#if ENABLE_ZOOKEEPER

#ifndef RAMCLOUD_ZOOSTORAGE_H
#define RAMCLOUD_ZOOSTORAGE_H

#include <zookeeper/zookeeper.h>

#include "ExternalStorage.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * This class provides a wrapper around the ZooKeeper storage system to
 * implement the ExternalStorage interface. See ExternalStorage for API
 * documentation. This class is thread-safe.
 */
class ZooStorage: public ExternalStorage {
  PUBLIC:
    explicit ZooStorage(string& serverInfo, Dispatch* dispatch);
    virtual ~ZooStorage();
    virtual void becomeLeader(const char* name, const string& leaderInfo);
    virtual bool get(const char* name, Buffer* value);
    virtual void getChildren(const char* name, vector<Object>* children);
    virtual void remove(const char* name);
    virtual void set(Hint flavor, const char* name, const char* value,
            int valueLength = -1);

  PRIVATE:
    /**
     * This class is used to update the leader object in order to
     * maintain the leader's legitimacy.
     */
    class LeaseRenewer: public WorkerTimer {
      public:
        explicit LeaseRenewer(ZooStorage* zooStorage);
        virtual ~LeaseRenewer() {}
        virtual void handleTimerEvent();

        /// Copy of constructor argument:
        ZooStorage* zooStorage;

      private:
        DISALLOW_COPY_AND_ASSIGN(LeaseRenewer);
    };


    /// Monitor-style lock: acquired by all externally visible methods. It's
    /// not totally clear whether ZooKeeper itself is thread-safe, but even
    /// if it were, this class does things like reopening the server
    /// connection, which can't be done if other threads are using the
    /// connection.
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// Copy of serverInfo argument from constructor.
    string serverInfo;

    /// Information about the current ZooKeeper connection, or NULL
    /// if none.
    zhandle_t* zoo;

    /// True means that we once were leader, but at some point we lost
    /// leadership. From this point on, no operations will be allowed;
    /// the application should commit suicide.
    bool lostLeadership;

    /// If we are unable to connect to the ZooKeeper server, this variable
    /// determines how often we retry (units: milliseconds).
    int connectionRetryMs;

    /// When a server is waiting to become a leader, it checks the leader
    /// object this often (units: milliseconds); if the object hasn't changed
    /// since the last check, the current server attempts to become leader.
    int checkLeaderIntervalMs;

    /// When a server is leader, it renews its lease this often (units: rdtsc
    /// cycles). This value must be quite a bit less than
    /// checkLeaderIntervalCycles.  A value of zero is used during testing;
    /// it means "don't renew the lease or even start the timer".
    uint64_t renewLeaseIntervalCycles;

    /// This variable holds the most recently seen version number of the
    /// leader object; -1 means that we haven't yet read a version number.
    /// During leader election, this allows us to tell if the current leader
    /// has updated leader object. Once we become leader, it allows us to
    /// detect if we lose leadership.
    int32_t leaderVersion;

    /// Copy of name argument from becomeLeader: name of the object used
    /// for leader election and identification. The current leader renews
    /// its lease by updating this object; if another server detects that
    /// a long period has gone by without the object being updated, then it
    /// claims leadership for itself. The contents of the object will be
    /// set from leaderInfo.
    string leaderObject;

    /// Copy of leaderInfo argument from becomeLeader; empty if becomeLeader
    /// has not been invoked yet.
    string leaderInfo;

    /// Used to update leaderObject.
    Tub<LeaseRenewer> leaseRenewer;

    /// Used for scheduling leaseRenewer.
    Dispatch* dispatch;

    /// Used to force errors during testing.  If a value is non-zero, it
    /// is overrides the status returned by a ZooKeeper procedure. The
    /// values are always 0 during normal operation.
    int testStatus1;
    int testStatus2;

    bool checkLeader(Lock& lock);
    void close(Lock& lock);
    void createParent(Lock& lock, const char* childName);
    void handleError(Lock& lock, int status);
    void open(Lock& lock);
    void removeInternal(Lock& lock, const char* name);
    bool renewLease(Lock& lock);
    void setInternal(Lock& lock, Hint flavor, const char* name,
            const char* value, int valueLength);
    const char* stateString(int state);

    DISALLOW_COPY_AND_ASSIGN(ZooStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_ZOOSTORAGE_H

#endif // ENABLE_ZOOKEEPER
