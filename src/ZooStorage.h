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

#ifndef RAMCLOUD_ZOOSTORAGE_H
#define RAMCLOUD_ZOOSTORAGE_H

#include "ExternalStorage.h"
#include "zookeeper/zookeeper.h"

namespace RAMCloud {

/**
 * This class provides a wrapper around the ZooKeeper storage system to
 * implement the ExternalStorage interface. See ExternalStorage for API
 * documentation.
 */
class ZooStorage: public ExternalStorage {
  PUBLIC:
    explicit ZooStorage(string* serverInfo);
    virtual ~ZooStorage();
    virtual void becomeLeader(const char* name, const string* serverInfo);
    virtual bool get(const char* name, Buffer* value);
    virtual void getChildren(const char* name, vector<Object>* children);
    virtual void remove(const char* name);
    virtual void set(Hint flavor, const char* name, const char* value,
            int valueLength = -1);

  PRIVATE:
    /// Copy of serverInfo argument from constructor.
    string serverInfo;

    /// Information about the current ZooKeeper connection, or NULL
    /// if none.
    zhandle_t* zoo;

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

    /// This variable is used during leader election; it holds the version
    /// number read previously from the leader object.  -1 means that we
    /// haven't yet read a version number.
    int32_t leaderVersion;

    /// If we are unable to connect to ZooKeeper server, this variable
    /// determines how often we retry (units: milliseconds).
    static const int CONNECTION_RETRY_MS = 1000;

    /// When a server is waiting to become a leader, it checks the leader
    /// object this often; if the object hasn't changed since the last check,
    /// the current server attempts to become leader.
    static const int CHECK_LEADER_INTERVAL_MS = 200;

    bool checkLeader();
    void close();
    void createParent(const char* childName);
    void handleError(int status);
    void open();
    const char* stateString(int state);

    DISALLOW_COPY_AND_ASSIGN(ZooStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_ZOOSTORAGE_H

