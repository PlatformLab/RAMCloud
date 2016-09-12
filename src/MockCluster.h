/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_MOCKCLUSTER_H
#define RAMCLOUD_MOCKCLUSTER_H

#include "BindTransport.h"
#include "CoordinatorService.h"
#include "Log.h"
#include "Logger.h"
#include "MockExternalStorage.h"
#include "Segment.h"
#include "Server.h"
#include "ServerList.h"
#include "ServiceMask.h"
#include "TableManager.h"

namespace RAMCloud {

/**
 * Utility to make setting up unit testing involving many "servers" painless.
 * The details of how each server in the cluster are created is specified by
 * a ServerConfig (just as it is during normal execution, though some additional
 * fields may be consulted).  Details about each of those fields is documented
 * as part of ServerConfig.
 *
 * Internally this class uses BindTransport to allow the servers to communicate.
 * It tries to take care of as many of the details of testing as possible.  It
 * registers and unregisters the transport, it creates a coordinator, as servers
 * are added it registers them with the coordinator, it can auto-assign service
 * locators to each of the servers, and, finally, it provides factories to
 * easily construct clients to the created services.
 *
 * All resources returned by an instance are cleaned up automatically and are
 * valid for the lifetime of the MockCluster instance.  You shouldn't ever
 * free anything returned by a MockCluster.  Everything is public so feel free
 * to get at the internals of this class, but expect your unit tests to break
 * from time-to-time as this class is updated.
 *
 * There are some limitations to be aware of:
 *  - Keep in mind that RAMCloud code doesn't behave well when backup servers
 *    go away before (or while) masters go away.  Masters make every effort to
 *    back their data up, and if there aren't enough backups this means waiting
 *    forever.
 *  - Control over the details of the coordinator is limited to just choosing
 *    its service locator.  The coordinator of the MockCluster doesn't run
 *    additional services like the AdminService.  For now, those must be created
 *    manually and registered.  In the future, perhaps the coordinator will
 *    have a CoordinatorConfig of some kind to make this easier.  It was
 *    just left out because it wouldn't have simplified our test much at the
 *    time of writing.
 */
class MockCluster {
  public:
     explicit MockCluster(Context* context,
         string coordinatorLocator = "mock:host=coordinator");
    ~MockCluster();
    Server* addServer(ServerConfig config);
    void syncCoordinatorServerList();
    void haltCoordinatorServerListUpdater();

    /// Caller-supplied context that we manage to provide access to
    /// the cluster (see documentation for constructor parameter).
    Context* linkedContext;

    /// Context that will be used for the cluster coordinator.
    Context coordinatorContext;

    /// Dummy version for use in coordinatorContext.
    MockExternalStorage externalStorage;

    /**
     * Transport the servers in the cluster use to communicate.  Unlike a
     * normal tranport more than one server uses it and each is registered
     * with a seperate service locator that other servers use to address rpcs
     * to it.
     *
     * Clients for the services of each of the servers can be created using
     * service locators or the get() method can be used as a shortcut.
     */
    BindTransport transport;

    /// Locator of the coordinator of the MockCluster.
    string coordinatorLocator;

    /// The coordinator of the MockCluster.
    Tub<CoordinatorService> coordinator;

    /// Servers in the cluster; used to delete them when the cluster goes away.
    vector<Server*> servers;

    /// Contexts in this array correspond to entries in the \c servers vector.
    /// Used to clean them up when the cluster goes away.
    vector<Context*> contexts;

    /// If linkedContext didn't already have a server list, we use this for it.
    Tub<ServerList> linkedContextServerList;

    DISALLOW_COPY_AND_ASSIGN(MockCluster);
};

} // namespace RAMCloud

#endif
