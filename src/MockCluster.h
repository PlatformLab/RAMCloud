/* Copyright (c) 2012 Stanford University
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
#include "Segment.h"
#include "Server.h"
#include "ServiceMask.h"

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
 *    additional services like the PingService.  For now, those must be created
 *    manually and registered.  In the future, perhaps the coordinator will
 *    have a CoordinatorConfig of some kind to make this easier.  It was
 *    just left out because it wouldn't have simpilfied our test much at the
 *    time of writing.
 */
class MockCluster {
  public:
    /**
     * Create a MockCluster for unit testing with a running coordinator.
     * Currently, the coordinator does not run additional services (for
     * example, PingService).  Create and register those manually (with
     * #transport) if you need them.
     *
     * \param coordinatorLocator
     *      The service locator that other servers in the MockCluster will
     *      use to talk to the coordinator.
     */
    explicit MockCluster(string coordinatorLocator = "mock:host=coordinator")
        : transport()
        , mockRegistrar(transport)
        , coordinatorLocator(coordinatorLocator)
        , coordinator()
        , coordinatorClient()
        , servers()
    {
        coordinator.construct();
        transport.addService(*coordinator, coordinatorLocator,
                             COORDINATOR_SERVICE);
        coordinatorClient.construct(coordinatorLocator.c_str());
    }

    /**
     * Added servers are deleted in reverse order of their creation to avoid
     * weird shutdown deadlock issues (for example, master try indefinitely to
     * flush their logs to backups on shutdown).
     */
    ~MockCluster()
    {
        for (auto it = servers.rbegin(); it != servers.rend(); ++it) {
            RAMCLOUD_TEST_LOG("%s", (*it)->config.services.toString().c_str());
            delete *it;
        }
    }

    /**
     * Add a new server to the cluster; notice \a config is passed
     * by value; the caller is NOT responsible for freeing the returned Server.
     * A ServerConfig can be obtained from ServerConfig::forTesting
     * which will work for most tests.  From there different tests will need
     * to override a few things.  Most importantly, for any configuration it
     * is important to select which services will run on the server.
     * For example, config.services = {MASTER_SERVICE, PING_SERVICE}.
     * Examples of other things that can be configured include number of
     * replicas, size of segments, number of segment frames on the backup
     * storage, etc.).
     *
     * If config.coordinatorLocator is left blank (which is its default)
     * then the server will enlist with the coordinator which was created
     * along with this cluster.  If config.localLocator is left blank (which
     * is its default) it will be assigned a service locator of
     * "mock:host=serverX" where X starts at 0 and increases each time
     * addServer() is called.  Notice, that because each server contains
     * a copy of its ServerConfig \a config is NOT modified to reflect these
     * tweaks to the service locators.  The generated locators can be obtained
     * from the config field in Server.  In order to reduce breakage with older
     * unit tests and to make for clearer hostnames, its possible to override
     * config.localLocator to control the locator the added server listens at.
     *
     * Look at ServerConfig for all the details on what configuration of
     * a server is possible.
     *
     * Example usage:
     *
     * ServerConfig config = ServerConfig::forTesting();
     * config.services = {BACKUP_SERVICE, PING_SERVICE};
     * auto* backup = cluster.get<BackupClient>(cluster.addServer(config));
     * backup->openSegment({99, 0}, 10);
     *
     * \return
     *      A pointer to the new Server that has been added to the cluster
     *      and enlisted with the coordinator.  The caller is NOT responsible
     *      for freeing the returned Server.
     */
    Server* addServer(ServerConfig config) {
        if (config.coordinatorLocator == "")
            config.coordinatorLocator = coordinatorLocator;
        if (config.localLocator == "") {
            size_t nextIdx = servers.size();
            config.localLocator = format("mock:host=server%lu", nextIdx);
        }
        std::unique_ptr<Server> server(new Server(config));
        server->startForTesting(transport);
        servers.push_back(server.get());
        return server.release();
    }

    /**
     * Return a client to a particular server in the cluster.
     *
     * \tparam T
     *      Type of the client desired to talk to \a server.
     * \param server
     *      Which server the returned client should send requests to.
     */
    template <typename T>
    std::unique_ptr<T> get(Server* server) {
        return std::unique_ptr<T>(
            new T(
                Context::get().
                    transportManager->
                        getSession(server->
                            config.localLocator.c_str())));
    }

    /**
     * Return a client to this cluster's coordinator; important, the caller
     * is NOT responsible for freeing returned the client.  Also, the client is
     * shared among all callers of this method (i.e. it always returns a pointer
     * to the same client).
     */
    CoordinatorClient* getCoordinatorClient() {
        return coordinatorClient.get();
    }

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

    /// Registers and deregisters the transport with the TransportManager.
    TransportManager::MockRegistrar mockRegistrar;

    /// Locator of the coordinator of the MockCluster.
    string coordinatorLocator;

    /// The coordinator of the MockCluster.
    Tub<CoordinatorService> coordinator;

    /// A client to the coordinator of the MockCluster.
    Tub<CoordinatorClient> coordinatorClient;

    /// Servers in the cluster; used to delete them when the cluster goes away.
    vector<Server*> servers;
};

} // namespace RAMCloud

#endif
