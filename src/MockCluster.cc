/* Copyright (c) 2012-2015 Stanford University
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

#include "TestUtil.h"
#include "MockCluster.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Create a MockCluster for unit testing with a running coordinator.
 * Currently, the coordinator does not run additional services (for
 * example, PingService).  Create and register those manually (with
 * #transport) if you need them.
 *
 * \param context
 *      An external context, owned by the caller, that will be updated
 *      here so that it can be used to access the cluster. This context will
 *      include a BindTransport that can access all of the servers in the
 *      cluster, a serverList that includes all the servers in the cluster,
 *      and a coordinatorSession that provides access to the coordinator
 *      for the cluster.
 * \param coordinatorLocator
 *      The service locator that other servers in the MockCluster will
 *      use to talk to the coordinator.
 */
MockCluster::MockCluster(Context* context, string coordinatorLocator)
    : linkedContext(context)
    , coordinatorContext()
    , externalStorage(true)
    , transport(&coordinatorContext)
    , coordinatorLocator(coordinatorLocator)
    , coordinator()
    , servers()
    , contexts()
    , linkedContextServerList()
{
    if (linkedContext->serverList == NULL) {
        linkedContextServerList.construct(linkedContext);
    }
    linkedContext->transportManager->registerMock(&transport);
    linkedContext->coordinatorSession->setLocation(coordinatorLocator.c_str());

    new CoordinatorServerList(&coordinatorContext);
    coordinatorContext.transportManager->registerMock(&transport);
    coordinatorContext.coordinatorSession->setLocation(
            coordinatorLocator.c_str());
    coordinatorContext.externalStorage = &externalStorage;

    coordinator.construct(&coordinatorContext, 1000, true);
    transport.registerServer(&coordinatorContext, coordinatorLocator);
}

/**
 * Destructor for MockCluster.
 */
MockCluster::~MockCluster()
{
    // Servers must be deleted in reverse order of their creation to avoid
    // weird shutdown deadlock issues (for example, master try indefinitely to
    // flush their logs to backups on shutdown).
    for (auto it = servers.rbegin(); it != servers.rend(); ++it) {
        RAMCLOUD_TEST_LOG("%s", (*it)->config.services.toString().c_str());
        delete *it;
    }

    // Delete the context that we created, and cleanup all the modifications
    // that we made to them.
    foreach (Context* context, contexts) {
        delete context->serverList;
        delete context;
    }

    // Cleanup modifications we made to the coordinator context->
    delete coordinatorContext.coordinatorServerList;

    // Cleanup modifications we made to the caller's context->
    if (linkedContextServerList && (linkedContext->serverList ==
            linkedContextServerList.get())) {
        linkedContext->serverList = NULL;
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
Server*
MockCluster::addServer(ServerConfig config) {
    if (config.coordinatorLocator == "")
        config.coordinatorLocator = coordinatorLocator;
    if (config.localLocator == "") {
        size_t nextIdx = servers.size();
        config.localLocator = format("mock:host=server%lu", nextIdx);
    }
    Context* context = new Context();
    new ServerList(context);
    context->transportManager->registerMock(&transport);
    contexts.push_back(context);

    Server* server = new Server(context, &config);
    servers.push_back(server);
    transport.registerServer(context, config.localLocator);
    server->startForTesting(transport);

    ServerList* sl = static_cast<ServerList*>(linkedContext->serverList);
    sl->testingAdd({server->serverId, config.localLocator,
                    config.services, 100, ServerStatus::UP});
    syncCoordinatorServerList();
    return server;
}

void
MockCluster::syncCoordinatorServerList()
{
    if (coordinatorContext.coordinatorServerList)
        coordinatorContext.coordinatorServerList->sync();
}

void
MockCluster::haltCoordinatorServerListUpdater()
{
    if (coordinatorContext.coordinatorServerList)
        coordinatorContext.coordinatorServerList->haltUpdater();
}

} // namespace RAMCloud
