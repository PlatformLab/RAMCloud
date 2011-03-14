/* Copyright (c) 2009-2010 Stanford University
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

#include <pthread.h>

#include <boost/scoped_ptr.hpp>

#include "BackupClient.h"
#include "BenchUtil.h"
#include "CoordinatorServer.h"
#include "FailureDetector.h"
#include "MasterClient.h"
#include "ProtoBuf.h"
#include "Recovery.h"

/*
 * Since recoveries are fired off in a separate thread (to ensure that the
 * FailureDetector's pings are promptedly responded to for clients of the
 * Coordinator), we need to ensure mutual exclusion of the transport
 * non-reentrant transport layer. Not doing so properly results in lots
 * of really weird errors, as one might expect.
 *
 * The following mutex is a sledgehammer solution. Any thread that might
 * use the transport must first acquire the lock.
 *
 * Note that this also provides synchronisation for CoordinatorServer
 * members, such as the server lists. failureDetectorServerThread also
 * uses this lock to get exclusive access to the server lists for
 * GET_SERVER_LIST rpcs. This is somewhat dangerous, as a long wait on
 * the lock could result in clients thinking the coordinator is down.
 * If this becomes problematic, we can use trylock and abort requests
 * that wait too long, since they're not strictly necessary and will be
 * retried by the FailureDetector.
 */
static pthread_mutex_t biglock = PTHREAD_MUTEX_INITIALIZER;
#define bigLock()       pthread_mutex_lock(&biglock);
#define bigUnlock()     pthread_mutex_unlock(&biglock);

namespace RAMCloud {

void* failureDetectorServerThread(void* arg);

CoordinatorServer::CoordinatorServer(string localLocator)
    : nextServerId(1)
    , backupList()
    , masterList()
    , tabletMap()
    , tables()
    , nextTableId(0)
    , nextTableMasterIdx(0)
    , failureDetectorFd(-1)
    , failureProbeFd(-1)
    , recoveringMasters()
    , mockRecovery(NULL)
{
    failureDetectorFd = socket(PF_INET, SOCK_DGRAM, 0);
    if (failureDetectorFd == -1)
        throw Exception(HERE, "failed to create failureDetector socket");

    failureProbeFd = socket(PF_INET, SOCK_DGRAM, 0);
    if (failureProbeFd == -1)
        throw Exception(HERE, "failed to create failureProbe socket");

    sockaddr_in sin =
        FailureDetector::serviceLocatorStringToSockaddrIn(localLocator);
    if (bind(failureDetectorFd,
      reinterpret_cast<sockaddr*>(&sin), sizeof(sin))) {
        close(failureDetectorFd);
        throw Exception(HERE, "failed to bind failureDetector socket");
    }
    LOG(NOTICE, "listening for FailureDetector messages on %s:%d",
        inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

    pthread_t tid;
    if (pthread_create(&tid, NULL, failureDetectorServerThread, this))
        DIE("coordinator: couldn't spawn failure detector server thread");
}

CoordinatorServer::~CoordinatorServer()
{
    // delete wills
    foreach (const ProtoBuf::ServerList::Entry& master, masterList.server())
        delete reinterpret_cast<ProtoBuf::Tablets*>(master.user_data());
    close(failureDetectorFd);
    close(failureProbeFd);
}

void
CoordinatorServer::run()
{
    while (true) {
        bigLock();
        handleRpc<CoordinatorServer>(true);
        bigUnlock();
    }
}

void
CoordinatorServer::dispatch(RpcType type,
                            Transport::ServerRpc& rpc,
                            Responder& responder)
{
    switch (type) {
        case CreateTableRpc::type:
            callHandler<CreateTableRpc, CoordinatorServer,
                        &CoordinatorServer::createTable>(rpc);
            break;
        case DropTableRpc::type:
            callHandler<DropTableRpc, CoordinatorServer,
                        &CoordinatorServer::dropTable>(rpc);
            break;
        case OpenTableRpc::type:
            callHandler<OpenTableRpc, CoordinatorServer,
                        &CoordinatorServer::openTable>(rpc);
            break;
        case EnlistServerRpc::type:
            callHandler<EnlistServerRpc, CoordinatorServer,
                        &CoordinatorServer::enlistServer>(rpc);
            break;
        case GetServerListRpc::type:
            callHandler<GetServerListRpc, CoordinatorServer,
                        &CoordinatorServer::getServerList>(rpc);
            break;
        case GetTabletMapRpc::type:
            callHandler<GetTabletMapRpc, CoordinatorServer,
                        &CoordinatorServer::getTabletMap>(rpc);
            break;
        case HintServerDownRpc::type:
            callHandler<HintServerDownRpc, CoordinatorServer,
                        &CoordinatorServer::hintServerDown>(rpc, responder);
            break;
        case TabletsRecoveredRpc::type:
            callHandler<TabletsRecoveredRpc, CoordinatorServer,
                        &CoordinatorServer::tabletsRecovered>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, CoordinatorServer,
                        &CoordinatorServer::ping>(rpc);
            break;
        case BackupQuiesceRpc::type:
            callHandler<BackupQuiesceRpc, CoordinatorServer,
                        &CoordinatorServer::quiesce>(rpc);
            break;
        case SetWillRpc::type:
            callHandler<SetWillRpc, CoordinatorServer,
                        &CoordinatorServer::setWill>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::createTable(const CreateTableRpc::Request& reqHdr,
                               CreateTableRpc::Response& respHdr,
                               Transport::ServerRpc& rpc)
{
    if (masterList.server_size() == 0)
        throw RetryException(HERE);

    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    if (tables.find(name) != tables.end())
        return;
    uint32_t tableId = nextTableId++;
    tables[name] = tableId;

    uint32_t masterIdx = nextTableMasterIdx++ % masterList.server_size();
    ProtoBuf::ServerList_Entry& master(*masterList.mutable_server(masterIdx));

    // Create tablet map entry.
    ProtoBuf::Tablets_Tablet& tablet(*tabletMap.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_object_id(0);
    tablet.set_end_object_id(~0UL);
    tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
    tablet.set_server_id(master.server_id());
    tablet.set_service_locator(master.service_locator());

    // Create will entry. The tablet is empty, so it doesn't matter where it
    // goes or in how many partitions, initially. It just has to go somewhere.
    ProtoBuf::Tablets& will(
        *reinterpret_cast<ProtoBuf::Tablets*>(master.user_data()));
    ProtoBuf::Tablets_Tablet& willEntry(*will.add_tablet());
    willEntry.set_table_id(tableId);
    willEntry.set_start_object_id(0);
    willEntry.set_end_object_id(~0UL);
    willEntry.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
    uint64_t maxPartitionId;
    if (will.tablet_size() > 1)
        maxPartitionId = will.tablet(will.tablet_size() - 2).user_data();
    else
        maxPartitionId = -1;
    willEntry.set_user_data(maxPartitionId + 1);

    // Inform the master.
    MasterClient masterClient(
        transportManager.getSession(master.service_locator().c_str()));
    ProtoBuf::Tablets masterTabletMap;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tabletMap.tablet()) {
        if (tablet.server_id() == master.server_id())
            *masterTabletMap.add_tablet() = tablet;
    }
    masterClient.setTablets(masterTabletMap);

    LOG(NOTICE, "Created table '%s' with id %u on master %lu",
                name, tableId, master.server_id());
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
}

/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::dropTable(const DropTableRpc::Request& reqHdr,
                             DropTableRpc::Response& respHdr,
                             Transport::ServerRpc& rpc)
{
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it = tables.find(name);
    if (it == tables.end())
        return;
    uint32_t tableId = it->second;
    tables.erase(it);
    int32_t i = 0;
    while (i < tabletMap.tablet_size()) {
        if (tabletMap.tablet(i).table_id() == tableId) {
            tabletMap.mutable_tablet()->SwapElements(
                                            tabletMap.tablet_size() - 1, i);
            tabletMap.mutable_tablet()->RemoveLast();
        } else {
            ++i;
        }
    }
    // TODO(ongaro): update only affected masters, filter tabletMap for those
    // tablets belonging to each master
    const string& locator(masterList.server(0).service_locator());
    MasterClient master(transportManager.getSession(locator.c_str()));
    master.setTablets(tabletMap);

    LOG(NOTICE, "Dropped table '%s' with id %u", name, tableId);
    LOG(DEBUG, "There are now %d tablets in the map", tabletMap.tablet_size());
}

/**
 * Top-level server method to handle the OPEN_TABLE request.
 * \copydetails Server::ping
 */
void
CoordinatorServer::openTable(const OpenTableRpc::Request& reqHdr,
                             OpenTableRpc::Response& respHdr,
                             Transport::ServerRpc& rpc)
{
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    Tables::iterator it(tables.find(name));
    if (it == tables.end())
        throw TableDoesntExistException(HERE);
    respHdr.tableId = it->second;
}

/**
 * Handle the ENLIST_SERVER RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::enlistServer(const EnlistServerRpc::Request& reqHdr,
                                EnlistServerRpc::Response& respHdr,
                                Transport::ServerRpc& rpc)
{
    uint64_t serverId = nextServerId++;
    ProtoBuf::ServerType serverType =
        static_cast<ProtoBuf::ServerType>(reqHdr.serverType);
    const uint32_t readSpeed = reqHdr.readSpeed;
    const uint32_t writeSpeed = reqHdr.writeSpeed;
    const char *serviceLocator = getString(rpc.recvPayload, sizeof(reqHdr),
                                           reqHdr.serviceLocatorLength);

    ProtoBuf::ServerList& serverList(serverType == ProtoBuf::MASTER
                                     ? masterList
                                     : backupList);
    ProtoBuf::ServerList_Entry& server(*serverList.add_server());
    server.set_server_type(serverType);
    server.set_server_id(serverId);
    server.set_service_locator(serviceLocator);

    if (server.server_type() == ProtoBuf::MASTER) {
        // create empty will
        server.set_user_data(
            reinterpret_cast<uint64_t>(new ProtoBuf::Tablets));
        LOG(DEBUG, "Master enlisted with id %lu, sl [%s]", serverId,
            serviceLocator);
    } else {
        LOG(DEBUG, "Backup enlisted with id %lu, sl [%s]", serverId,
            serviceLocator);
        LOG(DEBUG, "Backup id %lu has %u MB/s read %u MB/s write ",
            serverId, readSpeed, writeSpeed);
        server.set_user_data(readSpeed);
    }
    respHdr.serverId = serverId;
}

/**
 * Handle the GET_SERVER_LIST RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::getServerList(const GetServerListRpc::Request& reqHdr,
                                 GetServerListRpc::Response& respHdr,
                                 Transport::ServerRpc& rpc)
{
LOG(DEBUG, "getServerList: %d", reqHdr.serverType);
    switch (reqHdr.serverType) {
    case MASTER:
        respHdr.serverListLength = serializeToResponse(rpc.replyPayload,
                                                       masterList);
        break;

    case BACKUP:
        respHdr.serverListLength = serializeToResponse(rpc.replyPayload,
                                                       backupList);
        break;

    default:
        throw RequestFormatError(HERE);
    }
}

/**
 * Handle the GET_TABLET_MAP RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::getTabletMap(const GetTabletMapRpc::Request& reqHdr,
                                GetTabletMapRpc::Response& respHdr,
                                Transport::ServerRpc& rpc)
{
    CycleCounter<Metric> _(&metrics->coordinator.getTabletMapTicks);
    respHdr.tabletMapLength = serializeToResponse(rpc.replyPayload,
                                                  tabletMap);
}

/**
 * Handle the HINT_SERVER_DOWN RPC.
 * \copydetails Server::ping
 * \param responder
 *      Functor to respond to the RPC before returning from this method. Used
 *      to avoid deadlock between first master and coordinator.
 */
void
CoordinatorServer::hintServerDown(const HintServerDownRpc::Request& reqHdr,
                                  HintServerDownRpc::Response& respHdr,
                                  Transport::ServerRpc& rpc,
                                  Responder& responder)
{
    string serviceLocator(getString(rpc.recvPayload, sizeof(reqHdr),
                                    reqHdr.serviceLocatorLength));
    responder();

    // reqHdr, respHdr, and rpc are off-limits now
    hintServerDown(serviceLocator);
}

/**
 * Do the heavy lifting of a HINT_SERVER_DOWN rpc. This includes poking
 * the purportedly dead host's FailureDetector to confirm death and
 * firing up a recovery if necessary.
 * 
 * \param serviceLocator
 *      The ServiceLocator string of the purportedly failed server.
 */
void
CoordinatorServer::hintServerDown(string serviceLocator)
{
    LOG(DEBUG, "Hint server down: %s", serviceLocator.c_str());

#ifndef TESTING
    if (!isServerDead(serviceLocator)) {
        LOG(DEBUG, "Server isn't dead!");
        return;
    }
#endif

    LOG(DEBUG, "Server is assumed dead; recovering!");

    // is it a master?
    for (int32_t i = 0; i < masterList.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& master(masterList.server(i));
        if (master.service_locator() == serviceLocator) {
            uint64_t serverId = master.server_id();
            boost::scoped_ptr<ProtoBuf::Tablets> will(
                reinterpret_cast<ProtoBuf::Tablets*>(master.user_data()));

            masterList.mutable_server()->SwapElements(
                                            masterList.server_size() - 1, i);
            masterList.mutable_server()->RemoveLast();

            // master is off-limits now

            foreach (ProtoBuf::Tablets::Tablet& tablet,
                     *tabletMap.mutable_tablet()) {
                if (tablet.server_id() == serverId)
                    tablet.set_state(ProtoBuf::Tablets_Tablet::RECOVERING);
            }

            LOG(DEBUG, "Trying partition recovery on %lu with %u masters "
                "and %u backups", serverId, masterList.server_size(),
                backupList.server_size());

            BaseRecovery* recovery = NULL;
            if (mockRecovery != NULL) {
                (*mockRecovery)(serverId, *will, masterList, backupList);
                recovery = mockRecovery;
            } else {
                recovery = new Recovery(serverId, *will,
                                        masterList, backupList);
            }

            // Keep track of recovery for each of the tablets its working on
            foreach (ProtoBuf::Tablets::Tablet& tablet,
                     *tabletMap.mutable_tablet()) {
                if (tablet.server_id() == serverId)
                    tablet.set_user_data(reinterpret_cast<uint64_t>(recovery));
            }

            recovery->start();
LOG(DEBUG, "recovery->start() returned");
            return;
        }
    }

    // is it a backup?
    for (int32_t i = 0; i < backupList.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& backup(backupList.server(i));
        if (backup.service_locator() == serviceLocator) {
            backupList.mutable_server()->SwapElements(
                                            backupList.server_size() - 1, i);
            backupList.mutable_server()->RemoveLast();

            // backup is off-limits now

            // TODO(ongaro): inform masters they need to replicate more
            return;
        }
    }
}

/**
 * Determine whether a server is actually dead or not.
 *
 * \param[in] serviceLocator
 *      The ServiceLocator string of the server to check.
 */
bool
CoordinatorServer::isServerDead(string serviceLocator)
{
    // it would be nice to use FailureDetector::pingServer here, but that
    // fires off an HSD to the coordinator if it times out, which isn't
    // what we want
    uint64_t nonce = generateRandom();
    PingRpc::Request req;
    req.common.type = PING;
    req.nonce = nonce;
    sockaddr_in sin = FailureDetector::serviceLocatorStringToSockaddrIn(
        serviceLocator);
    ssize_t r = sendto(failureProbeFd, &req, sizeof(req), 0,
        reinterpret_cast<sockaddr*>(&sin), sizeof(sin));
    if (r != sizeof(req)) {
        throw Exception(HERE,
            format("sendto failed; couldn't ping server! (r = %Zd)", r));
    }

    uint64_t startUsec = cyclesToNanoseconds(rdtsc()) / 1000;
    while (true) {
        uint64_t nowUsec = cyclesToNanoseconds(rdtsc()) / 1000;
        if (nowUsec > startUsec + FailureDetector::TIMEOUT_USECS)
            break;

        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(failureProbeFd, &fds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = FailureDetector::TIMEOUT_USECS - (nowUsec - startUsec);
        select(failureProbeFd + 1, &fds, NULL, NULL, &tv);

        if (FD_ISSET(failureProbeFd, &fds)) {
            PingRpc::Response resp;
            ssize_t r = recv(failureProbeFd, &resp, sizeof(resp), 0);
            if (r == sizeof(resp) && resp.nonce == nonce)
                return false;
        }
    }

    return true;
}

/**
 * Handle the TABLETS_RECOVERED RPC.
 * \copydetails Server::ping
 */
void
CoordinatorServer::tabletsRecovered(const TabletsRecoveredRpc::Request& reqHdr,
                                    TabletsRecoveredRpc::Response& respHdr,
                                    Transport::ServerRpc& rpc)
{
    CycleCounter<Metric> ticks(&metrics->coordinator.tabletsRecoveredTicks);
    if (reqHdr.status != STATUS_OK) {
        // we'll need to restart a recovery of that partition elsewhere
        // right now this just leaks the recovery object in the tabletMap
        LOG(ERROR, "A recovery master failed to recover its partition");
    }

    ProtoBuf::Tablets recoveredTablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload,
                                downCast<uint32_t>(sizeof(reqHdr)),
                                reqHdr.tabletsLength, recoveredTablets);
    ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload,
                                downCast<uint32_t>(sizeof(reqHdr)) +
                                reqHdr.tabletsLength,
                                reqHdr.willLength, *newWill);

    LOG(NOTICE, "called by masterId %lu with %u tablets, %u will entries",
        reqHdr.masterId, recoveredTablets.tablet_size(),
        newWill->tablet_size());

    // update the will
    setWill(reqHdr.masterId, rpc.recvPayload,
        downCast<uint32_t>(sizeof(reqHdr)) + reqHdr.tabletsLength,
        reqHdr.willLength);

    // update tablet map to point to new owner and mark as available
    foreach (const ProtoBuf::Tablets::Tablet& recoveredTablet,
             recoveredTablets.tablet())
    {
        foreach (ProtoBuf::Tablets::Tablet& tablet,
                 *tabletMap.mutable_tablet())
        {
            if (recoveredTablet.table_id() == tablet.table_id() &&
                recoveredTablet.start_object_id() == tablet.start_object_id() &&
                recoveredTablet.end_object_id() == tablet.end_object_id())
            {
                LOG(NOTICE, "Recovery complete on tablet %lu,%lu,%lu",
                    tablet.table_id(), tablet.start_object_id(),
                    tablet.end_object_id());
                BaseRecovery* recovery =
                    reinterpret_cast<Recovery*>(tablet.user_data());
                tablet.set_state(ProtoBuf::Tablets_Tablet::NORMAL);
                tablet.set_user_data(0);
                // The caller has filled in recoveredTablets with new service
                // locator and server id of the recovery master,
                // so just copy it over.
                tablet.set_service_locator(recoveredTablet.service_locator());
                tablet.set_server_id(recoveredTablet.server_id());
                bool recoveryComplete =
                    recovery->tabletsRecovered(recoveredTablets);
                if (recoveryComplete) {
                    LOG(NOTICE, "Recovery completed");
                    ticks.stop();
                    delete recovery;
                    // dump the tabletMap out for easy debugging
                    LOG(DEBUG, "Coordinator tabletMap:");
#ifndef __INTEL_COMPILER
                    foreach (const ProtoBuf::Tablets::Tablet& tablet,
                             tabletMap.tablet()) {
                        LOG(DEBUG, "table: %lu [%lu:%lu] state: %u owner: %lu",
                            tablet.table_id(), tablet.start_object_id(),
                            tablet.end_object_id(), tablet.state(),
                            tablet.server_id());
                    }
#endif
                    return;
                }
            }
        }
    }
}

/**
 * Top-level server method to handle the PING request.
 *
 * For debugging it print out statistics on the RPCs that it has
 * handled and instructs all the machines in the RAMCloud to do
 * so also (by pinging them all).
 *
 * \copydetails Server::ping
 */
void
CoordinatorServer::ping(const PingRpc::Request& reqHdr,
                        PingRpc::Response& respHdr,
                        Transport::ServerRpc& rpc)
{
    // dump out all the RPC stats for all the hosts so far
    foreach (const ProtoBuf::ServerList::Entry& server,
             backupList.server())
        BackupClient(transportManager.getSession(
            server.service_locator().c_str())).ping();
    foreach (const ProtoBuf::ServerList::Entry& server,
             masterList.server()) {
        try {
            MasterClient(transportManager.getSession(
                server.service_locator().c_str())).ping();
        } catch (...) {
            LOG(DEBUG, "ping failed on [%s]; skipping",
                server.service_locator().c_str());
        }
    }

    Server::ping(reqHdr, respHdr, rpc);
}

/**
 * Have all backups flush their dirty segments to storage.
 * \copydetails Server::ping
 */
void
CoordinatorServer::quiesce(const BackupQuiesceRpc::Request& reqHdr,
                           BackupQuiesceRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    foreach (auto& server, backupList.server()) {
        BackupClient(transportManager.getSession(
                        server.service_locator().c_str())).quiesce();
    }
}

/**
 * Update the Will associated with a specific Master. This is used
 * by Masters to keep their partitions balanced for efficient
 * recovery.
 *
 * \copydetails Server::ping
 */
void
CoordinatorServer::setWill(const SetWillRpc::Request& reqHdr,
                           SetWillRpc::Response& respHdr,
                           Transport::ServerRpc& rpc)
{
    CycleCounter<Metric> _(&metrics->coordinator.setWillTicks);
    if (!setWill(reqHdr.masterId, rpc.recvPayload, sizeof(reqHdr),
        reqHdr.willLength)) {
        // TODO(ongaro): should be some other error or silent
        throw RequestFormatError(HERE);
    }
}

/**
 * Set the Will for a given master.
 *
 * \param[in] masterId
 *      The ID of the master, whose Will we're setting.
 * \param[in] buffer
 *      Buffer containing the Will data.
 * \param[in] offset
 *      Offset of the Will data in the Buffer.
 * \param[in] length
 *      Length of the Will data in bytes.
 */
bool
CoordinatorServer::setWill(uint64_t masterId, Buffer& buffer,
    uint32_t offset, uint32_t length)
{
    foreach (auto& master, *masterList.mutable_server()) {
        if (master.server_id() == masterId) {
            ProtoBuf::Tablets* oldWill =
                reinterpret_cast<ProtoBuf::Tablets*>(master.user_data());

            ProtoBuf::Tablets* newWill = new ProtoBuf::Tablets();
            ProtoBuf::parseFromResponse(buffer, offset, length, *newWill);
            master.set_user_data(reinterpret_cast<uint64_t>(newWill));

            LOG(NOTICE, "Master %lu updated its Will (now %d entries, was %d)",
                masterId, newWill->tablet_size(), oldWill->tablet_size());

            delete oldWill;
            return true;
        }
    }

    LOG(WARNING, "Master %lu could not be found!!", masterId);
    return false;
}

/**
 * The thread that services incoming RPCs from FailureDetectors running
 * on other nodes.
 *
 * \param[in] arg
 *      Pointer to the CoordinatorServer object that this thread is servicing.
 */
void*
failureDetectorServerThread(void* arg)
{
    CoordinatorServer* coordinator = reinterpret_cast<CoordinatorServer*>(arg);

    while (true) {
        char buf[FailureDetector::MAXIMUM_MTU_BYTES];
        struct sockaddr_in sin;
        socklen_t sinLength = sizeof(sin);

        ssize_t r = recvfrom(coordinator->failureDetectorFd, buf, sizeof(buf),
            0, reinterpret_cast<sockaddr*>(&sin), &sinLength);
        if (r < 0) {
            LOG(WARNING, "recvfrom failed with %zd", r);
            break;
        }

        coordinator->failureDetectorHandler(buf, r, sin);
    }

    return NULL;
}

struct hintServerDownThreadArgs {
    string* locator;
    CoordinatorServer* coordinator;
};

/**
 * Jumping off point for handling a hintServerDown message via the UDP
 * FailureDetector socket. We cannot handle it in that thread, since we
 * risk not responding to ping requests, which can cause our clients to
 * think we're dead and abort their RPCs.
 *
 * This is really starting to get messy.
 */
void*
hintServerDownThread(void* arg)
{
    hintServerDownThreadArgs* args =
        reinterpret_cast<hintServerDownThreadArgs*>(arg);
    bigLock();
    args->coordinator->hintServerDown(*args->locator);
    bigUnlock();
    delete args->locator;
    delete args;
    return NULL;
}

/**
 * Handle an incoming message from a FailureDetector on our UDP port.
 * This must run in its own thread so that we can quickly reply to
 * ping requests from clients who suspect their RPCs to us may have
 * timed out.
 *
 * There are three possible messages: HintServerDown, GetServerList,
 * an Ping.
 *
 * The first is just a hint and the sender expects no response. The
 * second is an actual request, so we should reply. Since we're using
 * UDP, the requested server list must fit within the network MTU. This
 * is hacky, but it keeps things simple. In the future we'll want this
 * entire mechanism to be based on our RPC subsystem, but that will
 * require some significant changes.
 * 
 * The third simply generates a ping response.
 *
 * \param[in] buf
 *      Pointer to the incoming RPC to handle.
 * \param[in] length
 *      Length of the RPC in bytes.
 * \param[in] sin
 *      The sockaddr_in of the RPC's sender.
 */
void
CoordinatorServer::failureDetectorHandler(char* buf, ssize_t length,
    sockaddr_in sin)
{
    RpcRequestCommon* req = reinterpret_cast<RpcRequestCommon*>(buf);
    if (req->type == HINT_SERVER_DOWN) {
        HintServerDownRpc::Request* hSDReq =
            reinterpret_cast<HintServerDownRpc::Request*>(buf);

        if ((sizeof(*hSDReq) + hSDReq->serviceLocatorLength) >
          FailureDetector::MAXIMUM_MTU_BYTES) {
            LOG(WARNING, "bad serviceLocatorLength: %u",
                hSDReq->serviceLocatorLength);
            return;
        }

        char locatorBuf[hSDReq->serviceLocatorLength + 1];
        memcpy(locatorBuf, &buf[sizeof(*hSDReq)], hSDReq->serviceLocatorLength);
        locatorBuf[hSDReq->serviceLocatorLength] = '\0';

        LOG(DEBUG, "HintServerDown [%s] from %s:%d", locatorBuf,
            inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

        // ignore HSDs for a recovering server
        // this needs to be much smarter, but should do for the moment
        if (recoveringMasters.find(locatorBuf) != recoveringMasters.end())
            return;
        recoveringMasters[locatorBuf] = true;

        // fire off another thread to handle this. if we don't, then we can't
        // answer pings and our clients may start to assume we're dead and
        // abort RPCs
        hintServerDownThreadArgs* args = new hintServerDownThreadArgs();
        args->coordinator = this;
        args->locator = new string(locatorBuf);
        pthread_t tid;
        if (pthread_create(&tid, NULL, hintServerDownThread, args)) {
            LOG(ERROR, "failed to create hintServerDown thread!");
            delete args->locator;
            delete args;
        }
    } else if (req->type == GET_SERVER_LIST) {
        LOG(DEBUG, "GetServerList request from %s:%d",
            inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

        GetServerListRpc::Request* getReq =
            reinterpret_cast<GetServerListRpc::Request*>(buf);
        char responseBuf[FailureDetector::MAXIMUM_MTU_BYTES];
        GetServerListRpc::Response* resp =
            reinterpret_cast<GetServerListRpc::Response*>(responseBuf);

        std::ostringstream ostream;
        bigLock();
        if (getReq->serverType == MASTER)
            masterList.SerializePartialToOstream(&ostream);
        else
            backupList.SerializePartialToOstream(&ostream);
        bigUnlock();
        string str(ostream.str());

        if (sizeof(*resp) + str.length() > FailureDetector::MAXIMUM_MTU_BYTES) {
            LOG(WARNING, "cannot fit %s list into rpc",
                (getReq->serverType == MASTER) ? "master" : "server");
            return;
        }

        memcpy(&responseBuf[sizeof(*resp)], str.c_str(), str.length());
        resp->common.status = STATUS_OK;
        resp->serverListLength = downCast<uint32_t>(str.length());

        ssize_t r = sendto(failureDetectorFd, responseBuf,
            sizeof(*resp) + str.length(), 0, reinterpret_cast<sockaddr*>(&sin),
            sizeof(sin));
        if (r != static_cast<ssize_t>(sizeof(*resp) + str.length())) {
            LOG(WARNING, "failed to send GetServerList reply; r = %zd, "
                "errno %d: %s", r, errno, strerror(errno));
        }
    } else if (req->type == PING) {
        LOG(DEBUG, "Ping request from %s:%d",
            inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

        PingRpc::Request *req = reinterpret_cast<PingRpc::Request*>(buf);
        PingRpc::Response resp;
        resp.common.status = STATUS_OK;
        resp.nonce = req->nonce;
        ssize_t r = sendto(failureDetectorFd, &resp, sizeof(resp), 0,
            reinterpret_cast<sockaddr*>(&sin), sizeof(sin));
        if (r != sizeof(resp))
            LOG(WARNING, "sendto returned wrong number of bytes (%Zd)", r);
    } else {
        LOG(WARNING, "weird rpc type received: %d", req->type);
    }
}

} // namespace RAMCloud
