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

#include <boost/scoped_ptr.hpp>

#include "Buffer.h"
#include "ClientException.h"
#include "MasterServer.h"
#include "ObjectTub.h"
#include "ProtoBuf.h"
#include "RecoverySegmentIterator.h"
#include "Rpc.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Transport.h"
#include "TransportManager.h"
#include "Will.h"

namespace RAMCloud {

// --- SegmentLocatorChooser ---

/**
 * \param list
 *      A list of servers along with the segment id they have backed up.
 *      See Recovery for details on this format.
 */
SegmentLocatorChooser::SegmentLocatorChooser(const ProtoBuf::ServerList& list)
    : map()
    , ids()
{
    foreach (const ProtoBuf::ServerList::Entry& server, list.server()) {
        if (!server.has_segment_id()) {
            LOG(WARNING,
                "List of backups for recovery must contain segmentIds");
            continue;
        }
        if (server.server_type() != ProtoBuf::BACKUP) {
            LOG(WARNING,
                "List of backups for recovery shouldn't contain MASTERs");
            continue;
        }
        map.insert(make_pair(server.segment_id(), server.service_locator()));
    }
    std::transform(map.begin(), map.end(), back_inserter(ids),
                   &first<uint64_t, string>);
    // not the most efficient approach in the world...
    SegmentIdList::iterator newEnd = std::unique(ids.begin(), ids.end());
    ids.erase(newEnd, ids.end());
}

/**
 * Provide locators for potential backup server to contact to find
 * segment data during recovery.
 *
 * \param segmentId
 *      A segment id for a segment that needs to be recovered.
 * \return
 *      A service locator string indicating a location where this segment
 *      can be recovered from.
 * \throw SegmentRecoveryFailedException
 *      If the requested segment id has no remaining potential backup
 *      locations.
 */
const string&
SegmentLocatorChooser::get(uint64_t segmentId)
{
    LocatorMap::size_type count = map.count(segmentId);

    ConstLocatorRange range = map.equal_range(segmentId);
    if (range.first == range.second)
        throw SegmentRecoveryFailedException(HERE);

    // Using rdtsc() as a fast random number generator. Perhaps a bad idea.
    LocatorMap::size_type random = rdtsc() % count;
    LocatorMap::const_iterator it = range.first;
    for (LocatorMap::size_type i = 0; i < random; ++i)
        ++it;
    return it->second;
}

/**
 * Returns a randomly ordered list of segment ids which acts as a
 * schedule for recovery.
 */
const SegmentLocatorChooser::SegmentIdList&
SegmentLocatorChooser::getSegmentIdList()
{
    return ids;
}

/**
 * Remove the locator as a potential backup location for a particular
 * segment.
 *
 * \param segmentId
 *      The id of a segment which could not be located at the specified
 *      backup locator.
 * \param locator
 *      The locator string that should not be returned from future calls
 *      to get for this particular #segmentId.
 */
void
SegmentLocatorChooser::markAsDown(uint64_t segmentId, const string& locator)
{
    LocatorRange range = map.equal_range(segmentId);
    if (range.first == map.end())
        return;

    for (LocatorMap::iterator it = range.first;
         it != range.second; ++it)
    {
        if (it->second == locator) {
            map.erase(it);
            return;
        }
    }
}

// --- MasterServer ---

void objectEvictionCallback(LogEntryType type,
                            const void* p,
                            const uint64_t entryLength,
                            const uint64_t lenghtInLog,
                            const LogTime,
                            void* cookie);
void tombstoneEvictionCallback(LogEntryType type,
                               const void* p,
                               const uint64_t entryLength,
                               const uint64_t lenghtInLog,
                               const LogTime,
                               void* cookie);

/**
 * Construct a MasterServer.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param coordinator
 *      A client to the coordinator for the RAMCloud this Master is in.
 * \param replicas
 *      The number of backups required before writes are considered safe.
 */
MasterServer::MasterServer(const ServerConfig config,
                           CoordinatorClient* coordinator,
                           uint32_t replicas)
    : config(config)
    , coordinator(coordinator)
    // Permit a NULL coordinator for testing/benchmark purposes.
    , serverId(coordinator ? coordinator->enlistServer(MASTER,
                                                       config.localLocator)
                           : 0)
    , backup(coordinator, serverId, replicas)
    , bytesWritten(0)
    , log(serverId, config.logBytes, Segment::SEGMENT_SIZE, &backup)
    , objectMap(config.hashTableBytes / ObjectMap::bytesPerCacheLine(), 2)
    , tablets()
{
    LOG(NOTICE, "My server ID is %lu", serverId);
    log.registerType(LOG_ENTRY_TYPE_OBJ, objectEvictionCallback, this);
    log.registerType(LOG_ENTRY_TYPE_OBJTOMB, tombstoneEvictionCallback, this);
}

MasterServer::~MasterServer()
{
    std::set<Table*> tables;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
    foreach (Table* table, tables)
        delete table;
}

void
MasterServer::dispatch(RpcType type, Transport::ServerRpc& rpc,
                       Responder& responder)
{
    switch (type) {
        case CreateRpc::type:
            callHandler<CreateRpc, MasterServer,
                        &MasterServer::create>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, MasterServer,
                        &MasterServer::ping>(rpc);
            break;
        case ReadRpc::type:
            callHandler<ReadRpc, MasterServer,
                        &MasterServer::read>(rpc);
            break;
        case RecoverRpc::type:
            callHandler<RecoverRpc, MasterServer,
                        &MasterServer::recover>(rpc, responder);
            break;
        case RemoveRpc::type:
            callHandler<RemoveRpc, MasterServer,
                        &MasterServer::remove>(rpc);
            break;
        case SetTabletsRpc::type:
            callHandler<SetTabletsRpc, MasterServer,
                        &MasterServer::setTablets>(rpc);
            break;
        case WriteRpc::type:
            callHandler<WriteRpc, MasterServer,
                        &MasterServer::write>(rpc);
            break;
        default:
            throw UnimplementedRequestError(HERE);
    }
}

void __attribute__ ((noreturn))
MasterServer::run()
{
    while (true)
        handleRpc<MasterServer>();
}

/**
 * Top-level server method to handle the CREATE request.
 * See the documentation for the corresponding method in RamCloudClient for
 * complete information about what this request does.
 * \copydetails Server::ping
 */
void
MasterServer::create(const CreateRpc::Request& reqHdr,
                     CreateRpc::Response& respHdr,
                     Transport::ServerRpc& rpc)
{
    Table& t(getTable(reqHdr.tableId, ~0UL));
    uint64_t id = t.AllocateKey(&objectMap);

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    storeData(reqHdr.tableId, id, &rejectRules,
              &rpc.recvPayload, sizeof(reqHdr), reqHdr.length,
              &respHdr.version);
    respHdr.id = id;
}

/**
 * Top-level server method to handle the PING request.
 *
 * For debugging it print out statistics on the RPCs that it has
 * handled along with some stats on amount of data written to the
 * master.
 *
 * \copydetails Server::ping
 */
void
MasterServer::ping(const PingRpc::Request& reqHdr,
                   PingRpc::Response& respHdr,
                   Transport::ServerRpc& rpc)
{
    LOG(NOTICE, "Bytes written: %lu", bytesWritten);
    LOG(NOTICE, "Bytes logged : %lu", log.getBytesAppended());

    Server::ping(reqHdr, respHdr, rpc);
}

/**
 * Top-level server method to handle the READ request.
 * \copydetails create
 */
void
MasterServer::read(const ReadRpc::Request& reqHdr,
                   ReadRpc::Response& respHdr,
                   Transport::ServerRpc& rpc)
{
    // We must throw an exception if the table does not exist. Also, we might
    // have an entry in the hash table that's invalid because its tablet no
    // longer lives here.
    getTable(reqHdr.tableId, reqHdr.id);

    uint8_t type;
    const Objectable* o = objectMap.lookup(reqHdr.tableId, reqHdr.id, &type);
    if (!o) {
        throw ObjectDoesntExistException(HERE);
    }

    assert(type == 0);
    const Object* obj = o->asObject();
    respHdr.version = obj->version;
    rejectOperation(&reqHdr.rejectRules, obj->version);
    Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
                                  obj->data,
                                  static_cast<uint32_t>(obj->data_len));
    // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
    // from scribbling over obj->data.
    respHdr.length = obj->data_len;
}

/**
 * Callback used to purge the tombstones from the hash table. Invoked by
 * HashTable::forEach.
 */
void
recoveryCleanup(const Objectable *maybeTomb, uint8_t type, void *cookie)
{
    if (type) {
        const ObjectTombstone *tomb = maybeTomb->asObjectTombstone();
        MasterServer *server = reinterpret_cast<MasterServer*>(cookie);
        bool ret = server->objectMap.remove(tomb->table, tomb->id);
        assert(ret);
        free(const_cast<ObjectTombstone*>(tomb));
    }
}

/**
 * Remove leftover tombstones in the hash table added during recovery.
 * This method exists independently for testing purposes.
 */
void
MasterServer::removeTombstones()
{
    objectMap.forEach(recoveryCleanup, this);
}

// used in recover()
struct Task {
    Task(uint64_t masterId, uint64_t segmentId,
         const char* backupLocator, uint64_t partitionId)
        : segmentId(segmentId)
        , backupLocator(backupLocator)
        , response()
        , client(transportManager.getSession(backupLocator))
        , rpc(client, masterId, segmentId, partitionId, response)
    {}
    uint64_t segmentId;
    const char* backupLocator;
    Buffer response;
    BackupClient client;
    BackupClient::GetRecoveryData rpc;
    DISALLOW_COPY_AND_ASSIGN(Task);
};

/**
 * Helper for public recover() method.
 * Collect all the filtered log segments from backups for a set of tablets
 * formerly belonging to a crashed master which is being recovered and pass
 * them to the recovery master to have them replayed.
 *
 * \param masterId
 *      The id of the crashed master for which recoveryMaster will be taking
 *      over ownership of tablets.
 * \param partitionId
 *      The partition id of tablets inside the crashed master's will that
 *      this master is recovering.
 * \param backups
 *      A list of backup locators along with a segmentId specifying for each
 *      segmentId a backup who can provide a filtered recovery data segment.
 *      A particular segment may be listed more than once if it has multiple
 *      viable backups, hence a particular backup locator can also be listed
 *      many times.
 */
void
MasterServer::recover(uint64_t masterId,
                      uint64_t partitionId,
                      const ProtoBuf::ServerList& backups)
{
    LOG(NOTICE, "Recovering master %lu, partition %lu, %u hosts",
        masterId, partitionId, backups.server_size());

#if TESTING
    if (!mockRandomValue)
        srand(rdtsc());
    else
        srand(0);
#else
    srand(rdtsc());
#endif

#ifdef PERF_DEBUG_RECOVERY_SERIAL
    ObjectTub<Task> tasks[1];
#else
    ObjectTub<Task> tasks[4];
#endif

    SegmentLocatorChooser chooser(backups);
    auto segIdsIt = chooser.getSegmentIdList().begin();
    auto segIdsEnd = chooser.getSegmentIdList().end();
    uint32_t activeSegments = 0;

    // Start RPCs
    foreach (auto& task, tasks) {
        if (segIdsIt == segIdsEnd)
            break;
        uint64_t segmentId = *segIdsIt++;
        task.construct(masterId, segmentId,
                       chooser.get(segmentId).c_str(),
                       partitionId);
        ++activeSegments;
    }

    // As RPCs complete, process them and start more
    while (activeSegments > 0) {
        foreach (auto& task, tasks) {
            if (!task || !task->rpc.isReady())
                continue;
            LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                task->segmentId, task->backupLocator);
            try {
                (task->rpc)();
            } catch (const TransportException& e) {
                LOG(DEBUG, "Couldn't contact %s, trying next backup; "
                    "failure was: %s", task->backupLocator, e.str().c_str());
                // TODO(ongaro): try to get this segment from other backups
                throw SegmentRecoveryFailedException(HERE);
            } catch (const ClientException& e) {
                LOG(DEBUG, "getRecoveryData failed on %s, trying next backup; "
                    "failure was: %s", task->backupLocator, e.str().c_str());
                // TODO(ongaro): try to get this segment from other backups
                throw SegmentRecoveryFailedException(HERE);
            }

            uint32_t responseLen = task->response.getTotalLength();
            LOG(DEBUG, "Recovering segment %lu with size %u",
                task->segmentId, responseLen);
            recoverSegment(task->segmentId,
                           task->response.getRange(0, responseLen),
                           responseLen);
            task.destroy();
            if (segIdsIt == segIdsEnd) {
                --activeSegments;
                continue;
            }
            uint64_t segmentId = *segIdsIt++;
            task.construct(masterId, segmentId,
                           chooser.get(segmentId).c_str(),
                           partitionId);
        }
    }

    log.sync();
}

/**
 * Top-level server method to handle the RECOVER request.
 * \copydetails Server::ping
 * \param responder
 *      Functor to respond to the RPC before returning from this method.
 */
void
MasterServer::recover(const RecoverRpc::Request& reqHdr,
                      RecoverRpc::Response& respHdr,
                      Transport::ServerRpc& rpc,
                      Responder& responder)
{
    const auto& masterId = reqHdr.masterId;
    const auto& partitionId = reqHdr.partitionId;
    ProtoBuf::Tablets recoveryTablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload, sizeof(reqHdr),
                                reqHdr.tabletsLength, recoveryTablets);
    ProtoBuf::ServerList backups;
    ProtoBuf::parseFromResponse(rpc.recvPayload,
                                sizeof(reqHdr) + reqHdr.tabletsLength,
                                reqHdr.serverListLength, backups);
    LOG(DEBUG, "Starting recovery of %u tablets on masterId %lu",
        recoveryTablets.tablet_size(), serverId);
    responder();

    // reqHdr, respHdr, and rpc are off-limits now

    // Recover Segments, firing MasterServer::recoverSegment for each one.
    recover(masterId, partitionId, backups);

    // Free recovery tombstones left in the hash table.
    removeTombstones();

    // Once the coordinator and the recovery master agree that the
    // master has taken over for the tablets it can update its tables
    // and begin serving requests.

    // Update the recoveryTablets to reflect the fact that this master is
    // going to try to become the owner.
    foreach (ProtoBuf::Tablets::Tablet& tablet,
             *recoveryTablets.mutable_tablet()) {
        LOG(NOTICE, "set tablet %lu %lu %lu to locator %s, id %lu",
                 tablet.table_id(), tablet.start_object_id(),
                 tablet.end_object_id(), config.localLocator.c_str(), serverId);
        tablet.set_service_locator(config.localLocator);
        tablet.set_server_id(serverId);
    }

    coordinator->tabletsRecovered(recoveryTablets);
    // Ok - we're free to start serving now.

    // Union the new tablets into an updated tablet map
    ProtoBuf::Tablets newTablets(tablets);
    newTablets.mutable_tablet()->MergeFrom(recoveryTablets.tablet());
    // and set ourself as open for business.
    setTablets(newTablets);
    // TODO(stutsman) update local copy of the will
}

/**
 * Given a RecoverySegmentIterator for the Segment we're currently
 * recovering, advance it and issue prefetches on the hash tables.
 * This is used exclusively by recoverSegment().
 *
 * \param[in] i
 *      A RecoverySegmentIterator to use for prefetching. Note that this
 *      method modifies the iterator, so the caller should not use
 *      it for its own iteration.
 */
void
MasterServer::recoverSegmentPrefetcher(RecoverySegmentIterator& i)
{
    i.next();

    if (i.isDone())
        return;

    LogEntryType type = i.getType();
    uint64_t objId = ~0UL, tblId = ~0UL;

    if (type == LOG_ENTRY_TYPE_OBJ) {
        const Object *recoverObj = reinterpret_cast<const Object *>(
                     i.getPointer());
        objId = recoverObj->id;
        tblId = recoverObj->table;
    } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
        const ObjectTombstone *recoverTomb =
            reinterpret_cast<const ObjectTombstone *>(i.getPointer());
        objId = recoverTomb->id;
        tblId = recoverTomb->table;
    } else {
        return;
    }

    objectMap.prefetchBucket(tblId, objId);
}

/**
 * Replay a filtered segment from a crashed Master that this Master is taking
 * over for.
 *
 * \param segmentId
 *      The segmentId of the segment as it was in the log of the crashed Master.
 * \param buffer 
 *      A pointer to a valid segment which has been pre-filtered of all
 *      objects except those that pertain to the tablet ranges this Master
 *      will be responsible for after the recovery completes.
 * \param bufferLength
 *      Length of the buffer in bytes.
 */
void
MasterServer::recoverSegment(uint64_t segmentId, const void *buffer,
    uint64_t bufferLength)
{
    LOG(DEBUG, "recoverSegment %lu, ...", segmentId);

    RecoverySegmentIterator i(buffer, bufferLength);
#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_PREFETCH
    RecoverySegmentIterator prefetch(buffer, bufferLength);
#endif

#ifdef PERF_DEBUG_RECOVERY_REC_SEG_JUST_ITER
    for (; !i.isDone(); i.next());
    return;
#endif
    while (!i.isDone()) {
        LogEntryType type = i.getType();

#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_PREFETCH
        recoverSegmentPrefetcher(prefetch);
#endif

        if (type == LOG_ENTRY_TYPE_OBJ) {
            const Object *recoverObj = reinterpret_cast<const Object *>(
                i.getPointer());
            uint64_t objId = recoverObj->id;
            uint64_t tblId = recoverObj->table;

#ifdef PERF_DEBUG_RECOVERY_REC_SEG_NO_HT
            const Object *localObj = 0;
            const ObjectTombstone *tomb = 0;
#else
            uint8_t type = 0;
            const Objectable *o = objectMap.lookup(tblId, objId, &type);
            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            if (type)
                tomb = o->asObjectTombstone();
            else
                localObj = o->asObject();
#endif

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version + 1;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverObj->version >= minSuccessor) {
#ifdef PERF_DEBUG_RECOVERY_REC_SEG_NO_LOG
                const Object* newObj = localObj;
#else
                // write to log (with lazy backup flush) & update hash table
                uint64_t lengthInLog;
                LogTime logTime;
                const Object *newObj = reinterpret_cast<const Object*>(
                    log.append(LOG_ENTRY_TYPE_OBJ, recoverObj,
                                recoverObj->size(), &lengthInLog,
                                &logTime, false));

                // update the TabletProfiler
                Table& t(getTable(recoverObj->table, recoverObj->id));
                t.profiler.track(recoverObj->id, lengthInLog, logTime);
#endif

#ifndef PERF_DEBUG_RECOVERY_REC_SEG_NO_HT
                objectMap.replace(tblId, objId, newObj, 0);
#endif

                // nuke the tombstone, if it existed
                if (tomb != NULL)
                    free(const_cast<ObjectTombstone *>(tomb));

                // nuke the old object, if it existed
                if (localObj != NULL)
                    log.free(localObj);
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            const ObjectTombstone *recoverTomb =
                reinterpret_cast<const ObjectTombstone *>(i.getPointer());
            uint64_t objId = recoverTomb->id;
            uint64_t tblId = recoverTomb->table;

            uint8_t type = 0;
            const Objectable *o = objectMap.lookup(tblId, objId, &type);
            const Object *localObj = NULL;
            const ObjectTombstone *tomb = NULL;
            if (type)
                tomb = o->asObjectTombstone();
            else
                localObj = o->asObject();

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverTomb->objectVersion >= minSuccessor) {
                // allocate memory for the tombstone & update hash table
                // TODO(ongaro): Change to new with copy constructor?
                ObjectTombstone *newTomb = reinterpret_cast<ObjectTombstone *>(
                    xmalloc(sizeof(*newTomb)));
                memcpy(newTomb, const_cast<ObjectTombstone *>(recoverTomb),
                    sizeof(*newTomb));
                objectMap.replace(tblId, objId, newTomb, 1);

                // nuke the old tombstone, if it existed
                if (tomb != NULL)
                    free(const_cast<ObjectTombstone *>(tomb));

                // nuke the object, if it existed
                if (localObj != NULL)
                    log.free(localObj);
            }
        }

        i.next();
    }
    LOG(NOTICE, "Segment %lu replay complete", segmentId);
}

/**
 * Top-level server method to handle the REMOVE request.
 * \copydetails create
 */
void
MasterServer::remove(const RemoveRpc::Request& reqHdr,
                     RemoveRpc::Response& respHdr,
                     Transport::ServerRpc& rpc)
{
    Table& t(getTable(reqHdr.tableId, reqHdr.id));
    uint8_t type;
    const Objectable *o = objectMap.lookup(reqHdr.tableId, reqHdr.id, &type);
    if (o == NULL) {
        rejectOperation(&reqHdr.rejectRules, VERSION_NONEXISTENT);
        return;
    }

    assert(type == 0);
    const Object *obj = o->asObject();
    respHdr.version = obj->version;

    // Abort if we're trying to delete the wrong version.
    rejectOperation(&reqHdr.rejectRules, respHdr.version);

    t.RaiseVersion(obj->version + 1);

    ObjectTombstone tomb(log.getSegmentId(obj), obj);

    // Mark the deleted object as free first, since the append could
    // invalidate it
    log.free(obj);

    // Write the tombstone into the Log, update our tablet
    // counters, and remove from the hash table.
    uint64_t lengthInLog;
    LogTime logTime;

    log.append(LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb),
        &lengthInLog, &logTime);
    t.profiler.track(obj->id, lengthInLog, logTime);
    objectMap.remove(reqHdr.tableId, reqHdr.id);
}

/**
 * Set the list of tablets that this master serves.
 *
 * Notice that this method does nothing about the objects and data
 * for a particular tablet.  That is, the log and hashtable must already
 * contain a consistent view of the tablet before being set as an active
 * tablet with this method.
 *
 * \param newTablets
 *      The new set of tablets this master is serving.
 */
void
MasterServer::setTablets(const ProtoBuf::Tablets& newTablets)
{
    typedef std::map<uint32_t, Table*> Tables;
    Tables tables;

    // create map from table ID to Table of pre-existing tables
    foreach (const ProtoBuf::Tablets::Tablet& oldTablet, tablets.tablet()) {
        tables[oldTablet.table_id()] =
            reinterpret_cast<Table*>(oldTablet.user_data());
    }

    // overwrite tablets with new tablets
    tablets = newTablets;

    // delete pre-existing tables that no longer live here
#ifdef __INTEL_COMPILER
    for (Tables::iterator it(tables.begin()); it != tables.end(); ++it) {
        Tables::value_type oldTable = *it;
        for (uint32_t i = 0; i < tablets.tablet_size(); ++i) {
            const ProtoBuf::Tablets::Tablet& newTablet(tablets.tablet(i));
#else
    foreach (Tables::value_type oldTable, tables) {
        foreach (const ProtoBuf::Tablets::Tablet& newTablet,
                 tablets.tablet()) {
#endif
            if (oldTable.first == newTablet.table_id())
                goto next;
        }
        delete oldTable.second;
        oldTable.second = NULL;
      next:
        { /* pass */ }
    }

    // create new Tables and assign all new tablets tables
    LOG(NOTICE, "Now serving tablets:");
#ifdef __INTEL_COMPILER
    for (uint32_t i = 0; i < tablets.tablet_size(); ++i) {
        ProtoBuf::Tablets::Tablet& newTablet(*tablets.mutable_tablet(i));
#else
    foreach (ProtoBuf::Tablets::Tablet& newTablet, *tablets.mutable_tablet()) {
#endif
        LOG(NOTICE, "table: %20lu, start: %20lu, end  : %20lu",
            newTablet.table_id(), newTablet.start_object_id(),
            newTablet.end_object_id());
        Table* table = tables[newTablet.table_id()];
        if (table == NULL) {
            table = new Table(newTablet.table_id());
            tables[newTablet.table_id()] = table;
        }
        newTablet.set_user_data(reinterpret_cast<uint64_t>(table));
    }
}

/**
 * Top-level server method to handle the SET_TABLETS request.
 * \copydetails create
 */
void
MasterServer::setTablets(const SetTabletsRpc::Request& reqHdr,
                         SetTabletsRpc::Response& respHdr,
                         Transport::ServerRpc& rpc)
{
    ProtoBuf::Tablets newTablets;
    ProtoBuf::parseFromRequest(rpc.recvPayload, sizeof(reqHdr),
                               reqHdr.tabletsLength, newTablets);
    setTablets(newTablets);
}

/**
 * Top-level server method to handle the WRITE request.
 * \copydetails create
 */
void
MasterServer::write(const WriteRpc::Request& reqHdr,
                    WriteRpc::Response& respHdr,
                    Transport::ServerRpc& rpc)
{
    storeData(reqHdr.tableId, reqHdr.id,
              &reqHdr.rejectRules, &rpc.recvPayload, sizeof(reqHdr),
              static_cast<uint32_t>(reqHdr.length), &respHdr.version);
}

void
MasterServer::calculateWill()
{
#define MAX_BYTES 640 * 1024 * 1024
#define MAX_REFS 10 * 1000 * 1000
    Will will(tablets, MAX_BYTES, MAX_REFS);
    will.debugDump();
}

/**
 * Ensures that this master owns the tablet for the given object
 * and returns the corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 * \param objectId
 *      Identifier for a desired object.
 *
 * \return
 *      The Table of which the tablet containing this object is a part.
 *
 * \exception TableDoesntExist
 *      Thrown if that tablet isn't owned by this server.
 */
// TODO(ongaro): Masters don't know whether tables exist.
// This be something like ObjectNotHereException.
Table&
MasterServer::getTable(uint32_t tableId, uint64_t objectId) {

    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet()) {
        if (tablet.table_id() == tableId &&
            tablet.start_object_id() <= objectId &&
            objectId <= tablet.end_object_id()) {
            return *reinterpret_cast<Table*>(tablet.user_data());
        }
    }
    throw TableDoesntExistException(HERE);
}

/**
 * Check a set of RejectRules against the current state of an object
 * to decide whether an operation is allowed.
 *
 * \param rejectRules
 *      Specifies conditions under which the operation should fail.
 * \param version
 *      The current version of an object, or VERSION_NONEXISTENT
 *      if the object does not currently exist (used to test rejectRules)
 *
 * \return
 *      The return value is STATUS_OK if none of the reject rules
 *      indicate that the operation should be rejected. Otherwise
 *      the return value indicates the reason for the rejection.
 */
void
MasterServer::rejectOperation(const RejectRules* rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules->doesntExist)
            throw ObjectDoesntExistException(HERE);
        return;
    }
    if (rejectRules->exists)
        throw ObjectExistsException(HERE);
    if (rejectRules->versionLeGiven && version <= rejectRules->givenVersion)
        throw WrongVersionException(HERE);
    if (rejectRules->versionNeGiven && version != rejectRules->givenVersion)
        throw WrongVersionException(HERE);
}

//-----------------------------------------------------------------------
// Everything below here is "old" code, meaning it probably needs to
// get refactored at some point, it doesn't follow the coding conventions,
// and there are no unit tests for it.
//-----------------------------------------------------------------------

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and evicts
 * an Object (i.e. an entry of type LOG_ENTRY_TYPE_OBJ).
 *
 * Upon return, the object will be discarded. Objects must therefore be
 * perpetuated when the object being evicted is exactly the object referenced
 * by the hash table. Otherwise, it's an old object and a tombstone for it
 * exists.
 *
 * \param[in]  type
 *      LogEntryType of the evictee (LOG_ENTRY_TYPE_OBJ).
 * \param[in]  p
 *      Opaque pointer to the immutable entry in the log. 
 * \param[in]  entryLength
 *      Size of the log entry being evicted in bytes. This value is only
 *      the number of bytes of the buffer given to the Log::append() method.
 * \param[in]  lengthInLog
 *      objectLength plus all bytes of overhead consumed in the Log. This
 *      represents the total amount of system memory consumed by the
 *      Log::append() operation that wrote this entry.
 * \param[in]  logTime
 *      The LogTime corresponding to the append operation that wrote this
 *      entry.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
objectEvictionCallback(LogEntryType type,
                       const void* p,
                       const uint64_t entryLength,
                       const uint64_t lengthInLog,
                       const LogTime logTime,
                       void* cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJ);

    MasterServer *svr = static_cast<MasterServer *>(cookie);
    assert(svr != NULL);

    Log& log = svr->log;

    const Object *evictObj = static_cast<const Object *>(p);
    assert(evictObj != NULL);

    Table *t = NULL;
    try {
        t = &svr->getTable(evictObj->table, evictObj->id);
    } catch (TableDoesntExistException& e) {
        // That tablet doesn't exist on this server anymore.
        // Just remove the hash table entry, if it exists.
        svr->objectMap.remove(evictObj->table, evictObj->id);
        return;
    }

    uint8_t hashTableType;
    const Objectable *o =
        svr->objectMap.lookup(evictObj->table, evictObj->id, &hashTableType);
    assert(hashTableType == 0);
    const Object *hashTblObj = o->asObject();

    // simple pointer comparison suffices
    if (hashTblObj == evictObj) {
        uint64_t newLengthInLog;
        LogTime newLogTime;
        const Object *newObj = (const Object *)log.append(LOG_ENTRY_TYPE_OBJ,
            evictObj, evictObj->size(), &newLengthInLog, &newLogTime);
        t->profiler.track(evictObj->id, newLengthInLog, newLogTime);
        svr->objectMap.replace(evictObj->table, evictObj->id, newObj);
    }

    // remove the evicted entry whether it is discarded or not
    t->profiler.untrack(evictObj->id, lengthInLog, logTime);
}

/**
 * Callback used by the LogCleaner when it's cleaning a Segment and evicts
 * an ObjectTombstone (i.e. an entry of type LOG_ENTRY_TYPE_OBJTOMB).
 *
 * Tombstones are perpetuated when the Segment they reference is still
 * valid in the system.
 *
 * \param[in]  type
 *      LogEntryType of the evictee (LOG_ENTRY_TYPE_OBJTOMB).
 * \param[in]  p
 *      Opaque pointer to the immutable entry in the log.
 * \param[in]  entryLength
 *      Size of the log entry being evicted in bytes. This value is only
 *      the number of bytes of the buffer given to the Log::append() method.
 * \param[in]  lengthInLog
 *      objectLength plus all bytes of overhead consumed in the Log. This
 *      represents the total amount of system memory consumed by the
 *      Log::append() operation that wrote this entry.
 * \param[in]  logTime
 *      The LogTime corresponding to the append operation that wrote this
 *      entry.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
tombstoneEvictionCallback(LogEntryType type,
                          const void* p,
                          const uint64_t entryLength,
                          const uint64_t lengthInLog,
                          const LogTime logTime,
                          void* cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJTOMB);

    MasterServer *svr = static_cast<MasterServer *>(cookie);
    assert(svr != NULL);

    Log& log = svr->log;

    const ObjectTombstone *tomb =
        static_cast<const ObjectTombstone *>(p);
    assert(tomb != NULL);

    Table *t = NULL;
    try {
        t = &svr->getTable(tomb->table, tomb->id);
    } catch (TableDoesntExistException& e) {
        // That tablet doesn't exist on this server anymore.
        return;
    }

    // see if the referant is still there
    if (log.isSegmentLive(tomb->segmentId)) {
        uint64_t newLengthInLog;
        LogTime newLogTime;
        log.append(LOG_ENTRY_TYPE_OBJTOMB, tomb, sizeof(*tomb),
            &newLengthInLog, &newLogTime);
        t->profiler.track(tomb->id, newLengthInLog, newLogTime);
    }

    // remove the evicted entry whether it is discarded or not
    t->profiler.untrack(tomb->id, lengthInLog, logTime);
}

void
MasterServer::storeData(uint64_t tableId, uint64_t id,
                        const RejectRules* rejectRules, Buffer* data,
                        uint32_t dataOffset, uint32_t dataLength,
                        uint64_t* newVersion)
{
    Table& t(getTable(tableId, id));

    uint8_t type;
    const Object *obj = NULL;
    const Objectable *o = objectMap.lookup(tableId, id, &type);
    if (o != NULL) {
        assert(type == 0);
        obj = o->asObject();
    }

    uint64_t version = (obj != NULL) ? obj->version : VERSION_NONEXISTENT;
    uint64_t lengthInLog;
    LogTime logTime;

    try {
        rejectOperation(rejectRules, version);
    } catch (...) {
        *newVersion = version;
        throw;
    }

    DECLARE_OBJECT(newObject, dataLength);

    newObject->id = id;
    newObject->table = tableId;
    if (o != NULL)
        newObject->version = obj->version + 1;
    else
        newObject->version = t.AllocateVersion();
    assert(obj == NULL || newObject->version > obj->version);
    // TODO(stutsman): dm's super-fast checksum here
    newObject->checksum = 0x0BE70BE70BE70BE7ULL;
    newObject->data_len = dataLength;
    data->copy(dataOffset, dataLength, newObject->data);

    // If the Object is being overwritten, we need to mark the previous space
    // used as free and add a tombstone that references it.
    if (o != NULL) {
        // Mark the old object as freed _before_ writing the new object to the
        // log. If we do it afterwards, the LogCleaner could be triggered and
        // `o' could be reclaimed before log->append() returns. The subsequent
        // free then breaks, as that Segment may have been cleaned.
        log.free(o);

        uint64_t segmentId = log.getSegmentId(o);
        ObjectTombstone tomb(segmentId, obj);
        log.append(LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb), &lengthInLog,
            &logTime);
        t.profiler.track(id, lengthInLog, logTime);
    }

    const Object *objPtr = (const Object *)log.append(LOG_ENTRY_TYPE_OBJ,
        newObject, newObject->size(), &lengthInLog, &logTime);
    t.profiler.track(id, lengthInLog, logTime);
    objectMap.replace(tableId, id, objPtr);

    *newVersion = objPtr->version;
    bytesWritten += dataLength;
}

} // namespace RAMCloud
