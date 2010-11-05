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

#include "Buffer.h"
#include "ClientException.h"
#include "MasterServer.h"
#include "ProtoBuf.h"
#include "Rpc.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "Transport.h"
#include "TransportManager.h"

namespace RAMCloud {

void objectEvictionCallback(LogEntryType type,
                            const void* p,
                            uint64_t len,
                            void* cookie);
void tombstoneEvictionCallback(LogEntryType type,
                               const void* p,
                               uint64_t len,
                               void* cookie);

/**
 * Construct a MasterServer.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param coordinator
 *      A client to the coordinator for the RAMCloud this Master is in.
 * \param backup
 *      Provides a mechanism for replicating changes to other servers.
 */
MasterServer::MasterServer(const ServerConfig& config,
                           CoordinatorClient& coordinator,
                           BackupManager& backup)
    : config(config)
    , coordinator(coordinator)
    , serverId(0)
    , backup(backup)
    , log(0)
    , objectMap(config.hashTableBytes / ObjectMap::bytesPerCacheLine())
    , tombstoneMap(NULL)
    , tablets()
{
    serverId = coordinator.enlistServer(MASTER, config.localLocator);
    LOG(NOTICE, "My server ID is %lu", serverId);
    log = new Log(serverId, config.logBytes, Segment::SEGMENT_SIZE, &backup);
    log->registerType(LOG_ENTRY_TYPE_OBJ, objectEvictionCallback, this);
    log->registerType(LOG_ENTRY_TYPE_OBJTOMB, tombstoneEvictionCallback, this);
}

MasterServer::~MasterServer()
{
    std::set<Table*> tables;
    foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
        tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
    foreach (Table* table, tables)
        delete table;
    delete log;
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
            callHandler<PingRpc, Server,
                        &Server::ping>(rpc);
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

    const Object* o = objectMap.lookup(reqHdr.tableId, reqHdr.id);
    if (!o) {
        throw ObjectDoesntExistException(HERE);
    }

    respHdr.version = o->version;
    rejectOperation(&reqHdr.rejectRules, o->version);
    Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
                                  o->data, static_cast<uint32_t>(o->data_len));
    // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
    // from scribbling over o->data.
    respHdr.length = o->data_len;
}

/**
 * Callback used to purge the recovery tombstone hash table. Invoked by
 * HashTable::forEach.
 */
static void
recoveryCleanup(const ObjectTombstone *tomb, void *cookie)
{
    free(const_cast<ObjectTombstone *>(tomb));
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
    uint64_t masterId = reqHdr.masterId;
    ProtoBuf::Tablets tablets;
    ProtoBuf::parseFromResponse(rpc.recvPayload, sizeof(reqHdr),
                                reqHdr.tabletsLength, tablets);
    ProtoBuf::ServerList backups;
    ProtoBuf::parseFromResponse(rpc.recvPayload,
                                sizeof(reqHdr) + reqHdr.tabletsLength,
                                reqHdr.serverListLength, backups);
    responder();
    // reqHdr, respHdr, and rpc are off-limits now

    // Allocate a recovery hash table for the tombstones.
    tombstoneMap = new ObjectTombstoneMap(64 * 1024 * 1024 /
        ObjectTombstoneMap::bytesPerCacheLine());

    // Recover Segments, firing MasterServer::recoverSegment for each one.
    backup.recover(*this, masterId, tablets, backups);

    // Free recovery tombstones left in the hash table and deallocate it.
    tombstoneMap->forEach(recoveryCleanup, NULL);
    delete tombstoneMap;
    tombstoneMap = NULL;
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

    SegmentIterator i(buffer, bufferLength, true);
    while (!i.isDone()) {
        LogEntryType type = i.getType();

        if (type == LOG_ENTRY_TYPE_OBJ) {
            const Object *recoverObj = reinterpret_cast<const Object *>(
                i.getPointer());
            uint64_t objId = recoverObj->id;
            uint64_t tblId = recoverObj->table;

            const Object *localObj = objectMap.lookup(tblId, objId);
            const ObjectTombstone *tomb = tombstoneMap->lookup(tblId, objId);

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version + 1;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverObj->version >= minSuccessor) {
                // write to log & update hash table
                const Object *newObj = reinterpret_cast<const Object*>(
                    log->append(LOG_ENTRY_TYPE_OBJ, recoverObj,
                                recoverObj->size()));
                assert(newObj != NULL);
                objectMap.replace(tblId, objId, newObj);

                // nuke the tombstone, if it existed
                if (tomb != NULL) {
                    tombstoneMap->remove(tblId, objId);
                    free(const_cast<ObjectTombstone *>(tomb));
                }

                // nuke the old object, if it existed
                if (localObj != NULL) {
                    log->free(localObj);
                }
            }
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            const ObjectTombstone *recoverTomb =
                reinterpret_cast<const ObjectTombstone *>(i.getPointer());
            uint64_t objId = recoverTomb->objectId;
            uint64_t tblId = recoverTomb->tableId;

            const Object *localObj = objectMap.lookup(tblId, objId);
            const ObjectTombstone *tomb = tombstoneMap->lookup(tblId, objId);

            // can't have both a tombstone and an object in the hash tables
            assert(tomb == NULL || localObj == NULL);

            uint64_t minSuccessor = 0;
            if (localObj != NULL)
                minSuccessor = localObj->version;
            else if (tomb != NULL)
                minSuccessor = tomb->objectVersion + 1;

            if (recoverTomb->objectVersion >= minSuccessor) {
                // allocate memory for the tombstone & update hash table
                ObjectTombstone *newTomb = reinterpret_cast<ObjectTombstone *>(
                    xmalloc(sizeof(*newTomb)));
                memcpy(newTomb, const_cast<ObjectTombstone *>(recoverTomb),
                    sizeof(*newTomb));
                tombstoneMap->replace(tblId, objId, newTomb);

                // nuke the old tombstone, if it existed
                if (tomb != NULL) {
                    free(const_cast<ObjectTombstone *>(tomb));
                }

                // nuke the object, if it existed
                if (localObj != NULL) {
                    objectMap.remove(tblId, objId);
                    log->free(localObj);
                }
            }
        }

        i.next();
    }
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
    const Object* o = objectMap.lookup(reqHdr.tableId, reqHdr.id);
    if (o == NULL) {
        rejectOperation(&reqHdr.rejectRules, VERSION_NONEXISTENT);
        return;
    }
    respHdr.version = o->version;

    // Abort if we're trying to delete the wrong version.
    rejectOperation(&reqHdr.rejectRules, respHdr.version);

    t.RaiseVersion(o->version + 1);

    ObjectTombstone tomb(tomb.segmentId, o);

    // Mark the deleted object as free first, since the append could
    // invalidate it
    log->free(o);
    const void* ret = log->append(
        LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb));
    assert(ret);
    objectMap.remove(reqHdr.tableId, reqHdr.id);
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
    typedef std::map<uint32_t, Table*> Tables;
    Tables tables;

    // create map from table ID to Table of pre-existing tables
    foreach (const ProtoBuf::Tablets::Tablet& oldTablet, tablets.tablet()) {
        tables[oldTablet.table_id()] =
            reinterpret_cast<Table*>(oldTablet.user_data());
    }

    // overwrite tablets with new tablets
    ProtoBuf::parseFromRequest(rpc.recvPayload, sizeof(reqHdr),
                               reqHdr.tabletsLength, tablets);

    // delete pre-existing tables that no longer live here
    foreach (Tables::value_type oldTable, tables) {
        foreach (const ProtoBuf::Tablets::Tablet& newTablet,
                 tablets.tablet()) {
            if (oldTable.first == newTablet.table_id())
                goto next;
        }
        delete oldTable.second;
        oldTable.second = NULL;
      next:
        { /* pass */ }
    }

    // create new Tables and assign all new tablets tables
    foreach (ProtoBuf::Tablets::Tablet& newTablet, *tablets.mutable_tablet()) {
        Table* table = tables[newTablet.table_id()];
        if (table == NULL) {
            table = new Table(newTablet.table_id());
            tables[newTablet.table_id()] = table;
        }
        newTablet.set_user_data(reinterpret_cast<uint64_t>(table));
    }
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

struct obj_replay_cookie {
    MasterServer *server;
    uint64_t usedBytes;
};

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
 * \param[in]  len
 *      Size of the log entry being evicted in bytes.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
objectEvictionCallback(LogEntryType type,
                       const void* p,
                       uint64_t len,
                       void* cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJ);

    MasterServer *svr = static_cast<MasterServer *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const Object *evictObj = static_cast<const Object *>(p);
    assert(evictObj != NULL);

    try {
        svr->getTable(evictObj->table, evictObj->id);
    } catch (TableDoesntExistException& e) {
        // That tablet doesn't exist on this server anymore.
        // Just remove the hash table entry, if it exists.
        svr->objectMap.remove(evictObj->table, evictObj->id);
        return;
    }

    const Object *hashTblObj =
        svr->objectMap.lookup(evictObj->table, evictObj->id);

    // simple pointer comparison suffices
    if (hashTblObj == evictObj) {
        const Object *newObj = (const Object *)log->append(
            LOG_ENTRY_TYPE_OBJ, evictObj, evictObj->size());
        assert(newObj != NULL);
        svr->objectMap.replace(evictObj->table, evictObj->id, newObj);
    }
}

void
objectReplayCallback(LogEntryType type,
                     const void *p,
                     uint64_t len,
                     void *cookiep)
{
    obj_replay_cookie *cookie = static_cast<obj_replay_cookie *>(cookiep);
    MasterServer *server = cookie->server;

    //printf("ObjectReplayCallback: type %llu\n", type);

    // Used to determine free_bytes after passing over the segment
    cookie->usedBytes += len;

    switch (type) {
    case LOG_ENTRY_TYPE_OBJ: {
        const Object *obj = static_cast<const Object *>(p);
        assert(obj);

        server->objectMap.remove(obj->table, obj->id);
        server->objectMap.replace(obj->table, obj->id, obj);
    }
        break;
    case LOG_ENTRY_TYPE_OBJTOMB:
        assert(false);  //XXX- fixme
        break;
    case LOG_ENTRY_TYPE_SEGHEADER:
    case LOG_ENTRY_TYPE_SEGFOOTER:
        break;
    default:
        printf("!!! Unknown object type on log replay: 0x%llx", type);
    }
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
 * \param[in]  len
 *      Size of the log entry being evicted in bytes.
 * \param[in]  cookie
 *      The opaque state pointer registered with the callback.
 */
void
tombstoneEvictionCallback(LogEntryType type,
                          const void* p,
                          uint64_t len,
                          void* cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJTOMB);

    MasterServer *svr = static_cast<MasterServer *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const ObjectTombstone *tomb =
        static_cast<const ObjectTombstone *>(p);
    assert(tomb != NULL);

    // see if the referant is still there
    if (log->isSegmentLive(tomb->segmentId)) {
        const void *ret = log->append(
            LOG_ENTRY_TYPE_OBJTOMB, tomb, sizeof(*tomb));
        assert(ret != NULL);
    }
}

void
MasterServer::storeData(uint64_t tableId, uint64_t id,
                        const RejectRules* rejectRules, Buffer* data,
                        uint32_t dataOffset, uint32_t dataLength,
                        uint64_t* newVersion)
{
    Table& t(getTable(tableId, id));
    const Object *o = objectMap.lookup(tableId, id);
    uint64_t version = (o != NULL) ? o->version : VERSION_NONEXISTENT;
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
        newObject->version = o->version + 1;
    else
        newObject->version = t.AllocateVersion();
    assert(o == NULL || newObject->version > o->version);
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
        log->free(o);

        uint64_t segmentId = log->getSegmentId(o);
        ObjectTombstone tomb(segmentId, o);
        const void *p = log->append(LOG_ENTRY_TYPE_OBJTOMB,
            &tomb, sizeof(tomb));
        assert(p != NULL);
    }

    const Object *objPtr = (const Object *)log->append(
        LOG_ENTRY_TYPE_OBJ, newObject, newObject->size());
    assert(objPtr != NULL);
    objectMap.replace(tableId, id, objPtr);

    *newVersion = objPtr->version;
}

} // namespace RAMCloud
