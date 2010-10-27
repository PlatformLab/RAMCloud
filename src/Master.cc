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
#include "Master.h"
#include "Rpc.h"
#include "Segment.h"
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
 * Construct a Master.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param backupClient
 *      Provides a mechanism for replicating changes to other servers.
 *      If NULL then we create a default backup object.
 */
Master::Master(const ServerConfig* config,
               BackupClient* backupClient)
    : config(config)
    , coordinator(config->coordinatorLocator.c_str())
    , serverId(0)
    , backup(backupClient)
    , log(0)
{
    log = new Log(0, Segment::SEGMENT_SIZE * SEGMENT_COUNT,
        Segment::SEGMENT_SIZE);
    log->registerType(LOG_ENTRY_TYPE_OBJ, objectEvictionCallback, this);
    log->registerType(LOG_ENTRY_TYPE_OBJTOMB, tombstoneEvictionCallback, this);

    for (int i = 0; i < NUM_TABLES; i++) {
        tables[i] = NULL;
    }
}

Master::~Master()
{
    for (int i = 0; i < NUM_TABLES; i++) {
        delete tables[i];
    }

    delete log;
}

void
Master::dispatch(RpcType type, Transport::ServerRpc& rpc)
{
    switch (type) {
        case CreateRpc::type:
            callHandler<CreateRpc, Master, &Master::create>(rpc);
            break;
        case CreateTableRpc::type:
            callHandler<CreateTableRpc, Master, &Master::createTable>(rpc);
            break;
        case DropTableRpc::type:
            callHandler<DropTableRpc, Master, &Master::dropTable>(rpc);
            break;
        case OpenTableRpc::type:
            callHandler<OpenTableRpc, Master, &Master::openTable>(rpc);
            break;
        case PingRpc::type:
            callHandler<PingRpc, Server, &Server::ping>(rpc);
            break;
        case ReadRpc::type:
            callHandler<ReadRpc, Master, &Master::read>(rpc);
            break;
        case RemoveRpc::type:
            callHandler<RemoveRpc, Master, &Master::remove>(rpc);
            break;
        case WriteRpc::type:
            callHandler<WriteRpc, Master, &Master::write>(rpc);
            break;
        default:
            throw UnimplementedRequestError();
    }
}

void __attribute__ ((noreturn))
Master::run()
{
    serverId = coordinator.enlistServer(MASTER, config->localLocator);
    LOG(NOTICE, "My server ID is %lu", serverId);
    while (true)
        handleRpc<Master>();
}

/**
 * Top-level server method to handle the CREATE request.
 * See the documentation for the corresponding method in RamCloudClient for
 * complete information about what this request does.
 * \copydetails Server::ping
 */
void
Master::create(const CreateRpc::Request& reqHdr,
               CreateRpc::Response& respHdr,
               Transport::ServerRpc& rpc)
{
    Table* t = getTable(reqHdr.tableId);
    uint64_t id = t->AllocateKey();

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    storeData(reqHdr.tableId, id, &rejectRules,
              &rpc.recvPayload, sizeof(reqHdr), reqHdr.length,
              &respHdr.version);
    respHdr.id = id;
}

/**
 * Top-level server method to handle the CREATE_TABLE request.
 * \copydetails create
 */
void
Master::createTable(const CreateTableRpc::Request& reqHdr,
                    CreateTableRpc::Response& respHdr,
                    Transport::ServerRpc& rpc)
{
    int i;
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);

    // See if we already have a table with the given name.
    for (i = 0; i < NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            // Table already exists; do nothing.
            return;
        }
    }

    // Find an empty slot in the table of tables and use it for the
    // new table.
    for (i = 0; i < NUM_TABLES; i++) {
        if (tables[i] == NULL) {
            tables[i] = new Table();
            tables[i]->SetName(name);
            return;
        }
    }
    throw NoTableSpaceException();
}


/**
 * Top-level server method to handle the DROP_TABLE request.
 * \copydetails create
 */
void
Master::dropTable(const DropTableRpc::Request& reqHdr,
                  DropTableRpc::Response& respHdr,
                  Transport::ServerRpc& rpc)
{
    int i;
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    for (i = 0; i < NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            delete tables[i];
            tables[i] = NULL;
            break;
        }
    }
    // Note: it's not an error if the table doesn't exist.
}

/**
 * Top-level server method to handle the OPEN_TABLE request.
 * \copydetails create
 */
void
Master::openTable(const OpenTableRpc::Request& reqHdr,
                  OpenTableRpc::Response& respHdr,
                  Transport::ServerRpc& rpc)
{
    int i;
    const char* name = getString(rpc.recvPayload, sizeof(reqHdr),
                                 reqHdr.nameLength);
    for (i = 0; i < NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            respHdr.tableId = i;
            return;
        }
    }
    throw TableDoesntExistException();
}

/**
 * Top-level server method to handle the READ request.
 * \copydetails create
 */
void
Master::read(const ReadRpc::Request& reqHdr,
             ReadRpc::Response& respHdr,
             Transport::ServerRpc& rpc)
{
    Table* t = getTable(reqHdr.tableId);
    const Object* o = t->Get(reqHdr.id);
    if (!o) {
        // Automatic reject: can't read a non-existent object
        // Return null version
        throw ObjectDoesntExistException();
        return;
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
 * Top-level server method to handle the REMOVE request.
 * \copydetails create
 */
void
Master::remove(const RemoveRpc::Request& reqHdr,
               RemoveRpc::Response& respHdr,
               Transport::ServerRpc& rpc)
{
    Table* t = getTable(reqHdr.tableId);
    const Object* o = t->Get(reqHdr.id);
    if (o == NULL) {
        rejectOperation(&reqHdr.rejectRules, VERSION_NONEXISTENT);
        return;
    }
    respHdr.version = o->version;

    // Abort if we're trying to delete the wrong version.
    rejectOperation(&reqHdr.rejectRules, respHdr.version);

    t->RaiseVersion(o->version + 1);

    ObjectTombstone tomb(tomb.segmentId, o);

    // Mark the deleted object as free first, since the append could
    // invalidate it
    log->free(o);
    const void* ret = log->append(
        LOG_ENTRY_TYPE_OBJTOMB, &tomb, sizeof(tomb));
    assert(ret);
    t->Delete(reqHdr.id);
}

/**
 * Top-level server method to handle the WRITE request.
 * \copydetails create
 */
void
Master::write(const WriteRpc::Request& reqHdr,
              WriteRpc::Response& respHdr,
              Transport::ServerRpc& rpc)
{
    storeData(reqHdr.tableId, reqHdr.id,
              &reqHdr.rejectRules, &rpc.recvPayload, sizeof(reqHdr),
              static_cast<uint32_t>(reqHdr.length), &respHdr.version);
}

/**
 * Validates a table identifier and returns the corresponding Table.
 *
 * \param tableId
 *      Identifier for a desired table.
 *
 * \return
 *      The table corresponding to tableId.
 *
 * \exception TableDoesntExist
 *      Thrown if tableId does not correspond to a valid table.
 */
Table*
Master::getTable(uint32_t tableId) {
    if (tableId >= static_cast<uint32_t>(NUM_TABLES)) {
        throw TableDoesntExistException();
    }
    Table* t = tables[tableId];
    if (t == NULL) {
        throw TableDoesntExistException();
    }
    return t;
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
Master::rejectOperation(const RejectRules* rejectRules, uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules->doesntExist)
            throw ObjectDoesntExistException();
        return;
    }
    if (rejectRules->exists)
        throw ObjectExistsException();
    if (rejectRules->versionLeGiven && version <= rejectRules->givenVersion)
        throw WrongVersionException();
    if (rejectRules->versionNeGiven && version != rejectRules->givenVersion)
        throw WrongVersionException();
}

//-----------------------------------------------------------------------
// Everything below here is "old" code, meaning it probably needs to
// get refactored at some point, it doesn't follow the coding conventions,
// and there are no unit tests for it.
//-----------------------------------------------------------------------

struct obj_replay_cookie {
    Master *server;
    uint64_t used_bytes;
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

    Master *svr = static_cast<Master *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const Object *evict_obj = static_cast<const Object *>(p);
    assert(evict_obj != NULL);

    Table *tbl = svr->tables[evict_obj->table];
    assert(tbl != NULL);

    const Object *tbl_obj = tbl->Get(evict_obj->id);

    // simple pointer comparison suffices
    if (tbl_obj == evict_obj) {
        const Object *objp = (const Object *)log->append(
            LOG_ENTRY_TYPE_OBJ, evict_obj, evict_obj->size());
        assert(objp != NULL);
        tbl->Put(evict_obj->id, objp);
    }
}

void
objectReplayCallback(LogEntryType type,
                     const void *p,
                     uint64_t len,
                     void *cookiep)
{
    obj_replay_cookie *cookie = static_cast<obj_replay_cookie *>(cookiep);
    Master *server = cookie->server;

    //printf("ObjectReplayCallback: type %llu\n", type);

    // Used to determine free_bytes after passing over the segment
    cookie->used_bytes += len;

    switch (type) {
    case LOG_ENTRY_TYPE_OBJ: {
        const Object *obj = static_cast<const Object *>(p);
        assert(obj);

        Table *table = server->tables[obj->table];
        assert(table != NULL);

        table->Delete(obj->id);
        table->Put(obj->id, obj);
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

    Master *svr = static_cast<Master *>(cookie);
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
Master::storeData(uint64_t tableId, uint64_t id,
                  const RejectRules* rejectRules, Buffer* data,
                  uint32_t dataOffset, uint32_t dataLength,
                  uint64_t* newVersion)
{
    Table *t = getTable(tableId);
    const Object *o = t->Get(id);
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
        newObject->version = t->AllocateVersion();
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
        const void *p = log->append(LOG_ENTRY_TYPE_OBJTOMB, &tomb,sizeof(tomb));
        assert(p != NULL);
    }

    const Object *objp = (const Object *)log->append(
        LOG_ENTRY_TYPE_OBJ, newObject, newObject->size());
    assert(objp != NULL);
    t->Put(id, objp);

    *newVersion = objp->version;
}

} // namespace RAMCloud
