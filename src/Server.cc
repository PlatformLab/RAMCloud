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

/**
 * \file
 * Implementation of #RAMCloud::Server.
 */

#include <Buffer.h>
#include <ClientException.h>
#include <Metrics.h>
#include <Server.h>
#include <Rpc.h>
#include <Service.h>
#include <Transport.h>

namespace RAMCloud {

void objectEvictionCallback(log_entry_type_t type,
                         const void* p,
                         uint64_t len,
                         void* cookie);
void tombstoneEvictionCallback(log_entry_type_t type,
                         const void* p,
                         uint64_t len,
                         void* cookie);

/**
 * Construct a Server.
 *
 * \param config
 *      Contains various parameters that configure the operation of
 *      this server.
 * \param transport
 *      Transport object that this server uses to receive requests
 *      from clients and send replies back to them.
 * \param backupClient
 *      Provides a mechanism for replicating changes to other servers.
 *      If NULL then we create a default backup object.
 */
Server::Server(const ServerConfig* config,
               Transport* transport,
               BackupClient* backupClient)
    : config(config), transport(transport), backup(backupClient), log(0)
{
    void* p = xmalloc(SEGMENT_SIZE * SEGMENT_COUNT);

    if (!backup) {
        MultiBackupClient* multiBackup = new MultiBackupClient();
        if (BACKUP) {
            // This Service object will be deallocated in ~BackupHost().
            Service* s = new Service();
            s->setPort(BACKSVRPORT);
            s->setIp(BACKSVRADDR);

            // NOTE The backup client takes care of freeing the Service and
            // Transport objects.
            multiBackup->addHost(s, transport);
        }
        backup = multiBackup;
    }

    log = new Log(SEGMENT_SIZE, p, SEGMENT_SIZE * SEGMENT_COUNT, backup);
    log->registerType(LOG_ENTRY_TYPE_OBJECT, objectEvictionCallback, this);
    log->registerType(LOG_ENTRY_TYPE_OBJECT_TOMBSTONE,
            tombstoneEvictionCallback, this);

    for (int i = 0; i < RC_NUM_TABLES; i++) {
        tables[i] = NULL;
    }
}

/*
 * Destructor for Server objects.
 */
Server::~Server()
{
    for (int i = 0; i < RC_NUM_TABLES; i++) {
        if (tables[i] != NULL) {
            delete tables[i];
        }
    }
    delete backup;
}

/**
 * Top-level server method to handle the CREATE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::create(const CreateRequest* reqHdr, CreateResponse* respHdr,
        Transport::ServerRPC* rpc)
{
    Table* t = getTable(reqHdr->tableId);
    uint64_t id = t->AllocateKey();

    RejectRules rejectRules;
    memset(&rejectRules, 0, sizeof(RejectRules));
    rejectRules.exists = 1;

    respHdr->common.status = storeData(reqHdr->tableId, id, &rejectRules,
            &rpc->recvPayload, sizeof(*reqHdr), reqHdr->length,
            &respHdr->version);
    respHdr->id = id;
}

/**
 * Top-level server method to handle the CREATE_TABLE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::createTable(const CreateTableRequest* reqHdr,
                    CreateTableResponse* respHdr, Transport::ServerRPC* rpc)
{
    int i;
    const char* name = getString(&rpc->recvPayload, sizeof(*reqHdr),
            reqHdr->nameLength);

    // See if we already have a table with the given name.
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            // Table already exists; do nothing.
            return;
        }
    }

    // Find an empty slot in the table of tables and use it for the
    // new table.
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (tables[i] == NULL) {
            tables[i] = new Table();
            tables[i]->SetName(name);
            return;
        }
    }
    respHdr->common.status = STATUS_NO_TABLE_SPACE;
}


/**
 * Top-level server method to handle the DROP_TABLE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::dropTable(const DropTableRequest* reqHdr, DropTableResponse* respHdr,
            Transport::ServerRPC* rpc)
{
    int i;
    const char* name = getString(&rpc->recvPayload, sizeof(*reqHdr),
            reqHdr->nameLength);
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            delete tables[i];
            tables[i] = NULL;
            break;
        }
    }
    // Note: it's not an error if the table doesn't exist.
}

/**
 * Top-level server method to handle the OPEN_TABLE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::openTable(const OpenTableRequest* reqHdr, OpenTableResponse* respHdr,
            Transport::ServerRPC* rpc)
{
    int i;
    const char* name = getString(&rpc->recvPayload, sizeof(*reqHdr),
            reqHdr->nameLength);
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if ((tables[i] != NULL) && (strcmp(tables[i]->GetName(), name) == 0)) {
            respHdr->tableId = i;
            return;
        }
    }
    throw TableDoesntExistException();
}


/**
 * Top-level server method to handle the PING request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Ignored.
 * \param[out] respHdr
 *      Ignored.
 * \param[out] rpc
 *      Ignored.
 */
void
Server::ping(const PingRequest* reqHdr, PingResponse* respHdr,
            Transport::ServerRPC* rpc)
{
    // Nothing to do here.
}

/**
 * Top-level server method to handle the READ request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::read(const ReadRequest* reqHdr, ReadResponse* respHdr,
        Transport::ServerRPC* rpc)
{
    Table* t = getTable(reqHdr->tableId);
    const Object* o = t->Get(reqHdr->id);
    if (!o) {
        // Automatic reject: can't read a non-existent object
        // Return null version
        respHdr->common.status = STATUS_OBJECT_DOESNT_EXIST;
        respHdr->length = 0;
        respHdr->version = VERSION_NONEXISTENT;
        return;
    }

    respHdr->version = o->version;
    respHdr->common.status = rejectOperation(&reqHdr->rejectRules,
            o->version);
    if (respHdr->common.status)
        return;
    Buffer::Chunk::appendToBuffer(&rpc->replyPayload,
            o->data, static_cast<uint32_t>(o->data_len));
    // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
    // from scribbling over o->data.
    respHdr->length = o->data_len;
}

/**
 * Top-level server method to handle the REMOVE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::remove(const RemoveRequest* reqHdr, RemoveResponse* respHdr,
        Transport::ServerRPC* rpc)
{
    Table* t = getTable(reqHdr->tableId);
    const Object* o = t->Get(reqHdr->id);
    respHdr->version = o ? o->version : VERSION_NONEXISTENT;

    // Abort if we're trying to delete the wrong version.
    respHdr->common.status = rejectOperation(&reqHdr->rejectRules,
            respHdr->version);
    if ((respHdr->common.status != STATUS_OK) || !o) {
        return;
    }

    t->RaiseVersion(o->version + 1);

    ObjectTombstone tomb;
    log->getSegmentIdOffset(o, &tomb.segmentId, &tomb.segmentOffset);

    // Mark the deleted object as free first, since the append could
    // invalidate it
    log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());
    const void* ret = log->append(
        LOG_ENTRY_TYPE_OBJECT_TOMBSTONE, &tomb, sizeof(tomb));
    assert(ret);
    t->Delete(reqHdr->id);
}

/**
 * Top-level server method to handle the WRITE request.  See the
 * documentation for the corresponding method in Client for complete
 * information about what this request does.
 *
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters
 *      for this operation.
 * \param[out] respHdr
 *      Header for the response that will be returned to the client.
 *      The caller has pre-allocated the right amount of space in the
 *      response buffer for this type of request, and has zeroed out
 *      its contents (so, for example, status is already zero).
 * \param[out] rpc
 *      Complete information about the remote procedure call; can be
 *      used to read additional information beyond the request header
 *      and/or append additional information to the response buffer.
 */
void
Server::write(const WriteRequest* reqHdr, WriteResponse* respHdr,
        Transport::ServerRPC* rpc)
{
    respHdr->common.status = storeData(reqHdr->tableId, reqHdr->id,
            &reqHdr->rejectRules, &rpc->recvPayload, sizeof(*reqHdr),
            static_cast<uint32_t>(reqHdr->length), &respHdr->version);
}

/**
 * Wait for an incoming RPC request, handle it, and return after
 * sending a response.
 */
void
Server::handleRpc()
{
    Transport::ServerRPC* rpc = transport->serverRecv();
    Buffer* request = &rpc->recvPayload;
    RpcResponseCommon* responseCommon = NULL;
    try {
        const RpcRequestCommon* header = static_cast
                <const RpcRequestCommon*>(request->getRange(0,
                sizeof(RpcRequestCommon)));
        if (header == NULL) {
            throw MessageTooShortError();
        }
        Metrics::setup(header->perfCounter);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        Buffer* response = &rpc->replyPayload;
        switch (header->type) {

            #define CALL_HANDLER(nameInitialCap, nameLower) {                  \
                nameInitialCap##Response* respHdr =                            \
                        new(response, APPEND) nameInitialCap##Response;        \
                responseCommon = &respHdr->common;                             \
                const nameInitialCap##Request* reqHdr =                        \
                        static_cast<const nameInitialCap##Request*>(      \
                        request->getRange(0, sizeof(                           \
                        nameInitialCap##Request)));                            \
                if (reqHdr == NULL) {                                          \
                    throw MessageTooShortError();                              \
                }                                                              \
                /* Clear the response header, so that unused fields are zero;  \
                 * this makes tests more reproducible, and it is also needed   \
                 * to avoid possible security problems where random server     \
                 * info could leak out to clients through unused packet        \
                 * fields. */                                                  \
                memset(respHdr, 0, sizeof(nameInitialCap##Response));          \
                nameLower(reqHdr, respHdr, rpc);                               \
                break;                                                         \
            }

            case CREATE:        CALL_HANDLER(Create, create)
            case CREATE_TABLE:  CALL_HANDLER(CreateTable, createTable)
            case DROP_TABLE:    CALL_HANDLER(DropTable, dropTable)
            case OPEN_TABLE:    CALL_HANDLER(OpenTable, openTable)
            case PING:          CALL_HANDLER(Ping, ping)
            case READ:          CALL_HANDLER(Read, read)
            case REMOVE:        CALL_HANDLER(Remove, remove)
            case WRITE:         CALL_HANDLER(Write, write)
            default:
                throw UnimplementedRequestError();
        }
    }
    catch (ClientException e) {
        Buffer* response = &rpc->replyPayload;
        if (responseCommon == NULL) {
            responseCommon = new(response, APPEND) RpcResponseCommon;
        }
        responseCommon->status = e.status;
    }
    Metrics::mark(MARK_RPC_PROCESSING_END);
    responseCommon->counterValue = Metrics::read();
    rpc->sendReply();
}

void __attribute__ ((noreturn))
Server::run()
{
    if (config->restore) {
        restore();
    }
    log->init();

    while (true)
        handleRpc();
}

/**
 * Find and validate a string in a buffer.  This method is invoked
 * by RPC handlers expecting a null-terminated string to be present
 * in an incoming request. It makes sure that the buffer contains
 * adequate space for a string of a given length at a given location
 * in the buffer, and it verifies that the string is null-terminated
 * and non-empty.
 *
 * \param buffer
 *      Buffer containing the desired string; typically an RPC
 *      request payload.
 * \param offset
 *      Location of the first byte of the string within the buffer.
 * \param length
 *      Total length of the string, including terminating null
 *      character.
 *
 * \return
 *      Pointer that can be used to access the string.  The string is
 *      guaranteed to exist in its entirety and to be null-terminated.
 *      (One condition we don't check for: premature termination via a
 *      null character in the middle of the string).
 *
 * \exception MessageTooShort
 *      The buffer isn't large enough for the expected size of the
 *      string.
 * \exception RequestFormatError
 *      The string was not null-terminated or had zero length.
 */
const char*
Server::getString(Buffer* buffer, uint32_t offset, uint32_t length) {
    const char* result;
    if (length == 0) {
        throw RequestFormatError();
    }
    if (buffer->getTotalLength() < (offset + length)) {
        throw MessageTooShortError();
    }
    result = static_cast<const char*>(buffer->getRange(offset, length));
    if (result[length - 1] != '\0') {
        throw RequestFormatError();
    }
    return result;
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
Server::getTable(uint32_t tableId) {
    if (tableId >= RC_NUM_TABLES) {
        throw TableDoesntExistException();
    }
    Table* t = tables[tableId];
    if (t == NULL) {
        throw TableDoesntExistException();
    }
    return t;
}

/*
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
Status
Server::rejectOperation(const RejectRules* rejectRules,
                         uint64_t version)
{
    if (version == VERSION_NONEXISTENT) {
        if (rejectRules->doesntExist) {
            return STATUS_OBJECT_DOESNT_EXIST;
        }
        return STATUS_OK;
    }
    if (rejectRules->exists) {
        return STATUS_OBJECT_EXISTS;
    }
    if (rejectRules->versionLeGiven &&
            (version <= rejectRules->givenVersion)) {
        return STATUS_WRONG_VERSION;
    }
    if (rejectRules->versionNeGiven &&
            (version != rejectRules->givenVersion)) {
        return STATUS_WRONG_VERSION;
    }
    return STATUS_OK;
}

//-----------------------------------------------------------------------
// Everything below here is "old" code, meaning it probably needs to
// get refactored at some point, it doesn't follow the coding conventions,
// and there are no unit tests for it.
//-----------------------------------------------------------------------

struct obj_replay_cookie {
    Server *server;
    uint64_t used_bytes;
};

/**
 * Callback used by the log cleaner when it's cleaning a segment and evicts
 * an object (LOG_ENTRY_TYPE_OBJECT).
 *
 * Upon return, the object will be discarded. Objects must therefore be
 * perpetuated when the object being evicted is exactly the object referenced
 * by the hash table. Otherwise, it's an old object and a tombstone for it
 * exists.
 *
 * \param[in]  type     type of the evictee (LOG_ENTRY_TYPE_OBJECT)
 * \param[in]  p        opaque pointer to the immutable entry in the log 
 * \param[in]  len      size of the log entry being evicted
 * \param[in]  cookie   the opaque state pointer registered with the callback
 */
void
objectEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJECT);

    Server *svr = static_cast<Server *>(cookie);
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
            LOG_ENTRY_TYPE_OBJECT, evict_obj, evict_obj->size());
        assert(objp != NULL);
        tbl->Put(evict_obj->id, objp);
    }
}

void
objectReplayCallback(log_entry_type_t type,
                     const void *p,
                     uint64_t len,
                     void *cookiep)
{
    obj_replay_cookie *cookie = static_cast<obj_replay_cookie *>(cookiep);
    Server *server = cookie->server;

    //printf("ObjectReplayCallback: type %llu\n", type);

    // Used to determine free_bytes after passing over the segment
    cookie->used_bytes += len;

    switch (type) {
    case LOG_ENTRY_TYPE_OBJECT: {
        const Object *obj = static_cast<const Object *>(p);
        assert(obj);

        Table *table = server->tables[obj->table];
        assert(table != NULL);

        table->Delete(obj->id);
        table->Put(obj->id, obj);
    }
        break;
    case LOG_ENTRY_TYPE_OBJECT_TOMBSTONE:
        assert(false);  //XXX- fixme
        break;
    case LOG_ENTRY_TYPE_SEGMENT_HEADER:
    case LOG_ENTRY_TYPE_SEGMENT_CHECKSUM:
        break;
    default:
        printf("!!! Unknown object type on log replay: 0x%llx", type);
    }
}

void
segmentReplayCallback(Segment *seg, void *cookie)
{
    // TODO(stutsman) we can restore bytes_stored in the log easily
    // using the same approach as for the individual segments
    Server *server = static_cast<Server *>(cookie);

    obj_replay_cookie ocookie;
    ocookie.server = server;
    ocookie.used_bytes = 0;

    server->log->forEachEntry(seg, objectReplayCallback, &ocookie);
    seg->setUsedBytes(ocookie.used_bytes);
}

/**
 * Callback used by the log cleaner when it's cleaning a segment and evicts
 * a tombstone.
 *
 * Tombstones are perpetuated when the segment they reference is still
 * valid in the system.
 *
 * We can be more aggressive in cleaning tombstones (e.g. a tombstone can
 * be cleared before its referant object if a newer object or tombstone
 * exists), but we don't worry about that now.
 *
 * \param[in]  type     type of the evictee (LOG_ENTRY_TYPE_OBJECT_TOMBSTONE)
 * \param[in]  p        opaque pointer to the immutable entry in the log 
 * \param[in]  len      size of the log entry being evicted
 * \param[in]  cookie   the opaque state pointer registered with the callback
 */
void
tombstoneEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJECT_TOMBSTONE);

    Server *svr = static_cast<Server *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const ObjectTombstone *tomb =
        static_cast<const ObjectTombstone *>(p);
    assert(tomb != NULL);

    // see if the referant is still there
    if (log->isSegmentLive(tomb->segmentId)) {
        const void *ret = log->append(
            LOG_ENTRY_TYPE_OBJECT_TOMBSTONE, tomb, sizeof(*tomb));
        assert(ret != NULL);
    }
}

void
Server::restore()
{
    uint64_t restored_segs = log->restore();
    printf("Log was able to restore %llu segs\n", restored_segs);
    // TODO(stutsman) Walk the log here and rebuild metadata
    log->forEachSegment(segmentReplayCallback, restored_segs, this);
}

Status
Server::storeData(uint64_t tableId, uint64_t id,
                  const RejectRules *rejectRules, Buffer *data,
                  uint32_t dataOffset, uint32_t dataLength,
                  uint64_t *newVersion)
{
    Table *t = getTable(tableId);
    const Object *o = t->Get(id);
    uint64_t version = (o != NULL) ? o->version : VERSION_NONEXISTENT;
    Status status = rejectOperation(rejectRules, version);
    if (status) {
        *newVersion = version;
        return status;
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

    // mark the old object as freed _before_ writing the new object to the log.
    // if we do it afterwards, the log cleaner could be triggered and `o'
    // reclaimed // before log->append() returns. The subsequent free breaks,
    // as that segment may have been reset.
    if (o != NULL)
        log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());

    const Object *objp = (const Object *)log->append(
        LOG_ENTRY_TYPE_OBJECT, newObject, newObject->size());
    assert(objp != NULL);
    t->Put(id, objp);

    *newVersion = objp->version;
    return STATUS_OK;
}

} // namespace RAMCloud
