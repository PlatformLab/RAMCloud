/* Copyright (c) 2009 Stanford University
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

#include <arpa/inet.h>

#include <Buffer.h>
#include <Server.h>
#include <Service.h>
#include <Transport.h>

namespace RAMCloud {

const bool server_debug = false;

void ObjectEvictionCallback(log_entry_type_t type,
                         const void *p,
                         uint64_t len,
                         void *cookie);
void TombstoneEvictionCallback(log_entry_type_t type,
                         const void *p,
                         uint64_t len,
                         void *cookie);

Server::Server(const ServerConfig *sconfig,
               Transport* transIn,
               BackupClient *backupClient)
    : config(sconfig), trans(transIn), backup(backupClient), log(0)
{
    void *p = xmalloc(SEGMENT_SIZE * SEGMENT_COUNT);

    if (!backup) {
        MultiBackupClient *multiBackup = new MultiBackupClient();
        if (BACKUP) {
            // This Service object will be deallocated in ~BackupHost().
            Service *s = new Service();
            s->setPort(BACKSVRPORT);
            s->setIp(BACKSVRADDR);

            // NOTE The backup client takes care of freeing the Service and
            // Transport objects.
            multiBackup->addHost(s, trans);
        }
        backup = multiBackup;
    }

    log = new Log(SEGMENT_SIZE, p, SEGMENT_SIZE * SEGMENT_COUNT, backup);
    log->registerType(LOG_ENTRY_TYPE_OBJECT, ObjectEvictionCallback, this);
    log->registerType(LOG_ENTRY_TYPE_OBJECT_TOMBSTONE,
        TombstoneEvictionCallback, this);
}

Server::~Server()
{
    delete backup;
}

void
Server::Ping(Transport::ServerRPC *rpc)
{
    // nothing to do here
}

bool
Server::RejectOperation(const rcrpc_reject_rules *reject_rules,
                        uint64_t version)
{
    if (version == RCRPC_VERSION_NONE) {
        return reject_rules->object_doesnt_exist;
    }

    if (reject_rules->object_exists) {
        return true;
    }
    if (reject_rules->version_eq_given &&
        version == reject_rules->given_version) {
        return true;
    }
    if (reject_rules->version_gt_given &&
        version > reject_rules->given_version) {
        return true;
    }
    if ((reject_rules->version_eq_given || reject_rules->version_gt_given) &&
        version < reject_rules->given_version) {
        return true;
    }
    return false;
}

void
Server::Read(Transport::ServerRPC *rpc)
{
    const rcrpc_read_request *req;
    req = static_cast<rcrpc_read_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    rcrpc_read_response *resp;
    resp = new(&rpc->replyPayload, APPEND) rcrpc_read_response;

    if (server_debug)
        printf("Read from key %lu\n",
               req->key);

    // (will be updated below with var-length data)
    resp->buf_len = 0;
    resp->version = RCRPC_VERSION_NONE;

    Table *t = &tables[req->table];
    const Object *o = t->Get(req->key);
    if (!o) {
        /* automatic reject: can't read a non-existent object */
        /* leave RCRPC_VERSION_NONE in resp->version */
        return;
    }

    resp->version = o->version;

    if (!RejectOperation(&req->reject_rules, o->version)) {
        assert(o->data_len < (1UL << 32));
        Buffer::Chunk::appendToBuffer(&rpc->replyPayload,
                                      const_cast<char*>(o->data),
                                      static_cast<uint32_t>(o->data_len));
        // TODO(ongaro): We'll need a new type of Chunk to block the cleaner
        // from scribbling over o->data.
        resp->buf_len = o->data_len;
    }
}

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
ObjectEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJECT);

    Server *svr = reinterpret_cast<Server *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const Object *evict_obj = reinterpret_cast<const Object *>(p);
    assert(evict_obj != NULL);

    Table *tbl = &svr->tables[evict_obj->table];
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
TombstoneEvictionCallback(log_entry_type_t type,
                    const void *p,
                    uint64_t len,
                    void *cookie)
{
    assert(type == LOG_ENTRY_TYPE_OBJECT_TOMBSTONE);

    Server *svr = reinterpret_cast<Server *>(cookie);
    assert(svr != NULL);

    Log *log = svr->log;
    assert(log != NULL);

    const ObjectTombstone *tomb =
        reinterpret_cast<const ObjectTombstone *>(p);
    assert(tomb != NULL);

    // see if the referant is still there
    if (log->isSegmentLive(tomb->segmentId)) {
        const void *ret = log->append(
            LOG_ENTRY_TYPE_OBJECT_TOMBSTONE, tomb, sizeof(*tomb));
        assert(ret != NULL);
    }
}

bool
Server::StoreData(uint64_t table,
                  uint64_t key,
                  const rcrpc_reject_rules *reject_rules,
                  Buffer *data, uint32_t dataOffset, uint32_t dataLength,
                  uint64_t *new_version)
{
    Table *t = &tables[table];
    const Object *o = t->Get(key);

    if (o != NULL) {
        if (RejectOperation(reject_rules, o->version)) {
            *new_version = o->version;
            return false;
        }
    } else {
        if (RejectOperation(reject_rules, RCRPC_VERSION_NONE)) {
            *new_version = RCRPC_VERSION_NONE;
            return false;
        }
    }

    DECLARE_OBJECT(new_o, dataLength);

    new_o->id = key;
    new_o->table = table;
    if (o != NULL)
        new_o->version = o->version + 1;
    else
        new_o->version = t->AllocateVersion();
    assert(o == NULL || new_o->version > o->version);
    // TODO(stutsman): dm's super-fast checksum here
    new_o->checksum = 0x0BE70BE70BE70BE7ULL;
    new_o->data_len = dataLength;
    data->copy(dataOffset, dataLength, new_o->data);

    // mark the old object as freed _before_ writing the new object to the log.
    // if we do it afterwards, the log cleaner could be triggered and `o'
    // reclaimed // before log->append() returns. The subsequent free breaks,
    // as that segment may have been reset.
    if (o != NULL)
        log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());

    const Object *objp = (const Object *)log->append(
        LOG_ENTRY_TYPE_OBJECT, new_o, new_o->size());
    assert(objp != NULL);
    t->Put(key, objp);

    *new_version = objp->version;
    return true;
}

void
Server::Write(Transport::ServerRPC *rpc)
{
    const rcrpc_write_request *req;

    req = static_cast<rcrpc_write_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    rcrpc_write_response *resp;
    resp = new(&rpc->replyPayload, APPEND) rcrpc_write_response;

    if (server_debug) {
        printf("Write %lu bytes of data to key %lu\n",
               req->buf_len, req->key);
    }

    assert(req->buf_len < (1UL << 32));
    resp->written = StoreData(req->table, req->key, &req->reject_rules,
                              &rpc->recvPayload,
                              sizeof(*req),
                              static_cast<uint32_t>(req->buf_len),
                              &resp->version);
}

void
Server::InsertKey(Transport::ServerRPC *rpc)
{
    const rcrpc_insert_request *req;
    req = static_cast<rcrpc_insert_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    rcrpc_insert_response *resp;
    resp = new(&rpc->replyPayload, APPEND) rcrpc_insert_response;

    Table *t = &tables[req->table];
    uint64_t key = t->AllocateKey();

    rcrpc_reject_rules reject_rules;
    memset(&reject_rules, 0, sizeof(reject_rules));
    reject_rules.object_exists = true;

    assert(req->buf_len < (1UL << 32));
    bool r = StoreData(req->table, key, &reject_rules,
                       &rpc->recvPayload,
                       sizeof(*req),
                       static_cast<uint32_t>(req->buf_len),
                       &resp->version);
    assert(r);

    resp->key = key;
}

void
Server::DeleteKey(Transport::ServerRPC *rpc)
{
    const rcrpc_delete_request *req;
    req = static_cast<rcrpc_delete_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    rcrpc_delete_response *resp;
    resp = new(&rpc->replyPayload, APPEND) rcrpc_delete_response;

    resp->version = RCRPC_VERSION_NONE;
    resp->deleted = false;

    Table *t = &tables[req->table];
    const Object *o = t->Get(req->key);
    if (!o) {
        /* leave RCRPC_VERSION_NONE in resp->version */
        if (!RejectOperation(&req->reject_rules, RCRPC_VERSION_NONE))
            resp->deleted = true;
        return;
    }

    // abort if we're trying to delete the wrong version
    // the client will note the discrepancy and figure it out
    if (RejectOperation(&req->reject_rules, o->version)) {
        resp->version = o->version;
        return;
    }
    resp->deleted = true;

    t->RaiseVersion(o->version + 1);

    ObjectTombstone tomb;
    log->getSegmentIdOffset(o, &tomb.segmentId, &tomb.segmentOffset);

    // mark the deleted object as free first, since the append could
    // invalidate it
    log->free(LOG_ENTRY_TYPE_OBJECT, o, o->size());
    const void *ret = log->append(
        LOG_ENTRY_TYPE_OBJECT_TOMBSTONE, &tomb, sizeof(tomb));
    assert(ret);
    t->Delete(req->key);
}

void
Server::CreateTable(Transport::ServerRPC *rpc)
{
    const rcrpc_create_table_request *req;
    req = static_cast<rcrpc_create_table_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0) {
            // TODO(stutsman): Need to do better than this
            throw Exception("Table exists");
        }
    }
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), "") == 0) {
            tables[i].SetName(req->name);
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO(stutsman): Need to do better than this
        throw Exception("Out of tables");
    }
    if (server_debug)
        printf("create table -> %d\n", i);
}

void
Server::OpenTable(Transport::ServerRPC *rpc)
{
    const rcrpc_open_table_request *req;
    req = static_cast<rcrpc_open_table_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    rcrpc_open_table_response *resp;
    resp = new(&rpc->replyPayload, APPEND) rcrpc_open_table_response;

    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0)
            break;
    }
    if (i == RC_NUM_TABLES) {
        // TODO(stutsman): Need to do better than this
        throw Exception("No such table");
    }
    if (server_debug)
        printf("open table -> %d\n", i);

    resp->handle = i;
}

void
Server::DropTable(Transport::ServerRPC *rpc)
{
    const rcrpc_drop_table_request *req;
    req = static_cast<rcrpc_drop_table_request*>(
        rpc->recvPayload.getRange(0, sizeof(*req)));

    int i;
    for (i = 0; i < RC_NUM_TABLES; i++) {
        if (strcmp(tables[i].GetName(), req->name) == 0) {
            tables[i].SetName("");
            break;
        }
    }
    if (i == RC_NUM_TABLES) {
        // TODO(stutsman): Need to do better than this
        throw Exception("No such table");
    }
    if (server_debug)
        printf("drop table -> %d\n", i);
}

struct obj_replay_cookie {
    Server *server;
    uint64_t used_bytes;
};

void
ObjectReplayCallback(log_entry_type_t type,
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

        Table *table = &server->tables[obj->table];
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
SegmentReplayCallback(Segment *seg, void *cookie)
{
    // TODO(stutsman) we can restore bytes_stored in the log easily
    // using the same approach as for the individual segments
    Server *server = static_cast<Server *>(cookie);

    obj_replay_cookie ocookie;
    ocookie.server = server;
    ocookie.used_bytes = 0;

    server->log->forEachEntry(seg, ObjectReplayCallback, &ocookie);
    seg->setUsedBytes(ocookie.used_bytes);
}

void
Server::Restore()
{
    uint64_t restored_segs = log->restore();
    printf("Log was able to restore %llu segs\n", restored_segs);
    // TODO(stutsman) Walk the log here and rebuild metadata
    log->forEachSegment(SegmentReplayCallback, restored_segs, this);
}

void
Server::HandleRPC()
{
    Transport::ServerRPC *rpc = trans->serverRecv();
    rcrpc_header *reqHeader = static_cast<rcrpc_header*>(
        rpc->recvPayload.getRange(0, sizeof(*reqHeader)));
    if (reqHeader == NULL)
        return; // request too short
    rpc->recvPayload.truncateFront(sizeof(*reqHeader));

    //printf("got rpc type: 0x%08x, len 0x%08x\n",
    //       reqHeader->type, reqHeader->len);

    rcrpc_header *replyHeader = new(&rpc->replyPayload, PREPEND) rcrpc_header;

    try {
        switch ((enum RCRPC_TYPE) reqHeader->type) {

#define HANDLE(rcrpc_upper, rcrpc_lower, handler)                              \
        case RCRPC_##rcrpc_upper##_REQUEST:                                    \
            /* In C++, structs with no members have sizeof 0. */               \
            if (sizeof(rcrpc_##rcrpc_lower##_request) != 1 &&                  \
                rpc->recvPayload.getTotalLength() <                            \
                sizeof(rcrpc_##rcrpc_lower##_request))                         \
                throw Exception("payload too short");                          \
            Server::handler(rpc);                                              \
            replyHeader->type = RCRPC_##rcrpc_upper##_RESPONSE;                \
            /* In C++, structs with no members have sizeof 0. */               \
            assert(sizeof(rcrpc_##rcrpc_lower##_response) == 1 ||              \
                   rpc->replyPayload.getTotalLength() >=                       \
                   (sizeof(rcrpc_header) +                                     \
                    sizeof(rcrpc_##rcrpc_lower##_response)));                  \
            break;                                                             \
        case RCRPC_##rcrpc_upper##_RESPONSE:                                   \
            throw Exception("server received RPC response")

        HANDLE(PING, ping, Ping);
        HANDLE(READ, read, Read);
        HANDLE(WRITE, write, Write);
        HANDLE(INSERT, insert, InsertKey);
        HANDLE(DELETE, delete, DeleteKey);
        HANDLE(CREATE_TABLE, create_table, CreateTable);
        HANDLE(OPEN_TABLE, open_table, OpenTable);
        HANDLE(DROP_TABLE, drop_table, DropTable);
#undef HANDLE

        case RCRPC_ERROR_RESPONSE:
            throw Exception("server received RPC response");

        default:
            throw Exception("received unknown RPC type");
        }
    } catch (Exception e) {
        fprintf(stderr, "Error while processing RPC: %s\n", e.message.c_str());
        uint32_t msglen = static_cast<uint32_t>(e.message.length()) + 1;
        rpc->replyPayload.truncateEnd(rpc->replyPayload.getTotalLength() -
                                      sizeof(*replyHeader));
        snprintf(new(&rpc->replyPayload, APPEND) char[msglen], msglen,
                 "%s", e.message.c_str());
        replyHeader->type = RCRPC_ERROR_RESPONSE;
    }

    rpc->sendReply();
}

void __attribute__ ((noreturn))
Server::Run()
{
    if (config->restore) {
        Restore();
    }
    log->init();

    while (true)
        HandleRPC();
}

} // namespace RAMCloud
