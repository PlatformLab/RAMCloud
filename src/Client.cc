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

// RAMCloud pragma [CPPLINT=0]

#include <Common.h>

#if RC_CLIENT_SHARED
#include <semaphore.h>
#include <sys/mman.h>
#endif

#include <rcrpc.h>
#include <Buffer.h>
#include <Client.h>
#include <assert.h>

using RAMCloud::Buffer;
using RAMCloud::Service;
using RAMCloud::TCPTransport;
using RAMCloud::Transport;

#if RC_CLIENT_SHARED
struct rc_client_shared {
    sem_t sem;
};

static void
lock(struct rc_client *client)
{
    assert(sem_wait(&client->shared->sem) == 0);
}

static void
unlock(struct rc_client *client)
{
    assert(sem_post(&client->shared->sem) == 0);
}
#else
static void lock(struct rc_client *client) {}
static void unlock(struct rc_client *client) {}
#endif

/**
 * Connect to a %RAMCloud.
 *
 * The caller should later use rc_disconnect() to disconnect from the
 * %RAMCloud.
 *
 * \param[in]  client   a newly allocated client
 * \return error code (see values below)
 * \retval  0 on success
 * \retval other reserved for future use
 */
int
rc_connect(struct rc_client *client)
{
#if RC_CLIENT_SHARED
    void *s = mmap(NULL, sizeof(struct rc_client_shared),
                   PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
    client->shared = static_cast<rc_client_shared*>(s);
    assert(client->shared != MAP_FAILED);
    assert(sem_init(&client->shared->sem, 1, 1) == 0);
#endif

    client->serv = new Service();
    client->serv->setIp(SVRADDR);
    client->serv->setPort(SVRPORT);

    client->trans = new TCPTransport(NULL, 0);

    return 0;
}

/**
 * Disconnect from a %RAMCloud.
 *
 * \param[in]  client   a connected client
 */
void
rc_disconnect(struct rc_client *client)
{
    delete client->serv;
    delete client->trans;
}

/**
 * \var ERROR_MSG_LEN
 * The maximum length of a %RAMCloud error message.
 *
 * \TODO The max length of a %RAMCloud error message belongs with the RPC
 *      definitions.
 */
enum { ERROR_MSG_LEN = 256 };

/**
 * A buffer for the last %RAMCloud error that occurred.
 *
 * See rc_last_error() and rc_handle_errors().
 *
 * \TODO The error message should go in the client struct.
 */
static char rc_error_message[ERROR_MSG_LEN];

/**
 * Return the error message for the last %RAMCloud error that occurred.
 *
 * \return error message \n
 *      The returned pointer is owned by the callee. Do not free it. \n
 *      This value is undefined if no %RAMCloud error has occurred.
 * \warning This function is not reentrant.
 */
const char*
rc_last_error(void)
{
    return &rc_error_message[0];
}

/**
 * Print a %RAMCloud error from an RPC response.
 *
 * See rc_last_error() to retrieve the extracted error message.
 *
 * \param[in]  respBuf the RPC response, which must be of type
 *                     RCRPC_ERROR_RESPONSE.
 */
static void
rc_handle_errors(Buffer *respBuf)
{
    uint32_t messageLength = static_cast<uint32_t>(respBuf->getTotalLength() -
                                                   sizeof(rcrpc_header));
    char *message = static_cast<char*>(respBuf->getRange(sizeof(rcrpc_header),
                                                         messageLength));
    fprintf(stderr, "... '%s'\n", message);
    strncpy(&rc_error_message[0], message, messageLength);
}

static int
sendrcv_rpc(Service *s,
            Transport *trans,
            Buffer *req, enum RCRPC_TYPE req_type, size_t min_req_size,
            Buffer *resp, enum RCRPC_TYPE resp_type, size_t min_resp_size)
__attribute__ ((warn_unused_result));

/**
 * Send an RPC request and receive the response.
 *
 * This function should not be called directly. Rather, ::SENDRCV_RPC should be
 * used.
 *
 * \param[in] s     The service associated with the destination of this RPC.
 * \param[in] trans The transport layer to use for this RPC.
 * \param[in] req           See #SENDRCV_RPC.
 * \param[in] req_type      the RPC type expected in the header of the request
 * \param[in] min_req_size  the smallest acceptable size of the request
 * \param[out] resp         See #SENDRCV_RPC.
 * \param[in] resp_type     the RPC type expected in the header of the response
 * \param[in] min_resp_size the smallest acceptable size of the response
 * \return error code (see below)
 * \retval  0 success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 */
static int
sendrcv_rpc(Service *s,
            Transport* trans,
            Buffer *req, enum RCRPC_TYPE req_type, size_t min_req_size,
            Buffer *resp, enum RCRPC_TYPE resp_type, size_t min_resp_size)
{
    struct rcrpc_header reqHeader;
    struct rcrpc_header *respHeader;

    reqHeader.type = (uint32_t) req_type;
    if (min_req_size != 1) // In C++, structs with no members have sizeof 0.
        assert(req->getTotalLength() >= (uint32_t) min_req_size);
    req->prepend(&reqHeader, sizeof(reqHeader));

    trans->clientSend(s, req, resp)->getReply();

    respHeader = static_cast<rcrpc_header*>(
        resp->getRange(0, sizeof(*respHeader)));
    if (respHeader == NULL)
        return -1;

    if (respHeader->type == RCRPC_ERROR_RESPONSE) {
        rc_handle_errors(resp);
        return -1;
    }

    if (respHeader->type != static_cast<uint32_t>(resp_type))
        return -1;

    // In C++, structs with no members have sizeof 0.
    if (min_resp_size > 1 && (resp->getTotalLength() < sizeof(*respHeader) +
                              min_resp_size))
        return -1;

    return 0;
}

/**
 * Send an RPC request and receive the response.
 *
 * This is a wrapper around sendrcv_rpc() for convenience.
 *
 * The caller is required to have a rc_client struct in its scope under the
 * identifier \c client.
 *
 * \param[in] rcrpc_upper   the name of the RPC in uppercase as a literal
 * \param[in] rcrpc_lower   the name of the RPC in lowercase as a literal
 * \param[in] query  The request Buffer to send out, which should not have an
 *                   rcrpc_header yet.
 * \param[out] resp An empty Buffer to fill in with the response. The response
 *                  is guaranteed to be of the correct RPC type iff this call
 *                  returns 0. There will be an rcrpc_header on the front of
 *                  this for now.
 * \return error code as an \c int (see below)
 * \retval  0 success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 */
#define SENDRCV_RPC(rcrpc_upper, rcrpc_lower, query, resp)                     \
    ({                                                                         \
        sendrcv_rpc(                                                           \
                client->serv,                                                  \
                client->trans,                                                 \
                query,                                                         \
                RCRPC_##rcrpc_upper##_REQUEST,                                 \
                sizeof(rcrpc_##rcrpc_lower##_request),                         \
                resp,                                                          \
                RCRPC_##rcrpc_upper##_RESPONSE,                                \
                sizeof(rcrpc_##rcrpc_lower##_response));                       \
    })

/**
 * Determine which reject rule caused an operation to fail.
 *
 * \see rcrpc_reject_rules
 *
 * \param[in] reject_rules   the reasons for the operation to abort
 * \param[in] got_version    the version of the object, or #RCRPC_VERSION_NONE
 *      if it does not exist
 * \return error code as an \c int (see below)
 * \retval 0 No reject rule was violated.
 * \retval 1 The object doesn't exist and
 *      rcrpc_reject_rules.object_doesnt_exist is set.
 * \retval 2 The object exists and rcrpc_reject_rules.object_exists is set.
 * \retval 3 The object exists, rcrpc_reject_rules.version_eq_given is set,
 *      and \a got_version is equal to rcrpc_reject_rules.given_version.
 * \retval 4 The object exists, rcrpc_reject_rules.version_gt_given is set,
 *      and \a got_version is greater than rcrpc_reject_rules.given_version.
 * \retval 5 The object exists, rcrpc_reject_rules.version_eq_given or
 *      rcrpc_reject_rules.version_gt_given is set, and \a got_version is less
 *      than rcrpc_reject_rules.given_version. This should be considered a
 *      critical application consistency error since applications should never
 *      fabricate version numbers.
 * \TODO Try to get rid of the magic values 1-5.
 */
static int
reject_reason(const struct rcrpc_reject_rules *reject_rules,
              uint64_t got_version)
{
    if (got_version == RCRPC_VERSION_NONE) {
        if (reject_rules->object_doesnt_exist) {
            return 1;
        }
    } else {
        if (reject_rules->object_exists) {
            return 2;
        }
        if (reject_rules->version_eq_given &&
            got_version == reject_rules->given_version) {
            return 3;
        }
        if (reject_rules->version_gt_given &&
            got_version > reject_rules->given_version) {
            return 4;
        }
        if ((reject_rules->version_eq_given ||
             reject_rules->version_gt_given) &&
            got_version < reject_rules->given_version) {
            return 5;
        }
    }
    return 0;
}

/**
 * Verify connectivity with a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval other reserved for future use
 * \see #ping_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_ping(struct rc_client *client)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;

    int r = SENDRCV_RPC(PING, ping, &reqBuf, &respBuf);

    unlock(client);
    return r;
}

/**
 * Write (create or overwrite) an object in a %RAMCloud.
 *
 * This function can be used to create an object at a specified object ID. To
 * create an object with a server-assigned object ID, see rc_insert().
 *
 * If the object is created, the new version of the object is guaranteed to be
 * greater than that of any previous object that resided at the same \a table
 * and \a key. If the object overwrites an existing object at that \a key, the
 * new version number is guaranteed to be exactly 1 higher than the old version
 * number.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be written
 * \param[in]  key      the object ID of the object to be written
 * \param[in]  reject_rules see reject_reason()
 * \param[out] got_version
 *      the version of the object after the write took effect \n
 *      If the write did not occur, this is set to the object's current
 *      version. \n
 *      If the caller is not interested, got_version may be \c NULL.
 * \param[in]  buf      the object's data
 * \param[in]  len      the size of the object's data in bytes
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  positive on reject by \a reject_rules, see reject_reason()
 * \see #write_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_write(struct rc_client *client,
         uint64_t table,
         uint64_t key,
         const struct rcrpc_reject_rules *reject_rules,
         uint64_t *got_version,
         const char *buf,
         uint64_t len)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_write_request query;
    struct rcrpc_write_response *resp;

    query.table = table;
    query.key = key;
    memcpy(&query.reject_rules, reject_rules, sizeof(*reject_rules));
    query.buf_len = len;
    reqBuf.append(&query, sizeof(query));
    assert(len < (1UL << 32));
    reqBuf.append(const_cast<char*>(buf), static_cast<uint32_t>(len));

    int r = SENDRCV_RPC(WRITE, write, &reqBuf, &respBuf);
    if (r) {
        goto out;
    }

    resp = static_cast<rcrpc_write_response*>(
        respBuf.getRange(sizeof(rcrpc_header), sizeof(*resp)));

    if (got_version != NULL)
        *got_version = resp->version;

    if (!resp->written) {
        r = reject_reason(reject_rules, resp->version);
        assert(r > 0);
        goto out;
    }

  out:
    unlock(client);
    return r;
}

/**
 * Create an object in a %RAMCloud with a server-assigned object ID.
 *
 * The new object is assigned an object ID based on the table's object ID
 * allocation strategy, which is yet to be officially defined.
 *
 * If the object is written, the new version of the object is guaranteed to be
 * greater than that of any previous object that resided at the same \a table
 * and \a key.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be inserted
 * \param[in]  buf      the object's data
 * \param[in]  len      the size of the object's data in bytes
 * \param[out] key      the object ID of the object that was inserted
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval other reserved for future use
 * \bug Currently, the server may assign an object ID that is already in use
 *      and overwrite an existing object. To work around this, do not use
 *      rc_insert() on tables that also have objects with small
 *      application-assigned object IDs (see rc_write()).
 * \see #insert_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_insert(struct rc_client *client,
          uint64_t table,
          const char *buf,
          uint64_t len,
          uint64_t *key)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_insert_request query;
    struct rcrpc_insert_response *resp;

    query.table = table;
    query.buf_len = len;
    reqBuf.append(&query, sizeof(query));
    assert(len < (1UL << 32));
    reqBuf.append(const_cast<char*>(buf), static_cast<uint32_t>(len));

    int r = SENDRCV_RPC(INSERT, insert, &reqBuf, &respBuf);
    if (r) {
        goto out;
    }

    resp = static_cast<rcrpc_insert_response*>(
        respBuf.getRange(sizeof(rcrpc_header), sizeof(*resp)));

    *key = resp->key;

  out:
    unlock(client);
    return r;
}

/**
 * Delete an object from a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be deleted
 * \param[in]  key      the object ID of the object to be deleted
 * \param[in]  reject_rules see reject_reason()
 * \param[out] got_version
 *      the version of the object, if it exists \n
 *      If the delete did not occur, this is set to the object's current
 *      version. \n
 *      If the object no longer exists, \a got_version is undefined.
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  positive on reject by \a reject_rules, see reject_reason()
 * \see #delete_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_delete(struct rc_client *client,
          uint64_t table,
          uint64_t key,
          const struct rcrpc_reject_rules *reject_rules,
          uint64_t *got_version)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_delete_request query;
    struct rcrpc_delete_response *resp;

    query.table = table;
    query.key = key;
    memcpy(&query.reject_rules, reject_rules, sizeof(*reject_rules));
    reqBuf.append(&query, sizeof(query));

    int r = SENDRCV_RPC(DELETE, delete, &reqBuf, &respBuf);
    if (r) {
        goto out;
    }

    resp = static_cast<rcrpc_delete_response*>(
        respBuf.getRange(sizeof(rcrpc_header), sizeof(*resp)));

    if (got_version != NULL)
        *got_version = resp->version;

    if (!resp->deleted) {
        r = reject_reason(reject_rules, resp->version);
        assert(r > 0);
        goto out;
    }

  out:
    unlock(client);
    return r;
}

/**
 * Read an object from a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be read
 * \param[in]  key      the object ID of the object to be read
 * \param[in]  reject_rules see reject_reason() \n
 *      rcrpc_reject_rules.object_doesnt_exist must be set.
 * \param[out] got_version
 *      the current version of the object \n
 *      If the caller is not interested, \a got_version may be \c NULL. \n
 *      If the object does not exist, \a got_version is undefined.
 * \param[out] buf      the object's data
 * \param[out] len      the size of the object's data in bytes
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  positive on reject by \a reject_rules, see reject_reason()
 * \see #read_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_read(struct rc_client *client,
        uint64_t table,
        uint64_t key,
        const struct rcrpc_reject_rules *reject_rules,
        uint64_t *got_version,
        char *buf,
        uint64_t *len)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_read_request query;
    struct rcrpc_read_response *resp;

    query.table = table;
    query.key = key;
    memcpy(&query.reject_rules, reject_rules, sizeof(*reject_rules));
    query.reject_rules.object_doesnt_exist = true;
    reqBuf.append(&query, sizeof(query));

    int r = SENDRCV_RPC(READ, read, &reqBuf, &respBuf);
    if (r)
        goto out;

    resp = static_cast<rcrpc_read_response*>(
        respBuf.getRange(sizeof(rcrpc_header), sizeof(*resp)));

    if (got_version != NULL)
        *got_version = resp->version;

    *len = resp->buf_len;
    assert(resp->buf_len < (1UL << 32));
    // TODO(ongaro): Let's hope buf can fit buf_len bytes.
    respBuf.copy(sizeof(rcrpc_header) + sizeof(*resp),
                 static_cast<uint32_t>(resp->buf_len), buf);

    r = reject_reason(&query.reject_rules, resp->version);

  out:
    unlock(client);
    return r;
}

/**
 * Create a table in a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  name     a string of no more than 64 characters identifying the
 *      table
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (currently including table exists and system
 *      is out of space for tables; see rc_last_error())
 * \retval other reserved for future use
 * \bug I don't think the new table is guaranteed to contain no objects yet.
 * \TODO Table exists should not be a %RAMCloud error.
 * \see #create_table_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_create_table(struct rc_client *client, const char *name)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_create_table_request query;

    strncpy(query.name, name, sizeof(query.name));
    query.name[sizeof(query.name) - 1] = '\0';
    reqBuf.append(&query, sizeof(query));

    int r = SENDRCV_RPC(CREATE_TABLE, create_table, &reqBuf, &respBuf);

    unlock(client);
    return r;
}

/**
 * Open a table in a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  name     a string of no more than 64 characters identifying the
 *      table
 * \param[out] table_id a handle for the open table
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (currently including table does not exist; see
 *      rc_last_error())
 * \retval other reserved for future use
 * \TODO Table does not exist should not be a %RAMCloud error.
 * \see #open_table_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_open_table(struct rc_client *client, const char *name, uint64_t *table_id)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_open_table_request query;
    struct rcrpc_open_table_response *resp;

    strncpy(query.name, name, sizeof(query.name));
    query.name[sizeof(query.name) - 1] = '\0';
    reqBuf.append(&query, sizeof(query));

    int r = SENDRCV_RPC(OPEN_TABLE, open_table, &reqBuf, &respBuf);
    if (r)
        goto out;

    resp = static_cast<rcrpc_open_table_response*>(
        respBuf.getRange(sizeof(rcrpc_header), sizeof(*resp)));

    *table_id = resp->handle;

  out:
    unlock(client);
    return r;
}

/**
 * Delete a table in a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  name     a string of no more than 64 characters identifying the
 *      table
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (currently including table does not exist; see
 *      rc_last_error())
 * \retval other reserved for future use
 * \TODO Table does not exist should not be a %RAMCloud error.
 * \see #drop_table_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_drop_table(struct rc_client *client, const char *name)
{
    lock(client);

    Buffer reqBuf;
    Buffer respBuf;
    struct rcrpc_drop_table_request query;

    strncpy(query.name, name, sizeof(query.name));
    query.name[sizeof(query.name) - 1] = '\0';
    reqBuf.append(&query, sizeof(query));

    int r = SENDRCV_RPC(DROP_TABLE, drop_table, &reqBuf, &respBuf);

    unlock(client);
    return r;
}

/**
 * Allocate a new client.
 *
 * The caller should later use rc_free() to free the client struct.
 *
 * It is also legal for the caller to allocate memory for an rc_client struct
 * directly. This function is mostly a convenience for higher-level language
 * bindings.
 *
 * \return a newly allocated client, or \c NULL if the system is out of memory
 */
struct rc_client *
rc_new(void) {
    return reinterpret_cast<struct rc_client*>(xmalloc(sizeof(struct rc_client)));
}

/**
 * Free a client.
 *
 * This function should only be called on rc_client structs allocated with
 * rc_new().
 *
 * \param[in] client    a client previously allocated with rc_new()
 */
void
rc_free(struct rc_client *client)
{
    free(client);
}
