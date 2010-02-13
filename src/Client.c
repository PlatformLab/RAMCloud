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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <malloc.h>

#include <config.h>

#if RC_CLIENT_SHARED
#include <semaphore.h>
#include <sys/mman.h>
#endif

#include <rcrpc.h>

#include <Client.h>

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
    client->shared = mmap(NULL,
                          sizeof(struct rc_client_shared),
                          PROT_READ|PROT_WRITE,
                          MAP_SHARED|MAP_ANONYMOUS,
                          -1, 0);
    assert(client->shared != MAP_FAILED);
    assert(sem_init(&client->shared->sem, 1, 1) == 0);
#endif
    rc_net_init(&client->net, CLNTADDR, CLNTPORT, SVRADDR, SVRPORT);
    rc_net_connect(&client->net);
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
 * Detect a %RAMCloud error in an RPC response.
 *
 * See rc_last_error() to retrieve the extracted error message.
 *
 * \param[in]  resp_any the RPC response
 * \retval  0 there was no %RAMCloud error
 * \retval -1 there was a %RAMCloud error
 */
static int
rc_handle_errors(struct rcrpc_any *resp_any)
{
    struct rcrpc_error_response *resp;
    if (resp_any->header.type != RCRPC_ERROR_RESPONSE)
        return 0;
    resp = (struct rcrpc_error_response*) resp_any;
    fprintf(stderr, "... '%s'\n", resp->message);
    strncpy(&rc_error_message[0], resp->message, ERROR_MSG_LEN);
    return -1;
}

static int
sendrcv_rpc(struct rc_net *net,
            struct rcrpc_any *req,
            enum RCRPC_TYPE req_type, size_t min_req_size,
            struct rcrpc_any **respp,
            enum RCRPC_TYPE resp_type, size_t min_resp_size
           ) __attribute__ ((warn_unused_result));

/**
 * Send an RPC request and receive the response.
 *
 * This function should not be called directly. Rather, ::SENDRCV_RPC should be
 * used.
 *
 * \param[in] net   the network struct from the rc_client
 * \param[in] req   a pointer to the request
 *      The request should have its RPC type and length in the header already
 *      set.
 * \param[in] req_type      the RPC type expected in the header of the request
 * \param[in] min_req_size  the smallest acceptable size of the request
 * \param[out] respp   a pointer to a pointer to the response
 *      The response is guaranteed to be of the correct RPC type. \n
 *      The pointer will be set to \c NULL if a %RAMCloud error occurs.
 * \param[in] resp_type     the RPC type expected in the header of the response
 * \param[in] min_resp_size the smallest acceptable size of the response
 * \return error code (see below)
 * \retval  0 success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 */
static int
sendrcv_rpc(struct rc_net *net,
            struct rcrpc_any *req,
            enum RCRPC_TYPE req_type, size_t min_req_size,
            struct rcrpc_any **respp,
            enum RCRPC_TYPE resp_type, size_t min_resp_size)
{
    struct rcrpc_any *resp;
    int r;

    *respp = NULL;

    assert(req->header.type == req_type);
    assert(req->header.len >= min_req_size);

    assert(!rc_net_send_rpc(net, req));
    assert(!rc_net_recv_rpc(net, &resp));

    r = rc_handle_errors(resp);
    if (r == 0) {
        assert(resp->header.type == resp_type);
        assert(resp->header.len >= min_resp_size);
        *respp = resp;
    }
    return r;
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
 * \param[in] query  a \c struct \c rcrpc_*_request pointer to the request \n
 *      The request should have its RPC type and length in the header already
 *      set.
 * \param[out] respp a pointer to the \c struct \c rcrpc_*_response pointer
 *      which should point to the response \n
 *      The response is guaranteed to be of the correct RPC type. \n
 *      The pointer will be set to \c NULL if a %RAMCloud error occurs.
 * \return error code as an \c int (see below)
 * \retval  0 success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 */
#define SENDRCV_RPC(rcrpc_upper, rcrpc_lower, query, respp)                    \
    ({                                                                         \
        struct rcrpc_##rcrpc_lower##_request* _query = (query);                \
        struct rcrpc_##rcrpc_lower##_response** _respp = (respp);              \
        sendrcv_rpc(&client->net,                                              \
                (struct rcrpc_any*) _query,                                    \
                RCRPC_##rcrpc_upper##_REQUEST,                                 \
                sizeof(*_query),                                               \
                (struct rcrpc_any**) (_respp),                                 \
                RCRPC_##rcrpc_upper##_RESPONSE,                                \
                sizeof(**_respp));                                             \
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

    struct rcrpc_ping_request query;
    struct rcrpc_ping_response *resp;
    query.header.type = RCRPC_PING_REQUEST;
    query.header.len  = (uint32_t) RCRPC_PING_REQUEST_LEN;
    int r = SENDRCV_RPC(PING, ping, &query, &resp);

    unlock(client);
    return r;
}

/**
 * The maximum size of an object's data.
 *
 * This is estimated as roughly a little smaller than the maximum size of an
 * RPC.
 * \TODO Define the maximum object size more in a more stable way.
 */
#define MAX_DATA_WRITE_LEN (MAX_RPC_LEN - RCRPC_WRITE_REQUEST_LEN_WODATA - 256)

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

    assert(len <= MAX_DATA_WRITE_LEN);
    char query_buf[RCRPC_WRITE_REQUEST_LEN_WODATA + MAX_DATA_WRITE_LEN];
    struct rcrpc_write_request *query;
    struct rcrpc_write_response *resp;
    query = (struct rcrpc_write_request *) query_buf;

    query->header.type = RCRPC_WRITE_REQUEST;
    query->header.len  = (uint32_t) RCRPC_WRITE_REQUEST_LEN_WODATA + len;
    query->table = table;
    query->key = key;
    memcpy(&query->reject_rules, reject_rules, sizeof(*reject_rules));
    query->buf_len = len;
    memcpy(query->buf, buf, len);

    int r = SENDRCV_RPC(WRITE, write, query, &resp);
    if (r) {
        goto out;
    }

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

    assert(len <= MAX_DATA_WRITE_LEN);
    char query_buf[RCRPC_WRITE_REQUEST_LEN_WODATA + MAX_DATA_WRITE_LEN];
    struct rcrpc_insert_request *query;
    struct rcrpc_insert_response *resp;
    query = (struct rcrpc_insert_request *) query_buf;

    query->header.type = RCRPC_INSERT_REQUEST;
    query->header.len  = (uint32_t) RCRPC_INSERT_REQUEST_LEN_WODATA + len;
    query->table = table;
    query->buf_len = len;
    memcpy(query->buf, buf, len);

    int r = SENDRCV_RPC(INSERT, insert, query, &resp);
    if (r) {
        goto out;
    }
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

    struct rcrpc_delete_request query;
    struct rcrpc_delete_response *resp;

    query.header.type = RCRPC_DELETE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_DELETE_REQUEST_LEN;
    query.table = table;
    query.key = key;
    memcpy(&query.reject_rules, reject_rules, sizeof(*reject_rules));

    int r = SENDRCV_RPC(DELETE, delete, &query, &resp);
    if (r) {
        goto out;
    }

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

    struct rcrpc_read_request query;
    struct rcrpc_read_response *resp;

    query.header.type = RCRPC_READ_REQUEST;
    query.header.len  = (uint32_t) RCRPC_READ_REQUEST_LEN;
    query.table = table;
    query.key = key;
    memcpy(&query.reject_rules, reject_rules, sizeof(*reject_rules));
    query.reject_rules.object_doesnt_exist = true;

    int r = SENDRCV_RPC(READ, read, &query, &resp);
    if (r)
        goto out;

    if (got_version != NULL)
        *got_version = resp->version;

    *len = resp->buf_len;
    memcpy(buf, resp->buf, *len);

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

    struct rcrpc_create_table_request query;
    struct rcrpc_create_table_response *resp;

    query.header.type = RCRPC_CREATE_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_CREATE_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    int r = SENDRCV_RPC(CREATE_TABLE, create_table, &query, &resp);

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

    struct rcrpc_open_table_request query;
    struct rcrpc_open_table_response *resp;

    query.header.type = RCRPC_OPEN_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_OPEN_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    int r = SENDRCV_RPC(OPEN_TABLE, open_table, &query, &resp);
    if (r)
        goto out;
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

    struct rcrpc_drop_table_request query;
    struct rcrpc_drop_table_response *resp;

    query.header.type = RCRPC_DROP_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_DROP_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    int r = SENDRCV_RPC(DROP_TABLE, drop_table, &query, &resp);

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
    return malloc(sizeof(struct rc_client));
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
