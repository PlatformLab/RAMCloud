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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <malloc.h>

#include <config.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <client/client.h>

/**
 * A reserved version number representing no particular version.
 *
 * This constant is mostly a convenience for higher-level language bindings.
 * Others should use ::RCRPC_VERSION_ANY directly.
 */
const uint64_t rcrpc_version_any = RCRPC_VERSION_ANY;

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
rc_last_error()
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
    struct rcrpc_ping_request query;
    struct rcrpc_ping_response *resp;

    query.header.type = RCRPC_PING_REQUEST;
    query.header.len  = (uint32_t) RCRPC_PING_REQUEST_LEN;
    return SENDRCV_RPC(PING, ping, &query, &resp);
}

/**
 * The maximum size of an object's data and index entries together.
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
 * If the object is written, the new version of the object is guaranteed to be
 * greater than that of any previous object that resided at the same \a table
 * and \a key.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be written
 * \param[in]  key      the object ID of the object to be written
 * \param[in]  want_version
 *      ::RCRPC_VERSION_ANY or the version of the object to be written
 * \param[out] got_version
 *      the version of the object after the write took effect \n
 *      If the write did not occur (it must have been an overwrite), this is
 *      set to the object's current version. \n
 *      If the caller is not interested, got_version may be \c NULL.
 * \param[in]  buf      the object's data
 * \param[in]  len      the size of the object's data in bytes
 * \param[in]  index_entries_buf
 *      the object's index entries (array of rc_index_entry)
 * \param[in]  index_entries_len
 *      the size of the object's index entries in bytes
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  1 if requested version was specified in \a want_version and the
 *      object exists but this is not the object's current version
 * \retval other reserved for future use
 * \warning Watch out for the bad semantics of \a got_version when the object
 *      does not exist.
 * \see #write_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_write(struct rc_client *client,
         uint64_t table,
         uint64_t key,
         uint64_t want_version,
         uint64_t *got_version,
         const char *buf,
         uint64_t len,
         const char *index_entries_buf,
         uint64_t index_entries_len)
{
    assert(len <= MAX_DATA_WRITE_LEN);
    char query_buf[RCRPC_WRITE_REQUEST_LEN_WODATA + MAX_DATA_WRITE_LEN];
    struct rcrpc_write_request *query;
    struct rcrpc_write_response *resp;
    query = (struct rcrpc_write_request *) query_buf;
    char *var;

    query->header.type = RCRPC_WRITE_REQUEST;
    query->header.len  = (uint32_t) RCRPC_WRITE_REQUEST_LEN_WODATA + len +
                         index_entries_len;
    query->table = table;
    query->key = key;
    query->version = want_version;
    query->index_entries_len = index_entries_len;
    query->buf_len = len;
    var = query->var;
    memcpy(var, index_entries_buf, index_entries_len);
    var += index_entries_len;
    memcpy(var, buf, len);
    var += len;

    int r = SENDRCV_RPC(WRITE, write, query, &resp);

    if (got_version != NULL)
        *got_version = resp->version;

    if (want_version != RCRPC_VERSION_ANY && want_version != resp->version)
        return 1;

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
 * \param[in]  index_entries_buf
 *      the object's index entries (array of rc_index_entry)
 * \param[in]  index_entries_len
 *      the size of the object's index entries in bytes
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
          uint64_t *key,
          const char *index_entries_buf,
          uint64_t index_entries_len)
{
    assert(len <= MAX_DATA_WRITE_LEN);
    char query_buf[RCRPC_WRITE_REQUEST_LEN_WODATA + MAX_DATA_WRITE_LEN];
    struct rcrpc_insert_request *query;
    struct rcrpc_insert_response *resp;
    query = (struct rcrpc_insert_request *) query_buf;
    char *var;

    query->header.type = RCRPC_INSERT_REQUEST;
    query->header.len  = (uint32_t) RCRPC_INSERT_REQUEST_LEN_WODATA + len +
                         index_entries_len;
    query->table = table;
    query->index_entries_len = index_entries_len;
    query->buf_len = len;
    var = query->var;
    memcpy(var, index_entries_buf, index_entries_len);
    var += index_entries_len;
    memcpy(var, buf, len);
    var += len;

    int r = SENDRCV_RPC(INSERT, insert, query, &resp);
    if (r) {
        return r;
    }
    *key = resp->key;
    return 0;
}

/**
 * Delete an object from a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be deleted
 * \param[in]  key      the object ID of the object to be deleted
 * \param[in]  want_version
 *      ::RCRPC_VERSION_ANY or the version of the object to be deleted
 * \param[out] got_version
 *      the version of the object before the delete took effect \n
 *      If the delete did not occur, this is set to the object's current
 *      version. \n
 *      If the object does not exist, \a got_version is undefined.
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  1 if requested version was specified in \a want_version and not the
 *      object's current version
 * \retval  2 if the object does not exist
 * \retval other reserved for future use
 * \see #delete_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_delete(struct rc_client *client,
          uint64_t table,
          uint64_t key,
          uint64_t want_version,
          uint64_t *got_version)
{
    struct rcrpc_delete_request query;
    struct rcrpc_delete_response *resp;

    query.header.type = RCRPC_DELETE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_DELETE_REQUEST_LEN;
    query.table = table;
    query.key = key;
    query.version = want_version;

    int r = SENDRCV_RPC(DELETE, delete, &query, &resp);
    if (r) {
        return r;
    }

    if (got_version != NULL)
        *got_version = resp->version;

    if (resp->version == RCRPC_VERSION_ANY)
        return 2;

    if (want_version != RCRPC_VERSION_ANY && resp->version != want_version)
        return 1;

    return 0;
}

/**
 * Read an object from a %RAMCloud.
 *
 * \param[in]  client   a connected client
 * \param[in]  table    the table containing the object to be read
 * \param[in]  key      the object ID of the object to be read
 * \param[in]  want_version
 *      ::RCRPC_VERSION_ANY or the version of the object to be read
 * \param[out] got_version
 *      the current version of the object \n
 *      If the caller is not interested, \a got_version may be \c NULL. \n
 *      If the object does not exist, \a got_version is undefined.
 * \param[out] buf      the object's data
 * \param[out] len      the size of the object's data in bytes
 * \param[out] index_entries_buf
 *      the object's index entries (array of rc_index_entry) \n
 *      If the caller is not interested, \a index_entries_buf may be \c NULL.
 * \param[out] index_entries_len
 *      the size of the object's index entries in bytes \n
 *      Will not be set if \a index_entries_buf is \c NULL.
 * \return error code (see values below)
 * \retval  0 on success
 * \retval -1 on %RAMCloud error (see rc_last_error())
 * \retval  1 if requested version was specified in \a want_version and not the
 *      object's current version
 * \retval  2 if the object does not exist
 * \retval other reserved for future use
 * \see #read_RPC_doc_hook(), the underlying RPC which this wraps
 */
int
rc_read(struct rc_client *client,
        uint64_t table,
        uint64_t key,
        uint64_t want_version,
        uint64_t *got_version,
        char *buf,
        uint64_t *len,
        char *index_entries_buf,
        uint64_t *index_entries_len)
{
    struct rcrpc_read_request query;
    struct rcrpc_read_response *resp;
    char *var;

    query.header.type = RCRPC_READ_REQUEST;
    query.header.len  = (uint32_t) RCRPC_READ_REQUEST_LEN;
    query.table = table;
    query.key = key;
    query.version = want_version;
    int r = SENDRCV_RPC(READ, read, &query, &resp);
    if (r)
        return r;

    if (got_version != NULL)
        *got_version = resp->version;

    var = resp->var;
    if (index_entries_buf != NULL) {
        *index_entries_len = resp->index_entries_len;
        memcpy(index_entries_buf, var, *index_entries_len);
    }
    var += resp->index_entries_len;
    *len = resp->buf_len;
    memcpy(buf, var, *len);
    var += resp->buf_len;

    if (resp->version == RCRPC_VERSION_ANY)
        return 2;

    if (want_version != RCRPC_VERSION_ANY && resp->version != want_version)
        return 1;

    return 0;
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
    struct rcrpc_create_table_request query;
    struct rcrpc_create_table_response *resp;

    query.header.type = RCRPC_CREATE_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_CREATE_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    return SENDRCV_RPC(CREATE_TABLE, create_table, &query, &resp);
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
    struct rcrpc_open_table_request query;
    struct rcrpc_open_table_response *resp;

    query.header.type = RCRPC_OPEN_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_OPEN_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    int r = SENDRCV_RPC(OPEN_TABLE, open_table, &query, &resp);
    if (r)
        return r;
    *table_id = resp->handle;

    return 0;
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
    struct rcrpc_drop_table_request query;
    struct rcrpc_drop_table_response *resp;

    query.header.type = RCRPC_DROP_TABLE_REQUEST;
    query.header.len  = (uint32_t) RCRPC_DROP_TABLE_REQUEST_LEN;
    char *table_name = query.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    return SENDRCV_RPC(DROP_TABLE, drop_table, &query, &resp);
}

int
rc_create_index(struct rc_client *client,
                uint64_t table_id,
                enum RCRPC_INDEX_TYPE type,
                bool unique, bool range_queryable,
                uint16_t *index_id)
{
    struct rcrpc_create_index_request query;
    struct rcrpc_create_index_response *resp;

    query.header.type = RCRPC_CREATE_INDEX_REQUEST;
    query.header.len  = (uint32_t) RCRPC_CREATE_INDEX_REQUEST_LEN;
    query.table = table_id;
    query.type = (uint8_t) type;
    query.unique = unique;
    query.range_queryable = range_queryable;
    int r = SENDRCV_RPC(CREATE_INDEX, create_index, &query, &resp);
    if (r)
        return r;
    *index_id = resp->id;

    return 0;
}

int
rc_drop_index(struct rc_client *client, uint64_t table_id, uint16_t index_id)
{
    struct rcrpc_drop_index_request query;
    struct rcrpc_drop_index_response *resp;

    query.header.type = RCRPC_DROP_INDEX_REQUEST;
    query.header.len  = (uint32_t) RCRPC_DROP_INDEX_REQUEST_LEN;
    query.table = table_id;
    query.id = index_id;
    return SENDRCV_RPC(DROP_INDEX, drop_index, &query, &resp);
}

int
rc_unique_lookup(struct rc_client *client, uint64_t table,
                 uint16_t index_id, const char *key, uint64_t key_len,
                 bool *oid_present, uint64_t *oid)
{
    uint32_t query_len = (uint32_t) RCRPC_UNIQUE_LOOKUP_REQUEST_LEN_WODATA +
                         key_len;
    struct rcrpc_unique_lookup_request *query;
    struct rcrpc_unique_lookup_response *resp;
    char query_buf[query_len];
    query = (struct rcrpc_unique_lookup_request*) query_buf;

    query->header.type = RCRPC_UNIQUE_LOOKUP_REQUEST;
    query->header.len  = query_len;
    query->table = table;
    query->index_id = index_id;
    query->key_len = key_len;
    memcpy(query->key, key, key_len);

    int r = SENDRCV_RPC(UNIQUE_LOOKUP, unique_lookup, query, &resp);
    if (r) {
        return r;
    }

    *oid_present = (bool) resp->oid_present;
    if (*oid_present) {
        *oid = resp->oid;
    }
    return 0;
}

struct rc_multi_lookup_args *
rc_multi_lookup_args_new()
{
    return calloc(1, sizeof(struct rc_multi_lookup_args));
}

void
rc_multi_lookup_args_free(struct rc_multi_lookup_args *args)
{
    free(args);
}

void
rc_multi_lookup_set_index(struct rc_multi_lookup_args *args, uint64_t table,
                          uint16_t index_id)
{
    args->rpc.table = table;
    args->rpc.index_id = index_id;
}

void
rc_multi_lookup_set_key(struct rc_multi_lookup_args *args, const char *key,
                        uint64_t len)
{
    args->key = key;
    args->rpc.key_len = len;
}

void
rc_multi_lookup_set_start_following_oid(struct rc_multi_lookup_args *args,
                                        uint64_t oid)
{
    args->rpc.start_following_oid_present = true;
    args->start_following_oid = oid;
}

void
rc_multi_lookup_set_result_buf(struct rc_multi_lookup_args *args,
                               uint32_t *count, uint64_t *oids_buf,
                               bool *more)
{
    args->rpc.limit = *count;
    args->more = more;
    args->count = count;
    args->oids_buf = oids_buf;
}

int
rc_multi_lookup(struct rc_client *client,
                const struct rc_multi_lookup_args *args)
{
    struct rcrpc_multi_lookup_request *query;
    struct rcrpc_multi_lookup_response *resp;
    char *var;

    int query_buf_len;
    query_buf_len = RCRPC_MULTI_LOOKUP_REQUEST_LEN_WODATA;
    if (args->rpc.start_following_oid_present) {
        query_buf_len += sizeof(uint64_t);
    }
    query_buf_len += args->rpc.key_len;

    char query_buf[query_buf_len];
    query = (struct rcrpc_multi_lookup_request*) query_buf;

    memcpy(query, &args->rpc, sizeof(args->rpc));
    query->header.type = RCRPC_MULTI_LOOKUP_REQUEST;
    query->header.len = (uint32_t) query_buf_len;
    var = query->var;
    if (args->rpc.start_following_oid_present) {
        *((uint64_t*) var) = args->start_following_oid;
        var += sizeof(uint64_t);
    }
    memcpy(var, args->key, args->rpc.key_len);
    var += args->rpc.key_len;

    int r = SENDRCV_RPC(MULTI_LOOKUP, multi_lookup, query, &resp);
    if (r) {
        return r;
    }

    *args->count = resp->len;
    *args->more = (bool) resp->more;
    memcpy(args->oids_buf, resp->oids, *args->count * sizeof(uint64_t));

    return 0;
}

struct rc_range_query_args *
rc_range_query_args_new() {
    return calloc(1, sizeof(struct rc_range_query_args));
}

void
rc_range_query_args_free(struct rc_range_query_args *args)
{
    free(args);
}

void
rc_range_query_set_index(struct rc_range_query_args *args, uint64_t table,
                         uint16_t index_id) {
    args->rpc.table = table;
    args->rpc.index_id = index_id;
}

void
rc_range_query_set_key_start(struct rc_range_query_args *args, const char *key,
                             uint64_t len, bool inclusive) {
    args->rpc.key_start_present = true;
    args->rpc.key_start_inclusive = inclusive;
    args->key_start = key;
    args->key_start_len = len;
}

void rc_range_query_set_key_end(struct rc_range_query_args *args, const char *key,
                                uint64_t len, bool inclusive) {
    args->rpc.key_end_present = true;
    args->rpc.key_end_inclusive = inclusive;
    args->key_end = key;
    args->key_end_len = len;
}

void
rc_range_query_set_start_following_oid(struct rc_range_query_args *args,
                                       uint64_t oid) {
    args->rpc.start_following_oid_present = true;
    args->start_following_oid = oid;
}

void
rc_range_query_set_result_bufs(struct rc_range_query_args *args,
                               uint32_t *count, uint64_t *oids_buf,
                               uint64_t *oids_buf_len, char *keys_buf,
                               uint64_t *keys_buf_len, bool *more) {
    args->rpc.limit = *count;
    args->rpc.request_keys = (keys_buf != NULL);
    args->more = more;
    args->count = count;
    args->oids_buf = oids_buf;
    args->oids_buf_len = oids_buf_len;
    args->keys_buf = keys_buf;
    args->keys_buf_len = keys_buf_len;
}

int
rc_range_query(struct rc_client *client,
               const struct rc_range_query_args *args) {
    struct rcrpc_range_query_request *query;
    struct rcrpc_range_query_response *resp;
    char *var;

    int query_buf_len;
    query_buf_len = RCRPC_RANGE_QUERY_REQUEST_LEN_WODATA;
    if (args->rpc.start_following_oid_present) {
        query_buf_len += sizeof(uint64_t);
    }
    query_buf_len += sizeof(uint64_t) + args->key_start_len;
    query_buf_len += sizeof(uint64_t) + args->key_end_len;

    char query_buf[query_buf_len];
    query = (struct rcrpc_range_query_request*) query_buf;

    memcpy(query, &args->rpc, sizeof(args->rpc));
    query->header.type = RCRPC_RANGE_QUERY_REQUEST;
    query->header.len = (uint32_t) query_buf_len;
    var = query->var;
    if (args->rpc.start_following_oid_present) {
        *((uint64_t*) var) = args->start_following_oid;
        var += sizeof(uint64_t);
    }
    if (args->rpc.key_start_present) {
        *((uint64_t*) var) = args->key_start_len;
        var += sizeof(uint64_t);
        memcpy(var, args->key_start, args->key_start_len);
        var += args->key_start_len;
    }
    if (args->rpc.key_end_present) {
        *((uint64_t*) var) = args->key_end_len;
        var += sizeof(uint64_t);
        memcpy(var, args->key_end, args->key_end_len);
        var += args->key_end_len;
    }

    int r = SENDRCV_RPC(RANGE_QUERY, range_query, query, &resp);
    if (r) {
        return r;
    }

    //TODO(ongaro): I hope your buffer is large enough.
    *args->count = resp->len;
    *args->more = (bool) resp->more;
    var = resp->var;
    *args->oids_buf_len = (*args->count) * sizeof(uint64_t);
    memcpy(args->oids_buf, var, *args->oids_buf_len);
    var += *args->oids_buf_len;
    if (args->rpc.request_keys) {
        *args->keys_buf_len = resp->header.len -
                              RCRPC_RANGE_QUERY_RESPONSE_LEN_WODATA -
                              *args->oids_buf_len;
        memcpy(args->keys_buf, var, *args->keys_buf_len);
    }

    return 0;
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
rc_new() {
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
