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

#include <string.h>
#include <assert.h>
#include <malloc.h>

#include <config.h>

#include <shared/rcrpc.h>
#include <shared/net.h>

#include <client/client.h>

int
rc_connect(struct rc_client *client)
{
    rc_net_init(&client->net, CLNTADDR, CLNTPORT, SVRADDR, SVRPORT);
    rc_net_connect(&client->net);
    return 0;
}

void
rc_disconnect(struct rc_client *client)
{
}

// TODO(stutsman) we should put this in the client struct
enum { ERROR_MSG_LEN = 256 };
static char rc_error_message[ERROR_MSG_LEN];

const char*
rc_last_error()
{
    return &rc_error_message[0];
}

static int
rc_handle_errors(struct rcrpc *resp)
{
    if (resp->type != RCRPC_ERROR_RESPONSE)
        return 0;
    printf("... '%s'\n", resp->error_response.message);
    strncpy(&rc_error_message[0], resp->error_response.message, ERROR_MSG_LEN);
    return -1;
}

int
rc_ping(struct rc_client *client)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_PING_REQUEST;
    query.len  = (uint32_t) RCRPC_PING_REQUEST_LEN;

    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));

    return rc_handle_errors(resp);
}

int
rc_write(struct rc_client *client,
         uint64_t table,
         uint64_t key,
         const char *buf,
         uint64_t len,
         const char *index_entries_buf,
         uint64_t index_entries_len)
{
    assert(len <= MAX_DATA_WRITE_LEN);
    char query_buf[RCRPC_WRITE_REQUEST_LEN_WODATA + MAX_DATA_WRITE_LEN];
    struct rcrpc *query, *resp;
    query = (struct rcrpc *) query_buf;
    char *var;

    query->type = RCRPC_WRITE_REQUEST;
    query->len  = (uint32_t) RCRPC_WRITE_REQUEST_LEN_WODATA + len +
                  index_entries_len;
    query->write_request.table = table;
    query->write_request.key = key;
    query->write_request.index_entries_len = index_entries_len;
    query->write_request.buf_len = len;
    var = query->write_request.var;
    memcpy(var, index_entries_buf, index_entries_len);
    var += index_entries_len;
    memcpy(var, buf, len);
    var += len;

    assert(!rc_net_send_rpc(&client->net, query));
    assert(!rc_net_recv_rpc(&client->net, &resp));

    return rc_handle_errors(resp);
}

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
    struct rcrpc *query, *resp;
    query = (struct rcrpc *) query_buf;
    char *var;

    query->type = RCRPC_INSERT_REQUEST;
    query->len  = (uint32_t) RCRPC_INSERT_REQUEST_LEN_WODATA + len +
                  index_entries_len;
    query->insert_request.table = table;
    query->insert_request.index_entries_len = index_entries_len;
    query->insert_request.buf_len = len;
    var = query->insert_request.var;
    memcpy(var, index_entries_buf, index_entries_len);
    var += index_entries_len;
    memcpy(var, buf, len);
    var += len;

    assert(!rc_net_send_rpc(&client->net, query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    *key = resp->insert_response.key;
    return 0;
}

int
rc_delete(struct rc_client *client,
          uint64_t table,
          uint64_t key)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_DELETE_REQUEST;
    query.len  = (uint32_t) RCRPC_DELETE_REQUEST_LEN;
    query.delete_request.table = table;
    query.delete_request.key = key;

    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));

    return rc_handle_errors(resp);
}

int
rc_read(struct rc_client *client,
        uint64_t table,
        uint64_t key,
        char *buf,
        uint64_t *len,
        char *index_entries_buf,
        uint64_t *index_entries_len)
{
    struct rcrpc query, *resp;
    char *var;

    query.type = RCRPC_READ_REQUEST;
    query.len  = (uint32_t) RCRPC_READ_REQUEST_LEN;
    query.read_request.table = table;
    query.read_request.key = key;
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r)
        return r;

    var = resp->read_response.var;
    if (index_entries_buf != NULL) {
        *index_entries_len = resp->read_response.index_entries_len;
        memcpy(index_entries_buf, var, *index_entries_len);
    }
    var += resp->read_response.index_entries_len;
    *len = resp->read_response.buf_len;
    memcpy(buf, var, *len);
    var += resp->read_response.buf_len;

    return 0;
}

int
rc_create_table(struct rc_client *client, const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_CREATE_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_CREATE_TABLE_REQUEST_LEN;
    char *table_name = query.open_table_request.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));

    return rc_handle_errors(resp);
}

int
rc_open_table(struct rc_client *client, const char *name, uint64_t *table_id)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_OPEN_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_OPEN_TABLE_REQUEST_LEN;
    char *table_name = query.open_table_request.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r)
        return r;
    *table_id = resp->open_table_response.handle;

    return 0;
}

int
rc_drop_table(struct rc_client *client, const char *name)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_DROP_TABLE_REQUEST;
    query.len  = (uint32_t) RCRPC_DROP_TABLE_REQUEST_LEN;
    char *table_name = query.open_table_request.name;
    strncpy(table_name, name, sizeof(table_name));
    table_name[sizeof(table_name) - 1] = '\0';
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));

    return rc_handle_errors(resp);
}

int
rc_create_index(struct rc_client *client,
                uint64_t table_id,
                enum RCRPC_INDEX_TYPE type,
                bool unique, bool range_queryable,
                uint16_t *index_id)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_CREATE_INDEX_REQUEST;
    query.len  = (uint32_t) RCRPC_CREATE_INDEX_REQUEST_LEN;
    query.create_index_request.table = table_id;
    query.create_index_request.type = (uint8_t) type;
    query.create_index_request.unique = unique;
    query.create_index_request.range_queryable = range_queryable;
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r)
        return r;
    *index_id = resp->create_index_response.id;

    return 0;
}

int
rc_drop_index(struct rc_client *client, uint64_t table_id, uint16_t index_id)
{
    struct rcrpc query, *resp;

    query.type = RCRPC_DROP_INDEX_REQUEST;
    query.len  = (uint32_t) RCRPC_DROP_INDEX_REQUEST_LEN;
    query.drop_index_request.table = table_id;
    query.drop_index_request.id = index_id;
    assert(!rc_net_send_rpc(&client->net, &query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    return rc_handle_errors(resp);
}

int
rc_unique_lookup(struct rc_client *client, uint64_t table,
                 uint16_t index_id, const char *key, uint64_t key_len,
                 bool *oid_present, uint64_t *oid)
{
    uint32_t query_len = (uint32_t) RCRPC_UNIQUE_LOOKUP_REQUEST_LEN_WODATA +
                         key_len;
    struct rcrpc *query, *resp;
    char query_buf[query_len];
    query = (struct rcrpc*) query_buf;

    query->type = RCRPC_UNIQUE_LOOKUP_REQUEST;
    query->len  = query_len;
    query->unique_lookup_request.table = table;
    query->unique_lookup_request.index_id = index_id;
    query->unique_lookup_request.key_len = key_len;
    memcpy(query->unique_lookup_request.key, key, key_len);

    assert(!rc_net_send_rpc(&client->net, query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r) {
        return r;
    }

    *oid_present = (bool) resp->unique_lookup_response.oid_present;
    if (*oid_present) {
        *oid = resp->unique_lookup_response.oid;
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
    struct rcrpc *query, *resp;
    char *var;

    int query_buf_len;
    query_buf_len = RCRPC_MULTI_LOOKUP_REQUEST_LEN_WODATA;
    if (args->rpc.start_following_oid_present) {
        query_buf_len += sizeof(uint64_t);
    }
    query_buf_len += args->rpc.key_len;

    char query_buf[query_buf_len];
    query = (struct rcrpc*) query_buf;

    query->type = RCRPC_MULTI_LOOKUP_REQUEST;
    query->len = (uint32_t) query_buf_len;
    memcpy(&query->multi_lookup_request, &args->rpc, sizeof(args->rpc));
    var = query->multi_lookup_request.var;
    if (args->rpc.start_following_oid_present) {
        *((uint64_t*) var) = args->start_following_oid;
        var += sizeof(uint64_t);
    }
    memcpy(var, args->key, args->rpc.key_len);
    var += args->rpc.key_len;

    assert(!rc_net_send_rpc(&client->net, query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r) {
        return r;
    }

    *args->count = resp->multi_lookup_response.len;
    *args->more = (bool) resp->multi_lookup_response.more;
    memcpy(args->oids_buf, resp->multi_lookup_response.oids,
           *args->count * sizeof(uint64_t));

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
    struct rcrpc *query, *resp;
    char *var;

    int query_buf_len;
    query_buf_len = RCRPC_RANGE_QUERY_REQUEST_LEN_WODATA;
    if (args->rpc.start_following_oid_present) {
        query_buf_len += sizeof(uint64_t);
    }
    query_buf_len += sizeof(uint64_t) + args->key_start_len;
    query_buf_len += sizeof(uint64_t) + args->key_end_len;

    char query_buf[query_buf_len];
    query = (struct rcrpc*) query_buf;

    query->type = RCRPC_RANGE_QUERY_REQUEST;
    query->len = (uint32_t) query_buf_len;
    memcpy(&query->range_query_request, &args->rpc, sizeof(args->rpc));
    var = query->range_query_request.var;
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

    assert(!rc_net_send_rpc(&client->net, query));
    assert(!rc_net_recv_rpc(&client->net, &resp));
    int r = rc_handle_errors(resp);
    if (r) {
        return r;
    }

    //TODO(ongaro): I hope your buffer is large enough.
    *args->count = resp->range_query_response.len;
    *args->more = (bool) resp->range_query_response.more;
    var = resp->range_query_response.var;
    *args->oids_buf_len = (*args->count) * sizeof(uint64_t);
    memcpy(args->oids_buf, var, *args->oids_buf_len);
    var += *args->oids_buf_len;
    if (args->rpc.request_keys) {
        *args->keys_buf_len = resp->len -
                              RCRPC_RANGE_QUERY_RESPONSE_LEN_WODATA -
                              *args->oids_buf_len;
        memcpy(args->keys_buf, var, *args->keys_buf_len);
    }

    return 0;
}

struct rc_client *
rc_new() {
    return malloc(sizeof(struct rc_client *));
}

void
rc_free(struct rc_client *client)
{
    free(client);
}
