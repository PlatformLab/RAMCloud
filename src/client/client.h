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

#ifndef RAMCLOUD_CLIENT_CLIENT_H
#define RAMCLOUD_CLIENT_CLIENT_H

#include <inttypes.h>
#include <stdbool.h>
#include <shared/net.h>

struct rc_client {
    struct rc_net net;
};

struct rc_index_entry {
    // keep this identical to struct chunk_entry for now 
    uint64_t len;
    uint32_t index_id;
    uint32_t index_type;
    char data[0];                       // Variable length, but contiguous
};

#ifdef __cplusplus
extern "C" {
#endif
const uint64_t rcrpc_version_any;

int rc_connect(struct rc_client *client);
void rc_disconnect(struct rc_client *client);
int rc_ping(struct rc_client *client);
int rc_write(struct rc_client *client, uint64_t table, uint64_t key,
             uint64_t want_version, uint64_t *got_version,
             const char *buf, uint64_t len,
             const char *index_entries_buf, uint64_t index_entries_len);
int rc_insert(struct rc_client *client, uint64_t table, const char *buf,
              uint64_t len, uint64_t *key,
              const char *index_entries_buf, uint64_t index_entries_len);
int rc_delete(struct rc_client *client, uint64_t table, uint64_t key,
              uint64_t want_version, uint64_t *got_version);
int rc_read(struct rc_client *client, uint64_t table, uint64_t key,
            uint64_t want_version, uint64_t *got_version,
            char *buf, uint64_t *len,
            char *index_entries_buf, uint64_t *index_entries_len);
int rc_create_table(struct rc_client *client, const char *name);
int rc_open_table(struct rc_client *client, const char *name,
                  uint64_t *table_id);
int rc_drop_table(struct rc_client *client, const char *name);
int rc_create_index(struct rc_client *client, uint64_t table_id,
                    enum RCRPC_INDEX_TYPE type, bool unique,
                    bool range_queryable, uint16_t *index_id);
int rc_drop_index(struct rc_client *client, uint64_t table_id,
                  uint16_t index_id);
int rc_unique_lookup(struct rc_client *client, uint64_t table,
                     uint16_t index_id, const char *key, uint64_t key_len,
                     bool *oid_present, uint64_t *oid);

struct rc_multi_lookup_args {
    // please consider this struct opaque
    // stack allocation is OK if you memset it to 0

    struct rcrpc_multi_lookup_request rpc;

    // IN
    const char *key;
    uint64_t start_following_oid;

    // *IN, *OUT
    uint32_t *count;

    // *OUT
    bool *more;
    uint64_t *oids_buf;
};

struct rc_multi_lookup_args *rc_multi_lookup_args_new();
void rc_multi_lookup_args_free(struct rc_multi_lookup_args *args);
void rc_multi_lookup_set_index(struct rc_multi_lookup_args *args,
                               uint64_t table, uint16_t index_id);
void rc_multi_lookup_set_key(struct rc_multi_lookup_args *args,
                              const char *key, uint64_t len);
void rc_multi_lookup_set_start_following_oid(struct rc_multi_lookup_args *args,
                                             uint64_t oid);
void rc_multi_lookup_set_result_buf(struct rc_multi_lookup_args *args,
                                    uint32_t *count, uint64_t *oids_buf,
                                    bool *more);
int rc_multi_lookup(struct rc_client *client,
                    const struct rc_multi_lookup_args *args);

struct rc_range_query_args {
    // please consider this struct opaque
    // stack allocation is OK if you memset it to 0

    struct rcrpc_range_query_request rpc;

    // IN
    const char *key_start;
    uint64_t key_start_len;
    const char *key_end;
    uint64_t key_end_len;
    uint64_t start_following_oid;

    // *IN, *OUT
    uint32_t *count;
    uint64_t *oids_buf_len;
    uint64_t *keys_buf_len;

    // *OUT
    bool *more;
    uint64_t *oids_buf;
    char *keys_buf;

};

struct rc_range_query_args *rc_range_query_args_new();
void rc_range_query_args_free(struct rc_range_query_args *args);
void rc_range_query_set_index(struct rc_range_query_args *args, uint64_t table,
                              uint16_t index_id);
void rc_range_query_set_key_start(struct rc_range_query_args *args,
                                  const char *key, uint64_t len,
                                  bool inclusive);
void rc_range_query_set_key_end(struct rc_range_query_args *args,
                                const char *key, uint64_t len,
                                bool inclusive);
void rc_range_query_set_start_following_oid(struct rc_range_query_args *args,
                                            uint64_t oid);
void rc_range_query_set_result_bufs(struct rc_range_query_args *args,
                                    uint32_t *count, uint64_t *oids_buf,
                                    uint64_t *oids_buf_len, char *keys_buf,
                                    uint64_t *keys_buf_len, bool *more);
int rc_range_query(struct rc_client *client,
                   const struct rc_range_query_args *args);


/* These aren't strictly necessary, but they make life easier for
 * foreign languages because they don't have to know how to allocate a
 * structure of the correct size */
struct rc_client *rc_new();
void rc_free(struct rc_client *client);
const char* rc_last_error();
#ifdef __cplusplus
}
#endif

#endif
