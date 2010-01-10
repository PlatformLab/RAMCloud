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

#ifndef RAMCLOUD_SHARED_RCRPC_H
#define RAMCLOUD_SHARED_RCRPC_H

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#include <inttypes.h>
#include <stdbool.h>

#define RCRPC_HEADER_LEN                        sizeof(struct rcrpc_header)
#define RCRPC_PING_REQUEST_LEN                  sizeof(struct rcrpc_ping_request)
#define RCRPC_PING_RESPONSE_LEN                 sizeof(struct rcrpc_ping_response)
#define RCRPC_READ_REQUEST_LEN                  sizeof(struct rcrpc_read_request)
#define RCRPC_READ_RESPONSE_LEN_WODATA          sizeof(struct rcrpc_read_response)
#define RCRPC_WRITE_REQUEST_LEN_WODATA          sizeof(struct rcrpc_write_request)
#define RCRPC_WRITE_RESPONSE_LEN                sizeof(struct rcrpc_write_response)
#define RCRPC_INSERT_REQUEST_LEN_WODATA         sizeof(struct rcrpc_insert_request)
#define RCRPC_INSERT_RESPONSE_LEN               sizeof(struct rcrpc_insert_response)
#define RCRPC_DELETE_REQUEST_LEN                sizeof(struct rcrpc_delete_request)
#define RCRPC_DELETE_RESPONSE_LEN               sizeof(struct rcrpc_delete_response)
#define RCRPC_CREATE_TABLE_REQUEST_LEN          sizeof(struct rcrpc_create_table_request)
#define RCRPC_CREATE_TABLE_RESPONSE_LEN         sizeof(struct rcrpc_create_table_response)
#define RCRPC_OPEN_TABLE_REQUEST_LEN            sizeof(struct rcrpc_open_table_request)
#define RCRPC_OPEN_TABLE_RESPONSE_LEN           sizeof(struct rcrpc_open_table_response)
#define RCRPC_DROP_TABLE_REQUEST_LEN            sizeof(struct rcrpc_drop_table_request)
#define RCRPC_DROP_TABLE_RESPONSE_LEN           sizeof(struct rcrpc_drop_table_response)
#define RCRPC_CREATE_INDEX_REQUEST_LEN          sizeof(struct rcrpc_create_index_request)
#define RCRPC_CREATE_INDEX_RESPONSE_LEN         sizeof(struct rcrpc_create_index_response)
#define RCRPC_DROP_INDEX_REQUEST_LEN            sizeof(struct rcrpc_drop_index_request)
#define RCRPC_DROP_INDEX_RESPONSE_LEN           sizeof(struct rcrpc_drop_index_response)
#define RCRPC_RANGE_QUERY_REQUEST_LEN_WODATA    sizeof(struct rcrpc_range_query_request)
#define RCRPC_RANGE_QUERY_RESPONSE_LEN_WODATA   sizeof(struct rcrpc_range_query_response)
#define RCRPC_UNIQUE_LOOKUP_REQUEST_LEN_WODATA  sizeof(struct rcrpc_unique_lookup_request)
#define RCRPC_UNIQUE_LOOKUP_RESPONSE_LEN        sizeof(struct rcrpc_unique_lookup_response)
#define RCRPC_MULTI_LOOKUP_REQUEST_LEN_WODATA   sizeof(struct rcrpc_multi_lookup_request)
#define RCRPC_MULTI_LOOKUP_RESPONSE_LEN_WODATA  sizeof(struct rcrpc_multi_lookup_response)
#define RCRPC_ERROR_RESPONSE_LEN_WODATA         sizeof(struct rcrpc_error_response)

//namespace RAMCloud {

enum RCRPC_TYPE {
    RCRPC_PING_REQUEST,
    RCRPC_PING_RESPONSE,
    RCRPC_READ_REQUEST,
    RCRPC_READ_RESPONSE,
    RCRPC_WRITE_REQUEST,
    RCRPC_WRITE_RESPONSE,
    RCRPC_INSERT_REQUEST,
    RCRPC_INSERT_RESPONSE,
    RCRPC_DELETE_REQUEST,
    RCRPC_DELETE_RESPONSE,
    RCRPC_CREATE_TABLE_REQUEST,
    RCRPC_CREATE_TABLE_RESPONSE,
    RCRPC_OPEN_TABLE_REQUEST,
    RCRPC_OPEN_TABLE_RESPONSE,
    RCRPC_DROP_TABLE_REQUEST,
    RCRPC_DROP_TABLE_RESPONSE,
    RCRPC_CREATE_INDEX_REQUEST,
    RCRPC_CREATE_INDEX_RESPONSE,
    RCRPC_DROP_INDEX_REQUEST,
    RCRPC_DROP_INDEX_RESPONSE,
    RCRPC_RANGE_QUERY_REQUEST,
    RCRPC_RANGE_QUERY_RESPONSE,
    RCRPC_UNIQUE_LOOKUP_REQUEST,
    RCRPC_UNIQUE_LOOKUP_RESPONSE,
    RCRPC_MULTI_LOOKUP_REQUEST,
    RCRPC_MULTI_LOOKUP_RESPONSE,
    RCRPC_ERROR_RESPONSE,
};

enum RCRPC_INDEX_TYPE {
    // If you modify this, you should also update the equivalent in
    // bindings/python/ramcloud.py
    RCRPC_INDEX_TYPE_SINT8,
    RCRPC_INDEX_TYPE_UINT8,
    RCRPC_INDEX_TYPE_SINT16,
    RCRPC_INDEX_TYPE_UINT16,
    RCRPC_INDEX_TYPE_SINT32,
    RCRPC_INDEX_TYPE_UINT32,
    RCRPC_INDEX_TYPE_SINT64,
    RCRPC_INDEX_TYPE_UINT64,
    RCRPC_INDEX_TYPE_FLOAT32,
    RCRPC_INDEX_TYPE_FLOAT64,
    RCRPC_INDEX_TYPE_BYTES8,
    RCRPC_INDEX_TYPE_BYTES16,
    RCRPC_INDEX_TYPE_BYTES32,
    RCRPC_INDEX_TYPE_BYTES64,
};

static inline bool
is_valid_index_type(enum RCRPC_INDEX_TYPE type) {
    return type <= RCRPC_INDEX_TYPE_BYTES64;
}

static inline bool
is_varlen_index_type(enum RCRPC_INDEX_TYPE type) {
    return type >= RCRPC_INDEX_TYPE_BYTES8;
}

struct rcrpc_header {
    uint32_t type;
    uint32_t len;
};

struct rcrpc_any {
    struct rcrpc_header header;
    char opaque[0];
};

// ping RPC: no-op

struct rcrpc_ping_request {
    struct rcrpc_header header;
};

struct rcrpc_ping_response {
    struct rcrpc_header header;
};

#ifdef __cplusplus
#define RCRPC_VERSION_ANY (static_cast<uint64_t>(-1))
#else
#define RCRPC_VERSION_ANY ((uint64_t)(-1ULL))
#endif

struct rc_index_entry {
    // keep this identical to struct chunk_entry for now 
    uint64_t len;
    uint32_t index_id;
    uint32_t index_type;
    char data[0];                       // Variable length, but contiguous
};

// read RPC: Read an object.
//
// Let o be the object identified by in.table, in.key.
// If o does not exist: an rcrpc_error_response is sent instead.
// out.version is set to o's version.
// If in.version == RCRPC_VERSION_ANY || in.version == o's version:
//     out.index_entries of size in bytes out.index_entries_len is set to o's
//         index entries. It is an array of rc_index_entry.
//     out.buf of size out.buf_len is set to o's opaque blob.
// Else:
//     out.index_entries_len is 0 and out.index_entries is empty.
//     out.buf_len is 0 and out.index_entries is empty.
//
// Note: A response with an object that has no data and no index entries looks
// identical to a response to a request with a stale version number. The client
// should compare in.version with out.version to determine whether the response
// contains object data.
//
// TODO(ongaro): Using rcrpc_error_response for application error

struct rcrpc_read_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    uint64_t version;
};

struct rcrpc_read_response {
    struct rcrpc_header header;
    uint64_t version;
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

// write RPC: Update or create an object at a given key.
//
// Let o be the object identified by in.table, in.key.
// If o exists:
//     If in.version == RCRPC_VERSION_ANY || in.version == o's version:
//         o's opaque blob is set to in.buf of size in.buf_len.
//         o's index entries are set to in.index_entries of size
//             in.index_entries_len in bytes. It is an array of rc_index_entry.
//         o's version is increased.
//         o is sent to the backups and their ack is received.
//         out.version is set to o's new version.
//     Else:
//         out.version is set to o's existing version.
// Else:
//     o is created.
//     o's opaque blob is set to in.buf of size in.buf_len.
//     o's index entries are set to in.index_entries of size
//         in.index_entries_len in bytes. It is an array of rc_index_entry.
//     o's version is set to a value guaranteed to be greater than that of any
//         previous object that resided at in.table, in.key.
//     o is sent to the backups and their ack is received.
//     out.version is set to o's new version.
//
// TODO(ongaro): Should o be created if o did not exist and in.version is not
//               RCRPC_VERSION_ANY?

struct rcrpc_write_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    uint64_t version;
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

struct rcrpc_write_response {
    struct rcrpc_header header;
    uint64_t version;
};

// Insert RPC: Create an object at an assigned key.
//
// Let o be a new object inside in.table.
// o is assigned a key based on the table's key allocation strategy.
// o's version is set to a value guaranteed to be greater than that of any
//     previous object that resided at in.table with o's key.
// o's opaque blob is set to in.buf of size in.buf_len.
// o's index entries are set to in.index_entries of size
//     in.index_entries_len in bytes. It is an array of rc_index_entry.
// o is sent to the backups and their ack is received.
// out.key is set to o's key. 
// out.version is set to o's version.
//
// TODO(ongaro): What is the table's key allocation strategy?

struct rcrpc_insert_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

struct rcrpc_insert_response {
    struct rcrpc_header header;
    uint64_t key;
    uint64_t version;
};

// Delete RPC: Delete an object.
//
// Let o be the object identified by in.table, in.key.
// If o does not exist: an rcrpc_error_response is sent instead.
// o, including its index entries, is removed from the table.
// o's deletion is sent to the backups and their ack is received.
//
// TODO(ongaro): Using rcrpc_error_response for application error
// TODO(ongaro): Need conditional deletes

struct rcrpc_delete_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    uint64_t version;
};

struct rcrpc_delete_response {
    struct rcrpc_header header;
    uint64_t version;
};

// Create table RPC: Create a table.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_create_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_create_table_response {
    struct rcrpc_header header;
};

// Open table RPC: Open a table.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_open_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_open_table_response {
    struct rcrpc_header header;
    uint64_t handle;
};

// Open table RPC: Delete a table.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_drop_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_drop_table_response {
    struct rcrpc_header header;
};

// Create index RPC: Create an index.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_create_index_request {
    struct rcrpc_header header;
    uint64_t table;
    uint8_t type; /* from RCRPC_INDEX_TYPE */
    int unique:1;
    int range_queryable:1;
};

struct rcrpc_create_index_response {
    struct rcrpc_header header;
    uint16_t id;
};

// Delete index RPC: Delete an index.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_drop_index_request {
    struct rcrpc_header header;
    uint64_t table;
    uint16_t id;
};

struct rcrpc_drop_index_response {
    struct rcrpc_header header;
};

// Range query RPC: Range query an index.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_range_query_request {
    struct rcrpc_header header;
    uint64_t table;
    uint32_t limit;
    uint16_t index_id;
    int key_start_present:1;
    int key_end_present:1;
    int start_following_oid_present:1;
    int key_start_inclusive:1;
    int key_end_inclusive:1;
    int request_keys:1;

    // var[] is the concatenation of the following:
    // uint64_t start_following_oid if start_following_oid_present
    // uint64_t key_start_len if key_start_present
    // <index_type> key_start if key_start_present
    // uint64_t key_end_len if key_end_present
    // <index_type> key_end if key_end_present
    char var[0];                        /* Variable length */
};

struct rcrpc_range_query_response {
    struct rcrpc_header header;
    uint32_t len;
    int more:1;

    // var is the concatenation of the following:
    // uint64_t oids[len]
    // <index_type> keys[len] if request_keys
    char var[0];                        /* Variable length */
};

// Unique lookup RPC: Lookup a key in a unique index.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_unique_lookup_request {
    struct rcrpc_header header;
    uint64_t table;
    uint16_t index_id;
    uint64_t key_len;
    char key[0];                        /* Variable length */
};

struct rcrpc_unique_lookup_response {
    struct rcrpc_header header;
    int oid_present:1;
    uint64_t oid;
};

// Multi lookup RPC: Lookup a key in a multi index.
//
// TODO(ongaro): Missing documentation.

struct rcrpc_multi_lookup_request {
    struct rcrpc_header header;
    uint64_t table;
    uint32_t limit;
    uint16_t index_id;
    int start_following_oid_present:1;
    uint64_t key_len;

    // var[] is the concatenation of the following:
    // uint64_t start_following_oid if start_following_oid_present
    // <index_type> key
    char var[0];                        /* Variable length */
};

struct rcrpc_multi_lookup_response {
    struct rcrpc_header header;
    uint32_t len; /* number of oids */
    int more:1;
    uint64_t oids[0];                        /* Variable length */
};

struct rcrpc_error_response {
    struct rcrpc_header header;
    char message[0];                    /* Variable length */
};

//} // namespace RAMCloud

#endif

