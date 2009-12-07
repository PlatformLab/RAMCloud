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

#define RCRPC_HEADER_LEN                      ((size_t) &(((struct rcrpc *) 0)->ping_request))
#define RCRPC_PING_REQUEST_LEN                (RCRPC_HEADER_LEN + sizeof(struct rcrpc_ping_request))
#define RCRPC_PING_RESPONSE_LEN               (RCRPC_HEADER_LEN + sizeof(struct rcrpc_ping_response))
#define RCRPC_READ_REQUEST_LEN                (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read_request))
#define RCRPC_READ_RESPONSE_LEN_WODATA        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_read_response))
#define RCRPC_WRITE_REQUEST_LEN_WODATA        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write_request))
#define RCRPC_WRITE_RESPONSE_LEN              (RCRPC_HEADER_LEN + sizeof(struct rcrpc_write_response))
#define RCRPC_INSERT_REQUEST_LEN_WODATA       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_insert_request))
#define RCRPC_INSERT_RESPONSE_LEN             (RCRPC_HEADER_LEN + sizeof(struct rcrpc_insert_response))
#define RCRPC_DELETE_REQUEST_LEN              (RCRPC_HEADER_LEN + sizeof(struct rcrpc_delete_request))
#define RCRPC_DELETE_RESPONSE_LEN             (RCRPC_HEADER_LEN + sizeof(struct rcrpc_delete_response))
#define RCRPC_CREATE_TABLE_REQUEST_LEN        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_table_request))
#define RCRPC_CREATE_TABLE_RESPONSE_LEN       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_table_response))
#define RCRPC_OPEN_TABLE_REQUEST_LEN          (RCRPC_HEADER_LEN + sizeof(struct rcrpc_open_table_request))
#define RCRPC_OPEN_TABLE_RESPONSE_LEN         (RCRPC_HEADER_LEN + sizeof(struct rcrpc_open_table_response))
#define RCRPC_DROP_TABLE_REQUEST_LEN          (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_table_request))
#define RCRPC_DROP_TABLE_RESPONSE_LEN         (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_table_response))
#define RCRPC_CREATE_INDEX_REQUEST_LEN        (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_index_request))
#define RCRPC_CREATE_INDEX_RESPONSE_LEN       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_create_index_response))
#define RCRPC_DROP_INDEX_REQUEST_LEN          (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_index_request))
#define RCRPC_DROP_INDEX_RESPONSE_LEN         (RCRPC_HEADER_LEN + sizeof(struct rcrpc_drop_index_response))
#define RCRPC_RANGE_QUERY_REQUEST_LEN_WODATA  (RCRPC_HEADER_LEN + sizeof(struct rcrpc_range_query_request))
#define RCRPC_RANGE_QUERY_RESPONSE_LEN_WODATA (RCRPC_HEADER_LEN + sizeof(struct rcrpc_range_query_response))
#define RCRPC_ERROR_RESPONSE_LEN_WODATA       (RCRPC_HEADER_LEN + sizeof(struct rcrpc_error_response))

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
    RCRPC_INDEX_TYPE_STRING,
};


struct rcrpc_ping_request {
};

struct rcrpc_ping_response {
};


struct rcrpc_read_request {
    uint64_t table;
    uint64_t key;
};

struct rcrpc_read_response {
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

struct rcrpc_write_request {
    uint64_t table;
    uint64_t key;
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

struct rcrpc_write_response {
};

struct rcrpc_insert_request {
    uint64_t table;
    uint64_t index_entries_len;
    uint64_t buf_len;

    // var[] is the concatenation of the following:
    // char index_entries[index_entries_len]
    // char buf[buf_len]
    char var[0];                        /* Variable length */
};

struct rcrpc_insert_response {
    uint64_t key;
};

struct rcrpc_delete_request {
    uint64_t table;
    uint64_t key;
};

struct rcrpc_delete_response {
};


struct rcrpc_create_table_request {
    char name[64];
};

struct rcrpc_create_table_response {
};


struct rcrpc_open_table_request {
    char name[64];
};

struct rcrpc_open_table_response {
    uint64_t handle;
};


struct rcrpc_drop_table_request {
    char name[64];
};

struct rcrpc_drop_table_response {
};

struct rcrpc_create_index_request {
    uint64_t table;
    uint8_t type; /* from RCRPC_INDEX_TYPE */
    int unique:1;
    int range_queryable:1;
};

struct rcrpc_create_index_response {
    uint16_t id;
};

struct rcrpc_drop_index_request {
    uint64_t table;
    uint16_t id;
};

struct rcrpc_drop_index_response {
};

struct rcrpc_range_query_request {
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
    // <index_type> key_start if key_start_present
    // <index_type> key_end if key_end_present
    char var[0];                        /* Variable length */
};

struct rcrpc_range_query_response {
    uint32_t len;
    int more:1;

    // var is the concatenation of the following:
    // uint64_t oids[len]
    // <index_type> keys[len] if request_keys
    char var[0];                        /* Variable length */
};

struct rcrpc_error_response {
    char message[0];                    /* Variable length */
};

struct rcrpc {
    uint32_t type;
    uint32_t len;
    union {
        struct rcrpc_ping_request ping_request;
        struct rcrpc_ping_response ping_response;

        struct rcrpc_read_request read_request;
        struct rcrpc_read_response read_response;

        struct rcrpc_write_request write_request;
        struct rcrpc_write_response write_response;

        struct rcrpc_insert_request insert_request;
        struct rcrpc_insert_response insert_response;

        struct rcrpc_delete_request delete_request;
        struct rcrpc_delete_response delete_response;

        struct rcrpc_create_table_request create_table_request;
        struct rcrpc_create_table_response create_table_response;

        struct rcrpc_open_table_request open_table_request;
        struct rcrpc_open_table_response open_table_response;

        struct rcrpc_drop_table_request drop_table_request;
        struct rcrpc_drop_table_response drop_table_response;

        struct rcrpc_create_index_request create_index_request;
        struct rcrpc_create_index_response create_index_response;

        struct rcrpc_drop_index_request drop_index_request;
        struct rcrpc_drop_index_response drop_index_response;

        struct rcrpc_range_query_request range_query_request;
        struct rcrpc_range_query_response range_query_response;

        struct rcrpc_error_response error_response;
    };
};

//} // namespace RAMCloud

#endif

