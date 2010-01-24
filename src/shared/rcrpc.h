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

/**
 * The type of an RPC message.
 *
 * rcrpc_header.type should be set to one of these.
 */
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

/**
 * The type of an index entry.
 *
 * rc_index_entry.index_type should be set to one of these.
 *
 * \attention If you modify this, you should also update the equivalent in
 *      \c bindings/python/ramcloud.py .
 */
enum RCRPC_INDEX_TYPE {
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

/**
 * Returns whether the provided index type is within the valid range.
 */
static inline bool
is_valid_index_type(enum RCRPC_INDEX_TYPE type) {
    return type <= RCRPC_INDEX_TYPE_BYTES64;
}

/**
 * Returns whether the provided index type is of variable-length.
 *
 * \param [in] type a valid index type (see is_valid_index_type())
 */
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

#ifdef DOXYGEN
/** \cond FALSE */
/**
 * An internal hook used to work around technical limitations in the
 * documentation system.
 */
#define DOC_HOOK(rcrpc_lower) \
    static void \
    rcrpc_lower##_RPC_doc_hook(struct rcrpc_##rcrpc_lower##_request *in, \
                               struct rcrpc_##rcrpc_lower##_response *out)
/** \endcond */
#else
#define DOC_HOOK(rcrpc_lower)
#endif

/**
 * Verify network connectivity.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(ping);

struct rcrpc_ping_request {
    struct rcrpc_header header;
};

struct rcrpc_ping_response {
    struct rcrpc_header header;
};

#ifdef __cplusplus
/**
 * A reserved version number representing no particular version.
 */
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

/**
 * Read an object from a %RAMCloud.
 *
 * \li Let \c o be the object identified by \c in.table, \c in.key.
 * \li If \c o does not exist, an rcrpc_error_response is returned instead.
 * \li \c out.version is set to \c o's version.
 * \li If <tt>in.version == RCRPC_VERSION_ANY || in.version == o's version</tt>:
 *      <ul>
 *      <li> \c out.index_entries of size in bytes \c out.index_entries_len is
 *      set to \c o's index entries. It is an array of rc_index_entry.
 *      <li> \c out.buf of size \c out.buf_len is set to \c o's opaque blob.
 *      </ul>
 * \li Else:
 *      <ul>
 *      <li> \c out.index_entries_len is \c 0 and \c out.index_entries is
 *      empty.
 *      <li> \c out.buf_len is \c 0 and \c out.index_entries is empty.
 *      </ul>
 *
 * \note A response with an object that has no data and no index entries looks
 * identical to a response to a request with a stale version number. The caller
 * should compare \c in.version with \c out.version to determine whether the
 * response contains object data.
 *
 * \TODO Don't use rcrpc_error_response for an application error.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(read);

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

/**
 * Update or create an object at a given key.
 *
 * \li Let \c o be the object identified by \c in.table, \c in.key.
 * \li If \c o exists:
 *      <ul>
 *      <li> If <tt>in.version == #RCRPC_VERSION_ANY ||
 *                  in.version == o's version</tt>:
 *          <ul>
 *          <li> \c o's opaque blob is set to \c in.buf of size \c in.buf_len.
 *          <li> \c o's index entries are set to \c in.index_entries of size
 *              \c in.index_entries_len in bytes. It is an array of
 *              rc_index_entry.
 *          <li> \c o's version is increased.
 *          <li> \c o is sent to the backups and their ack is received.
 *          <li> \c out.version is set to \c o's new version.
 *          </ul>
 *      <li> Else:
 *          <ul>
 *          <li> \c out.version is set to \c o's existing version.
 *          </ul>
 *      </ul>
 * \li Else:
 *      <ul>
 *      <li> \c o is created.
 *      <li> \c o's opaque blob is set to \c in.buf of size \c in.buf_len.
 *      <li> \c o's index entries are set to \c in.index_entries of size
 *          \c in.index_entries_len in bytes. It is an array of rc_index_entry.
 *      <li> \c o's version is set to a value guaranteed to be greater than
 *          that of any previous object that resided at \c in.table, \c in.key.
 *      <li> \c o is sent to the backups and their ack is received.
 *      <li> \c out.version is set to \c o's new version.
 *      </ul>
 *
 * \TODO Should \c o be created if \c o did not exist and \c in.version is not
 *      #RCRPC_VERSION_ANY?
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(write);

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

/**
 * Create an object at an assigned key.
 *
 * \li Let \c o be a new object inside \c in.table.
 * \li \c o is assigned a key based on the table's key allocation strategy.
 * \li \c o's version is set to a value guaranteed to be greater than that of
 *      any previous object that resided at \c in.table with \c o's key.
 * \li \c o's opaque blob is set to \c in.buf of size \c in.buf_len.
 * \li \c o's index entries are set to \c in.index_entries of size
 *      \c in.index_entries_len in bytes. It is an array of rc_index_entry.
 * \li \c o is sent to the backups and their ack is received.
 * \li \c out.key is set to \c o's key.
 * \li \c out.version is set to \c o's version.
 *
 * \TODO Define the table's key allocation strategy.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(insert);

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

/**
 * Delete an object.
 *
 * \li Let \c o be the object identified by \c in.table, \c in.key.
 * \li If \c o does not exist: an rcrpc_error_response is sent instead.
 * \li \c out.version is set to \c o's existing version.
 * \li If <tt>in.version == RCRPC_VERSION_ANY || in.version == o's version</tt>:
 *      <ul>
 *      <li> \c o, including its index entries, is removed from the table.
 *      <li> \c o's deletion is sent to the backups and their ack is received.
 *      </ul>
 *
 * \TODO Don't use rcrpc_error_response for application errors.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(delete);

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

/**
 * Create a table.
 *
 * \li Let \c t be a new table identified by \c in.name.
 * \li If \c t exists: an rcrpc_error_response is sent instead.
 * \li If there system is out of space for tables: an rcrpc_error_response is
 *      sent instead.
 * \li A table identified by \c in.name is created.
 *
 * \TODO Don't use rcrpc_error_response for application errors.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(create_table);

struct rcrpc_create_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_create_table_response {
    struct rcrpc_header header;
};

/**
 * Open a table.
 *
 * \li Let \c t be the table identified by name.
 * \li If \c t does not exist: an rcrpc_error_response is sent instead.
 * \li \c out.handle is set to a handle to \c t.
 *
 * \TODO Don't use rcrpc_error_response for application errors.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(open_table);

struct rcrpc_open_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_open_table_response {
    struct rcrpc_header header;
    uint64_t handle;
};

/**
 * Delete a table.
 *
 * \li Let \c t be the table identified by \c name.
 * \li If \c t does not exist: an rcrpc_error_response is sent instead.
 * \li \c t is deleted.
 *
 * \TODO Don't use rcrpc_error_response for application errors.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(drop_table);

struct rcrpc_drop_table_request {
    struct rcrpc_header header;
    char name[64];
};

struct rcrpc_drop_table_response {
    struct rcrpc_header header;
};

/**
 * Create an index.
 *
 * \TODO Missing documentation.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(create_index);

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

/**
 * Delete an index.
 *
 * \TODO Missing documentation.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(drop_index);

struct rcrpc_drop_index_request {
    struct rcrpc_header header;
    uint64_t table;
    uint16_t id;
};

struct rcrpc_drop_index_response {
    struct rcrpc_header header;
};

/**
 * Range query an index.
 *
 * \TODO Missing documentation.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(range_query);

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

/**
 * Lookup a key in a unique index.
 *
 * \TODO Missing documentation.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(unique_lookup);

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

/**
 * Lookup a key in a multi index.
 *
 * \TODO Missing documentation.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(multi_lookup);

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

#undef DOC_HOOK

#endif

