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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_RCRPC_H
#define RAMCLOUD_RCRPC_H

#include <Common.h>
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
    RCRPC_ERROR_RESPONSE,
};

/**
 * Represents the perf_counter field of #rcrpc_header on the wire.
 *
 * Requests treat this space as a few control fields, documented
 * below.  Responses treat this space as a single integer performance
 * counter value.
 *
 * See #rc_select_perf_counter() to use performance counters or
 * Metrics to add new performance counters.
 */
struct rcrpc_perf_counter {
    union {
        /// Return value for responses, the performance counter
        /// readings from the last request
        uint32_t value;
        // Setup fields for performance counters on requests
        struct {
            /// A Mark serialized into a 12-bit field
            uint32_t beginMark : 12;
            /// A Mark serialized into a 12-bit field
            uint32_t endMark : 12;
            /// A PerfCounterType serialized into an 8-bit field
            uint32_t counterType : 8;
        };
    };
} __attribute__ ((__packed__));

struct rcrpc_header {
    uint32_t type;
    uint32_t len;
    rcrpc_perf_counter perf_counter;
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
 * A reserved version number meaning the object does not exist.
 */
#define RCRPC_VERSION_NONE (static_cast<uint64_t>(0))
#else
#define RCRPC_VERSION_NONE ((uint64_t)(0ULL))
#endif

/**
 * Rules by which a read, write, or delete operation will be rejected.
 *
 * Used by read_RPC_doc_hook(), write_RPC_doc_hook(), delete_RPC_doc_hook().
 *
 * The operations will be rejected under \b any of the following conditions:
 * \li The object doesn't exist and #object_doesnt_exist is set.
 * \li The object exists and #object_exists is set.
 * \li The object exists, #version_eq_given is set, and the object's version is
 *      equal to #given_version.
 * \li The object exists, #version_gt_given is set, and the object's version is
 *      greater than #given_version.
 * \li The object exists, #version_eq_given or #version_gt_given is set, and
 *      the object's version is less than #given_version. This should be considered
 *      a critical application consistency error since applications should
 *      never fabricate version numbers.
 */
struct rcrpc_reject_rules {
    uint8_t object_doesnt_exist:1;
    uint8_t object_exists:1;
    uint8_t version_eq_given:1;
    uint8_t version_gt_given:1;
    uint64_t given_version;
};

/**
 * Read an object from a %RAMCloud.
 *
 * \li Let \c o be the object identified by \c in.table, \c in.key.
 * \li \c in.reject_rules must have rcrpc_reject_rules.object_doesnt_exist set
 *      (it is meaningless to read an object that does not exist).
 * \li \c out.version is set to \c o's version, or #RCRPC_VERSION_NONE if \c o
 *      does not exist.
 * \li If the \c in.reject_rules reject the operation:
 *      <ul>
 *      <li> \c out.buf_len is \c 0 and \c out.buf is empty.
 *      </ul>
 * \li Else:
 *      <ul>
 *      <li> \c out.buf of size \c out.buf_len is set to \c o's opaque blob.
 *      </ul>
 *
 * \warning The caller can not distinguish the error cases based on \c out.buf_len.
 * Use \c in.reject_rules and \c out.version.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(read);

struct rcrpc_read_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    struct rcrpc_reject_rules reject_rules;
};

struct rcrpc_read_response {
    struct rcrpc_header header;
    uint64_t version;
    uint64_t buf_len;
    char buf[0];                        /* Variable length (see buf_len) */
};

/**
 * Update or create an object at a given key.
 *
 * \li Let \c o be the object identified by \c in.table, \c in.key.
 * \li If the \c in.reject_rules reject the operation:
 *      <ul>
 *      <li> \c out.written is cleared.
 *      </ul>
 * \li Else:
 *      <ul>
 *      <li> If \c o exists:
 *          <ul>
 *          <li> \c o's version is set to the old object's version plus 1.
 *          </ul>
 *      <li> Else:
 *          <ul>
 *          <li> \c o is created.
 *          <li> \c o's version is set to a value guaranteed to be greater than
 *              that of any previous object that resided at \c in.table, \c
 *              in.key.
 *          </ul>
 *      <li> \c o's opaque blob is set to \c in.buf of size \c in.buf_len.
 *      <li> \c o is sent to the backups and their ack is received.
 *      <li> \c out.written is set.
 *      </ul>
 * \li \c out.version is set to \c o's (new) version, or #RCRPC_VERSION_NONE if
 *      \c o does not exist.
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(write);

struct rcrpc_write_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    struct rcrpc_reject_rules reject_rules;
    uint64_t buf_len;
    char buf[0];                        /* Variable length (see buf_len) */
};

struct rcrpc_write_response {
    struct rcrpc_header header;
    uint64_t version;
    uint8_t written:1;
};

/**
 * Create an object at an assigned key.
 *
 * \li Let \c o be a new object inside \c in.table.
 * \li \c o is assigned a key based on the table's key allocation strategy.
 * \li \c o's version is set to a value guaranteed to be greater than that of
 *      any previous object that resided at \c in.table with \c o's key.
 * \li \c o's opaque blob is set to \c in.buf of size \c in.buf_len.
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
    uint64_t buf_len;
    char buf[0];                        /* Variable length (see buf_len) */
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
 * \li out.deleted is set if the \c in.reject_rules do not reject the operation.
 * \li If the \c in.reject_rules reject the operation:
 *      <ul>
 *      <li> \c out.deleted is cleared.
 *      </ul>
 * \li Else:
 *      <ul>
 *      <li> \c out.deleted is set.
 *      <li> If \c o exists:
 *          <ul>
 *          <li> \c o is removed from the table.
 *          <li> \c o's deletion is sent to the backups and their ack is
 *               received.
 *          </ul>
 *      </ul>
 * \li \c out.version is set to \c o's existing version if it exists (the
 *      operation was rejected), or #RCRPC_VERSION_NONE if \c o does not exist (the
 *      operation succeeded or \c o did not previously exist).
 *
 * \limit This function declaration is only a hook for documentation. The
 * function does not exist and should not be called.
 */
DOC_HOOK(delete);

struct rcrpc_delete_request {
    struct rcrpc_header header;
    uint64_t table;
    uint64_t key;
    struct rcrpc_reject_rules reject_rules;
};

struct rcrpc_delete_response {
    struct rcrpc_header header;
    uint64_t version;
    uint8_t deleted:1;
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

struct rcrpc_error_response {
    struct rcrpc_header header;
    char message[0];                    /* Variable length */
};

//} // namespace RAMCloud

#undef DOC_HOOK

#endif

