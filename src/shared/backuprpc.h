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

#ifndef RAMCLOUD_SHARED_BACKUPRPC_H
#define RAMCLOUD_SHARED_BACKUPRPC_H

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#include <inttypes.h>
#include <string>

namespace RAMCloud {

struct BackupRPCException {
    explicit BackupRPCException(std::string msg) : message(msg) {}
    std::string message;
};

struct backup_rpc_hdr {
    uint32_t type;
    uint32_t len;
};

struct backup_rpc_heartbeat_req {
};

struct backup_rpc_heartbeat_resp {
};

struct backup_rpc_write_req {
    uint64_t seg_num;
    uint32_t off;
    uint32_t len;
    char data[0];
};

struct backup_rpc_write_resp {
    uint32_t len;
    char message[0];
};

struct backup_rpc_commit_req {
    uint64_t seg_num;
};

struct backup_rpc_commit_resp {
};

struct backup_rpc_free_req {
    uint64_t seg_num;
};

struct backup_rpc_free_resp {
};

struct backup_rpc_getsegmentlist_req {
};

struct backup_rpc_getsegmentlist_resp {
    uint64_t seg_list_count;
    uint64_t seg_list[0];                       /* Variable length */
};

struct backup_rpc_retrieve_req {
    uint64_t seg_num;
};

struct backup_rpc_retrieve_resp {
    uint64_t data_len;
    char data[0];                       /* Variable length */
};

struct backup_rpc_error_resp {
    char message[0];                    /* Variable length, null terminated */
};

enum rc_backup_rpc_len {
    BACKUP_RPC_HDR_LEN                = (sizeof(struct backup_rpc_hdr)),
    BACKUP_RPC_HEARTBEAT_REQ_LEN      = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_heartbeat_req)),
    BACKUP_RPC_HEARTBEAT_RESP_LEN     = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_heartbeat_resp)),
    BACKUP_RPC_WRITE_REQ_LEN_WODATA   = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_write_req)),
    BACKUP_RPC_WRITE_RESP_LEN         = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_write_resp)),
    BACKUP_RPC_COMMIT_REQ_LEN         = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_commit_req)),
    BACKUP_RPC_COMMIT_RESP_LEN        = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_commit_resp)),
    BACKUP_RPC_FREE_REQ_LEN           = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_free_req)),
    BACKUP_RPC_FREE_RESP_LEN          = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_free_resp)),
    BACKUP_RPC_GETSEGMENTLIST_REQ_LEN       = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_getsegmentlist_req)),
    BACKUP_RPC_GETSEGMENTLIST_RESP_LEN_WODATA = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_getsegmentlist_resp)),
    BACKUP_RPC_RETRIEVE_REQ_LEN       = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_retrieve_req)),
    BACKUP_RPC_RETRIEVE_RESP_LEN_WODATA = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_retrieve_resp)),
    BACKUP_RPC_ERROR_RESP_LEN_WODATA = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_error_resp)),
};

enum backup_rpc_type {
    BACKUP_RPC_HEARTBEAT_REQ,
    BACKUP_RPC_HEARTBEAT_RESP,
    BACKUP_RPC_WRITE_REQ,
    BACKUP_RPC_WRITE_RESP,
    BACKUP_RPC_COMMIT_REQ,
    BACKUP_RPC_COMMIT_RESP,
    BACKUP_RPC_FREE_REQ,
    BACKUP_RPC_FREE_RESP,
    BACKUP_RPC_GETSEGMENTLIST_REQ,
    BACKUP_RPC_GETSEGMENTLIST_RESP,
    BACKUP_RPC_RETRIEVE_REQ,
    BACKUP_RPC_RETRIEVE_RESP,
    BACKUP_RPC_ERROR_RESP,
};

struct backup_rpc {
    union {
        struct backup_rpc_hdr hdr;
    };
    union {
        struct backup_rpc_heartbeat_req heartbeat_req;
        struct backup_rpc_heartbeat_resp heartbeat_resp;
        struct backup_rpc_write_req write_req;
        struct backup_rpc_write_resp write_resp;
        struct backup_rpc_commit_req commit_req;
        struct backup_rpc_commit_resp commit_resp;
        struct backup_rpc_free_req free_req;
        struct backup_rpc_free_resp free_resp;
        struct backup_rpc_getsegmentlist_req getsegmentlist_req;
        struct backup_rpc_getsegmentlist_resp getsegmentlist_resp;
        struct backup_rpc_retrieve_req retrieve_req;
        struct backup_rpc_retrieve_resp retrieve_resp;
        struct backup_rpc_error_resp error_resp;
    };
};

} // namespace RAMCloud

#endif

