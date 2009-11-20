#ifndef RAMCLOUD_SHARED_BACKUP_H
#define RAMCLOUD_SHARED_BACKUP_H

// #include <cinttypes> // this requires c++0x support because it's c99
// so we'll go ahead and use the C header
#include <inttypes.h>

namespace RAMCloud {

struct backup_rpc_hdr {
    uint32_t type;
    uint32_t len;
};

struct backup_rpc_heartbeat_req {
};

struct backup_rpc_heartbeat_resp {
    uint8_t ok;
};

enum rc_backup_rpc_len {
    BACKUP_RPC_HDR_LEN                = (sizeof(struct backup_rpc_hdr)),
    BACKUP_RPC_HEARTBEAT_REQ_LEN     = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_heartbeat_req)),
    BACKUP_RPC_HEARTBEAT_RESP_LEN     = (BACKUP_RPC_HDR_LEN + sizeof(struct backup_rpc_heartbeat_resp))
};

enum backup_rpc_type {
    BACKUP_RPC_HEARTBEAT_REQ,
    BACKUP_RPC_HEARTBEAT_RESP,
};

struct backup_rpc {
    union {
        struct backup_rpc_hdr hdr;
    };
    union {
        struct backup_rpc_heartbeat_req heartbeat_req;
        struct backup_rpc_heartbeat_resp heartbeat_resp;
    };
};

} // namespace RAMCloud

#endif

