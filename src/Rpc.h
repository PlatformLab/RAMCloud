/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * This file defines all the information related to the "wire format" for
 * RAMCloud remote procedure calls.
 */

#ifndef RAMCLOUD_RPC_H
#define RAMCLOUD_RPC_H

#include "RejectRules.h"
#include "Status.h"

namespace RAMCloud {

/**
 * This enum defines the traces for the "service" field in RPC headers,
 * which selects the particular service that will implement a given RPC.
 */
enum RpcServiceType {
    MASTER_SERVICE          = 3,
    BACKUP_SERVICE          = 4,
    COORDINATOR_SERVICE     = 5,
    PING_SERVICE            = 6,
    MAX_SERVICE             = 6,   // Highest legitimate value.
};

/**
 * This enum defines the choices for the "opcode" field in RPC
 * headers, which selects a particular operation to perform.  Each
 * RAMCloud service implements a subset of these operations 
 */
enum RpcOpcode {
    PING                    = 7,
    PROXY_PING              = 8,
    CREATE_TABLE            = 9,
    OPEN_TABLE              = 10,
    DROP_TABLE              = 11,
    CREATE                  = 12,
    READ                    = 13,
    WRITE                   = 14,
    REMOVE                  = 15,
    ENLIST_SERVER           = 16,
    GET_SERVER_LIST         = 17,
    GET_TABLET_MAP          = 18,
    SET_TABLETS             = 19,
    RECOVER                 = 20,
    HINT_SERVER_DOWN        = 21,
    TABLETS_RECOVERED       = 22,
    SET_WILL                = 23,
    REREPLICATE_SEGMENTS    = 24,
    FILL_WITH_TEST_DATA     = 25,
    MULTI_READ              = 26,
    BACKUP_CLOSE            = 128,
    BACKUP_FREE             = 129,
    BACKUP_GETRECOVERYDATA  = 130,
    BACKUP_OPEN             = 131,
    BACKUP_STARTREADINGDATA = 132,
    BACKUP_WRITE            = 133,
    BACKUP_RECOVERYCOMPLETE = 134,
    BACKUP_QUIESCE          = 135,
    ILLEGAL_RPC_TYPE        = 136,  // 1 + the highest legitimate RpcOpcode
};

/**
 * Each RPC request starts with this structure.
 */
struct RpcRequestCommon {
    uint16_t opcode;              // Operation to be performed (one of the
                                  // values in the RpcOpcode enum above).
    uint16_t service;             // Service to invoke for this RPC (one of
                                  // the values in the RpcServiceType enum
                                  // above).
};

/**
 * Each RPC response starts with this structure.
 */
struct RpcResponseCommon {
    Status status;                // Indicates whether the operation
                                  // succeeded; if not, it explains why.
};


// For each RPC there are two structures below, one describing
// the header for the request and one describing the header for
// the response.  For details on what the individual fields mean,
// see the documentation for the corresponding methods in RamCloudClient.cc;
// for the most part the arguments to those methods are passed directly
// to the corresponding fields of the RPC header, and the fields of
// the RPC response are returned as the results of the method.
//
// Fields with names such as "pad1" are included to make padding
// explicit (removing these fields has no effect on the layout of
// the records).

struct CreateRpc {
    static const RpcOpcode opcode = CREATE;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tableId;
        uint32_t length;              // Length of the value in bytes. The
                                      // actual bytes follow immediately after
                                      // this header.
        uint8_t async;
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t id;
        uint64_t version;
    };
};

struct FillWithTestDataRpc {
    static const RpcOpcode opcode = FILL_WITH_TEST_DATA;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t numObjects;        // Number of objects to add to tables
                                    // in round-robin fashion.
        uint32_t objectSize;        // Size of each object to add.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct MultiReadRpc {
    static const RpcOpcode opcode = MULTI_READ;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t count;
        struct Part {
            uint32_t tableId;
            uint64_t id;
            Part(uint32_t tableId, uint64_t id) : tableId(tableId), id(id) {}
        };
    };
    struct Response {
        // RpcResponseCommon contains a status field. But it is not used in
        // multiRead since there is a separate status for each object returned.
        // Included here to fulfill requirements in common code.
        RpcResponseCommon common;
        uint32_t count;
        // In buffer: Status, SegmentEntry and Object go here
        // Object has variable number of bytes (depending on data size.)
        // In case of an error, only Status goes here
    };
};

struct PingRpc {
    static const RpcOpcode opcode = RpcOpcode::PING;
    static const RpcServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t nonce;             // The nonce may be used to identify
                                    // replies to previously transmitted
                                    // pings.
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t nonce;             // This should be identical to what was
                                    // sent in the request being answered.
    };
};

struct ProxyPingRpc {
    static const RpcOpcode opcode = PROXY_PING;
    static const RpcServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t timeoutNanoseconds;   // Number of nanoseconds to wait for a
                                       // reply before responding negatively to
                                       // this RPC.
        uint32_t serviceLocatorLength; // Number of bytes in the serviceLocator,
                                       // including terminating NULL character.
                                       // The bytes of the service locator
                                       // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t replyNanoseconds;     // Number of nanoseconds it took to get
                                       // the reply. If a timeout occurred, the
                                       // value is -1.
    };
};

struct ReadRpc {
    static const RpcOpcode opcode = READ;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tableId;
        uint64_t id;
        uint32_t pad1;
        RejectRules rejectRules;
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
        uint32_t pad1;
    };
};

struct RecoverRpc {
    static const RpcOpcode opcode = RECOVER;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;
        uint64_t partitionId;
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // after the bytes for the Tablets. See
                                   // ProtoBuf::ServerList.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct RereplicateSegmentsRpc {
    static const RpcOpcode opcode = REREPLICATE_SEGMENTS;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t backupId;        // The server id of a crashed backup.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct RemoveRpc {
    static const RpcOpcode opcode = REMOVE;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t id;
        uint32_t tableId;
        uint32_t pad1;
        RejectRules rejectRules;
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    };
};

struct SetTabletsRpc {
    static const RpcOpcode opcode = SET_TABLETS;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;

    };
};

struct WriteRpc {
    static const RpcOpcode opcode = WRITE;
    static const RpcServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t id;
        uint32_t tableId;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
        RejectRules rejectRules;
        uint8_t async;
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    };
};

// Coordinator RPCs follow, see Coordinator.cc

struct CreateTableRpc {
    static const RpcOpcode opcode = CREATE_TABLE;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct DropTableRpc {
    static const RpcOpcode opcode = DROP_TABLE;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct OpenTableRpc {
    static const RpcOpcode opcode = OPEN_TABLE;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t tableId;
    };
};

enum ServerType {
    MASTER = 0,
    BACKUP = 1,
};

struct EnlistServerRpc {
    static const RpcOpcode opcode = ENLIST_SERVER;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint8_t serverType;
        uint8_t pad[3];
        uint32_t readSpeed;            // MB/s read speed if a BACKUP
        uint32_t writeSpeed;           // MB/s write speed if a BACKUP
        uint32_t serviceLocatorLength; // Number of bytes in the serviceLocator,
                                       // including terminating NULL character.
                                       // The bytes of the service locator
                                       // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t serverId;
    };
};

struct GetServerListRpc {
    static const RpcOpcode opcode = GET_SERVER_LIST;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint8_t serverType;        // Type of servers to get: MASTER or BACKUP
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // immediately after this header. See
                                   // ProtoBuf::ServerList.
    };
};

struct GetTabletMapRpc {
    static const RpcOpcode opcode = GET_TABLET_MAP;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t tabletMapLength;  // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
};

struct HintServerDownRpc {
    static const RpcOpcode opcode = HINT_SERVER_DOWN;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t serviceLocatorLength; // Number of bytes in the serviceLocator,
                                       // including terminating NULL character.
                                       // The bytes of the service locator
                                       // follow immediately after this header.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct TabletsRecoveredRpc {
    static const RpcOpcode opcode = TABLETS_RECOVERED;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;         // Server Id from whom the request is coming.
        Status status;             // Indicates whether the recovery
                                   // succeeded; if not, it explains why.
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
        uint32_t willLength;       // Number of bytes in the new will.
                                   // The bytes follow immediately after
                                   // the tablet map.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct SetWillRpc {
    static const RpcOpcode opcode = SET_WILL;
    static const RpcServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;         // Server Id from whom the request is coming.
        uint32_t willLength;       // Number of bytes in the will.
                                   // The bytes of the will map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

// -- Backup RPCs ---

struct BackupFreeRpc {
    static const RpcOpcode opcode = BACKUP_FREE;
    static const RpcServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to discard from backup.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupGetRecoveryDataRpc {
    static const RpcOpcode opcode = BACKUP_GETRECOVERYDATA;
    static const RpcServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to get data from.
        uint64_t partitionId;   ///< Partition id of :ecovery segment to fetch.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

// Note: this RPC is supported by the coordinator service as well as backups.
struct BackupQuiesceRpc {
    static const RpcOpcode opcode = BACKUP_QUIESCE;
    static const RpcServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupRecoveryCompleteRpc {
    static const RpcOpcode opcode = BACKUP_RECOVERYCOMPLETE;
    static const RpcServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id which was recovered.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupStartReadingDataRpc {
    static const RpcOpcode opcode = BACKUP_STARTREADINGDATA;
    static const RpcServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint32_t partitionsLength; // Number of bytes in the partition map.
                                   // The bytes of the partition map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t segmentIdCount;    ///< Number of segmentIds in reply payload.
        uint32_t primarySegmentCount;   ///< Count of segmentIds which prefix
                                        ///< the reply payload are primary.
        uint32_t digestBytes;       ///< Number of bytes for optional LogDigest.
        uint64_t digestSegmentId;   ///< SegmentId the LogDigest came from.
        uint32_t digestSegmentLen;  ///< Byte length of the LogDigest Segment.
        // A series of segmentIdCount uint64_t segmentIds follows.
        // If logDigestBytes != 0, then a serialised LogDigest follows
        // immediately after the last segmentId.
    };
};

struct BackupWriteRpc {
    static const RpcOpcode opcode = BACKUP_WRITE;
    static const RpcServiceType service = BACKUP_SERVICE;
    enum Flags {
        NONE = 0,
        OPEN = 1,
        CLOSE = 2,
        OPENCLOSE = OPEN | CLOSE,
        PRIMARY = 4,
        OPENPRIMARY = OPEN | PRIMARY,
        OPENCLOSEPRIMARY = OPEN | CLOSE | PRIMARY,
    };
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;          ///< Server from whom the request is coming.
        uint64_t segmentId;         ///< Target segment to update.
        uint32_t offset;            ///< Offset into this segment to write at.
        uint32_t length;            ///< Number of bytes to write.
        uint8_t flags;              ///< If open or close request.
        // Opaque byte string follows with data to write.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

// --- Magic numbers ---

/**
 * Largest allowable RAMCloud object, in bytes.  It's not clear whether
 * we will always need a size limit, or what the limit should be. For now
 * this guarantees that an object will fit inside a single RPC.
 */
#define MAX_OBJECT_SIZE 0x100000

/**
 * Version number that indicates "object doesn't exist": this
 * version will never be used in an actual object.
 */
#define VERSION_NONEXISTENT 0U

} // namespace RAMCloud

#endif // RAMCLOUD_RPC_H

