/* Copyright (c) 2010-2011 Stanford University
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

#include "Common.h"
#include "RejectRules.h"
#include "Status.h"

namespace RAMCloud {

/**
 * This enum defines the values for the "service" field in RPC headers,
 * which selects the particular service that will implement a given RPC.
 * Although an RPC may only be sent to one particular service, these bits
 * are sometimes masked together to describe multiple services. For example,
 * they are used when enlisting a server process (which supports one or more
 * distinct services) with the coordinator.
 */
typedef uint32_t ServiceTypeMask;
enum { MASTER_SERVICE       = 0x01 };
enum { BACKUP_SERVICE       = 0x02 };
enum { COORDINATOR_SERVICE  = 0x04 };
enum { PING_SERVICE         = 0x08 };
enum { MAX_SERVICE          = 0x08 };            // Highest legitimate bit.

/**
 * This enum defines the choices for the "opcode" field in RPC
 * headers, which selects a particular operation to perform.  Each
 * RAMCloud service implements a subset of these operations.  If you
 * change this table you must also reflect the changes in the following
 * locations:
 * - The definition of rpc in scripts/rawmetrics.py.
 * - The method opcodeSymbol in Rpc.cc.
 */
enum RpcOpcode {
    PING                    = 7,
    PROXY_PING              = 8,
    KILL                    = 9,
    CREATE_TABLE            = 10,
    OPEN_TABLE              = 11,
    DROP_TABLE              = 12,
    CREATE                  = 13,
    READ                    = 14,
    WRITE                   = 15,
    REMOVE                  = 16,
    ENLIST_SERVER           = 17,
    GET_SERVER_LIST         = 18,
    GET_TABLET_MAP          = 19,
    SET_TABLETS             = 20,
    RECOVER                 = 21,
    HINT_SERVER_DOWN        = 22,
    TABLETS_RECOVERED       = 23,
    SET_WILL                = 24,
    REREPLICATE_SEGMENTS    = 25,
    FILL_WITH_TEST_DATA     = 26,
    MULTI_READ              = 27,
    GET_METRICS             = 28,
    BACKUP_CLOSE            = 29,
    BACKUP_FREE             = 30,
    BACKUP_GETRECOVERYDATA  = 31,
    BACKUP_OPEN             = 32,
    BACKUP_STARTREADINGDATA = 33,
    BACKUP_WRITE            = 34,
    BACKUP_RECOVERYCOMPLETE = 35,
    BACKUP_QUIESCE          = 36,
    ILLEGAL_RPC_TYPE        = 37,  // 1 + the highest legitimate RpcOpcode
};

/**
 * Each RPC request starts with this structure.
 */
struct RpcRequestCommon {
    uint16_t opcode;              // Operation to be performed (one of the
                                  // values in the RpcOpcode enum above).
    uint16_t service;             // Service to invoke for this RPC (one of
                                  // the values in the ServiceTypeMask enum
                                  // above - i.e. only one bit should be set).
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

// Master RPCs follow, see MasterService.cc

struct CreateRpc {
    static const RpcOpcode opcode = CREATE;
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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

struct ReadRpc {
    static const RpcOpcode opcode = READ;
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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
    static const ServiceTypeMask service = MASTER_SERVICE;
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

// Coordinator RPCs follow, see CoordinatorService.cc

struct CreateTableRpc {
    static const RpcOpcode opcode = CREATE_TABLE;
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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

struct EnlistServerRpc {
    static const RpcOpcode opcode = ENLIST_SERVER;
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint8_t serviceMask;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint8_t serviceMask;       // Only get servers that support specified
                                   // services.
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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
    static const ServiceTypeMask service = COORDINATOR_SERVICE;
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

// Backup RPCs follow, see BackupService.cc

struct BackupFreeRpc {
    static const RpcOpcode opcode = BACKUP_FREE;
    static const ServiceTypeMask service = BACKUP_SERVICE;
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
    static const ServiceTypeMask service = BACKUP_SERVICE;
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
    static const ServiceTypeMask service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupRecoveryCompleteRpc {
    static const RpcOpcode opcode = BACKUP_RECOVERYCOMPLETE;
    static const ServiceTypeMask service = BACKUP_SERVICE;
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
    static const ServiceTypeMask service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;         ///< Server Id from whom the request is
                                   ///< coming.
        uint32_t partitionsLength; ///< Number of bytes in the partition map.
                                   ///< The bytes of the partition map follow
                                   ///< immediately after this header. See
                                   ///< ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t segmentIdCount;       ///< Number of segmentIds in reply
                                       ///< payload.
        uint32_t primarySegmentCount;  ///< Count of segmentIds which prefix
                                       ///< the reply payload are primary.
        uint32_t digestBytes;          ///< Number of bytes for optional
                                       ///< LogDigest.
        uint64_t digestSegmentId;      ///< SegmentId the LogDigest came from.
        uint32_t digestSegmentLen;     ///< Byte length of the LogDigest
                                       ///< Segment.
        // A series of segmentIdCount uint64_t segmentIds follows.
        // If logDigestBytes != 0, then a serialised LogDigest follows
        // immediately after the last segmentId.
    };
};

struct BackupWriteRpc {
    static const RpcOpcode opcode = BACKUP_WRITE;
    static const ServiceTypeMask service = BACKUP_SERVICE;
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

// Ping RPCs follow, see PingService.cc

struct GetMetricsRpc {
    static const RpcOpcode opcode = RpcOpcode::GET_METRICS;
    static const ServiceTypeMask service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t messageLength;    // Number of bytes in a
                                   // ProtoBuf::MetricList message that
                                   // follows immediately after this
                                   // header.
        // Variable-length byte string containing ProtoBuf::MetricList
        // follows.
    };
};

struct PingRpc {
    static const RpcOpcode opcode = RpcOpcode::PING;
    static const ServiceTypeMask service = PING_SERVICE;
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
    static const ServiceTypeMask service = PING_SERVICE;
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

struct KillRpc {
    static const RpcOpcode opcode = KILL;
    static const ServiceTypeMask service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
    };
};

namespace Rpc {
    extern const char* opcodeSymbol(uint32_t opcode);
    extern const char* opcodeSymbol(Buffer& buffer);
}

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

