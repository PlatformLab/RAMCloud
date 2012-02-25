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
 * Selects the particular service that will handle a given rpc.
 * A rpc may only be sent to one particular service; see ServiceMask for
 * situations dealing with sets of services on a particular Server.
 */
enum ServiceType {
    MASTER_SERVICE,
    BACKUP_SERVICE,
    COORDINATOR_SERVICE,
    PING_SERVICE,
    MEMBERSHIP_SERVICE,
    INVALID_SERVICE, // One higher than the max.
};

/**
 * Bits masked together to describe multiple services. These should never be
 * used other than for (de-)serialization to/from ServiceMasks which provide
 * a higher-level interface.
 */
typedef uint32_t SerializedServiceMask;
static_assert(INVALID_SERVICE < (sizeof(SerializedServiceMask) * 8),
              "SerializedServiceMask too small to represent all ServiceTypes.");

/**
 * This enum defines the choices for the "opcode" field in RPC
 * headers, which selects a particular operation to perform.  Each
 * RAMCloud service implements a subset of these operations.  If you
 * change this table you must also reflect the changes in the following
 * locations:
 * - The definition of rpc in scripts/rawmetrics.py.
 * - The method opcodeSymbol in Rpc.cc.
 * - RpcTest.cc's out-of-range test, if ILLEGAL_RPC_TYPE was changed.
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
    SET_MIN_OPEN_SEGMENT_ID = 25,
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
    SET_SERVER_LIST         = 37,
    UPDATE_SERVER_LIST      = 38,
    REQUEST_SERVER_LIST     = 39,
    GET_SERVER_ID           = 40,
    ILLEGAL_RPC_TYPE        = 41,  // 1 + the highest legitimate RpcOpcode
};

/**
 * Each RPC request starts with this structure.
 */
struct RpcRequestCommon {
    uint16_t opcode;              /// RpcOpcode of operation to be performed.
    uint16_t service;             /// ServiceType to invoke for this rpc.
} __attribute__((packed));

/**
 * Each RPC response starts with this structure.
 */
struct RpcResponseCommon {
    Status status;                // Indicates whether the operation
                                  // succeeded; if not, it explains why.
} __attribute__((packed));


// For each RPC there are two structures below, one describing
// the header for the request and one describing the header for
// the response.  For details on what the individual fields mean,
// see the documentation for the corresponding methods in RamCloudClient.cc;
// for the most part the arguments to those methods are passed directly
// to the corresponding fields of the RPC header, and the fields of
// the RPC response are returned as the results of the method.
//
// All structs are packed so that they have a standard byte representation.
// All fields are little endian.

// Master RPCs follow, see MasterService.cc

struct CreateRpc {
    static const RpcOpcode opcode = CREATE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tableId;
        uint32_t length;              // Length of the value in bytes. The
                                      // actual bytes follow immediately after
                                      // this header.
        uint8_t async;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t id;
        uint64_t version;
    } __attribute__((packed));
};

struct FillWithTestDataRpc {
    static const RpcOpcode opcode = FILL_WITH_TEST_DATA;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t numObjects;        // Number of objects to add to tables
                                    // in round-robin fashion.
        uint32_t objectSize;        // Size of each object to add.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct MultiReadRpc {
    static const RpcOpcode opcode = MULTI_READ;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t count;
        struct Part {
            uint32_t tableId;
            uint64_t id;
            Part(uint32_t tableId, uint64_t id) : tableId(tableId), id(id) {}
        };
    } __attribute__((packed));
    struct Response {
        // RpcResponseCommon contains a status field. But it is not used in
        // multiRead since there is a separate status for each object returned.
        // Included here to fulfill requirements in common code.
        RpcResponseCommon common;
        uint32_t count;
        // In buffer: Status, SegmentEntry and Object go here
        // Object has variable number of bytes (depending on data size.)
        // In case of an error, only Status goes here
    } __attribute__((packed));
};

struct ReadRpc {
    static const RpcOpcode opcode = READ;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tableId;
        uint64_t id;
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
    } __attribute__((packed));
};

struct RecoverRpc {
    static const RpcOpcode opcode = RECOVER;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;
        uint64_t partitionId;
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
        uint32_t numReplicas;      // Number of Replica entries in the replica
                                   // list. The bytes of the replica list
                                   // follow after the bytes for the Tablets.
                                   // See Replica below.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
    /**
     * Where to find a replica for a particular segment.
     */
    struct Replica {
        /**
         * The backup storing the replica.
         */
        uint64_t backupId;
        /**
         * The ID of the segment.
         */
        uint64_t segmentId;
        friend bool operator==(const Replica&, const Replica&);
        friend bool operator!=(const Replica&, const Replica&);
        friend std::ostream& operator<<(std::ostream& stream, const Replica&);
    } __attribute__((packed));
};

struct RemoveRpc {
    static const RpcOpcode opcode = REMOVE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t id;
        uint32_t tableId;
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    } __attribute__((packed));
};

struct SetTabletsRpc {
    static const RpcOpcode opcode = SET_TABLETS;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;

    } __attribute__((packed));
};

struct WriteRpc {
    static const RpcOpcode opcode = WRITE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t id;
        uint32_t tableId;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
        RejectRules rejectRules;
        uint8_t async;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    } __attribute__((packed));
};

// Coordinator RPCs follow, see CoordinatorService.cc

struct CreateTableRpc {
    static const RpcOpcode opcode = CREATE_TABLE;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct DropTableRpc {
    static const RpcOpcode opcode = DROP_TABLE;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct OpenTableRpc {
    static const RpcOpcode opcode = OPEN_TABLE;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t tableId;
    } __attribute__((packed));
};

struct EnlistServerRpc {
    static const RpcOpcode opcode = ENLIST_SERVER;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t replacesId; ///< Server id this server used to operate
                             ///< at; the coordinator must make
                             ///< sure this server is removed
                             ///< from the cluster before
                             ///< enlisting the calling server.
                             ///< If !isValid() then this step is
                             ///< skipped; the enlisting server
                             ///< is simply added.
        SerializedServiceMask serviceMask; ///< Which services are available
                                           ///< on the enlisting server.
        uint32_t readSpeed;            // MB/s read speed if a BACKUP
        uint32_t writeSpeed;           // MB/s write speed if a BACKUP
        uint32_t serviceLocatorLength; // Number of bytes in the serviceLocator,
                                       // including terminating NULL character.
                                       // The bytes of the service locator
                                       // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t serverId;             // Unique ServerId assigned to this
                                       // enlisting server process.
    } __attribute__((packed));
};

struct GetServerListRpc {
    static const RpcOpcode opcode = GET_SERVER_LIST;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        SerializedServiceMask serviceMask; ///< Only get servers with specified
                                           ///< services.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // immediately after this header. See
                                   // ProtoBuf::ServerList.
    } __attribute__((packed));
};

struct GetTabletMapRpc {
    static const RpcOpcode opcode = GET_TABLET_MAP;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t tabletMapLength;  // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    } __attribute__((packed));
};

struct HintServerDownRpc {
    static const RpcOpcode opcode = HINT_SERVER_DOWN;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t serverId;         // ServerId of the server suspected of being
                                   // dead. Poke it with a stick.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct TabletsRecoveredRpc {
    static const RpcOpcode opcode = TABLETS_RECOVERED;
    static const ServiceType service = COORDINATOR_SERVICE;
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
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct SetWillRpc {
    static const RpcOpcode opcode = SET_WILL;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;         // Server Id from whom the request is coming.
        uint32_t willLength;       // Number of bytes in the will.
                                   // The bytes of the will map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct RequestServerListRpc {
    static const RpcOpcode opcode = REQUEST_SERVER_LIST;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t serverId;         // ServerId the coordinator should send
                                   // the list to.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct SetMinOpenSegmentIdRpc {
    static const RpcOpcode opcode = SET_MIN_OPEN_SEGMENT_ID;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t serverId;         // ServerId the coordinator update the
                                   // minimum segment id for.
        uint64_t segmentId;        // Minimum segment id for replicas of open
                                   // segments during subsequent recoveries.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

// Backup RPCs follow, see BackupService.cc

struct BackupFreeRpc {
    static const RpcOpcode opcode = BACKUP_FREE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to discard from backup.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct BackupGetRecoveryDataRpc {
    static const RpcOpcode opcode = BACKUP_GETRECOVERYDATA;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to get data from.
        uint64_t partitionId;   ///< Partition id of :ecovery segment to fetch.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

// Note: this RPC is supported by the coordinator service as well as backups.
struct BackupQuiesceRpc {
    static const RpcOpcode opcode = BACKUP_QUIESCE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct BackupRecoveryCompleteRpc {
    static const RpcOpcode opcode = BACKUP_RECOVERYCOMPLETE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id which was recovered.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct BackupStartReadingDataRpc {
    static const RpcOpcode opcode = BACKUP_STARTREADINGDATA;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;         ///< Server Id from whom the request is
                                   ///< coming.
        uint32_t partitionsLength; ///< Number of bytes in the partition map.
                                   ///< The bytes of the partition map follow
                                   ///< immediately after this header. See
                                   ///< ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t segmentIdCount;       ///< Number of (segmentId, length) pairs
                                       ///< in the replica list following this
                                       ///< header.
        uint32_t primarySegmentCount;  ///< Count of segment replicas that are
                                       ///< primary. These appear at the start
                                       ///< of the replica list.
        uint32_t digestBytes;          ///< Number of bytes for optional
                                       ///< LogDigest.
        uint64_t digestSegmentId;      ///< SegmentId the LogDigest came from.
        uint32_t digestSegmentLen;     ///< Byte length of the LogDigest
                                       ///< segment replica.
        // An array of segmentIdCount replicas follows.
        // Each entry is a Replica (see below).
        //
        // If logDigestBytes != 0, then a serialised LogDigest follows
        // immediately after the replica list.
    } __attribute__((packed));
    /// Used in the Response to report which replicas the backup has.
    struct Replica {
        uint64_t segmentId;        ///< The segment ID.
        uint32_t segmentLength;    ///< The number of bytes written to the
                                   ///< segment if it is open, or ~0U for
                                   ///< closed segments.
    } __attribute__((packed));
};

struct BackupWriteRpc {
    static const RpcOpcode opcode = BACKUP_WRITE;
    static const ServiceType service = BACKUP_SERVICE;
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
        uint64_t masterId;        ///< Server from whom the request is coming.
        uint64_t segmentId;       ///< Target segment to update.
        uint32_t offset;          ///< Offset into this segment to write at.
        uint32_t length;          ///< Number of bytes to write.
        uint8_t flags;            ///< If open or close request.
        bool atomic;              ///< If true replica isn't valid until close.
        uint16_t pad;             ///< Makes request "square up" so that John's
                                  ///< weird string format distinguishes
                                  ///< beweeen the header and payload in tests.
        // Opaque byte string follows with data to write.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

// Ping RPCs follow, see PingService.cc

struct GetMetricsRpc {
    static const RpcOpcode opcode = RpcOpcode::GET_METRICS;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t messageLength;    // Number of bytes in a
                                   // ProtoBuf::MetricList message that
                                   // follows immediately after this
                                   // header.
        // Variable-length byte string containing ProtoBuf::MetricList
        // follows.
    } __attribute__((packed));
};

struct PingRpc {
    static const RpcOpcode opcode = RpcOpcode::PING;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t nonce;             // The nonce may be used to identify
                                    // replies to previously transmitted
                                    // pings.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t nonce;             // This should be identical to what was
                                    // sent in the request being answered.
        uint64_t serverListVersion; // Version of the server list this server
                                    // has. Used to determine if the issuer of
                                    // the ping request is out of date.
    } __attribute__((packed));
};

struct ProxyPingRpc {
    static const RpcOpcode opcode = PROXY_PING;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t timeoutNanoseconds;   // Number of nanoseconds to wait for a
                                       // reply before responding negatively to
                                       // this RPC.
        uint32_t serviceLocatorLength; // Number of bytes in the serviceLocator,
                                       // including terminating NULL character.
                                       // The bytes of the service locator
                                       // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t replyNanoseconds;     // Number of nanoseconds it took to get
                                       // the reply. If a timeout occurred, the
                                       // value is -1.
    } __attribute__((packed));
};

struct KillRpc {
    static const RpcOpcode opcode = KILL;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

// MembershipService RPCs

struct SetServerListRpc {
    static const RpcOpcode opcode = SET_SERVER_LIST;
    static const ServiceType service = MEMBERSHIP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // immediately after this header. See
                                   // ProtoBuf::ServerList.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct UpdateServerListRpc {
    static const RpcOpcode opcode = UPDATE_SERVER_LIST;
    static const ServiceType service = MEMBERSHIP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // immediately after this header. See
                                   // ProtoBuf::ServerList.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint8_t lostUpdates;       // If non-zero, the server was missing
                                   // one or more previous updates and
                                   // would like the entire list to be
                                   // sent again.
    } __attribute__((packed));
};

struct GetServerIdRpc {
    static const RpcOpcode opcode = GET_SERVER_ID;
    static const ServiceType service = MEMBERSHIP_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t serverId;            // ServerId of the server.
    } __attribute__((packed));
};

namespace Rpc {
    const char* serviceTypeSymbol(ServiceType type);
    const char* opcodeSymbol(uint32_t opcode);
    const char* opcodeSymbol(Buffer& buffer);
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

