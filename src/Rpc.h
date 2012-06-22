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
    GET_TABLE_ID            = 11,
    DROP_TABLE              = 12,
    READ                    = 13,
    WRITE                   = 14,
    REMOVE                  = 15,
    ENLIST_SERVER           = 16,
    GET_SERVER_LIST         = 17,
    GET_TABLET_MAP          = 18,
    RECOVER                 = 19,
    HINT_SERVER_DOWN        = 20,
    RECOVERY_MASTER_FINISHED = 21,
    SET_WILL                = 22,
    SET_MIN_OPEN_SEGMENT_ID = 23,
    FILL_WITH_TEST_DATA     = 24,
    MULTI_READ              = 25,
    GET_METRICS             = 26,
    BACKUP_CLOSE            = 27,
    BACKUP_FREE             = 28,
    BACKUP_GETRECOVERYDATA  = 29,
    BACKUP_OPEN             = 30,
    BACKUP_STARTREADINGDATA = 31,
    BACKUP_WRITE            = 32,
    BACKUP_RECOVERYCOMPLETE = 33,
    BACKUP_QUIESCE          = 34,
    SET_SERVER_LIST         = 35,
    UPDATE_SERVER_LIST      = 36,
    SEND_SERVER_LIST        = 37,
    GET_SERVER_ID           = 38,
    DROP_TABLET_OWNERSHIP   = 39,
    TAKE_TABLET_OWNERSHIP   = 40,
    BACKUP_ASSIGN_GROUP     = 41,
    GET_HEAD_OF_LOG         = 42,
    INCREMENT               = 43,
    PREP_FOR_MIGRATION      = 44,
    RECEIVE_MIGRATION_DATA  = 45,
    REASSIGN_TABLET_OWNERSHIP = 46,
    MIGRATE_TABLET          = 47,
    IS_REPLICA_NEEDED       = 48,
    SPLIT_TABLET            = 49,
    GET_SERVER_STATISTICS   = 50,
    SET_RUNTIME_OPTION      = 51,
    ILLEGAL_RPC_TYPE        = 52,  // 1 + the highest legitimate RpcOpcode
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

struct DropTabletOwnershipRpc {
    static const RpcOpcode opcode = DROP_TABLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
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

struct IncrementRpc {
    static const RpcOpcode opcode = INCREMENT;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual bytes of the key follow
                                      // immediately after this header.
        int64_t incrementValue;       // Value that the object will be
                                      // incremented by.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
        int64_t newValue;                // The new value of the object.
    } __attribute__((packed));
};

/**
 * Used by backups to determine if a particular replica is still needed
 * by a master.  This is only used in the case the backup has crashed, and
 * has since restarted.
 */
struct IsReplicaNeededRpc {
    static const RpcOpcode opcode = IS_REPLICA_NEEDED;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t backupServerId;
        uint64_t segmentId;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        bool needed;                // False if the master's segment is
                                    // fully replicated, true otherwise.
    } __attribute__((packed));
};

struct GetHeadOfLogRpc {
    static const RpcOpcode opcode = GET_HEAD_OF_LOG;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t headSegmentId;     // ID of head segment in the log.
        uint32_t headSegmentOffset; // Byte offset of head within the segment.
    } __attribute__((packed));
};

struct MigrateTabletRpc {
    static const RpcOpcode opcode = MIGRATE_TABLET;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;           // TabletId of the tablet to migrate.
        uint64_t firstKey;          // First key of the tablet to migrate.
        uint64_t lastKey;           // Last key of the tablet to migrate.
        uint64_t newOwnerMasterId;  // ServerId of the master to migrate to.
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
            uint64_t tableId;
            uint16_t keyLength;
            // In buffer: The actual key for this part
            // follows immediately after this.
            Part(uint64_t tableId, uint16_t keyLength)
                : tableId(tableId), keyLength(keyLength) {}
        } __attribute__((packed));
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

struct PrepForMigrationRpc {
    static const RpcOpcode opcode = PREP_FOR_MIGRATION;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;           // TableId of the tablet we'll move.
        uint64_t firstKey;          // First key in the tablet range.
        uint64_t lastKey;           // Last key in the tablet range.
        uint64_t expectedObjects;   // Expected number of objects to migrate.
        uint64_t expectedBytes;     // Expected total object bytes to migrate.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct ReadRpc {
    static const RpcOpcode opcode = READ;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual key follows
                                      // immediately after this header.
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

struct ReceiveMigrationDataRpc {
    static const RpcOpcode opcode = RECEIVE_MIGRATION_DATA;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;           // Id of the table this data belongs to.
        uint64_t firstKey;          // 1st key of the tablet range for the data.
        uint32_t segmentBytes;      // Length of the Segment containing migrated
                                    // data immediately following this header.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct RecoverRpc {
    static const RpcOpcode opcode = RECOVER;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t recoveryId;
        uint64_t crashedServerId;
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
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual key follows
                                      // immediately after this header.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    } __attribute__((packed));
};

struct TakeTabletOwnershipRpc {
    static const RpcOpcode opcode = TAKE_TABLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
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
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual bytes of the key follow
                                      // immediately after this header.
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after the key.
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
        uint32_t serverSpan;          // The number of servers across which
                                      // this table will be divided.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint64_t tableId;             // The id of the created table.
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

struct SplitTabletRpc {
    static const RpcOpcode opcode = SPLIT_TABLET;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
        uint64_t startKeyHash;        // Identify the to be split tablet by
                                      // providing its current start key hash.
        uint64_t endKeyHash;          // Identify the to be split tablet by
                                      // providing its current end key hash.
        uint64_t splitKeyHash;        // Indicate where to split the tablet.
                                      // This will be the first key of the
                                      // second tablet after the split.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};


struct SplitMasterTabletRpc {
    static const RpcOpcode opcode = SPLIT_TABLET;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;             // Id of the table that contains the to
                                      // be split tablet.
        uint64_t startKeyHash;        // Identify the to be split tablet by
                                      // providing its current start key hash.
        uint64_t endKeyHash;          // Identify the to be split tablet by
                                      // providing its current end key hash.
        uint64_t splitKeyHash;        // Indicate where to split the tablet.
                                      // This will be the first key of the
                                      // second tablet after the split.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct GetTableIdRpc {
    static const RpcOpcode opcode = GET_TABLE_ID;
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
        uint64_t tableId;
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

struct GetServerStatisticsRpc {
    static const RpcOpcode opcode = GET_SERVER_STATISTICS;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RpcRequestCommon common;
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
        uint32_t serverStatsLength;// Number of bytes in the ServerStatistics
                                   // protobuf. The bytes of the protobuf
                                   // follow immediately after this header.
                                   // See ProtoBuf::Tablets.
    } __attribute__((packed));
};

struct SetRuntimeOptionRpc {
    static const RpcOpcode opcode = SET_RUNTIME_OPTION;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint32_t optionLength; // Number of bytes in the name of the option
                               // to set including terminating NULL character.
        uint32_t valueLength;  // Number of bytes in string representing the
                               // value to set the option to including
                               // terminating NULL character.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
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

struct RecoveryMasterFinishedRpc {
    static const RpcOpcode opcode = RECOVERY_MASTER_FINISHED;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t recoveryId;
        uint64_t recoveryMasterId; // Server Id from whom the request is coming.
        bool successful;           // Indicates whether the recovery succeeded.
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
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

struct ReassignTabletOwnershipRpc {
    static const RpcOpcode opcode = REASSIGN_TABLET_OWNERSHIP;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t tableId;           // Id of the table whose tablet was moved.
        uint64_t firstKey;          // First key of the migrated tablet.
        uint64_t lastKey;           // Last key of the migrated tablet.
        uint64_t newOwnerMasterId;  // ServerId of the new master.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

struct SendServerListRpc {
    static const RpcOpcode opcode = SEND_SERVER_LIST;
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

struct BackupAssignGroupRpc {
    static const RpcOpcode opcode = BACKUP_ASSIGN_GROUP;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RpcRequestCommon common;
        uint64_t replicationId; ///< The new replication group Id assigned to
                                ///< the backup.
        uint32_t numReplicas;   ///< Following this field, we append a list of
                                ///< uint64_t ServerId's, which represent the
                                ///< servers in the replication group.
    } __attribute__((packed));
    struct Response {
        RpcResponseCommon common;
    } __attribute__((packed));
};

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
        uint32_t numReplicas;     ///< Following this field, we append a list
                                  ///< of uint64_t ServerId's, which represent
                                  ///< the servers in the replication group.
                                  ///< The list is only sent for an open
                                  ///< segment.
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

