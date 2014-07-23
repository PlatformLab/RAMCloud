/* Copyright (c) 2010-2014 Stanford University
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
 * RAMCloud remote procedure calls (i.e. the bits that are transmitted in
 * requests and responses).
 */

#ifndef RAMCLOUD_WIREFORMAT_H
#define RAMCLOUD_WIREFORMAT_H

#include "Common.h"
#include "LogMetadata.h"
#include "RejectRules.h"
#include "Segment.h"
#include "Status.h"

namespace RAMCloud { namespace WireFormat {

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
 * - The method opcodeSymbol in WireFormat.cc.
 * - WireFormatTest.cc's out-of-range test, if ILLEGAL_RPC_TYPE was changed.
 */
enum Opcode {
    PING                      = 7,
    PROXY_PING                = 8,
    KILL                      = 9,
    CREATE_TABLE              = 10,
    GET_TABLE_ID              = 11,
    DROP_TABLE                = 12,
    READ                      = 13,
    WRITE                     = 14,
    REMOVE                    = 15,
    ENLIST_SERVER             = 16,
    GET_SERVER_LIST           = 17,
    GET_TABLE_CONFIG          = 18,
    RECOVER                   = 19,
    HINT_SERVER_CRASHED       = 20,
    RECOVERY_MASTER_FINISHED  = 21,
    ENUMERATE                 = 22,
    SET_MASTER_RECOVERY_INFO  = 23,
    FILL_WITH_TEST_DATA       = 24,
    MULTI_OP                  = 25,
    GET_METRICS               = 26,
    BACKUP_FREE               = 28,
    BACKUP_GETRECOVERYDATA    = 29,
    BACKUP_STARTREADINGDATA   = 31,
    BACKUP_WRITE              = 32,
    BACKUP_RECOVERYCOMPLETE   = 33,
    BACKUP_QUIESCE            = 34,
    UPDATE_SERVER_LIST        = 35,
    BACKUP_STARTPARTITION     = 36,
    DROP_TABLET_OWNERSHIP     = 39,
    TAKE_TABLET_OWNERSHIP     = 40,
    GET_HEAD_OF_LOG           = 42,
    INCREMENT                 = 43,
    PREP_FOR_MIGRATION        = 44,
    RECEIVE_MIGRATION_DATA    = 45,
    REASSIGN_TABLET_OWNERSHIP = 46,
    MIGRATE_TABLET            = 47,
    IS_REPLICA_NEEDED         = 48,
    SPLIT_TABLET              = 49,
    GET_SERVER_STATISTICS     = 50,
    SET_RUNTIME_OPTION        = 51,
    GET_SERVER_CONFIG         = 52,
    GET_LOG_METRICS           = 53,
    VERIFY_MEMBERSHIP         = 55,
    GET_RUNTIME_OPTION        = 56,
    SERVER_CONTROL            = 57,
    SERVER_CONTROL_ALL        = 58,
    GET_SERVER_ID             = 59,
    READ_KEYS_AND_VALUE       = 60,
    LOOKUP_INDEX_KEYS         = 61,
    INDEXED_READ              = 62,
    INSERT_INDEX_ENTRY        = 63,
    REMOVE_INDEX_ENTRY        = 64,
    CREATE_INDEX              = 65,
    DROP_INDEX                = 66,
    DROP_INDEXLET_OWNERSHIP   = 67,
    TAKE_INDEXLET_OWNERSHIP   = 68,
    ILLEGAL_RPC_TYPE          = 69, // 1 + the highest legitimate Opcode
};

/**
 * This enum defines the different types of control operations
 * that could be used with SERVER_CONTROL RPC. Any new control
 * op should be added here.
 */
enum ControlOp {
    START_DISPATCH_PROFILER     = 1000,
    STOP_DISPATCH_PROFILER      = 1001,
    DUMP_DISPATCH_PROFILE       = 1002,
    GET_TIME_TRACE              = 1003,
    LOG_TIME_TRACE              = 1004,
    GET_CACHE_TRACE             = 1005,
    LOG_CACHE_TRACE             = 1006,
};

/**
 * Each RPC request starts with this structure.
 */
struct RequestCommon {
    uint16_t opcode;              /// Opcode of operation to be performed.
    uint16_t service;             /// ServiceType to invoke for this rpc.
} __attribute__((packed));

/**
 * Some RPCs include an explicit server id in the header, to detect
 * situations where a new server starts up with the same locator as an old
 * dead server; RPCs intended for the old server must be rejected by
 * the new server.
 */
struct RequestCommonWithId {
    uint16_t opcode;              /// Opcode of operation to be performed.
    uint16_t service;             /// ServiceType to invoke for this rpc.
    uint64_t targetId;            /// ServerId for which this RPC is
                                  /// intended. 0 means "ignore this field":
                                  /// for convenience during testing.
} __attribute__((packed));

/**
 * Each RPC response starts with this structure.
 */
struct ResponseCommon {
    Status status;                // Indicates whether the operation
                                  // succeeded; if not, it explains why.
} __attribute__((packed));

/**
 * When the response status is STATUS_RETRY, the full response looks like
 * this (it contains extra information for use by the requesting client).
 */
struct RetryResponse {
    ResponseCommon common;
    uint32_t minDelayMicros;      // Lower bound on client delay, in
                                  // microseconds.
    uint32_t maxDelayMicros;      // Upper bound on client delay, in
                                  // microseconds. The client should choose
                                  // a random number between minDelayMicros
                                  // and maxDelayMicros, and wait that long
                                  // before retrying the RPC.
    uint32_t messageLength;       // Number of bytes in a message that
                                  // describes the reason for the retry.
                                  // 0 means there is no message.
                                  // The message itself immediately follows
                                  // this header, and it must include a
                                  // terminating null character, which is
                                  // included in messageLength.
} __attribute__((packed));


// For each RPC there is a structure below, which contains the following:
//   * A field "opcode" defining the Opcode used in requests.
//   * A field "service" defining the ServiceType to use in requests.
//   * A struct Request, which defines the fixed fields in the header for
//     requests.
//   * A struct Response, which defines the header for responses to this
//     RPC.
// For details on the meaning of the individual fields in Request and
// Response structures, see the wrapper methods used to invoke the RPC
// (for example, for Read, see RamCloud::read); for the most part,
// arguments to the wrapper methods are passed directly to the corresponding
// fields of the Request structure, and fields of the Response are returned
// as results of the wrapper method.
//
// All structs are packed so that they have a standard byte representation.
// All fields are little endian.

// The RPCs below are in alphabetical order

struct BackupFree {
    static const Opcode opcode = BACKUP_FREE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to discard from backup.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct BackupGetRecoveryData {
    static const Opcode opcode = BACKUP_GETRECOVERYDATA;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t recoveryId;    ///< Identifies the recovery for which the
                                ///< recovery segment is requested.
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to get data from.
        uint64_t partitionId;   ///< Partition id of :ecovery segment to fetch.
    } __attribute__((packed));
    struct Response {
        Response()
            : common()
            , certificate()
        {}
        Response(const ResponseCommon& common,
                 const Segment::Certificate& certificate)
            : common(common)
            , certificate(certificate)
        {}
        ResponseCommon common;
        Segment::Certificate certificate; ///< Certificate for the segment
                                          ///< which follows this fields in
                                          ///< the response field. Used by
                                          ///< master to iterate over the
                                          ///< segment.
    } __attribute__((packed));
};

// Note: this RPC is supported by the coordinator service as well as backups.
struct BackupQuiesce {
    static const Opcode opcode = BACKUP_QUIESCE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct BackupRecoveryComplete {
    static const Opcode opcode = BACKUP_RECOVERYCOMPLETE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t masterId;      ///< Server Id which was recovered.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct BackupStartReadingData {
    static const Opcode opcode = BACKUP_STARTREADINGDATA;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t recoveryId;       ///< Identifies the recovery for which
                                   ///< information should be returned and
                                   ///< recovery segments should be built.
        uint64_t masterId;         ///< Server Id from whom the request is
                                   ///< coming.
        uint32_t partitionsLength; ///< Number of bytes in the partition map.
                                   ///< The bytes of the partition map follow
                                   ///< immediately after this header. See
                                   ///< ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t replicaCount;         ///< Number of entries in the replica
                                       ///< list following this header.
        uint32_t primaryReplicaCount;  ///< Count of segment replicas that are
                                       ///< primary. These appear at the start
                                       ///< of the replica list.
        uint32_t digestBytes;          ///< Number of bytes for optional
                                       ///< LogDigest.
        uint64_t digestSegmentId;      ///< SegmentId the LogDigest came from.
        uint64_t digestSegmentEpoch;   ///< Segment epoch of the replica from
                                       ///< which the digest was taken. Used
                                       ///< by the coordinator to determine if
                                       ///< the replica might have been
                                       ///< inconsistent. If it might've been
                                       ///< this digest will be discarded
                                       ///< by the coordinator for safety.
        uint32_t tableStatsBytes;      ///< Byte length of TableStats::Digest
                                       ///< that go after the LogDigest
        // An array of segmentIdCount replicas follows.
        // Each entry is a Replica (see below).
        //
        // If logDigestBytes != 0, then a serialised LogDigest follows
        // immediately after the replica list.
        // If tableStatsBytes != 0, then a TableStats::Digest follows.
    } __attribute__((packed));
    /// Used in the Response to report which replicas the backup has.
    struct Replica {
        uint64_t segmentId;        ///< The segment ID.
        uint64_t segmentEpoch;     ///< Epoch for the segment that the replica
                                   ///< was most recently updated in. Used by
                                   ///< the coordinator to filter out stale
                                   ///< replicas from the log.
        bool closed;               ///< Whether the replica was marked as
                                   ///< closed on the backup. If it was it
                                   ///< is inherently consistent and can be
                                   ///< used without scrutiny during recovery.
        Replica(uint64_t segmentId, uint64_t segmentEpoch, bool closed)
            : segmentId(segmentId)
            , segmentEpoch(segmentEpoch)
            , closed(closed)
        {}
        friend bool operator==(const Replica& left, const Replica& right) {
            return left.segmentId == right.segmentId &&
                   left.segmentEpoch == right.segmentEpoch &&
                   left.closed == right.closed;
        }
    } __attribute__((packed));
};

struct BackupStartPartitioningReplicas {
    static const Opcode opcode = BACKUP_STARTPARTITION;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t recoveryId;       ///< Identifies the recovery for which
                                   ///< information should be returned and
                                   ///< recovery segments should be built.
        uint64_t masterId;         ///< Server Id from whom the request is
                                   ///< coming.
        uint32_t partitionsLength; ///< Number of bytes in the partition map.
                                   ///< The bytes of the partition map follow
                                   ///< immediately after this header. See
                                   ///< ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct BackupWrite {
    static const Opcode opcode = BACKUP_WRITE;
    static const ServiceType service = BACKUP_SERVICE;
    struct Request {
        Request()
            : common()
            , masterId()
            , segmentId()
            , segmentEpoch()
            , offset()
            , length()
            , open()
            , close()
            , primary()
            , certificateIncluded()
            , certificate()
        {}
        Request(const RequestCommonWithId& common,
                uint64_t masterId,
                uint64_t segmentId,
                uint64_t segmentEpoch,
                uint32_t offset,
                uint32_t length,
                bool open,
                bool close,
                bool primary,
                bool certificateIncluded,
                const Segment::Certificate& certificate)
            : common(common)
            , masterId(masterId)
            , segmentId(segmentId)
            , segmentEpoch(segmentEpoch)
            , offset(offset)
            , length(length)
            , open(open)
            , close(close)
            , primary(primary)
            , certificateIncluded(certificateIncluded)
            , certificate(certificate)
        {}
        RequestCommonWithId common;
        uint64_t masterId;        ///< Server from whom the request is coming.
        uint64_t segmentId;       ///< Target segment to update.
        uint64_t segmentEpoch;    ///< Epoch for the segment that the replica
                                  ///< is being updated in. Used by
                                  ///< the coordinator to filter out stale
                                  ///< replicas from the log.
        uint32_t offset;          ///< Offset into this segment to write at.
        uint32_t length;          ///< Number of bytes to write.
        bool open;                ///< If open request.
        bool close;               ///< If close request.
        bool primary;             ///< If this replica should be considered
                                  ///< a primary and loaded immediately on
                                  ///< the start of recovery.
        bool certificateIncluded; ///< If false #certificate is undefined, if
                                  ///< true then it includes a valid certificate
                                  ///< that should be placed in the segment
                                  ///< after the data from this write.
        Segment::Certificate certificate; ///< Certificate which should be
                                          ///< written to storage
                                          ///< following the data included
                                          ///< in this rpc.
        // Opaque byte string follows with data to write.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct CreateTable {
    static const Opcode opcode = CREATE_TABLE;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
        uint32_t serverSpan;          // The number of servers across which
                                      // this table will be divided.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t tableId;             // The id of the created table.
    } __attribute__((packed));
};

struct DropTable {
    static const Opcode opcode = DROP_TABLE;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct CreateIndex {
    static const Opcode opcode = CREATE_INDEX;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;       // Id of table to which the index belongs.
        uint8_t indexType;      // Type of index.
        uint8_t indexId;        // Id of secondary keys in the index.
        uint8_t numIndexlets;   // Number of indexlets to partition the index
                                // key space.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct DropIndex {
    static const Opcode opcode = DROP_INDEX;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;       // Id of table to which the index belongs.
        uint8_t indexId;        // Id of secondary keys in the index.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct DropIndexletOwnership {
    static const Opcode opcode = DROP_INDEXLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;                   // Id of table to which the index
                                            // belongs.
        uint8_t indexId;                    // Id of secondary keys in index.
        uint16_t firstKeyLength;            // Length of firstKey in bytes.
        uint16_t firstNotOwnedKeyLength;    // Length of firstNotOwnedKey in
                                            // bytes.
        // In buffer: The actual bytes for firstKey and firstNotOwnedKey
        // go here. [firstKey, firstNotOwnedKey) defines the span of the
        // indexlet.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct DropTabletOwnership {
    static const Opcode opcode = DROP_TABLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct EnlistServer {
    static const Opcode opcode = ENLIST_SERVER;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        /// Server id this server used to operate at; the coordinator must
        /// make sure this server is removed from the cluster before
        /// enlisting the calling server.
        uint64_t replacesId;
        SerializedServiceMask serviceMask; ///< Which services are available
                                           ///< on the enlisting server.
        uint32_t readSpeed;                /// MB/s read speed if a BACKUP
        /// Number of bytes in the serviceLocator, including terminating NULL
        /// character.  The bytes of the service locator follow immediately
        /// after this header.
        uint32_t serviceLocatorLength;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t serverId;               /// Unique ServerId assigned to this
                                         /// enlisting server process.
    } __attribute__((packed));
};

struct Enumerate {
    static const Opcode opcode = ENUMERATE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        bool keysOnly;              // False means that full objects are
                                    // returned, containing both keys and data.
                                    // True means that the returned objects have
                                    // been truncated so that the object data
                                    // (normally the last field of the object)
                                    // is omitted.
        uint64_t tabletFirstHash;
        uint32_t iteratorBytes;     // Size of iterator in bytes. The
                                    // actual iterator follows
                                    // immediately after this header.
                                    // See EnumerationIterator.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t tabletFirstHash;
        uint32_t payloadBytes;      // Size of payload, where each object in
                                    // payload is a uint32_t size,
                                    // Object metadata, and key and data
                                    // blobs. The actual payload
                                    // follows immediately after this
                                    // header.
        uint32_t iteratorBytes;     // Size of iterator in bytes. The
                                    // actual iterator follows after
                                    // the payload. See
                                    // EnumerationIterator.
    } __attribute__((packed));
};

struct FillWithTestData {
    static const Opcode opcode = FILL_WITH_TEST_DATA;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t numObjects;        // Number of objects to add to tables
                                    // in round-robin fashion.
        uint32_t objectSize;        // Size of each object to add.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct GetHeadOfLog {
    static const Opcode opcode = GET_HEAD_OF_LOG;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t headSegmentId;     // ID of head segment in the log.
        uint32_t headSegmentOffset; // Byte offset of head within the segment.
    } __attribute__((packed));
};

struct GetLogMetrics {
    static const Opcode opcode = GET_LOG_METRICS;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t logMetricsLength; // Number of bytes in the log metrics
                                   // protocol buffer immediately follow-
                                   // ing this header.
    } __attribute__((packed));
};

struct GetMetrics {
    static const Opcode opcode = Opcode::GET_METRICS;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t messageLength;    // Number of bytes in a
                                   // ProtoBuf::MetricList message that
                                   // follows immediately after this
                                   // header.
        // Variable-length byte string containing ProtoBuf::MetricList
        // follows.
    } __attribute__((packed));
};

struct GetRuntimeOption {
    static const Opcode opcode = GET_RUNTIME_OPTION;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t optionLength; // Number of bytes in the name of the option
                               // to read including terminating NULL character.
                               // The actual bytes follow immediately after
                               // this header structure.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t valueLength;  // Number of bytes in string representing the
                               // value of corresponding option including
                               // terminating NULL character.  The actual
                               // bytes follow immediately after this header.
    } __attribute__((packed));
};

struct GetServerConfig {
    static const Opcode opcode = GET_SERVER_CONFIG;
    static const ServiceType service = MEMBERSHIP_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t serverConfigLength;   // Number of bytes in the server config
                                       // protocol buffer immediately follow-
                                       // ing this header.
    } __attribute__((packed));
};

struct GetServerId {
    static const Opcode opcode = GET_SERVER_ID;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t serverId;             // ServerId of the server that
                                       // processed the request.
    } __attribute__((packed));
};

struct GetServerList {
    static const Opcode opcode = GET_SERVER_LIST;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        SerializedServiceMask serviceMask; ///< Only get servers with specified
                                           ///< services.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t serverListLength; // Number of bytes in the server list.
                                   // The bytes of the server list follow
                                   // immediately after this header. See
                                   // ProtoBuf::ServerList.
    } __attribute__((packed));
};

struct GetServerStatistics {
    static const Opcode opcode = GET_SERVER_STATISTICS;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t serverStatsLength;// Number of bytes in the ServerStatistics
                                   // protobuf. The bytes of the protobuf
                                   // follow immediately after this header.
                                   // See ProtoBuf::Tablets.
    } __attribute__((packed));
};

struct GetTableId {
    static const Opcode opcode = GET_TABLE_ID;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t tableId;
    } __attribute__((packed));
};

struct GetTableConfig {
    static const Opcode opcode = GET_TABLE_CONFIG;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t tableConfigLength;  // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    } __attribute__((packed));
};

struct HintServerCrashed {
    static const Opcode opcode = HINT_SERVER_CRASHED;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t serverId;         // ServerId of the server suspected of being
                                   // dead. Poke it with a stick.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct Increment {
    static const Opcode opcode = INCREMENT;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual bytes of the key follow
                                      // immediately after this header.
        int64_t incrementValue;       // Value that the object will be
                                      // incremented by.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t version;
        int64_t newValue;                // The new value of the object.
    } __attribute__((packed));
};

/**
 * Used by a client to request objects from a master for which the
 * primary key hash matching one of the key hashes
 * and specified index key is in the range [first key, last key].
 */
struct IndexedRead {
    static const Opcode opcode = INDEXED_READ;
    static const ServiceType service = MASTER_SERVICE;

    struct Request {
        RequestCommon common;
        uint64_t tableId;               // Id of the table for the lookup.
        uint8_t indexId;                // Id of the index for the lookup.
        uint16_t firstKeyLength;        // Length of first key in bytes.
        uint16_t lastKeyLength;         // Length of last key in bytes.
        uint32_t numHashes;             // Number of key hashes to be looked up.
        // In buffer: The actual first key and last key go here.
        // In buffer: Key hashes for primary key for objects to be read go here.
    } __attribute__((packed));

    struct Response {
        ResponseCommon common;
        uint32_t numHashes;             // Number of key hashes for which
                                        // objects are being returned or
                                        // do not exist.
        uint32_t numObjects;            // Number of objects being returned.

        // In buffer: For each object being returned,
        // uint64_t version, uint32_t length and the actual object bytes
        // (all the keys and value) go here. The actual object bytes is
        // of variable length, indicated by the length.
    } __attribute__((packed));
};

/**
 * Used by a master to ask an index server to insert an index entry
 * for the object this master is currently writing.
 */
struct InsertIndexEntry {
    static const Opcode opcode = INSERT_INDEX_ENTRY;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;           // Id of the table containing the object
                                    // for which index entry is being inserted.
        uint8_t indexId;            // Id of the index for which the entry
                                    // is being inserted.
        uint16_t indexKeyLength;    // Length of index key in bytes.
        uint64_t primaryKeyHash;    // Hash of the primary key of the object for
                                    // for which index entry is being inserted.
        // In buffer: Actual bytes of the index key goes here.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

/**
 * Used by backups to determine if a particular replica is still needed
 * by a master.  This is only used in the case the backup has crashed, and
 * has since restarted.
 */
struct IsReplicaNeeded {
    static const Opcode opcode = IS_REPLICA_NEEDED;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t backupServerId;
        uint64_t segmentId;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        bool needed;                // False if the master's segment is
                                    // fully replicated, true otherwise.
    } __attribute__((packed));
};

struct Kill {
    static const Opcode opcode = KILL;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RequestCommon common;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

/**
 * Used by a client to request an index server to lookup primary key hashes for
 * objects having specified index key in the range [first key, last key].
 */
struct LookupIndexKeys {
    static const Opcode opcode = LOOKUP_INDEX_KEYS;
    static const ServiceType service = MASTER_SERVICE;

    struct Request {
        RequestCommon common;
        uint64_t tableId;               // Id of the table for the lookup.
        uint8_t indexId;                // Id of the index for the lookup.
        uint16_t firstKeyLength;        // Length of first key in bytes.
        uint64_t firstAllowedKeyHash;   // Smallest primary key hash value
                                        // allowed for firstKey.
        uint16_t lastKeyLength;         // Length of last key in bytes.
        uint32_t maxNumHashes;          // Max number of primary key hashes
                                        // to be returned.
        // In buffer: The actual first key and last key go here.
    } __attribute__((packed));

    struct Response {
        ResponseCommon common;
        uint32_t numHashes;     // Number of primary key hashes being returned.
        uint16_t nextKeyLength; // Length of next key to fetch.
        uint64_t nextKeyHash;   // Minimum allowed hash corresponding to
                                // next key to be fetched.
        // In buffer: Key hashes of primary keys for matching objects go here.
        // In buffer: Actual bytes for the next key for which
        // the client should send another lookup request (if any) goes here.
    } __attribute__((packed));
};

struct MigrateTablet {
    static const Opcode opcode = MIGRATE_TABLET;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;           // TabletId of the tablet to migrate.
        uint64_t firstKeyHash;      // First key of the tablet to migrate.
        uint64_t lastKeyHash;       // Last key of the tablet to migrate.
        uint64_t newOwnerMasterId;  // ServerId of the master to migrate to.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct MultiOp {
    static const Opcode opcode = MULTI_OP;
    static const ServiceType service = MASTER_SERVICE;

    /// Type of Multi Operation
    /// Note: Make sure INVALID is always last.
    enum OpType { READ, REMOVE, WRITE, INVALID };

    struct Request {
        RequestCommon common;
        uint32_t count; // Number of Part structures following this.
        OpType type;

        struct ReadPart {
            uint64_t tableId;
            uint16_t keyLength;
            // In buffer: The actual key for this part
            // follows immediately after this.
            ReadPart(uint64_t tableId, uint16_t keyLength)
                : tableId(tableId), keyLength(keyLength) {}
        } __attribute__((packed));

        struct RemovePart {
            uint64_t tableId;
            uint16_t keyLength;
            RejectRules rejectRules;

            // In buffer: The actual key for this part
            // follows immediately after this.
            RemovePart(uint64_t tableId, uint16_t keyLength,
                       RejectRules rejectRules)
                : tableId(tableId)
                , keyLength(keyLength)
                , rejectRules(rejectRules)
            {
            }
        } __attribute__((packed));

        struct WritePart {
            uint64_t tableId;
            uint32_t length;        // length of keysAndValue
            RejectRules rejectRules;

            // In buffer: KeysAndValue follow immediately after this
            WritePart(uint64_t tableId, uint32_t length,
                        RejectRules rejectRules)
                : tableId(tableId)
                , length(length)
                , rejectRules(rejectRules)
            {
            }
        } __attribute__((packed));
    } __attribute__((packed));
    struct Response {
        // RpcResponseCommon contains a status field. But it is not used in
        // multiRead since there is a separate status for each object returned.
        // Included here to fulfill requirements in common code.
        ResponseCommon common;
        uint32_t count; // Number of Part structures following this.

        struct ReadPart {
            // In buffer: Status/Part and object data go here. Object data are
            // a variable number of bytes (depending on data size.)

            // Each Response::Part contains the minimum object metadata we need
            // returned, followed by the object data itself.
            //
            // TODO(Ankita): This was a quick way to get multiRead working such
            // that it doesn't depend on unrelated Segment internals. A nicer
            // solution might be to construct segments on the master and send
            // those in the response. However, that'd currently incur an extra
            // copy. I doubt the copy would have much impact, but it's worth
            // considering. Not sending the whole object also means we lose the
            // checksum, but we weren't using it anyway.
            /// Status of the request

            Status status;

            /// Version of the object.
            uint64_t version;

            /// Length of the object data following this struct.
            uint32_t length;
        } __attribute__((packed));

        struct RemovePart {
            // Each Response::Part contains the Status for the newly written
            // object removed and the version.

            /// Status of the remove operation.
            Status status;

            /// Version of the object that was removed, if it existed.
            uint64_t version;
        } __attribute__((packed));

        struct WritePart {
            // Each Response::Part contains the Status for the newly written
            ///object returned and the version.

            /// Status of the write operation.
            Status status;

            /// Version of the written object.
            uint64_t version;
        } __attribute__((packed));
    } __attribute__((packed));
};

struct Ping {
    static const Opcode opcode = Opcode::PING;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t callerId;          // ServerId of the caller, or invalid
                                    // server id.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct PrepForMigration {
    static const Opcode opcode = PREP_FOR_MIGRATION;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;           // TableId of the tablet we'll move.
        uint64_t firstKeyHash;      // First key in the tablet range.
        uint64_t lastKeyHash;       // Last key in the tablet range.
        uint64_t expectedObjects;   // Expected number of objects to migrate.
        uint64_t expectedBytes;     // Expected total object bytes to migrate.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct ProxyPing {
    static const Opcode opcode = PROXY_PING;
    static const ServiceType service = PING_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t serverId;             // ServerId of the server to ping.
        uint64_t timeoutNanoseconds;   // Number of nanoseconds to wait for a
                                       // reply before responding negatively to
                                       // this RPC.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t replyNanoseconds;     // Number of nanoseconds it took to get
                                       // the reply. If a timeout occurred, the
                                       // value is -1.
    } __attribute__((packed));
};

struct Read {
    static const Opcode opcode = READ;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual key follows
                                      // immediately after this header.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t version;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
    } __attribute__((packed));
};

struct ReadKeysAndValue {
    static const Opcode opcode = READ_KEYS_AND_VALUE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual key follows
                                      // immediately after this header.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t version;
        uint32_t length;              // Length of the object's keys and value
                                      // as defined in Object.h in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
    } __attribute__((packed));
};

struct ReassignTabletOwnership {
    static const Opcode opcode = REASSIGN_TABLET_OWNERSHIP;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;           // Id of the table whose tablet was moved.
        uint64_t firstKeyHash;      // First key hash of the migrated tablet.
        uint64_t lastKeyHash;       // Last key hash of the migrated tablet.
        uint64_t newOwnerId;        // ServerId of the new master.
        uint64_t ctimeSegmentId;    // Segment id of log head before migration.
        uint64_t ctimeSegmentOffset;// Offset in log head before migration.
                                    // Used with above to set the migrated
                                    // tablet's log ``creation time'' on the
                                    // coordinator.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct ReceiveMigrationData {
    static const Opcode opcode = RECEIVE_MIGRATION_DATA;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        Request()
            : common()
            , tableId()
            , firstKeyHash()
            , segmentBytes()
            , certificate()
        {}
        RequestCommonWithId common;
        uint64_t tableId;           // Id of the table this data belongs to.
        uint64_t firstKeyHash;      // Start of the tablet range for the data.
        uint32_t segmentBytes;      // Length of the Segment containing migrated
                                    // data immediately following this header.
        Segment::Certificate certificate; // Certificate for the segment
                                          // which follows this fields in
                                          // the response field. Used by
                                          // master to iterate over the
                                          // segment.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct Recover {
    static const Opcode opcode = RECOVER;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
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
        ResponseCommon common;
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

struct RecoveryMasterFinished {
    static const Opcode opcode = RECOVERY_MASTER_FINISHED;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t recoveryId;
        uint64_t recoveryMasterId; // Server Id from whom the request is coming.
        bool successful;           // Indicates whether the recovery succeeded.
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        bool cancelRecovery;
    } __attribute__((packed));
};

struct Remove {
    static const Opcode opcode = REMOVE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint16_t keyLength;           // Length of the key in bytes.
                                      // The actual key follows
                                      // immediately after this header.
        RejectRules rejectRules;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t version;
    } __attribute__((packed));
};

/**
 * Used by a master to ask an index server to remove an index entry
 * for the data this master is removing (or has removed in the past).
 */
struct RemoveIndexEntry {
    static const Opcode opcode = REMOVE_INDEX_ENTRY;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint8_t indexId;
        uint16_t indexKeyLength;
        uint64_t primaryKeyHash;
        // In buffer: Actual bytes of the index key go here.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct ServerControl {
    static const Opcode opcode = Opcode::SERVER_CONTROL;
    static const ServiceType service = PING_SERVICE;

    /// Distinguishes between the ObjectServerControl, IndexServerControl,
    /// and ServerControl types.
    /// Note: Make sure INVALID is always last.
    enum ServerControlType {
        OBJECT,                     // ObjectServerControl type.
        INDEX,                      // IndexServerControl type.
        SERVER_ID,                  // ServerControl type.
        INVALID                     // Invalid type used for testing.
    };

    struct Request {
        RequestCommon common;
        ServerControlType type;     // Defines which arguments the server checks
                                    // before proceeding.
        ControlOp controlOp;        // The control operation to be initiated
                                    // in a server.
        uint64_t tableId;           // TableId of owned target object/indexlet.
        uint8_t indexId;            // IndexId of owned target indexlet.
        uint16_t keyLength;         // Length of key/secondary key of the
                                    // owned target object/indexlet.
        uint32_t inputLength;       // Length of the input data for the
                                    // control operation, in bytes.
        // Data follows immediately after this header in the following order:
        // key      (keyLength bytes of key data)
        // input    (inputLength bytes of input data)
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t serverId;          // ServerId of the responding server.
        uint32_t outputLength;      // Length of the output data returning
                                    // from the server, in bytes. The actual
                                    // data follow immediately after the header.
    } __attribute__((packed));
};

struct ServerControlAll {
    static const Opcode opcode = Opcode::SERVER_CONTROL_ALL;
    static const ServiceType service = COORDINATOR_SERVICE;

    struct Request {
        RequestCommon common;
        ControlOp controlOp;        // The control operation to be initiated
                                    // in a server.
        uint32_t inputLength;       // Length of the input data for the
                                    // control operation, in bytes.
        // In buffer: Input Data (if any).
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint32_t serverCount;       // Number of servers that executed and
                                    // responded to the control rpc.
        uint32_t respCount;         // Number of ServerControl responses
                                    // included in this response. All responses
                                    // and headers will not in total exceed the
                                    // MAX_RPC_LEN; responses may be dropped to
                                    // meet this limit.  If responses are
                                    // dropped, the respCount will be less than
                                    // the serverCount.
        uint32_t totalRespLength;   // Length of all appended ServerControl
                                    // responses including their headers.
                                    // 0 or more ServerControl response entries
                                    // are appended back to back immediately
                                    // following the header.  Entries consist of
                                    // a ServerControl::Response header followed
                                    // by the ServerConrol::Response data; an
                                    // entry is exactly the contents of a
                                    // ServerControlRpc's replyPayload.
    } __attribute__((packed));
};

struct SetMasterRecoveryInfo {
    static const Opcode opcode = SET_MASTER_RECOVERY_INFO;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t serverId;         // ServerId the coordinator update the
                                   // minimum segment id for.
        uint32_t infoLength;       // Bytes in the protobuf which follows
                                   // this header. Stored by the coordinator
                                   // for this server.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct SetRuntimeOption {
    static const Opcode opcode = SET_RUNTIME_OPTION;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t optionLength; // Number of bytes in the name of the option
                               // to set including terminating NULL character.
                               // The actual bytes follow immediately after
                               // this header structure.
        uint32_t valueLength;  // Number of bytes in string representing the
                               // value to set the option to including
                               // terminating NULL character.  The actual
                               // bytes follow immediately after the bytes
                               // corresponding to optionLength.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct SplitMasterTablet {
    static const Opcode opcode = SPLIT_TABLET;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;             // Id of the table that contains the to
                                      // be split tablet.
        uint64_t splitKeyHash;        // Indicate where to split the tablet.
                                      // This will be the first key of the
                                      // second tablet after the split.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct SplitTablet {
    static const Opcode opcode = SPLIT_TABLET;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint32_t nameLength;          // Number of bytes in the name,
                                      // including terminating NULL
                                      // character. The bytes of the name
                                      // follow immediately after this header.
        uint64_t splitKeyHash;        // Indicate where to split the tablet.
                                      // This will be the first key of the
                                      // second tablet after the split.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct TakeTabletOwnership {
    static const Opcode opcode = TAKE_TABLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct TakeIndexletOwnership {
    static const Opcode opcode = TAKE_INDEXLET_OWNERSHIP;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommonWithId common;
        uint64_t tableId;                   // Id of table to which the index
                                            // belongs.
        uint8_t indexId;                    // Type of index.
        uint64_t indexletTableId;           // Id of the table that will hold
                                            // objects for this indexlet.
        uint16_t firstKeyLength;            // Length of fistKey in bytes.
        uint16_t firstNotOwnedKeyLength;    // Length of firstNotOwnedKey in
                                            // bytes.
        // In buffer: The actual bytes for firstKey and firstNotOwnedKey
        // go here. [firstKey, firstNotOwnedKey) defines the span of the
        // indexlet.
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct UpdateServerList {
    static const Opcode opcode = UPDATE_SERVER_LIST;
    static const ServiceType service = MEMBERSHIP_SERVICE;
    struct Request {
        RequestCommonWithId common;

        // Immediately following this header are one or more groups,
        // where each group consists of a Part object (defined below)
        // followed by a serialized ProtoBuf::ServerList.
        struct Part {
            uint32_t serverListLength; // Number of bytes in the server list.
                                       // The bytes of the server list follow
                                       // immediately after this header. See
                                       // ProtoBuf::ServerList.
        }  __attribute__((packed));
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t currentVersion;      // The server list version number of the
                                      // RPC recipient, after processing this
                                      // request.
    } __attribute__((packed));
};

struct VerifyMembership {
    static const Opcode opcode = VERIFY_MEMBERSHIP;
    static const ServiceType service = COORDINATOR_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t serverId;            // Is this server still in cluster?
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
    } __attribute__((packed));
};

struct Write {
    static const Opcode opcode = WRITE;
    static const ServiceType service = MASTER_SERVICE;
    struct Request {
        RequestCommon common;
        uint64_t tableId;
        uint32_t length;              // Includes the total size of the
                                      // keysAndValue blob in bytes.These
                                      // follow immediately after this header
        RejectRules rejectRules;
        uint8_t async;
    } __attribute__((packed));
    struct Response {
        ResponseCommon common;
        uint64_t version;
    } __attribute__((packed));
};

// DON'T DEFINE NEW RPC TYPES HERE!! Put them in alphabetical order above.

Status getStatus(Buffer* buffer);
const char* serviceTypeSymbol(ServiceType type);
const char* opcodeSymbol(uint32_t opcode);
const char* opcodeSymbol(Buffer* buffer);

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

}} // namespace WireFormat namespace RAMCloud

#endif // RAMCLOUD_WIREFORMAT_H

