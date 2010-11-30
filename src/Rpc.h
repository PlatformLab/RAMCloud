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
 * This enum defines the choices for the "type" field in RPC
 * headers, which selects the particular operation to perform.
 */
enum RpcType {
    PING                    = 7,
    CREATE_TABLE            = 8,
    OPEN_TABLE              = 9,
    DROP_TABLE              = 10,
    CREATE                  = 11,
    READ                    = 12,
    WRITE                   = 13,
    REMOVE                  = 14,
    ENLIST_SERVER           = 15,
    GET_BACKUP_LIST         = 16,
    GET_TABLET_MAP          = 17,
    SET_TABLETS             = 18,
    RECOVER                 = 19,
    HINT_SERVER_DOWN        = 20,
    TABLETS_RECOVERED       = 21,
    BACKUP_CLOSE            = 128,
    BACKUP_FREE             = 129,
    BACKUP_GETRECOVERYDATA  = 130,
    BACKUP_OPEN             = 131,
    BACKUP_STARTREADINGDATA = 132,
    BACKUP_WRITE            = 133,
    ILLEGAL_RPC_TYPE        = 134,  // 1 + the highest legitimate RpcType
};

/**
 * Wire representation for a token requesting that the server collect a
 * particular performance metric while executing an RPC.
 */
struct RpcPerfCounter {
    uint32_t beginMark   : 12;    // This is actually a Mark; indicates when
                                  // the server should start measuring.
    uint32_t endMark     : 12;    // This is also a Mark; indicates when
                                  // the server should stop measuring.
    uint32_t counterType : 8;     // This is actually a PerfCounterType;
                                  // indicates what the server should measure
                                  // (time, cache misses, etc.).
};

/**
 * Each RPC request starts with this structure.
 */
struct RpcRequestCommon {
    RpcType type;                 // Operation to be performed.
    RpcPerfCounter perfCounter;   // Selects a single performance metric
                                  // for the server to collect while
                                  // executing this request. Zero means
                                  // don't collect anything.
};

/**
 * Each RPC response starts with this structure.
 */
struct RpcResponseCommon {
    Status status;                // Indicates whether the operation
                                  // succeeded; if not, it explains why.
    uint32_t counterValue;        // Value of the performance metric
                                  // collected by the server, or 0 if none
                                  // was requested.
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
    static const RpcType type = CREATE;
    struct Request {
        RpcRequestCommon common;
        uint32_t tableId;
        uint32_t length;              // Length of the value in bytes. The
                                      // actual bytes follow immediately after
                                      // this header.
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t id;
        uint64_t version;
    };
};

struct PingRpc {
    static const RpcType type = PING;
    struct Request {
        RpcRequestCommon common;
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct ReadRpc {
    static const RpcType type = READ;
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
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
        uint32_t pad1;
    };
};

struct RecoverRpc {
    static const RpcType type = RECOVER;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;
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

struct RemoveRpc {
    static const RpcType type = REMOVE;
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
    static const RpcType type = SET_TABLETS;
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
    static const RpcType type = WRITE;
    struct Request {
        RpcRequestCommon common;
        uint64_t id;
        uint32_t tableId;
        uint32_t length;              // Length of the object's value in bytes.
                                      // The actual bytes of the object follow
                                      // immediately after this header.
        RejectRules rejectRules;
    };
    struct Response {
        RpcResponseCommon common;
        uint64_t version;
    };
};

// Coordinator RPCs follow, see Coordinator.cc

struct CreateTableRpc {
    static const RpcType type = CREATE_TABLE;
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
    static const RpcType type = DROP_TABLE;
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
    static const RpcType type = OPEN_TABLE;
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
    static const RpcType type = ENLIST_SERVER;
    struct Request {
        RpcRequestCommon common;
        uint8_t serverType;
        uint8_t pad[3];
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

struct GetBackupListRpc {
    static const RpcType type = GET_BACKUP_LIST;
    struct Request {
        RpcRequestCommon common;
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
    static const RpcType type = GET_TABLET_MAP;
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
    static const RpcType type = HINT_SERVER_DOWN;
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
    static const RpcType type = TABLETS_RECOVERED;
    struct Request {
        RpcRequestCommon common;
        Status status;             // Indicates whether the recovery
                                   // succeeded; if not, it explains why.
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

// -- Backup RPCs ---

struct BackupCloseRpc {
    static const RpcType type = BACKUP_CLOSE;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to flush to disk.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupFreeRpc {
    static const RpcType type = BACKUP_FREE;
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
    static const RpcType type = BACKUP_GETRECOVERYDATA;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to get data from.
        uint32_t tabletsLength;    // Number of bytes in the tablet map.
                                   // The bytes of the tablet map follow
                                   // immediately after this header. See
                                   // ProtoBuf::Tablets.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupOpenRpc {
    static const RpcType type = BACKUP_OPEN;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
        uint64_t segmentId;     ///< Target segment to open on the backup.
    };
    struct Response {
        RpcResponseCommon common;
    };
};

struct BackupStartReadingDataRpc {
    static const RpcType type = BACKUP_STARTREADINGDATA;
    struct Request {
        RpcRequestCommon common;
        uint64_t masterId;      ///< Server Id from whom the request is coming.
    };
    struct Response {
        RpcResponseCommon common;
        uint32_t segmentIdCount;    ///< Number of segmentIds in reply payload.
        // A series of segmentIdCount uint64_t segmentIds follows.
    };
};

struct BackupWriteRpc {
    static const RpcType type = BACKUP_WRITE;
    enum Flags {
        NONE = 0,
        OPEN = 1,
        CLOSE = 2,
        OPENCLOSE = 3,
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
#define VERSION_NONEXISTENT 0

} // namespace RAMCloud

#endif // RAMCLOUD_RPC_H

