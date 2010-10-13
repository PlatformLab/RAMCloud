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
    BACKUP_COMMIT           = 128,
    BACKUP_FREE             = 129,
    BACKUP_GETSEGMENTLIST   = 130,
    BACKUP_WRITE            = 131,
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
struct EnlistServerRpc {
    static const RpcType type = ENLIST_SERVER;
    struct Request {
        RpcRequestCommon common;
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

// -- Backup RPCs ---

struct BackupCommitRequest {
    RpcRequestCommon common;
    uint64_t segmentNumber;     ///< Target segment to flush to disk.
};
struct BackupCommitResponse {
    RpcResponseCommon common;
};

struct BackupFreeRequest {
    RpcRequestCommon common;
    uint64_t segmentNumber;     ///< Target segment to free on disk.
};
struct BackupFreeResponse {
    RpcResponseCommon common;
};

struct BackupGetSegmentListRequest {
    RpcRequestCommon common;
};
struct BackupGetSegmentListResponse {
    RpcResponseCommon common;
    uint32_t segmentListCount;  ///< Number of 64-bit segment ids which follow.
    // Back-to-back packing of 64-bit segment ids, segmentListCount total.
};

struct BackupWriteRequest {
    RpcRequestCommon common;
    uint64_t segmentNumber;     ///< Target segment to update.
    uint32_t offset;            ///< Offset into this segment to write at.
    uint32_t length;            ///< Number of bytes to write.
    // Opaque byte string follows with data to write.
};
struct BackupWriteResponse {
    RpcResponseCommon common;
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

