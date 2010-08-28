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

#include <RejectRules.h>
#include <Status.h>

namespace RAMCloud {

/**
 * This enum defines the choices for the "type" field in RPC
 * headers, which selects the particular operation to perform.
 */
enum RpcType {
    PING           = 7,
    CREATE_TABLE   = 8,
    OPEN_TABLE     = 9,
    DROP_TABLE     = 10,
    CREATE         = 11,
    READ           = 12,
    WRITE          = 13,
    REMOVE         = 14
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
// see the documentation for the corresponding methods in Client.cc;
// for the most part the arguments to those methods are passed directly
// to the corresponding fields of the RPC header, and the fields of
// the RPC response are returned as the results of the method.
//
// Fields with names such as "pad1" are included to make padding
// explicit (removing these fields has no effect on the layout of
// the records).

struct CreateRequest {
    RpcRequestCommon common;
    uint32_t tableId;
    uint32_t length;              // Length of the value in bytes. The
                                  // actual bytes follow immediately after
                                  // this header.
};
struct CreateResponse {
    RpcResponseCommon common;
    uint64_t id;
    uint64_t version;
};

struct CreateTableRequest {
    RpcRequestCommon common;
    uint32_t nameLength;          // Number of bytes in the name,
                                  // including terminating NULL
                                  // character. The bytes of the name
                                  // follow immediately after this header.
};
struct CreateTableResponse {
    RpcResponseCommon common;
};

struct DropTableRequest {
    RpcRequestCommon common;
    uint32_t nameLength;          // Number of bytes in the name,
                                  // including terminating NULL
                                  // character. The bytes of the name
                                  // follow immediately after this header.
};
struct DropTableResponse {
    RpcResponseCommon common;
};

struct OpenTableRequest {
    RpcRequestCommon common;
    uint32_t nameLength;          // Number of bytes in the name,
                                  // including terminating NULL
                                  // character. The bytes of the name
                                  // follow immediately after this header.
};
struct OpenTableResponse {
    RpcResponseCommon common;
    uint32_t tableId;
};

struct PingRequest {
    RpcRequestCommon common;
};
struct PingResponse {
    RpcResponseCommon common;
};

struct ReadRequest {
    RpcRequestCommon common;
    uint64_t id;
    uint32_t tableId;
    uint32_t pad1;
    RejectRules rejectRules;
};
struct ReadResponse {
    RpcResponseCommon common;
    uint64_t version;
    uint32_t length;              // Length of the object's value in bytes.
                                  // The actual bytes of the object follow
                                  // immediately after this header.
    uint32_t pad1;
};

struct RemoveRequest {
    RpcRequestCommon common;
    uint64_t id;
    uint32_t tableId;
    uint32_t pad1;
    RejectRules rejectRules;
};
struct RemoveResponse {
    RpcResponseCommon common;
    uint64_t version;
};

struct WriteRequest {
    RpcRequestCommon common;
    uint64_t id;
    uint32_t tableId;
    uint32_t length;              // Length of the object's value in bytes.
                                  // The actual bytes of the object follow
                                  // immediately after this header.
    RejectRules rejectRules;
};
struct WriteResponse {
    RpcResponseCommon common;
    uint64_t version;
};

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

