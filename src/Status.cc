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

#include "Common.h"
#include "Status.h"
#include "ClientException.h"

namespace RAMCloud {

// The following table maps from a Status value to a human-readable
// message describing the problem.
static const char* messages[] = {
    "operation succeeded",                       // OK
    "unknown table (may exist elsewhere)",       // UNKNOWN_TABLET
    "table doesn't exist",                       // TABLE_DOESNT_EXIST
    "object doesn't exist",                      // OBJECT_DOESNT_EXIST
    "object already exists",                     // OBJECT_EXISTS
    "object has wrong version",                  // WRONG_VERSION
    "insufficient space to create new table",    // STATUS_NO_TABLE_SPACE
    "RPC request message too short",             // MESSAGE_TOO_SHORT
    "invalid RPC request type",                  // UNIMPLEMENTED_REQUEST
    "RPC request improperly formatted",          // REQUEST_FORMAT_ERROR
    "RPC response improperly formatted",         // RESPONSE_FORMAT_ERROR
    "couldn't connect to RAMCloud cluster",      // COULDNT_CONNECT
    "bad segment id",                            // BACKUP_BAD_SEGMENT_ID
    "backup rejected replica open request",      // BACKUP_OPEN_REJECTED
    "segment overflow",                          // BACKUP_SEGMENT_OVERFLOW
    "malformed segment",                         // BACKUP_MALFORMED_SEGMENT
    "segment recovery failed",                   // SEGMENT_RECOVERY_FAILED
    "retry",                                     // RETRY
    "service not available",                     // SERVICE_NOT_AVAILABLE
    "operation took too long",                   // STATUS_TIMEOUT
    "server doesn't exist",                      // SERVER_NOT_UP
    "internal RAMCloud error",                   // STATUS_INTERNAL_ERROR
    "object is invalid for the chosen operation",// STATUS_INVALID_OBJECT
    "tablet doesn't exist",                      // STATUS_TABLET_DOESNT_EXIST
    "reading data should preceed partitioning",  // STATUS_PARTITION_BEFORE_READ
    "rpc sent to wrong server id",               // STATUS_WRONG_SERVER
    "invoking server does not appear to be in the cluster",
                                                 // STATUS_CALLER_NOT_IN_CLUSTER
    "request is too large",                      // STATUS_REQUEST_TOO_LARGE
    "unknown indexlet (may exist elsewhere)",    // STATUS_UNKNOWN_INDEXLET
    "unknown index",                             // STATUS_UNKNOWN_INDEX
    "invalid parameter",                         // STATUS_INVALID_PARAMETER
};

// The following table maps from a Status value to the internal name
// for the Status.
static const char* symbols[] = {
    "STATUS_OK",
    "STATUS_UNKNOWN_TABLET",
    "STATUS_TABLE_DOESNT_EXIST",
    "STATUS_OBJECT_DOESNT_EXIST",
    "STATUS_OBJECT_EXISTS",
    "STATUS_WRONG_VERSION",
    "STATUS_NO_TABLE_SPACE",
    "STATUS_MESSAGE_TOO_SHORT",
    "STATUS_UNIMPLEMENTED_REQUEST",
    "STATUS_REQUEST_FORMAT_ERROR",
    "STATUS_RESPONSE_FORMAT_ERROR",
    "STATUS_COULDNT_CONNECT",
    "STATUS_BACKUP_BAD_SEGMENT_ID",
    "STATUS_BACKUP_OPEN_REJECTED",
    "STATUS_BACKUP_SEGMENT_OVERFLOW",
    "STATUS_BACKUP_MALFORMED_SEGMENT",
    "STATUS_SEGMENT_RECOVERY_FAILED",
    "STATUS_RETRY",
    "STATUS_SERVICE_NOT_AVAILABLE",
    "STATUS_TIMEOUT",
    "STATUS_SERVER_NOT_UP",
    "STATUS_INTERNAL_ERROR",
    "STATUS_INVALID_OBJECT",
    "STATUS_TABLET_DOESNT_EXIST",
    "STATUS_PARTITION_BEFORE_READ",
    "STATUS_WRONG_SERVER",
    "STATUS_CALLER_NOT_IN_CLUSTER",
    "STATUS_REQUEST_TOO_LARGE",
    "STATUS_UNKNOWN_INDEXLET",
    "STATUS_UNKNOWN_INDEX",
    "STATUS_INVALID_PARAMETER",
};

/**
 * Given a Status value, return a human-readable string describing the
 * problem.
 *
 * \param status
 *      RAMCloud status code (presumably returned to indicate how an
 *      operation failed).
 *
 * \return
 *      See above.
 */
const char*
statusToString(Status status)
{
    uint32_t index = status;
    if (index >= (sizeof(messages)/sizeof(char*))) {       // NOLINT
        return "unrecognized RAMCloud error";
    }
    return messages[index];
}

/**
 * Given a Status value, return a human-readable string containing
 * the symbolic name for the status (as used in the Status type),
 * such as STATUS_OBJECT_DOESNT_EXIST.
 *
 * \param status
 *      RAMCloud status code (presumably returned to indicate how an
 *      operation failed).
 *
 * \return
 *      See above.
 */
const char*
statusToSymbol(Status status)
{
    uint32_t index = status;
    if (index >= (sizeof(symbols)/sizeof(char*))) {        // NOLINT
        return "STATUS_UNKNOWN";
    }
    return symbols[index];
}

}  // namespace RAMCloud
