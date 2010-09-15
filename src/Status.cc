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

#include "Common.h"
#include "Status.h"
#include "ClientException.h"

namespace RAMCloud {

// The following table maps from a Status value to a human-readable
// message describing the problem.
static const char* messages[] = {
    "operation succeeded",                       // OK
    "table doesn't exist",                       // TABLE_DOESNT_EXIST
    "object doesn't exist",                      // OBJECT_DOESNT_EXIST
    "object already exists",                     // OBJECT_EXISTS
    "object has wrong version",                  // WRONG_VERSION
    "insufficient space to create new table",    // STATUS_NO_TABLE_SPACE
    "RPC request message too short",             // MESSAGE_TOO_SHORT
    "invalid RPC request type",                  // UNIMPLEMENTED_REQUEST
    "RPC request improperly formatted",          // REQUEST_FORMAT_ERROR
    "RPC response improperly formatted",         // RESPONSE_FORMAT_ERROR
    "couldn't connect to RAMCloud cluster"       // COULDNT_CONNECT
};

// The following table maps from a Status value to the internal name
// for the Status.
static const char* symbols[] = {
    "STATUS_OK",
    "STATUS_TABLE_DOESNT_EXIST",
    "STATUS_OBJECT_DOESNT_EXIST",
    "STATUS_OBJECT_EXISTS",
    "STATUS_WRONG_VERSION",
    "STATUS_NO_TABLE_SPACE",
    "STATUS_MESSAGE_TOO_SHORT",
    "STATUS_UNIMPLEMENTED_REQUEST",
    "STATUS_REQUEST_FORMAT_ERROR",
    "STATUS_RESPONSE_FORMAT_ERROR",
    "STATUS_COULDNT_CONNECT"
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
