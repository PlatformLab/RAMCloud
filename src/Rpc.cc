/* Copyright (c) 2011 Stanford University
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

#include "Rpc.h"
#include "Buffer.h"

namespace RAMCloud {

/**
 * Given an RpcOpcode, return a human-readable string containing
 * the symbolic name for the opcode, such as "PING"
 *
 * \param opcode
 *      Identifies the operation requested in an RPC; must be one
 *      of the values defined for RpcOpcode
 *
 * \return
 *      See above.
 */
const char*
Rpc::opcodeSymbol(uint32_t opcode)
{
    switch (opcode) {
        case PING:                       return "PING";
        case PROXY_PING:                 return "PROXY_PING";
        case KILL:                       return "KILL";
        case CREATE_TABLE:               return "CREATE_TABLE";
        case OPEN_TABLE:                 return "OPEN_TABLE";
        case DROP_TABLE:                 return "DROP_TABLE";
        case CREATE:                     return "CREATE";
        case READ:                       return "READ";
        case WRITE:                      return "WRITE";
        case REMOVE:                     return "REMOVE";
        case ENLIST_SERVER:              return "ENLIST_SERVER";
        case GET_SERVER_LIST:            return "GET_SERVER_LIST";
        case GET_TABLET_MAP:             return "GET_TABLET_MAP";
        case SET_TABLETS:                return "SET_TABLETS";
        case RECOVER:                    return "RECOVER";
        case HINT_SERVER_DOWN:           return "HINT_SERVER_DOWN";
        case TABLETS_RECOVERED:          return "TABLETS_RECOVERED";
        case SET_WILL:                   return "SET_WILL";
        case REREPLICATE_SEGMENTS:       return "REREPLICATE_SEGMENTS";
        case FILL_WITH_TEST_DATA:        return "FILL_WITH_TEST_DATA";
        case MULTI_READ:                 return "MULTI_READ";
        case GET_METRICS:                return "GET_METRICS";
        case BACKUP_CLOSE:               return "BACKUP_CLOSE";
        case BACKUP_FREE:                return "BACKUP_FREE";
        case BACKUP_GETRECOVERYDATA:     return "BACKUP_GETRECOVERYDATA";
        case BACKUP_OPEN:                return "BACKUP_OPEN";
        case BACKUP_STARTREADINGDATA:    return "BACKUP_STARTREADINGDATA";
        case BACKUP_WRITE:               return "BACKUP_WRITE";
        case BACKUP_RECOVERYCOMPLETE:    return "BACKUP_RECOVERYCOMPLETE";
        case BACKUP_QUIESCE:             return "BACKUP_QUIESCE";
        case SET_SERVER_LIST:            return "SET_SERVER_LIST";
        case UPDATE_SERVER_LIST:         return "UDPATE_SERVER_LIST";
        case REQUEST_SERVER_LIST:        return "REQUEST_SERVER_LIST";
        case ILLEGAL_RPC_TYPE:           return "ILLEGAL_RPC_TYPE";
    }

    // Never heard of this RPC; return the numeric value. The shared buffer
    // below isn't thread-safe, but the worst that will happen is that the
    // return value will get garbled, and this code should never be executed
    // in a production system anyway.

    static char symbol[50];
    snprintf(symbol, sizeof(symbol), "unknown(%d)", opcode);
    return symbol;
}


/**
 * Given a buffer containing an RPC request, return a human-readable string
 * containing the symbolic name for the request's opcode, such as "PING".
 *
 * \param buffer
 *      Must contain a well-formed RPC request.
 *
 * \return
 *      A symbolic name for the request's opcode.
 */
const char*
Rpc::opcodeSymbol(Buffer& buffer)
{
    const RpcRequestCommon* header = buffer.getStart<RpcRequestCommon>();
    if (header == NULL)
        return "null";
    return opcodeSymbol(header->opcode);
}

}  // namespace RAMCloud
