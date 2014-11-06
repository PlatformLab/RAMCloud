/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Logger.h"
#include "LinearizableObjectRpcWrapper.h"
#include "RamCloud.h"

namespace RAMCloud {

/**
 * Constructor for ObjectRpcWrapper objects.
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param linearizable
 *      RPC will be linearizable if we set this flag true.
 * \param tableId
 *      The table containing the desired object.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It need not be null terminated.  The caller is responsible for ensuring
 *      that this key remains valid until the call completes.
 * \param keyLength
 *      Size in bytes of key.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer. Any existing contents
 *      of this buffer will be cleared automatically by the transport.
 */
LinearizableObjectRpcWrapper::LinearizableObjectRpcWrapper(
        RamCloud* ramcloud, bool linearizable, uint64_t tableId,
        const void* key, uint16_t keyLength, uint32_t responseHeaderLength,
        Buffer* response)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
                       responseHeaderLength, response)
    , linearizabilityOn(linearizable)
    , assignedRpcId(0)
{
}

/**
 * Alternate constructor for ObjectRpcWrapper objects, in which the desired
 * server is specified with a key hash, rather than a key value.
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param linearizable
 *      RPC will be linearizable if we set this flag true.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet (and, hence, the
 *      server storing that tablet).
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked here to ensure that they
 *      contain at least this much data, and a pointer to the header
 *      will be stored in the responseHeader for the use of wrapper
 *      subclasses.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer.
 */
LinearizableObjectRpcWrapper::LinearizableObjectRpcWrapper(
        RamCloud* ramcloud, bool linearizable, uint64_t tableId,
        uint64_t keyHash, uint32_t responseHeaderLength, Buffer* response)
    : ObjectRpcWrapper(ramcloud, tableId, keyHash,
                       responseHeaderLength, response)
    , linearizabilityOn(linearizable)
    , assignedRpcId(0)
{
}

/**
 * Fills request header with linearizability informations.
 * This function should be invoked in the constructor of every linearizable
 * RPCs.
 * \param reqHdr
 *      Pointer to request header to be filled with linearizability info.
 */
template <typename RpcRequest>
void
LinearizableObjectRpcWrapper::fillLinearizabilityHeader(RpcRequest* reqHdr)
{
    if (linearizabilityOn) {
        assignedRpcId = ramcloud->clientContext->rpcTracker->newRpcId();
        reqHdr->lease = ramcloud->clientLease.getLease();
        reqHdr->rpcId = assignedRpcId;
        reqHdr->ackId = ramcloud->clientContext->rpcTracker->ackId();
    } else {
        reqHdr->rpcId = 0; // rpcId starts from 1. 0 means non-linearizable
        reqHdr->ackId = 0;
    }
}

/**
 * Process linearizability information in response header.
 * This funtion should be invoked in the wait() of every linearizable RPCs.
 * \param respHdr
 *      Pointer to response header. Used for checking final RPC status.
 */
template <typename RpcResponse>
void
LinearizableObjectRpcWrapper::handleLinearizabilityResp(
        const RpcResponse* respHdr)
{
    if (linearizabilityOn) {
        if (respHdr->common.status == STATUS_OK) {
           ramcloud->clientContext->rpcTracker->rpcFinished(assignedRpcId);
        }
    }
}

template void LinearizableObjectRpcWrapper::fillLinearizabilityHeader
    <WireFormat::Write::Request>(WireFormat::Write::Request* reqHdr);
template void LinearizableObjectRpcWrapper::handleLinearizabilityResp
    <WireFormat::Write::Response>(const WireFormat::Write::Response* respHdr);

} // namespace RAMCloud
