/* Copyright (c) 2014-2015 Stanford University
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

#include "ClientLeaseAgent.h"
#include "Logger.h"
#include "LinearizableObjectRpcWrapper.h"
#include "RamCloud.h"
#include "RpcTracker.h"

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
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
                       responseHeaderLength, response)
    , linearizabilityOn(linearizable)
    , ramcloud(ramcloud)
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
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, keyHash,
                       responseHeaderLength, response)
    , linearizabilityOn(linearizable)
    , ramcloud(ramcloud)
    , assignedRpcId(0)
{
}

/**
 * In addition to regular RPC cleanup, it marks the rpc finished in RpcTracker.
 */
LinearizableObjectRpcWrapper::~LinearizableObjectRpcWrapper()
{
    if (linearizabilityOn && assignedRpcId) {
        ramcloud->rpcTracker->rpcFinished(assignedRpcId);
        assignedRpcId = 0;
    }
}

/**
 * In addition to regular RPC cancel, it marks the rpc finished in RpcTracker.
 * Once an RPC is cancelled, it will not be retried nor be finished with wait().
 * So we should acknowledge the RPC regardless of receipt of its response.
 */
void
LinearizableObjectRpcWrapper::cancel()
{
    RpcWrapper::cancel();
    if (linearizabilityOn && assignedRpcId) {
        ramcloud->rpcTracker->rpcFinished(assignedRpcId);
        assignedRpcId = 0;
    }
}

/**
 * Indicates whether a response has been received for an RPC.  Used for
 * asynchronous processing of RPCs.  Calling this method will also ensure
 * the ClientLease remains valid.
 *
 * \return
 *      True means that the RPC has finished or been canceled; #wait will
 *      not block.  False means that the RPC is still being processed.
 */
bool
LinearizableObjectRpcWrapper::isReady()
{
    // Poke the client lease to keep it valid.
    ramcloud->clientLeaseAgent->poll();
    return RpcWrapper::isReady();
}

/**
 * Fills request header with linearizability information.
 * This function should be invoked in the constructor of every linearizable
 * RPC.
 * \param reqHdr
 *      Pointer to request header to be filled with linearizability info.
 */
template <typename RpcRequest>
void
LinearizableObjectRpcWrapper::fillLinearizabilityHeader(RpcRequest* reqHdr)
{
    if (linearizabilityOn) {
        assignedRpcId = ramcloud->rpcTracker->newRpcId(this);
        assert(assignedRpcId);
        reqHdr->lease = ramcloud->clientLeaseAgent->getLease();
        reqHdr->rpcId = assignedRpcId;
        reqHdr->ackId = ramcloud->rpcTracker->ackId();
    } else {
        reqHdr->rpcId = 0; // rpcId starts from 1. 0 means non-linearizable
        reqHdr->ackId = 0;
    }
}

/**
 * Overrides RpcWrapper::waitInternal.
 * Call #RpcWrapper::waitInternal and process linearizability information.
 * Marks the current RPC as finished in RpcTracker.
 *
 * \param dispatch
 *      Dispatch to use for polling while waiting.
 * \param abortTime
 *      If Cycles::rdtsc() exceeds this time then return even if the
 *      RPC has not completed. All ones means wait forever.
 *
 * \return
 *      The return value is true if the RPC completed or failed, and false if
 *      abortTime passed with no response yet.
 *
 * \throw RpcCanceledException
 *      The RPC has previously been canceled, so it doesn't make sense
 *      to wait for it.
 */
bool
LinearizableObjectRpcWrapper::waitInternal(Dispatch* dispatch,
                                           uint64_t abortTime)
{
    if (!ObjectRpcWrapper::waitInternal(dispatch, abortTime)) {
        return false; // Aborted by timeout. Shouldn't process RPC's response.
    }
    if (!linearizabilityOn || !assignedRpcId) {
        return true;
    }

#ifdef DEBUG_BUILD
    RpcState state = getState();
    assert(state == FINISHED || state == CANCELED);
#endif

    ramcloud->rpcTracker->rpcFinished(assignedRpcId);
    assignedRpcId = 0;
    return true;
    }

// See RpcTracker::TrackedRpc for documentation.
void LinearizableObjectRpcWrapper::tryFinish()
{
    RAMCLOUD_TEST_LOG("called");
    // In this case we might as well wait.
    waitInternal(ramcloud->clientContext->dispatch);
}

/*
 * Following line is necessary to tell compiler to instantiate the templatized
 * method "fillLinearizabilityHeader" with WireFormat::Write::Request, etc.
 * This manual instantiation is necessary if the method is defined in cc file.
 */
template void LinearizableObjectRpcWrapper::fillLinearizabilityHeader
    <WireFormat::Write::Request>(WireFormat::Write::Request* reqHdr);
template void LinearizableObjectRpcWrapper::fillLinearizabilityHeader
    <WireFormat::Increment::Request>(WireFormat::Increment::Request* reqHdr);
template void LinearizableObjectRpcWrapper::fillLinearizabilityHeader
    <WireFormat::Remove::Request>(WireFormat::Remove::Request* reqHdr);

} // namespace RAMCloud
