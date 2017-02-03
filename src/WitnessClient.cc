/* Copyright (c) 2011-2016 Stanford University
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

#include "WitnessClient.h"
#include "Common.h"
#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ServerIdRpcWrapper.h"
#include "ShortMacros.h"
#include "TransportManager.h"
#include "WireFormat.h"
#include "ObjectRpcWrapper.h"


namespace RAMCloud {

/**
 * Start a witness role in a master which will receive new update RPC request
 * for a target master.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param requestTarget
 *      Identifier for the master server being watched over.
 */
uint64_t
WitnessClient::witnessStart(Context* context, ServerId serverId,
        ServerId requestTarget)
{
    WitnessStartRpc rpc(context, serverId, requestTarget);
    return rpc.wait();
}

/**
 * Constructor for WitnessStartRpc: initiates an RPC in the same way as
 * #MasterClient::witnessStart, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param serverId
 *      Identifier for the target server.
 * \param requestTarget
 *      Identifier for the master server being watched over.
 */
WitnessStartRpc::WitnessStartRpc(
        Context* context, ServerId serverId, ServerId requestTarget)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::WitnessStart::Response))
{
    WireFormat::WitnessStart::Request* reqHdr(
            allocHeader<WireFormat::WitnessStart>(serverId));
    reqHdr->targetMasterId = requestTarget.getId();
    send();
}

/**
 * Wait for an WitnessStart RPC to complete.
 *
 * \return
 *      Base pointer for the buffer reserved for witness logging.
 *
 * \throw ServerNotUpException
 *      The target server for this RPC is not part of the cluster;
 *      if it ever existed, it has since crashed.
 */
uint64_t
WitnessStartRpc::wait()
{
    waitAndCheckErrors();
    const WireFormat::WitnessStart::Response* respHdr(
            getResponseHeader<WireFormat::WitnessStart>());
    return respHdr->bufferBasePtr;
}

/**
 * Ask witness for recorded RPC requests for a tablet.
 *
 * \param context
 *      Overall information about this RAMCloud server or client.
 * \param witnessId
 *      Identifier for the witness server who have recorded RPC requests
 *      for the crashed master.
 * \param response
 *      Buffer for RPC response. The lifetime of buffer should be longer than
 *      returned vectors. The return value will have pointers to this buffer.
 * \param crashedServerId
 *      Identifier for the crashed master.
 * \param tableId
 *      Table id of the tablet whose ownership is being reassigned.
 * \param startKeyHash
 *      First key hash that is part of the range of key hashes for the tablet.
 * \param endKeyHash
 *      Last key hash that is part of the range of key hashes for the tablet.
 * \param[in,out] continuation
 *      Continuation handle if the recovery data is too large to fit in a
 *      single RPC. For initial request, value should be 0. After method
 *      returns, continuation will be set to 0 if no more recovery data left
 *      in witness.
 *
 * \return
 *      List of RPC requests recorded in witness.
 */
std::vector<ClientRequest>
WitnessClient::witnessGetData(Context* context, ServerId witnessId,
        Buffer* response, ServerId crashedServerId, uint64_t tableId,
        uint64_t startKeyHash, uint64_t endKeyHash, int16_t* continuation)
{
    std::vector<ClientRequest> requests;
    WitnessGetDataRpc rpc(context, witnessId, response, crashedServerId,
        tableId, startKeyHash, endKeyHash, *continuation);
    rpc.wait(continuation, &requests);
    return requests;
}

WitnessGetDataRpc::WitnessGetDataRpc(Context* context, ServerId witnessId,
        Buffer* response, ServerId crashedServerId, uint64_t tableId,
        uint64_t startKeyHash, uint64_t endKeyHash, int16_t continuation)
    : ServerIdRpcWrapper(context, witnessId,
            sizeof(WireFormat::WitnessGetRecoveryData::Response), response)
{
    WireFormat::WitnessGetRecoveryData::Request* reqHdr(
            allocHeader<WireFormat::WitnessGetRecoveryData>(witnessId));
    reqHdr->crashedMasterId = crashedServerId.getId();
    reqHdr->tableId = tableId;
    reqHdr->firstKeyHash = startKeyHash;
    reqHdr->lastKeyHash = endKeyHash;
    reqHdr->continuation = continuation;
    send();
}

void
WitnessGetDataRpc::wait(int16_t* continuation,
        std::vector<ClientRequest>* requests)
{
    waitAndCheckErrors();
    const WireFormat::WitnessGetRecoveryData::Response* respHdr(
            getResponseHeader<WireFormat::WitnessGetRecoveryData>());
    *continuation = respHdr->continuation;
    uint32_t offset = sizeof32(WireFormat::WitnessGetRecoveryData::Response);
    for (int i = 0; i < respHdr->numOps; ++i) {
        ClientRequest request;
        request.size = *(response->getOffset<int16_t>(offset));
        request.data = response->getRange(offset + 2, request.size);
        offset += 2 + request.size;
        requests->push_back(request);
    }
}

}  // namespace RAMCloud
