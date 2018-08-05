/* Copyright (c) 2010-2016 Stanford University
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

#include "MilliSortClient.h"

namespace RAMCloud {

void
MilliSortClient::initMilliSort(Context* context, ServerId serverId,
        uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
        bool fromClient)
{
    InitMilliSortRpc rpc(context, serverId, dataTuplesPerServer,
            nodesPerPivotServer, fromClient);
    rpc.wait();
}

InitMilliSortRpc::InitMilliSortRpc(Context* context, ServerId serverId,
        uint32_t dataTuplesPerServer, uint32_t nodesPerPivotServer,
        bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::InitMilliSort::Response))
{
    WireFormat::InitMilliSort::Request* reqHdr(
            allocHeader<WireFormat::InitMilliSort>());
    reqHdr->dataTuplesPerServer = dataTuplesPerServer;
    reqHdr->nodesPerPivotServer = nodesPerPivotServer;
    reqHdr->fromClient = fromClient;
    send();
}

void
InitMilliSortRpc::appendRequest(Buffer* request, uint32_t dataTuplesPerServer,
        uint32_t nodesPerPivotServer, bool fromClient)
{
    WireFormat::InitMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::InitMilliSort>(request));
    reqHdr->dataTuplesPerServer = dataTuplesPerServer;
    reqHdr->nodesPerPivotServer = nodesPerPivotServer;
    reqHdr->fromClient = fromClient;
}

int
InitMilliSortRpc::getNodesInited()
{
    return getResponseHeader<WireFormat::InitMilliSort>()->numNodesInited;
}

void
MilliSortClient::startMilliSort(Context* context, ServerId serverId,
        bool fromClient)
{
    StartMilliSortRpc rpc(context, serverId, fromClient);
    rpc.wait();
}

StartMilliSortRpc::StartMilliSortRpc(Context* context, ServerId serverId,
        bool fromClient)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::StartMilliSort::Response))
{
    WireFormat::StartMilliSort::Request* reqHdr(
            allocHeader<WireFormat::StartMilliSort>());
    reqHdr->fromClient = fromClient;
    send();
}

void
StartMilliSortRpc::appendRequest(Buffer* request, bool fromClient)
{
    WireFormat::StartMilliSort::Request* reqHdr(
            RpcWrapper::allocHeader<WireFormat::StartMilliSort>(request));
    reqHdr->fromClient = fromClient;
}

SendDataRpc::SendDataRpc(Context* context, ServerId serverId, uint32_t dataId,
        uint32_t length, const void* data)
    : ServerIdRpcWrapper(context, serverId,
            sizeof(WireFormat::SendData::Response))
{
    appendRequest(&request, dataId, length, data);
    send();
}

void
SendDataRpc::appendRequest(Buffer* request, uint32_t dataId, uint32_t length,
        const void* data)
{
    WireFormat::SendData::Request* reqHdr(
            allocHeader<WireFormat::SendData>(request));
    reqHdr->dataId = dataId;
    request->appendExternal(data, length);
}


}  // namespace RAMCloud
