/* Copyright (c) 2009 Stanford University
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
 * Implementations for master server-side backup RPC stubs.  The
 * classes herein send requests to the backup servers transparently to
 * handle all the backup needs of the masters.
 */

#include "BackupClient.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Mark.h"
#include "PerfCounterType.h"
#include "Rpc.h"

namespace RAMCloud {

/**
 * Constructor for BackupHost.
 * \param[in] session
 *      The Session by which to communicate with the BackupHost.
 */
BackupHost::BackupHost(Transport::SessionRef session)
        : counterValue(0)
        , session(session)
        , status(STATUS_OK)
{
}

BackupHost::~BackupHost()
{
}

void
BackupHost::commitSegment(uint64_t serverId,
                          uint64_t segmentId)
{
    Buffer req, resp;
    BackupCommitRpc::Request& reqHdr(allocHeader<BackupCommitRpc>(req));
    reqHdr.serverId = serverId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupCommitRpc>(session, req, resp);
    checkStatus();
}

void
BackupHost::freeSegment(uint64_t serverId,
                        uint64_t segmentId)
{
    Buffer req, resp;
    BackupFreeRpc::Request& reqHdr(allocHeader<BackupFreeRpc>(req));
    reqHdr.serverId = serverId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupFreeRpc>(session, req, resp);
    checkStatus();
}

vector<BackupClient::RecoveredObject>
BackupHost::getRecoveryData(uint64_t serverId, const TabletMap& tablets)
{
    Buffer req, resp;
    BackupGetRecoveryDataRpc::Request&
        reqHdr(allocHeader<BackupGetRecoveryDataRpc>(req));
    reqHdr.serverId = serverId;
    // TODO(stutsman) pass tablets argument!
    const BackupGetRecoveryDataRpc::Response&
        respHdr(sendRecv<BackupGetRecoveryDataRpc>(session, req, resp));
    checkStatus();

    uint64_t recoveredObjectCount = respHdr.recoveredObjectCount;
    resp.truncateFront(sizeof(respHdr));
    BackupClient::RecoveredObject const * recoveredObjectsRaw =
        resp.getStart<RecoveredObject>();
    vector<BackupClient::RecoveredObject> objects;
    std::copy(recoveredObjectsRaw,
              recoveredObjectsRaw + recoveredObjectCount,
              objects.begin());
    return objects;
}

void
BackupHost::openSegment(uint64_t serverId,
                        uint64_t segmentId)
{
    Buffer req, resp;
    BackupOpenRpc::Request& reqHdr(allocHeader<BackupOpenRpc>(req));
    reqHdr.serverId = serverId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupOpenRpc>(session, req, resp);
    checkStatus();
}

vector<uint64_t>
BackupHost::startReadingData(uint64_t serverId)
{
    Buffer req, resp;
    BackupStartReadingDataRpc::Request&
        reqHdr(allocHeader<BackupStartReadingDataRpc>(req));
    reqHdr.serverId = serverId;
    // TODO(stutsman) pass tablets argument!
    const BackupStartReadingDataRpc::Response&
        respHdr(sendRecv<BackupStartReadingDataRpc>(session, req, resp));
    checkStatus();

    uint64_t segmentIdCount = respHdr.segmentIdCount;
    resp.truncateFront(sizeof(respHdr));
    uint64_t const * segmentIdsRaw = resp.getStart<uint64_t>();
    vector<uint64_t> segmentIds;
    std::copy(segmentIdsRaw,
              segmentIdsRaw + segmentIdCount,
              segmentIds.begin());
    return segmentIds;
}

void
BackupHost::writeSegment(uint64_t serverId,
                         uint64_t segmentId,
                         uint32_t offset,
                         const void *buf,
                         uint32_t length)
{
    Buffer req, resp;
    BackupWriteRpc::Request& reqHdr(allocHeader<BackupWriteRpc>(req));
    reqHdr.serverId = serverId;
    reqHdr.segmentId = segmentId;
    reqHdr.offset = offset;
    reqHdr.length = length;
    Buffer::Chunk::appendToBuffer(&req, buf, length);
    sendRecv<BackupWriteRpc>(session, req, resp);
    checkStatus();
}

BackupManager::BackupManager()
    : host(0)
{
}

BackupManager::~BackupManager()
{
    if (host)
        delete host;
}

void
BackupManager::addHost(Transport::SessionRef session)
{
    if (host)
        DIE("%s: Adding multiple hosts not implemented", __func__);
    host = new BackupHost(session);
}

void
BackupManager::commitSegment(uint64_t serverId,
                             uint64_t segmentId)
{
    if (host)
        host->commitSegment(serverId, segmentId);
}

void
BackupManager::freeSegment(uint64_t serverId,
                           uint64_t segmentId)
{
    if (host)
        host->freeSegment(serverId, segmentId);
}

vector<BackupClient::RecoveredObject>
BackupManager::getRecoveryData(uint64_t serverId,
                               const TabletMap& tablets)
{
    if (host)
        return host->getRecoveryData(serverId, tablets);
    return vector<BackupClient::RecoveredObject>();
}

void
BackupManager::openSegment(uint64_t serverId,
                           uint64_t segmentId)
{
    if (host)
        host->openSegment(serverId, segmentId);
}

vector<uint64_t>
BackupManager::startReadingData(uint64_t serverId)
{
    if (host)
        return host->startReadingData(serverId);
    return vector<uint64_t>();
}

void
BackupManager::writeSegment(uint64_t serverId,
                            uint64_t segmentId,
                            uint32_t offset,
                            const void *data,
                            uint32_t len)
{
    if (host)
        host->writeSegment(serverId, segmentId, offset, data, len);
}

} // namespace RAMCloud
