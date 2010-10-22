/* Copyright (c) 2009-2010 Stanford University
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

#include "BackupClient.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Mark.h"
#include "PerfCounterType.h"
#include "Rpc.h"

namespace RAMCloud {

/**
 * Create a BackupHost.
 * \param session
 *      The Session by which to communicate with the backup server.
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

// See BackupClient::commitSegment().
void
BackupHost::commitSegment(uint64_t masterId,
                          uint64_t segmentId)
{
    Buffer req, resp;
    BackupCommitRpc::Request& reqHdr(allocHeader<BackupCommitRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupCommitRpc>(session, req, resp);
    checkStatus();
}

// See BackupClient::freeSegment().
void
BackupHost::freeSegment(uint64_t masterId,
                        uint64_t segmentId)
{
    Buffer req, resp;
    BackupFreeRpc::Request& reqHdr(allocHeader<BackupFreeRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupFreeRpc>(session, req, resp);
    checkStatus();
}

// See BackupClient::getRecoveryData().
vector<BackupClient::RecoveredObject>
BackupHost::getRecoveryData(uint64_t masterId, const TabletMap& tablets)
{
    Buffer req, resp;
    BackupGetRecoveryDataRpc::Request&
        reqHdr(allocHeader<BackupGetRecoveryDataRpc>(req));
    reqHdr.masterId = masterId;
    // TODO(stutsman) pass tablets argument!
    // TODO(stutsman) complete unit test
    const BackupGetRecoveryDataRpc::Response&
        respHdr(sendRecv<BackupGetRecoveryDataRpc>(session, req, resp));
    checkStatus();

    uint64_t recoveredObjectCount = respHdr.recoveredObjectCount;
    resp.truncateFront(sizeof(respHdr));
    BackupClient::RecoveredObject const * recoveredObjectsRaw =
        resp.getStart<RecoveredObject>();
    return vector<BackupClient::RecoveredObject>(recoveredObjectsRaw,
                                                 recoveredObjectsRaw +
                                                    recoveredObjectCount);
}

// See BackupClient::openSegment().
void
BackupHost::openSegment(uint64_t masterId,
                        uint64_t segmentId)
{
    Buffer req, resp;
    BackupOpenRpc::Request& reqHdr(allocHeader<BackupOpenRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    sendRecv<BackupOpenRpc>(session, req, resp);
    checkStatus();
}

// See BackupClient::startReadingData().
vector<uint64_t>
BackupHost::startReadingData(uint64_t masterId)
{
    Buffer req, resp;
    BackupStartReadingDataRpc::Request&
        reqHdr(allocHeader<BackupStartReadingDataRpc>(req));
    reqHdr.masterId = masterId;
    const BackupStartReadingDataRpc::Response&
        respHdr(sendRecv<BackupStartReadingDataRpc>(session, req, resp));
    checkStatus();

    uint64_t segmentIdCount = respHdr.segmentIdCount;
    resp.truncateFront(sizeof(respHdr));
    uint64_t const * segmentIdsRaw = resp.getStart<uint64_t>();
    return vector<uint64_t>(segmentIdsRaw, segmentIdsRaw + segmentIdCount);
}

// See BackupClient::writeSegment().
void
BackupHost::writeSegment(uint64_t masterId,
                         uint64_t segmentId,
                         uint32_t offset,
                         const void *buf,
                         uint32_t length)
{
    Buffer req, resp;
    BackupWriteRpc::Request& reqHdr(allocHeader<BackupWriteRpc>(req));
    reqHdr.masterId = masterId;
    reqHdr.segmentId = segmentId;
    reqHdr.offset = offset;
    reqHdr.length = length;
    Buffer::Chunk::appendToBuffer(&req, buf, length);
    sendRecv<BackupWriteRpc>(session, req, resp);
    checkStatus();
}

// --- BackupManager ---

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with.  See addHost() to add remote backups.
 */
BackupManager::BackupManager()
    : host(0)
{
}

/// Free up all BackupHosts.
BackupManager::~BackupManager()
{
    if (host)
        delete host;
}

/**
 * \param session
 *      The session over which to communicate with the backup.  See
 *      TransportManager.
 */
void
BackupManager::addHost(Transport::SessionRef session)
{
    if (host)
        DIE("%s: Adding multiple hosts not implemented", __func__);
    host = new BackupHost(session);
}

// See BackupClient::commitSegment.
void
BackupManager::commitSegment(uint64_t masterId,
                             uint64_t segmentId)
{
    if (host)
        host->commitSegment(masterId, segmentId);
}

// See BackupClient::freeSegment.
void
BackupManager::freeSegment(uint64_t masterId,
                           uint64_t segmentId)
{
    if (host)
        host->freeSegment(masterId, segmentId);
}

// See BackupClient::getRecoveryData.
vector<BackupClient::RecoveredObject>
BackupManager::getRecoveryData(uint64_t masterId,
                               const TabletMap& tablets)
{
    if (host)
        return host->getRecoveryData(masterId, tablets);
    return vector<BackupClient::RecoveredObject>();
}

// See BackupClient::openSegment.
void
BackupManager::openSegment(uint64_t masterId,
                           uint64_t segmentId)
{
    if (host)
        host->openSegment(masterId, segmentId);
}

// See BackupClient::startReadingData.
vector<uint64_t>
BackupManager::startReadingData(uint64_t masterId)
{
    if (host)
        return host->startReadingData(masterId);
    return vector<uint64_t>();
}

// See BackupClient::writeSegment.
void
BackupManager::writeSegment(uint64_t masterId,
                            uint64_t segmentId,
                            uint32_t offset,
                            const void *data,
                            uint32_t len)
{
    if (host)
        host->writeSegment(masterId, segmentId, offset, data, len);
}

} // namespace RAMCloud
