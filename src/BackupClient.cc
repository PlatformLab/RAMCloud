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
BackupHost::writeSegment(uint64_t segNum,
                         uint32_t offset,
                         const void *data,
                         uint32_t len)
{
    Buffer req, resp;
    BackupWriteRequest* reqHdr;
    const BackupWriteResponse* respHdr;

    reqHdr = new(&req, APPEND) BackupWriteRequest;
    reqHdr->common.type = BACKUP_WRITE;
    reqHdr->common.perfCounter.beginMark = MARK_NONE;
    reqHdr->common.perfCounter.endMark = MARK_NONE;
    reqHdr->common.perfCounter.counterType = PERF_COUNTER_TSC;
    reqHdr->segmentNumber = segNum;
    reqHdr->offset = offset;
    reqHdr->length = len;
    Buffer::Chunk::appendToBuffer(&req, data, len);
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const BackupWriteResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL)
        throwShortResponseError(&resp);
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK)
        ClientException::throwException(status);
}

void
BackupHost::freeSegment(uint64_t segNum)
{
    Buffer req, resp;
    BackupFreeRequest* reqHdr;
    const BackupFreeResponse* respHdr;

    reqHdr = new(&req, APPEND) BackupFreeRequest;
    reqHdr->common.type = BACKUP_FREE;
    reqHdr->common.perfCounter.beginMark = MARK_NONE;
    reqHdr->common.perfCounter.endMark = MARK_NONE;
    reqHdr->common.perfCounter.counterType = PERF_COUNTER_TSC;
    reqHdr->segmentNumber = segNum;
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const BackupFreeResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL)
        throwShortResponseError(&resp);
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK)
        ClientException::throwException(status);
}

void
BackupHost::commitSegment(uint64_t segNum)
{
    Buffer req, resp;
    BackupFreeRequest* reqHdr;
    const BackupFreeResponse* respHdr;

    reqHdr = new(&req, APPEND) BackupFreeRequest;
    reqHdr->common.type = BACKUP_COMMIT;
    reqHdr->common.perfCounter.beginMark = MARK_NONE;
    reqHdr->common.perfCounter.endMark = MARK_NONE;
    reqHdr->common.perfCounter.counterType = PERF_COUNTER_TSC;
    reqHdr->segmentNumber = segNum;
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const BackupFreeResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL)
        throwShortResponseError(&resp);
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK)
        ClientException::throwException(status);
}

uint32_t
BackupHost::getSegmentList(uint64_t *list,
                           uint32_t maxSize)
{
    Buffer req, resp;
    BackupGetSegmentListRequest* reqHdr;
    const BackupGetSegmentListResponse* respHdr;

    reqHdr = new(&req, APPEND) BackupGetSegmentListRequest;
    reqHdr->common.type = BACKUP_GETSEGMENTLIST;
    reqHdr->common.perfCounter.beginMark = MARK_NONE;
    reqHdr->common.perfCounter.endMark = MARK_NONE;
    reqHdr->common.perfCounter.counterType = PERF_COUNTER_TSC;
    Buffer::Chunk::appendToBuffer(&req, list, maxSize * sizeof(*list));
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const BackupGetSegmentListResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL)
        throwShortResponseError(&resp);
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK)
        ClientException::throwException(status);

    uint32_t segmentListCount = respHdr->segmentListCount;
    if (maxSize < segmentListCount)
        throw BackupClientException("Provided a segment id buffer "
                                    "that was too small");

    const uint64_t* tmpList = static_cast<const uint64_t*>(
             resp.getRange(0, segmentListCount * sizeof(*list)));
    memcpy(list, tmpList, segmentListCount * sizeof(*list));

    return segmentListCount;
}

/**
 * Generate an appropriate exception when an RPC response arrives that
 * is too short to hold the full response header expected for this RPC.
 * If this method is invoked it means the server found a problem before
 * dispatching to a type-specific handler (e.g. the server didn't understand
 * the RPC's type, or basic authentication failed).
 *
 * \param response
 *      Contains the full response message from the server.
 *
 * \exception ClientException
 *      This method always throws an exception; it never returns.
 *      The exact type of the exception will depend on the status
 *      value present in the packet (if any).
 */
void
BackupHost::throwShortResponseError(Buffer* response)
{
    const RpcResponseCommon* common =
            static_cast<const RpcResponseCommon*>(
            response->getRange(0, sizeof(RpcResponseCommon)));
    if (common != NULL) {
        counterValue = common->counterValue;
        if (common->status == STATUS_OK) {
            // This makes no sense: the server claims to have handled
            // the RPC correctly, but it didn't return the right size
            // response for this RPC; report an error.
            status = STATUS_RESPONSE_FORMAT_ERROR;
        } else {
            status = common->status;
        }
    } else {
        // The packet wasn't even long enough to hold a standard
        // header.
        status = STATUS_RESPONSE_FORMAT_ERROR;
    }
    ClientException::throwException(status);
}

MultiBackupClient::MultiBackupClient()
    : host(0)
{
}

MultiBackupClient::~MultiBackupClient()
{
    if (host)
        delete host;
}

void
MultiBackupClient::addHost(Transport::SessionRef session)
{
    if (host)
        throw BackupClientException("Only one backup host currently supported");
    host = new BackupHost(session);
}

void
MultiBackupClient::writeSegment(uint64_t segNum,
                                uint32_t offset,
                                const void *data,
                                uint32_t len)
{
    if (host)
        host->writeSegment(segNum, offset, data, len);
}

void
MultiBackupClient::commitSegment(uint64_t segNum)
{
    if (host)
        host->commitSegment(segNum);
}

void
MultiBackupClient::freeSegment(uint64_t segNum)
{
    if (host)
        host->freeSegment(segNum);
}

uint32_t
MultiBackupClient::getSegmentList(uint64_t *list,
                                  uint32_t maxSize)
{
    if (host) {
        return host->getSegmentList(list, maxSize);
    }
    return 0;
}

} // namespace RAMCloud
