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
 * Implementation of the backup server, currently all backup Rpc
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the Rpcs.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cerrno>

#include "BackupServer.h"
#include "Buffer.h"
#include "ClientException.h"
#include "Log.h"
#include "Metrics.h"
#include "Rpc.h"
#include "Segment.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Create a BackupServer which is ready to run().
 *
 * \param logPath
 *      The file in which to store the local log on disk.
 * \param logOpenFlags
 *      Extra options for the log file open
 *      (most likely O_DIRECT or the default, 0).
 */
BackupServer::BackupServer(const char *logPath, int logOpenFlags)
    : freeMap(true)
    , logFd(-1)
    , openSegmentId(INVALID_SEGMENT_NUM)
    , seg(0)
{
    static_assert(LOG_SPACE == SEGMENT_FRAMES * SEGMENT_SIZE);

    logFd = open(logPath,
                  O_CREAT | O_RDWR | logOpenFlags,
                  0666);
    if (logFd == -1)
        throw BackupLogIOException(errno);

    seg = static_cast<char*>(xmemalign(getpagesize(), SEGMENT_SIZE));

    if (!(logOpenFlags & O_DIRECT))
        reserveSpace();

    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++)
        segmentAtFrame[i] = INVALID_SEGMENT_NUM;
}

/**
 * Flush open segments to disk and shutdown.
 */
BackupServer::~BackupServer()
{
    flushSegment();
    free(seg);

    int r = close(logFd);
    if (r == -1)
        LOG(ERROR, "Couldn't close backup log");
}

/**
 * Accept and dispatch RPCs until the end of time.
 */
void __attribute__ ((noreturn))
BackupServer::run()
{
    while (true)
        handleRpc();
}

// - private -

/**
 * Commit the specified segment to permanent storage.
 *
 * The backup chooses an empty segment frame, marks it as in-use,
 * an writes the current segment into the chosen segment frame.  After
 * this Write() cannot be called for this segment number any more.
 * The segment will be restored on recovery unless the client later
 * calls Free() on it.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to commit.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 * \exception BackupException if INVALID_SEGMENT_NUM is passed as
 *                seg_num.
 * \exception BackupException if seg_num passed is not the active
 *                segment number.
 * \exception BackupLogIOException if there are no free segment frames
 *                on the storage.  Indicates the backup's storage file
 *                is full.
 * \exception BackupLogIOException if there is an error seeking to the
 *                segment frame in the storage.
 * \exception BackupLogIOException if there is an error writing to the
 *                segment frame in the storage.
 * \bug Currently, the backup only allows writes to a single, open
 * segment.  Flush needs to take an argument to identify which segment
 * to flush. The precise interface for multi-segment operation needs
 * to be defined.
 */
void
BackupServer::commitSegment(const BackupCommitRpc::Request* reqHdr,
                            BackupCommitRpc::Response* respHdr,
                            Transport::ServerRpc* rpc)
{
    // Write out the current segment to disk if any
    if (reqHdr->segmentId == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    else if (reqHdr->segmentId != openSegmentId)
        throw BackupException("Cannot commit a segment other than the most "
                              "recently written one at the moment");

    LOG(DEBUG, "Now writing to segment %lu", reqHdr->segmentId);
    flushSegment();

    // Close the segment
    openSegmentId = INVALID_SEGMENT_NUM;
}

/**
 * Pure function.  Given a segment frame number return the position in
 * the storage where the segment frame begins.
 *
 * \param[in] segFrame
 *     The target segment frame for which the file address is needed
 * \return
 *     The offset into the backup file where that segment frame
 *     begins.
 */
static inline uint64_t
segFrameOff(uint64_t segFrame)
{
    return segFrame * SEGMENT_SIZE;
}

/**
 * Flush the active segment to permanent storage.
 *
 * The backup chooses an empty segment frame, marks it as in-use,
 * an writes the current segment into the chosen segment frame.  This
 * function does none of the bookkeeping associated with clients,
 * rather it is used internally by other methods (segmentCommit())
 * so that they can focus on such details.
 *
 * \exception BackupLogIOException
 *     if there are no free segment frames on the storage.  Indicates
 *     the backup's storage file is full.
 * \exception BackupLogIOException
 *     If there is an error seeking to the segment frame in the storage.
 * \exception BackupLogIOException
 *     If there is an error writing to the segment frame in the storage.
 * \bug
 *     Currently, the backup only allows writes to a single, open
 *     segment.  Flush needs to take an argument to identify which segment
 *     to flush. The precise interface for multi-segment operation needs
 *     to be defined.
 * \bug
 *     Flush shouldn't fail on account of no free frames.  We
 *     shouldn't allow a new segment to be opened in the first place.
 *     That means the write should fail instead.
 */
void
BackupServer::flushSegment()
{
    int64_t next = freeMap.nextSet(0);
    if (next == -1)
        throw BackupException("Out of free segment frames");

    LOG(DEBUG, "Write active segment (%ld) to frame %ld", openSegmentId, next);

    segmentAtFrame[next] = openSegmentId;
    freeMap.clear(next);

    off_t off = lseek(logFd, segFrameOff(next), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);
    ssize_t r = write(logFd, seg, SEGMENT_SIZE);
    if (r != SEGMENT_SIZE)
        throw BackupLogIOException(errno);

    memset(&seg[0], 0, SEGMENT_SIZE);
}

/**
 * Given a segment id return the segment frame in the backup file
 * where that segment is stored.
 *
 * \param segmentId
 *     The target segment for which the frame number
 *     is needed.
 * \return
 *     The frame number in the backup file where that
 *     segment id is stored.
 * \exception BackupException
 *     If no such segment id is stored in the file.
 * \bug Linear search, we should consider some other type of data
 *      structure to make this efficient when the number of
 *      segment frames gets large
 */
uint64_t
BackupServer::frameForSegmentId(uint64_t segmentId)
{
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segmentAtFrame[i] == segmentId) {
            return i;
        }
    }
    throw BackupException("No such segment stored on backup");
}

/**
 * Removes the specified segment from permanent storage.
 *
 * After this call completes the segment number segmentId will no longer
 * be in permanent storage and will not be recovered during recover.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 * \exception BackupException if INVALID_SEGMENT_NUM is passed as
 *                segmentId.
 * \exception BackupLogIOException if there are no free segment frames
 *                on the storage.  Indicates the backup's storage file
 *                is full.
 * \exception BackupLogIOException if there is an error seeking to the
 *                segment frame in the storage.
 * \exception BackupLogIOException if there is an error writing to the
 *                segment frame in the storage.
 * \bug Currently, the backup only allows writes to a single, open
 * segment.  Flush needs to take an argument to identify which segment
 * to flush. The precise interface for multi-segment operation needs
 * to be defined.
 */
void
BackupServer::freeSegment(const BackupFreeRpc::Request* reqHdr,
                          BackupFreeRpc::Response* respHdr,
                          Transport::ServerRpc* rpc)
{
    LOG(DEBUG, "Free segment %lu\n", reqHdr->segmentId);
    if (reqHdr->segmentId == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    // Note - this call throws BackupException if no such segment
    // exists, which is exactly what we want to happen if we are fed a
    // segmentId we have no idea about
    uint64_t frameNumber = frameForSegmentId(reqHdr->segmentId);
    segmentAtFrame[frameNumber] = INVALID_SEGMENT_NUM;
    freeMap.set(frameNumber);
    LOG(DEBUG, "Freed segment in frame %lu\n", frameNumber);
}

/**
 * Return the data for a particular tablet that was recovered by a call
 * to startReadingData().
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of RecoveredObjects being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of RecoveredObjects
 *      follows the respHdr in this buffer of length
 *      respHdr->recoveredObjectCount.
 */
void
BackupServer::getRecoveryData(const BackupGetRecoveryDataRpc::Request* reqHdr,
                              BackupGetRecoveryDataRpc::Response* respHdr,
                              Transport::ServerRpc* rpc)
{
    LOG(ERROR, "Unimplemented: %s", __func__);
    throw UnimplementedRequestError();
}

/**
 * Wait for an incoming RPC request, handle it, and return after
 * sending a response.
 */
void
BackupServer::handleRpc()
{
    Transport::ServerRpc* rpc = transportManager.serverRecv();
    Buffer* request = &rpc->recvPayload;
    RpcResponseCommon* responseCommon = NULL;
    try {
        const RpcRequestCommon* header = request->getStart<RpcRequestCommon>();
        if (header == NULL) {
            throw MessageTooShortError();
        }
        Metrics::setup(header->perfCounter);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        Buffer* response = &rpc->replyPayload;
        switch (header->type) {

            #define CALL_HANDLER(Rpc, handler) {                               \
                Rpc::Response* respHdr = new(response, APPEND) Rpc::Response;  \
                responseCommon = &respHdr->common;                             \
                const Rpc::Request* reqHdr =                                   \
                        request->getStart<Rpc::Request>();                     \
                if (reqHdr == NULL) {                                          \
                    throw MessageTooShortError();                              \
                }                                                              \
                /* Clear the response header, so that unused fields are zero;  \
                 * this makes tests more reproducible, and it is also needed   \
                 * to avoid possible security problems where random server     \
                 * info could leak out to clients through unused packet        \
                 * fields. */                                                  \
                memset(respHdr, 0, sizeof(Rpc::Response));                     \
                handler(reqHdr, respHdr, rpc);                                 \
                break;                                                         \
            }

            case BACKUP_COMMIT:
                CALL_HANDLER(BackupCommitRpc, commitSegment);
            case BACKUP_FREE:
                CALL_HANDLER(BackupFreeRpc, freeSegment);
            case BACKUP_GETRECOVERYDATA:
                CALL_HANDLER(BackupGetRecoveryDataRpc, getRecoveryData);
            case BACKUP_OPEN:
                CALL_HANDLER(BackupOpenRpc, openSegment);
            case BACKUP_STARTREADINGDATA:
                CALL_HANDLER(BackupStartReadingDataRpc, startReadingData);
            case BACKUP_WRITE:
                CALL_HANDLER(BackupWriteRpc, writeSegment);
            default:
                throw UnimplementedRequestError();
        }
    } catch (ClientException& e) {
        Buffer* response = &rpc->replyPayload;
        if (responseCommon == NULL) {
            responseCommon = new(response, APPEND) RpcResponseCommon;
        }
        responseCommon->status = e.status;
    }
    Metrics::mark(MARK_RPC_PROCESSING_END);
    responseCommon->counterValue = Metrics::read();
    rpc->sendReply();
}

/**
 * Allocate space to receive backup writes for a segment.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 */
void
BackupServer::openSegment(const BackupOpenRpc::Request* reqHdr,
                          BackupOpenRpc::Response* respHdr,
                          Transport::ServerRpc* rpc)
{
    LOG(ERROR, "Unimplemented: %s", __func__);
    if (reqHdr->segmentId == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");

    // Allocate a segment or pull it from pool.
    // Enter it into the table of open segments.
    throw UnimplementedRequestError();
}

/**
 * Reserves the full backup space needed on disk during operation.
 *
 * This is intended for use when the backup is running against a
 * normal file rather than a block device.
 *
 * \exception BackupLogIOException thrown containing both the errno
 * and error message from the call to ftruncate if the space cannot be
 * reserved
 */
void
BackupServer::reserveSpace()
{
    LOG(DEBUG, "Reserving %lu bytes of log space\n", LOG_SPACE);
    int r = ftruncate(logFd, LOG_SPACE);
    if (r == -1)
        throw BackupLogIOException(errno);
}

/**
 * Begin reading disk data for a Master and bucketing the objects in the
 * Segments according to a requested TabletMap, returning a list of backed
 * up Segments this backup has for the Master.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments.
 * \param respHdr
 *      Header for the Rpc response containing a count of segmentIds being
 *      returned.
 * \param rpc
 *      The Rpc being serviced.  A back-to-back list of uint64_t segmentIds
 *      follows the respHdr in this buffer of length respHdr->segmentIdCount.
 */
void
BackupServer::startReadingData(const BackupStartReadingDataRpc::Request* reqHdr,
                               BackupStartReadingDataRpc::Response* respHdr,
                               Transport::ServerRpc* rpc)
{
    LOG(ERROR, "Unimplemented: %s", __func__);
    throw UnimplementedRequestError();
}

/**
 * Store an opaque string of bytes in a currently \a open segment on
 * this backup server.
 *
 * \param reqHdr
 *      Header of the Rpc request which contains the Rpc arguments except
 *      the data to be written.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced, used for access to the opaque bytes to
 *      be written which follow reqHdr.
 * \exception BackupException
 *     If the requested segmentId to write is  INVALID_SEGMENT_NUM
 *     or another segment was written to without a following
 *     commitSegment() before writing to this segmentId.
 * \exception BackupSegmentOverflowException
 *     If the write request is beyond the end of the segment.
 * \bug
 *     Currently, the backup only allows writes to a single, open
 *     segment.  If a master tries to write to another segment before
 *     calling commitSegment() the write will fail.
 *     The precise interface for multi-segment operation needs
 *     to be defined.
 */
void
BackupServer::writeSegment(const BackupWriteRpc::Request* reqHdr,
                           BackupWriteRpc::Response* respHdr,
                           Transport::ServerRpc* rpc)
{
    LOG(DEBUG, "Writing %u bytes at %u to %lu:%lu", reqHdr->length,
                                                    reqHdr->offset,
                                                    reqHdr->serverId,
                                                    reqHdr->segmentId);
    if (reqHdr->segmentId == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    if (openSegmentId == INVALID_SEGMENT_NUM)
        openSegmentId = reqHdr->segmentId;
    else if (openSegmentId != reqHdr->segmentId)
        throw BackupException("Backup server currently doesn't "
                              "allow multiple ongoing segments");

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (reqHdr->length > SEGMENT_SIZE ||
        reqHdr->offset > SEGMENT_SIZE ||
        reqHdr->length + reqHdr->offset > SEGMENT_SIZE)
        throw BackupSegmentOverflowException();
    memcpy(&seg[reqHdr->offset],
           &rpc->recvPayload + sizeof(*reqHdr),
           reqHdr->length);
}

} // namespace RAMCloud

