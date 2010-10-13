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
    , logFD(-1)
    , openSegNum(INVALID_SEGMENT_NUM)
    , seg(0)
{
    static_assert(LOG_SPACE == SEGMENT_FRAMES * SEGMENT_SIZE);

    logFD = open(logPath,
                  O_CREAT | O_RDWR | logOpenFlags,
                  0666);
    if (logFD == -1)
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

    int r = close(logFD);
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
BackupServer::commitSegment(const BackupCommitRequest* reqHdr,
                            BackupCommitResponse* respHdr,
                            Transport::ServerRpc* rpc)
{
    // Write out the current segment to disk if any
    if (reqHdr->segmentNumber == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    else if (reqHdr->segmentNumber != openSegNum)
        throw BackupException("Cannot commit a segment other than the most "
                              "recently written one at the moment");

    LOG(DEBUG, "Now writing to segment %lu", reqHdr->segmentNumber);
    flushSegment();

    // Close the segment
    openSegNum = INVALID_SEGMENT_NUM;
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

    LOG(DEBUG, "Write active segment (%ld) to frame %ld", openSegNum, next);

    segmentAtFrame[next] = openSegNum;
    freeMap.clear(next);

    off_t off = lseek(logFD, segFrameOff(next), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);
    ssize_t r = write(logFD, seg, SEGMENT_SIZE);
    if (r != SEGMENT_SIZE)
        throw BackupLogIOException(errno);

    memset(&seg[0], 0, SEGMENT_SIZE);
}

/**
 * Given a segment id return the segment frame in the backup file
 * where that segment is stored.
 *
 * \param segNum
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
BackupServer::frameForSegNum(uint64_t segNum)
{
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segmentAtFrame[i] == segNum) {
            return i;
        }
    }
    throw BackupException("No such segment stored on backup");
}

/**
 * Removes the specified segment from permanent storage.
 *
 * After this call completes the segment number segNum will no longer
 * be in permanent storage and will not be recovered during recover.
 *
 * \param reqHdr
 *      Header of the Rpc request containing the segment number to free.
 * \param respHdr
 *      Header for the Rpc response.
 * \param rpc
 *      The Rpc being serviced.
 * \exception BackupException if INVALID_SEGMENT_NUM is passed as
 *                segNum.
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
BackupServer::freeSegment(const BackupFreeRequest* reqHdr,
                          BackupFreeResponse* respHdr,
                          Transport::ServerRpc* rpc)
{
    LOG(DEBUG, "Free segment %lu\n", reqHdr->segmentNumber);
    if (reqHdr->segmentNumber == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    // Note - this call throws BackupException if no such segment
    // exists, which is exactly what we want to happen if we are fed a
    // segNum we have no idea about
    uint64_t frameNumber = frameForSegNum(reqHdr->segmentNumber);
    segmentAtFrame[frameNumber] = INVALID_SEGMENT_NUM;
    freeMap.set(frameNumber);
    LOG(DEBUG, "Freed segment in frame %lu\n", frameNumber);
}

/**
 * Return a list of all the segment numbers stored on this server.
 *
 * Calling this method while a segment is active will cause the backup
 * to assume the master has crashed and is trying to recover.  It
 * will, therefore, first commit the current open segment before
 * returning the segment list.
 *
 * \param reqHdr
 *      Header of the Rpc request.
 * \param respHdr
 *      Header for the Rpc response containing the number of segment numbers
 *      being returned following the respHdr.
 * \param rpc
 *      The Rpc being serviced, used to append segment numbers to the response
 *      following respHdr.
 * \bug
 *     Currently this method relies on soft-state on the backup that
 *     isn't recovered after crash
 */
void
BackupServer::getSegmentList(const BackupGetSegmentListRequest* reqHdr,
                             BackupGetSegmentListResponse* respHdr,
                             Transport::ServerRpc* rpc)
{
    if (openSegNum != INVALID_SEGMENT_NUM) {
        LOG(WARNING, "getSegmentList: We must be in recovery, writing out "
                     "current active segment before proceeding");
        // TODO(stutsman) redundant with commitSegment but withouy RPC wrap
        flushSegment();
        // Close the segment
        openSegNum = INVALID_SEGMENT_NUM;
    }

    uint32_t count = 0;
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segmentAtFrame[i] != INVALID_SEGMENT_NUM)
            count++;
    }
    LOG(DEBUG, "Final active segment count to return %u", count);

    uint64_t* list = new(&rpc->replyPayload, APPEND) uint64_t[count];
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segmentAtFrame[i] != INVALID_SEGMENT_NUM) {
            *list = segmentAtFrame[i];
            list++;
        }
    }

    respHdr->segmentListCount = count;
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
        const RpcRequestCommon* header = static_cast
                <const RpcRequestCommon*>(request->getRange(0,
                sizeof(RpcRequestCommon)));
        if (header == NULL) {
            throw MessageTooShortError();
        }
        Metrics::setup(header->perfCounter);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        Buffer* response = &rpc->replyPayload;
        switch (header->type) {

            #define CALL_HANDLER(nameInitialCap, nameLower) {                  \
                nameInitialCap##Response* respHdr =                            \
                        new(response, APPEND) nameInitialCap##Response;        \
                responseCommon = &respHdr->common;                             \
                const nameInitialCap##Request* reqHdr =                        \
                        static_cast<const nameInitialCap##Request*>(           \
                        request->getRange(0, sizeof(                           \
                        nameInitialCap##Request)));                            \
                if (reqHdr == NULL) {                                          \
                    throw MessageTooShortError();                              \
                }                                                              \
                /* Clear the response header, so that unused fields are zero;  \
                 * this makes tests more reproducible, and it is also needed   \
                 * to avoid possible security problems where random server     \
                 * info could leak out to clients through unused packet        \
                 * fields. */                                                  \
                memset(respHdr, 0, sizeof(nameInitialCap##Response));          \
                nameLower(reqHdr, respHdr, rpc);                               \
                break;                                                         \
            }

            case BACKUP_WRITE:
                CALL_HANDLER(BackupWrite, writeSegment);
            case BACKUP_COMMIT:
                CALL_HANDLER(BackupCommit, commitSegment);
            case BACKUP_FREE:
                CALL_HANDLER(BackupFree, freeSegment);
            case BACKUP_GETSEGMENTLIST:
                CALL_HANDLER(BackupGetSegmentList, getSegmentList);
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
    int r = ftruncate(logFD, LOG_SPACE);
    if (r == -1)
        throw BackupLogIOException(errno);
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
 *     If the requested segmentNumber to write is  INVALID_SEGMENT_NUM 
 *     or another segment was written to without a following
 *     commitSegment() before writing to this segNum.
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
BackupServer::writeSegment(const BackupWriteRequest* reqHdr,
                           BackupWriteResponse* respHdr,
                           Transport::ServerRpc* rpc)
{
    if (reqHdr->segmentNumber == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    if (openSegNum == INVALID_SEGMENT_NUM)
        openSegNum = reqHdr->segmentNumber;
    else if (openSegNum != reqHdr->segmentNumber)
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

