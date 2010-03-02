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
 * Implementation of the backup server, currently all backup RPC
 * requests are handled by this module including all the heavy lifting
 * to complete the work requested by the RPCs.
 */

#include <BackupServer.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <Segment.h>
#include <Log.h>

#include <cstdio>

#include <cerrno>

namespace RAMCloud {

const bool debug_rpc = false;
const bool debug_backup = false;

static const uint64_t RESP_BUF_LEN = (1 << 20);

BackupException::~BackupException() {}

BackupServer::BackupServer(Net *net_impl, const char *logPath)
    : net(net_impl), logFD(-1), seg(0), unalignedSeg(0),
      openSegNum(INVALID_SEGMENT_NUM), freeMap(true)
{
    logFD = open(logPath,
                  O_CREAT | O_RDWR | BACKUP_LOG_FLAGS,
                  0666);
    if (logFD == -1)
        throw BackupLogIOException(errno);

    const int pagesize = getpagesize();
    unalignedSeg = reinterpret_cast<char *>(xmalloc(SEGMENT_SIZE + pagesize));
    assert(unalignedSeg != 0);
    seg = reinterpret_cast<char *>(
        ((reinterpret_cast<intptr_t>(unalignedSeg) +
          pagesize - 1) /
         pagesize) * pagesize);

    if (!(BACKUP_LOG_FLAGS & O_DIRECT))
        reserveSpace();

    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++)
        segmentAtFrame[i] = INVALID_SEGMENT_NUM;
}

BackupServer::~BackupServer()
{
    flushSegment();
    free(unalignedSeg);

    int r = close(logFD);
    if (r == -1) {
        // TODO(stutsman) might want to log this
        // but it's probably not a great idea to crash us just because
        // we can't close a file
    }
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
    if (debug_backup)
        printf("Reserving %llu bytes of log space\n", LOG_SPACE);
    int r = ftruncate(logFD, LOG_SPACE);
    if (r == -1)
        throw BackupLogIOException(errno);
}

/**
 * Transmit a backup_rpc.
 *
 * \param[in] rpc
 *     A pointer to the backup_rpc buffer to send.
 */
void
BackupServer::sendRPC(const backup_rpc *rpc)
{
    net->Send(rpc, rpc->hdr.len);
}

/**
 * Receive a backup_rpc.
 *
 * \param[out] rpc
 *     A pointer to a pointer to a backup_rpc struct that is populated
 *     with the location of the incoming backup_rpc message.
 */
void
BackupServer::recvRPC(backup_rpc **rpc)
{
    size_t len = net->Recv(reinterpret_cast<void**>(rpc));
    assert(len == (*rpc)->hdr.len);
}

/**
 * Store an opaque string of bytes in a currently \a open segment on
 * this backup server.
 *
 * \param[in] segNum
 *     the target segment to update
 * \param[in] offset
 *     the offset into this segment
 * \param[in] data
 *     a pointer to the start of the opaque byte string
 * \param[in] len
 *     the size of the byte string
 * \exception BackupException
 *     If INVALID_SEGMENT_NUM is passed as
 *     seg_num or another segment was written to without a following
 *     commitSegment() before writing to this segNum.
 * \exception BackupSegmentOverflowException
 *     If len + offset is beyond the end of the segment.
 * \bug
 *     Currently, the backup only allows writes to a single, open
 *     segment.  If a master tries to write to another segment before
 *     calling commitSegment() the write will fail.
 *     The precise interface for multi-segment operation needs
 *     to be defined.
 */
void
BackupServer::writeSegment(uint64_t segNum,
                           uint32_t offset,
                           const void *data,
                           uint32_t len)
{
    if (segNum == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    if (openSegNum == INVALID_SEGMENT_NUM)
        openSegNum = segNum;
    else if (openSegNum != segNum)
        throw BackupException("Backup server currently doesn't "
                              "allow multiple ongoing segments");

    // Need to check all three conditions because overflow is possible
    // on the addition
    if (len > SEGMENT_SIZE ||
        offset > SEGMENT_SIZE ||
        len + offset > SEGMENT_SIZE)
        throw BackupSegmentOverflowException();
    memcpy(&seg[offset], data, len);
}

/**
 * Given a segment id return the segment frame in the backup file
 * where that segment is stored.
 *
 * \param[in] segNum
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
    struct timeval start, end, res;
    gettimeofday(&start, NULL);

    int64_t next = freeMap.nextSet(0);
    if (next == -1)
        throw BackupException("Out of free segment frames");

    if (debug_backup)
        printf("Write active segment (%ld) to frame %ld\n", openSegNum, next);

    segmentAtFrame[next] = openSegNum;
    freeMap.clear(next);

    off_t off = lseek(logFD, segFrameOff(next), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);
    ssize_t r = write(logFD, seg, SEGMENT_SIZE);
    if (r != SEGMENT_SIZE)
        throw BackupLogIOException(errno);

    memset(&seg[0], 0, SEGMENT_SIZE);

    gettimeofday(&end, NULL);
    timersub(&end, &start, &res);

    if (debug_backup)
        printf("Flush in %d s %d us\n", res.tv_sec, res.tv_usec);
}

/**
 * Commit the specified segment to permanent storage.
 *
 * The backup chooses an empty segment frame, marks it as in-use,
 * an writes the current segment into the chosen segment frame.  After
 * this Write() cannot be called for this segment number any more.
 * The segment will be restored on recovery unless the client later
 * calls Free() on it.
 *
 * \param[in] segNum the segment number to persist and close.
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
BackupServer::commitSegment(uint64_t segNum)
{
    // Write out the current segment to disk if any
    if (segNum == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    else if (segNum != openSegNum)
        throw BackupException("Cannot commit a segment other than the most "
                              "recently written one at the moment");

    if (debug_backup)
        printf(">>> Now writing to segment %lu\n", segNum);
    flushSegment();

    // Close the segment
    openSegNum = INVALID_SEGMENT_NUM;
}

/**
 * Removes the specified segment from permanent storage.
 *
 * After this call completes the segment number segNum will no longer
 * be in permanent storage and will not be recovered during recover.
 *
 * \param[in] segNum the segment number to remove from permanent
 *                storage.
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
BackupServer::freeSegment(uint64_t segNum)
{
    if (debug_backup)
        printf("Free segment %llu\n", segNum);
    if (segNum == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    // Note - this call throws BackupException if no such segment
    // exists, which is exactly what we want to happen if we are fed a
    // segNum we have no idea about
    uint64_t frameNumber = frameForSegNum(segNum);
    segmentAtFrame[frameNumber] = INVALID_SEGMENT_NUM;
    freeMap.set(frameNumber);
    if (debug_backup)
        printf("Freed segment in frame %llu\n", frameNumber);
}

/**
 * Given a segment number return a list of object ids and version
 * numbers that are stored in that segment.
 *
 * Calling this method while a segment is active will cause the backup
 * to assume the master has crashed and is trying to recover.  It
 * will, therefore, first commit the current open segment before
 * returning the segment list.
 *
 * \param[out] list
 *     The place in memory to store the segment ids.
 * \param[in] maxSize
 *     The number of elements that the list buffer can hold.
 * \return
 *     The number of elements actually placed in list.
 * \exception BackupException
 *     If there are more than maxSize segment ids to return.
 * \bug
 *     Currently this method relies on soft-state on the backup that
 *     isn't recovered after crash
 */
uint32_t
BackupServer::getSegmentList(uint64_t *list,
                             uint32_t maxSize)
{
    if (openSegNum != INVALID_SEGMENT_NUM) {
        if (debug_backup)
            printf("!!! GetSegmentList: We must be in recovery, writing out "
                   "current active segment before proceeding\n");
        commitSegment(openSegNum);
    }
    if (debug_backup)
        printf("Max segs to return %llu\n", maxSize);

    uint32_t count = 0;
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segmentAtFrame[i] != INVALID_SEGMENT_NUM) {
            if (count == maxSize)
                throw BackupException("Buffer too short to for segment ids");
            *list = segmentAtFrame[i];
            list++;
            count++;
        }
    }
    if (debug_backup)
        printf("Final active segment count to return %llu\n", count);
    return count;
}

/**
 * Given a pointer to an Object and an offset of where it occurs into
 * its segment return populate meta with the object metadata relevant
 * for master recovery.
 *
 * \param[in] p
 *     A pointer to the Object.
 * \param[in] offset
 *     Its position in its segment.
 * \param[out] meta
 *     A metadata structure to be populated.
 */
void
BackupServer::extractMetadata(const void *p,
                              uint64_t offset,
                              RecoveryObjectMetadata *meta)
{
    const Object *obj = reinterpret_cast<const Object *>(p);
    meta->key = obj->key;
    meta->table = obj->table;
    meta->version = obj->version;
    meta->offset = offset;
    meta->length = obj->data_len;
}

/**
 * Given a segment number return a list of object metadata sufficient
 * for recovery that are stored in that segment.
 *
 * \param[in] segNum
 *     The segment number from which to extract the metadata.
 * \param[out] list
 *     The place to store the metadata.
 * \param[in] maxSize
 *     The number of elements that the list buffer can hold.
 * \return
 *     The number of elements actually placed in list.
 * \exception BackupException
 *     If INVALID_SEGMENT_NUM is passed as seg_num.
 * \exception BackupLogIOException
 *     If there is an error seeking to or reading the segment frame in
 *     the storage.
 * \bug
 *     Currently this method relies on soft-state on the backup that
 *     isn't recovered after crash
 */
uint32_t
BackupServer::getSegmentMetadata(uint64_t segNum,
                                 RecoveryObjectMetadata *list,
                                 uint32_t maxSize)
{
    if (debug_backup)
        printf("Max elements to return %llu\n", maxSize);

    uint32_t count = 0;

    char buf[SEGMENT_SIZE];
    retrieveSegment(segNum, &buf[0]);

    // TODO(stutsman) NULL backup_client is dangerous - we may want to
    // decouple segments from backups somehow
    Segment seg(&buf[0], SEGMENT_SIZE, 0);

    LogEntryIterator iterator(&seg);
    const log_entry *entry;
    const void *p;
    uint64_t offset;

    // Walk the buffer and pull out metadata
    while (iterator.getNextAndOffset(&entry, &p, &offset)) {
        if (entry->type != LOG_ENTRY_TYPE_OBJECT)
            continue;
        RecoveryObjectMetadata *meta = &list[count++];
        extractMetadata(p, offset, meta);
    }

    if (debug_backup)
        printf("Final elements count to return %llu\n", count);
    return count;
}

/**
 * Given a segment number load that segment's data into buf.
 * buf must be allocated to hold SEGMENT_SIZE data.
 *
 * \param[in] segNum
 *     The segment number from which to load into buf.
 * \param[in] buf
 *     The place in memory to store the segment data.
 *     buf must be allocated to hold SEGMENT_SIZE data.
 * \exception BackupException
 *     If INVALID_SEGMENT_NUM is passed as seg_num.
 * \exception BackupLogIOException
 *     If there is an error seeking to or reading the segment frame in
 *     the storage.
 */
void
BackupServer::retrieveSegment(uint64_t segNum, void *buf)
{
    if (debug_backup)
        printf("Retrieving segment %llu from disk\n", segNum);
    if (segNum == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    uint64_t segFrame = frameForSegNum(segNum);
    if (debug_backup)
        printf("Found segment %llu in segment frame %llu\n", segNum,
               segFrame);

    struct timeval start, end, res;
    gettimeofday(&start, NULL);

    if (debug_backup)
        printf("Seeking to %llu\n", segFrameOff(segFrame));
    off_t offset = lseek(logFD, segFrameOff(segFrame), SEEK_SET);
    if (offset == -1)
        throw BackupLogIOException(errno);

    uint64_t readLen = SEGMENT_SIZE;
    if (debug_backup)
        printf("About to read %llu bytes at %llu to %p\n",
               readLen,
               segFrameOff(segFrame),
               buf);
    // if we use O_DIRECT this must be an aligned buffer
    ssize_t r = read(logFD, buf, readLen);
    if (static_cast<uint64_t>(r) != readLen) {
        if (debug_backup)
            printf("Read %ld bytes\n", r);
        throw BackupLogIOException(errno);
    }

    gettimeofday(&end, NULL);
    timersub(&end, &start, &res);
    if (debug_backup)
        printf("Retrieve in %d s %d us\n", res.tv_sec, res.tv_usec);
}

// ---- RPC Dispatch Code ----

/** See writeSegment() */
void
BackupServer::handleWrite(const backup_rpc *req, backup_rpc *resp)
{
    uint64_t seg_num = req->write_req.seg_num;
    uint32_t offset = req->write_req.off;
    uint32_t len = req->write_req.len;
    if (debug_backup)
        printf(">>> Handling Write to offset 0x%x length %d\n", offset, len);
    writeSegment(seg_num, offset, &req->write_req.data[0], len);

    resp->hdr.type = BACKUP_RPC_WRITE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
}

/** See getSegmentList() */
void
BackupServer::handleGetSegmentList(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling GetSegmentList\n");

    resp->getsegmentlist_resp.seg_list_count =
        (RESP_BUF_LEN - sizeof(backup_rpc)) /
        sizeof(uint64_t);
    getSegmentList(resp->getsegmentlist_resp.seg_list,
                   resp->getsegmentlist_resp.seg_list_count);
    if (debug_backup)
        printf(">>>>>> GetSegmentList returning %llu ids\n",
            resp->getsegmentlist_resp.seg_list_count);

    resp->hdr.type = BACKUP_RPC_GETSEGMENTLIST_RESP;
    resp->hdr.len = static_cast<uint32_t>(
        BACKUP_RPC_GETSEGMENTLIST_RESP_LEN_WODATA +
        sizeof(uint64_t) * resp->getsegmentlist_resp.seg_list_count);
}

/** See getSegmentMetadata() */
void
BackupServer::handleGetSegmentMetadata(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling GetSegmentMetadata\n");

    resp->getsegmentmetadata_resp.list_count =
        (RESP_BUF_LEN - sizeof(backup_rpc)) /
        sizeof(RecoveryObjectMetadata);
    getSegmentMetadata(req->getsegmentmetadata_req.seg_num,
                       resp->getsegmentmetadata_resp.list,
                       resp->getsegmentmetadata_resp.list_count);
    if (debug_backup)
        printf(">>>>>> GetSegmentMetadata returning %llu ids\n",
            resp->getsegmentmetadata_resp.list_count);

    resp->hdr.type = BACKUP_RPC_GETSEGMENTMETADATA_RESP;
    resp->hdr.len = static_cast<uint32_t>(
        BACKUP_RPC_GETSEGMENTMETADATA_RESP_LEN_WODATA +
        sizeof(RecoveryObjectMetadata) *
        resp->getsegmentmetadata_resp.list_count);
}

/** See retrieveSegment() */
void
BackupServer::handleRetrieve(const backup_rpc *req, backup_rpc *resp)
{
    const backup_rpc_retrieve_req *rreq = &req->retrieve_req;
    backup_rpc_retrieve_resp *rresp = &resp->retrieve_resp;
    if (debug_backup)
        printf(">>> Handling Retrieve - seg_num %lu\n", rreq->seg_num);

    retrieveSegment(rreq->seg_num, &rresp->data[0]);

    resp->hdr.type = BACKUP_RPC_RETRIEVE_RESP;
    resp->hdr.len = (uint32_t) (BACKUP_RPC_RETRIEVE_RESP_LEN_WODATA +
                                rresp->data_len);
}

/** See commitSegment() */
void
BackupServer::handleCommit(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling Commit - total msg len %lu\n", req->hdr.len);

    commitSegment(req->commit_req.seg_num);

    resp->hdr.type = BACKUP_RPC_COMMIT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_COMMIT_RESP_LEN;
}

/** See freeSegment() */
void
BackupServer::handleFree(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling Free - total msg len %lu\n", req->hdr.len);

    freeSegment(req->free_req.seg_num);

    resp->hdr.type = BACKUP_RPC_FREE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_FREE_RESP_LEN;
}

void
BackupServer::handleHeartbeat(const backup_rpc *req, backup_rpc *resp)
{
    resp->hdr.type = BACKUP_RPC_HEARTBEAT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_HEARTBEAT_RESP_LEN;
}

/**
 * Main dispatch routine for incoming RPC requests.  RPC unboxing
 * happens in the "handle" routines which then call a private method
 * to do all the heavy lifting.
 */
void
BackupServer::handleRPC()
{
    // TODO(stutsman) if we're goingt to have to xmalloc this we should just
    // always keep one around
    char *resp_buf = static_cast<char *>(xmalloc(RESP_BUF_LEN));
    backup_rpc *req;
    backup_rpc *resp = reinterpret_cast<backup_rpc *>(&resp_buf[0]);

    recvRPC(&req);

    if (debug_rpc)
        printf("got rpc type: 0x%08x, len 0x%08x\n",
               req->hdr.type, req->hdr.len);

    try {
        switch ((enum backup_rpc_type) req->hdr.type) {
        case BACKUP_RPC_HEARTBEAT_REQ: handleHeartbeat(req, resp); break;
        case BACKUP_RPC_WRITE_REQ:     handleWrite(req, resp);     break;
        case BACKUP_RPC_COMMIT_REQ:    handleCommit(req, resp);    break;
        case BACKUP_RPC_FREE_REQ:      handleFree(req, resp);      break;
        case BACKUP_RPC_GETSEGMENTLIST_REQ:
            handleGetSegmentList(req, resp);
            break;
        case BACKUP_RPC_GETSEGMENTMETADATA_REQ:
            handleGetSegmentMetadata(req, resp);
            break;
        case BACKUP_RPC_RETRIEVE_REQ:  handleRetrieve(req, resp);  break;

        case BACKUP_RPC_HEARTBEAT_RESP:
        case BACKUP_RPC_WRITE_RESP:
        case BACKUP_RPC_COMMIT_RESP:
        case BACKUP_RPC_FREE_RESP:
        case BACKUP_RPC_GETSEGMENTLIST_RESP:
        case BACKUP_RPC_GETSEGMENTMETADATA_RESP:
        case BACKUP_RPC_RETRIEVE_RESP:
        case BACKUP_RPC_ERROR_RESP:
        default:
            throw BackupInvalidRPCOpException();
        };
    } catch (BackupException e) {
        fprintf(stderr, "Error while processing RPC: %s\n", e.message.c_str());
        size_t emsglen = e.message.length();
        size_t rpclen = BACKUP_RPC_ERROR_RESP_LEN_WODATA + emsglen + 1;
        assert(rpclen <= MAX_RPC_LEN);
        snprintf(&resp->error_resp.message[0],
                 MAX_RPC_LEN - emsglen - 1, "%s", e.message.c_str());
        resp->hdr.type = BACKUP_RPC_ERROR_RESP;
        // TODO(stutsman) this cast is bad, types should match
        resp->hdr.len = static_cast<uint32_t>(rpclen);
    }
    sendRPC(resp);
    free(resp_buf);
}

void __attribute__ ((noreturn))
BackupServer::run()
{
    while (true)
        handleRPC();
}


} // namespace RAMCloud

