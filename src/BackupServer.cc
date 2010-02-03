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

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <config.h>
#include <malloc.h>

#include <BackupServer.h>

#include <shared/backuprpc.h>
#include <shared/Segment.h>
#include <shared/Log.h>

#include <cstdio>
#include <cassert>

#include <cerrno>

namespace RAMCloud {

enum { debug_rpc = false };
enum { debug_backup = false };

static const uint64_t RESP_BUF_LEN = (1 << 20);

BackupException::~BackupException() {}

BackupServer::BackupServer(Net *net_impl, const char *logPath)
    : net(net_impl), log_fd(-1), seg(0), unaligned_seg(0),
      seg_num(INVALID_SEGMENT_NUM), free_map(true)
{
    log_fd = open(logPath,
                  O_CREAT | O_RDWR | BACKUP_LOG_FLAGS,
                  0666);
    if (log_fd == -1)
        throw BackupLogIOException(errno);

    const int pagesize = getpagesize();
    unaligned_seg = reinterpret_cast<char *>(malloc(SEGMENT_SIZE + pagesize));
    assert(unaligned_seg);
    seg = reinterpret_cast<char *>(((reinterpret_cast<intptr_t>(unaligned_seg) +
                                     pagesize - 1) /
                                    pagesize) * pagesize);

    if (!(BACKUP_LOG_FLAGS & O_DIRECT))
        ReserveSpace();

    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++)
        segments[i] = INVALID_SEGMENT_NUM;
}

BackupServer::~BackupServer()
{
    Flush();
    free(unaligned_seg);

    int r = close(log_fd);
    if (r == -1) {
        // TODO(stutsman) need to check to see if we are aborting already
        // is this really worth throwing over?  It'll likely cause an abort
        throw BackupLogIOException(errno);
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
BackupServer::ReserveSpace()
{
    if (debug_backup)
        printf("Reserving %llu bytes of log space\n", LOG_SPACE);
    int r = ftruncate(log_fd, LOG_SPACE);
    if (r == -1)
        throw BackupLogIOException(errno);
}

void
BackupServer::SendRPC(struct backup_rpc *rpc)
{
    net->Send(rpc, rpc->hdr.len);
}

void
BackupServer::RecvRPC(struct backup_rpc **rpc)
{
    size_t len = net->Recv(reinterpret_cast<void**>(rpc));
    assert(len == (*rpc)->hdr.len);
}

/**
 * Store an opaque string of bytes in a currently \a open segment on
 * this backup server.
 *
 * \param[in]  seg_num  the target segment to update
 * \param[in]  off      the offset into this segment
 * \param[in]  data     a pointer to the start of the opaque byte
 *                      string
 * \param[in]  len      the size of the byte string
 * \exception  BackupException if INVALID_SEGMENT_NUM is passed as
 *                      seg_num or another segment was written to
 *                      without a following Commit() before
 *                      writing to this seg_num
 * \exception  BackupSegmentOverflowException if len + offset is
 *                      beyond the end of the segment
 * \bug Currently, the backup only allows writes to a single, open
 * segment.  If a master tries to write to another segment before
 * calling Commit() the write will fail.  The precise interface for
 * multi-segment operation need to be defined.
*/
void
BackupServer::Write(uint64_t seg_num,
                    uint64_t off,
                    const char *data,
                    uint64_t len)
{
    if (seg_num == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    if (this->seg_num == INVALID_SEGMENT_NUM)
        this->seg_num = seg_num;
    else if (this->seg_num != seg_num)
        throw BackupException("Backup server currently doesn't "
                              "allow multiple ongoing segments");

    //debug_dump64(data, len);
    if (len > SEGMENT_SIZE ||
        off > SEGMENT_SIZE ||
        len + off > SEGMENT_SIZE)
        throw BackupSegmentOverflowException();
    memcpy(&seg[off], data, len);
}

/**
 * Given a segment number return the segment frame in the backup file
 * where that segment is stored.  Notice this returns an L-value.
 *
 * \param[in]  seg_num  the target segment for which the frame number
 *                      is needed
 * \return a reference to the frame in the backup file where that
 *                      segment number is stored
 * \exception  BackupException if no such segment number is stored in
 *                      the file
*/
uint64_t&
BackupServer::FrameForSegNum(uint64_t seg_num)
{
    uint64_t seg_frame = INVALID_SEGMENT_NUM;
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++)
        if (segments[i] == seg_num) {
            seg_frame = i;
            break;
        }
    if (seg_frame == INVALID_SEGMENT_NUM)
        throw BackupException("No such segment stored on backup");
    return segments[seg_frame];
}

/**
 * Pure function.  Given a segment frame number return the position in
 * the storage where the segment frame begins.
 *
 * \param[in]  seg_frame the target segment frame for which the file
 *                       address is needed
 * \return the offset into the backup file where that segment frame
 *         begins
*/
static inline uint64_t
SegFrameOff(uint64_t seg_frame)
{
    return seg_frame * SEGMENT_SIZE;
}

/**
 * Flush the active segment to permanent storage.
 *
 * The backup chooses an empty segment frame, marks it as in-use,
 * an writes the current segment into the chosen segment frame.  This
 * function does none of the bookkeeping associated with clients,
 * rather it is used internally by other methods (Commit()) so that
 * they can focus on such details.
 *
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
BackupServer::Flush()
{
    struct timeval start, end, res;
    gettimeofday(&start, NULL);

    int64_t next = free_map.nextSet(0);
    if (next == -1)
        throw BackupLogIOException("Out of free segment frames");

    if (debug_backup)
        printf("Write active segment to frame %ld\n", next);

    segments[next] = seg_num;
    free_map.clear(next);

    off_t off = lseek(log_fd, SegFrameOff(next), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);
    ssize_t r = write(log_fd, seg, SEGMENT_SIZE);
    if (r != SEGMENT_SIZE)
        throw BackupLogIOException(errno);

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
 * \param[in] seg_num the segment number to persist and close.
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
BackupServer::Commit(uint64_t seg_num)
{
    // Write out the current segment to disk if any
    if (seg_num == INVALID_SEGMENT_NUM)
        throw BackupException("Invalid segment number");
    else if (seg_num != this->seg_num)
        throw BackupException("Cannot commit a segment other than the most "
                              "recently written one at the moment");

    if (debug_backup)
        printf(">>> Now writing to segment %lu\n", seg_num);
    Flush();

    // Close the segment
    this->seg_num = INVALID_SEGMENT_NUM;
}

/**
 * Removed the specified segment from permanent storage.
 *
 * After this call completes the segment number seg_num will no longer
 * be in permanent storage and will not be recovered during recover.
 *
 * \param[in] seg_num the segment number to remove from permanent
 *                storage.
 * \exception BackupException if INVALID_SEGMENT_NUM is passed as
 *                seg_num.
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
BackupServer::Free(uint64_t seg_num)
{
    if (debug_backup)
        printf("Free segment %llu\n", seg_num);
    if (seg_num == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    // Note - this call throws BackupException if no such segment
    // exists, which is exactly what we want to happen if we are fed a
    // seg_num we have no idea about
    uint64_t &frame = FrameForSegNum(seg_num);
    frame = INVALID_SEGMENT_NUM;
    free_map.set(frame);
    if (debug_backup)
        printf("Freed segment in frame %llu\n", frame);
}

void
BackupServer::GetSegmentList(uint64_t *list,
                             uint64_t *count)
{
    if (seg_num != INVALID_SEGMENT_NUM) {
        if (debug_backup)
            printf("!!! GetSegmentList: We must be in recovery, writing out "
                   "current active segment before proceeding\n");
        Commit(seg_num);
    }
    uint64_t max = *count;
    if (debug_backup)
        printf("Max segs to return %llu\n", max);

    uint64_t c = 0;
    for (uint64_t i = 0; i < SEGMENT_FRAMES; i++) {
        if (segments[i] != INVALID_SEGMENT_NUM) {
            if (c == max)
                throw BackupException("Buffer too short to for segment ids");
            *list = segments[i];
            list++;
            c++;
        }
    }
    if (debug_backup)
        printf("Final active segment count to return %llu\n", c);
    *count = c;
}

void
BackupServer::GetSegmentMetadata(uint64_t seg_num,
                                 uint64_t *list,
                                 uint64_t *count)
{
    /* uint64_t stream format
       <id, offset>*
       where count refers to the total number of uint64_ts in the
       list, that is count / 2 is the number of pairs in the list
     */
    if (seg_num != INVALID_SEGMENT_NUM) {
        if (debug_backup)
            printf("!!! GetSegmentMetadata: We must be in recovery, writing "
                   "out current active segment before proceeding\n");
        Commit(seg_num);
    }
    uint64_t max = *count;
    if (debug_backup)
        printf("Max elements to return %llu\n", max);

    uint64_t c = 0;

    char buf[SEGMENT_SIZE];
    uint64_t len;
    Retrieve(seg_num, &buf[0], &len);
    assert(len == SEGMENT_SIZE);

    // Walk the buffer and pull out metadata - need Steve's iter stuff
    // TODO(stutsman) NULL backup_client is dangerous - we may want to
    // make Segment smarter about NULL backups
    Segment seg(&buf[0], SEGMENT_SIZE, 0);
    LogEntryIterator lei(&seg);

    if (debug_backup)
        printf("Final elements count to return %llu\n", c);
    *count = c;
}

// TODO(stutsman) why is len even here right now - the way the
// function is written it can't even be used
void
BackupServer::Retrieve(uint64_t seg_num, char *buf, uint64_t *len)
{
    if (debug_backup)
        printf("Retrieving segment %llu from disk\n", seg_num);
    if (seg_num == INVALID_SEGMENT_NUM)
        throw BackupException("What the hell are you feeding me? "
                              "Bad segment number!");

    uint64_t seg_frame = FrameForSegNum(seg_num);
    if (debug_backup)
        printf("Found segment %llu in segment frame %llu\n", seg_num,
               seg_frame);

    struct timeval start, end, res;
    gettimeofday(&start, NULL);

    if (debug_backup)
        printf("Seeking to %llu\n", SegFrameOff(seg_frame));
    off_t off = lseek(log_fd, SegFrameOff(seg_frame), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);

    uint64_t read_len = SEGMENT_SIZE;
    if (debug_backup)
        printf("About to read %llu bytes at %llu to %p\n",
               read_len,
               SegFrameOff(seg_frame),
               buf);
    // if we use O_DIRECT this must be an aligned buffer
    ssize_t r = read(log_fd, buf, read_len);
    if (static_cast<uint64_t>(r) != read_len) {
        if (debug_backup)
            printf("Read %ld bytes\n", r);
        throw BackupLogIOException(errno);
    }
    *len = r;

    gettimeofday(&end, NULL);
    timersub(&end, &start, &res);
    if (debug_backup)
        printf("Retrieve in %d s %d us\n", res.tv_sec, res.tv_usec);
}

// ---- RPC Dispatch Code ----

void
BackupServer::HandleWrite(const backup_rpc *req, backup_rpc *resp)
{
    uint64_t seg_num = req->write_req.seg_num;
    uint64_t off = req->write_req.off;
    uint64_t len = req->write_req.len;
    if (debug_backup)
        printf(">>> Handling Write to offset 0x%x length %d\n", off, len);
    Write(seg_num, off, &req->write_req.data[0], len);

    resp->hdr.type = BACKUP_RPC_WRITE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
}

void
BackupServer::HandleGetSegmentList(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling GetSegmentList\n");

    resp->getsegmentlist_resp.seg_list_count =
        (RESP_BUF_LEN - sizeof(backup_rpc)) /
        sizeof(uint64_t);
    GetSegmentList(resp->getsegmentlist_resp.seg_list,
                   &resp->getsegmentlist_resp.seg_list_count);
    if (debug_backup)
        printf(">>>>>> GetSegmentList returning %llu ids\n",
            resp->getsegmentlist_resp.seg_list_count);

    resp->hdr.type = BACKUP_RPC_GETSEGMENTLIST_RESP;
    resp->hdr.len = static_cast<uint32_t>(
        BACKUP_RPC_GETSEGMENTLIST_RESP_LEN_WODATA +
        sizeof(uint64_t) * resp->getsegmentlist_resp.seg_list_count);
}

void
BackupServer::HandleRetrieve(const backup_rpc *req, backup_rpc *resp)
{
    const backup_rpc_retrieve_req *rreq = &req->retrieve_req;
    backup_rpc_retrieve_resp *rresp = &resp->retrieve_resp;
    if (debug_backup)
        printf(">>> Handling Retrieve - seg_num %lu\n", rreq->seg_num);

    Retrieve(rreq->seg_num, &rresp->data[0], &rresp->data_len);

    resp->hdr.type = BACKUP_RPC_RETRIEVE_RESP;
    resp->hdr.len = (uint32_t) (BACKUP_RPC_RETRIEVE_RESP_LEN_WODATA +
                                rresp->data_len);
}

void
BackupServer::HandleCommit(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling Commit - total msg len %lu\n", req->hdr.len);

    Commit(req->commit_req.seg_num);

    resp->hdr.type = BACKUP_RPC_COMMIT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_COMMIT_RESP_LEN;
}

void
BackupServer::HandleFree(const backup_rpc *req, backup_rpc *resp)
{
    if (debug_backup)
        printf(">>> Handling Free - total msg len %lu\n", req->hdr.len);

    Free(req->free_req.seg_num);

    resp->hdr.type = BACKUP_RPC_FREE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_FREE_RESP_LEN;
}

void
BackupServer::HandleHeartbeat(const backup_rpc *req, backup_rpc *resp)
{
    resp->hdr.type = BACKUP_RPC_HEARTBEAT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_HEARTBEAT_RESP_LEN;
}

void
BackupServer::HandleRPC()
{
    // TODO(stutsman) if we're goingt to have to malloc this we should just
    // always keep one around
    char *resp_buf = static_cast<char *>(malloc(RESP_BUF_LEN));
    backup_rpc *req;
    backup_rpc *resp = reinterpret_cast<backup_rpc *>(&resp_buf[0]);

    RecvRPC(&req);

    if (debug_rpc)
        printf("got rpc type: 0x%08x, len 0x%08x\n",
               req->hdr.type, req->hdr.len);

    try {
        switch ((enum backup_rpc_type) req->hdr.type) {
        case BACKUP_RPC_HEARTBEAT_REQ: HandleHeartbeat(req, resp); break;
        case BACKUP_RPC_WRITE_REQ:     HandleWrite(req, resp);     break;
        case BACKUP_RPC_COMMIT_REQ:    HandleCommit(req, resp);    break;
        case BACKUP_RPC_FREE_REQ:      HandleFree(req, resp);      break;
        case BACKUP_RPC_GETSEGMENTLIST_REQ:
            HandleGetSegmentList(req, resp);
            break;
        case BACKUP_RPC_RETRIEVE_REQ:  HandleRetrieve(req, resp);  break;

        case BACKUP_RPC_HEARTBEAT_RESP:
        case BACKUP_RPC_WRITE_RESP:
        case BACKUP_RPC_COMMIT_RESP:
        case BACKUP_RPC_FREE_RESP:
        case BACKUP_RPC_GETSEGMENTLIST_RESP:
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
    SendRPC(resp);
    free(resp_buf);
}

void __attribute__ ((noreturn))
BackupServer::Run()
{
    while (true)
        HandleRPC();
}


} // namespace RAMCloud

