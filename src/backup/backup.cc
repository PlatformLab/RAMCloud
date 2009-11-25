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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <config.h>

#include <backup/backup.h>

#include <shared/backuprpc.h>

#include <cstdio>
#include <cassert>

#include <cerrno>

namespace RAMCloud {

BackupException::~BackupException() {}

BackupServer::BackupServer(Net *net_impl, const char *logPath)
    : net(net_impl), log_fd(-1), seg(0), free_map(true), last_seg_written(0)
{
    log_fd = open(logPath,
                  O_CREAT | O_TRUNC | O_WRONLY,
                  0666);
    if (log_fd == -1)
        throw BackupLogIOException(errno);

    seg = new char[SEGMENT_SIZE];

    ReserveSpace();
}

BackupServer::~BackupServer()
{
    Flush();
    delete[] seg;

    int r = close(log_fd);
    if (r == -1) {
        // TODO(stutsman) need to check to see if we are aborting already
        // is this really worth throwing over?  It'll likely cause an abort
        throw BackupLogIOException(errno);
    }
}

void
BackupServer::ReserveSpace()
{
    printf("Reserving %d bytes of log space\n", LOG_SPACE);
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

void
BackupServer::Heartbeat(const backup_rpc *req, backup_rpc *resp)
{
    resp->hdr.type = BACKUP_RPC_HEARTBEAT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_HEARTBEAT_RESP_LEN;
    resp->heartbeat_resp.ok = 1;
}

void
BackupServer::DoWrite(const char *data, uint64_t off, uint64_t len)
{
    //debug_dump64(data, len);
    if (len > SEGMENT_SIZE ||
        off > SEGMENT_SIZE ||
        len + off > SEGMENT_SIZE)
        throw BackupSegmentOverflowException();
    memcpy(&seg[off], data, len);
}

void
BackupServer::Write(const backup_rpc *req, backup_rpc *resp)
{

    uint64_t off = req->write_req.off;
    uint64_t len = req->write_req.len;
    printf("Handling Write to offset Ox%x length %d\n", off, len);
    DoWrite(&req->write_req.data[0], off, len);

    resp->hdr.type = BACKUP_RPC_WRITE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
    resp->write_resp.ok = 1;
}

static inline int64_t
SegFrameOff(int64_t segnum)
{
    return segnum * SEGMENT_SIZE;
}

void
BackupServer::Flush()
{
    printf("Flushing active segment to disk\n");

    int64_t next = free_map.NextFree(last_seg_written);
    if (next == -1)
        throw BackupLogIOException("Out of free segment frames");
    printf("Write active segment to frame %ld\n", next);

    off_t off = lseek(log_fd, SegFrameOff(next), SEEK_SET);
    if (off == -1)
        throw BackupLogIOException(errno);
    ssize_t r = write(log_fd, seg, SEGMENT_SIZE);
    if (r != SEGMENT_SIZE)
        throw BackupLogIOException(errno);

    free_map.Clear(next);
    last_seg_written = next;
}

void
BackupServer::DoCommit()
{
    Flush();
}

void
BackupServer::Commit(const backup_rpc *req, backup_rpc *resp)
{
    printf(">>> Handling Commit - total msg len %lu\n", req->hdr.len);

    DoCommit();

    resp->hdr.type = BACKUP_RPC_COMMIT_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_COMMIT_RESP_LEN;
    resp->commit_resp.ok = 1;
}

void
BackupServer::HandleRPC()
{
    struct backup_rpc *req;
    struct backup_rpc resp;

    RecvRPC(&req);

    //printf("got rpc type: 0x%08x, len 0x%08x\n", req->hdr.type, req->hdr.len);

    try {
        switch ((enum backup_rpc_type) req->hdr.type) {
            case BACKUP_RPC_HEARTBEAT_REQ: Heartbeat(req, &resp); break;
            case BACKUP_RPC_WRITE_REQ:     Write(req, &resp);     break;
            case BACKUP_RPC_COMMIT_REQ:    Commit(req, &resp);    break;

            case BACKUP_RPC_HEARTBEAT_RESP:
            case BACKUP_RPC_WRITE_RESP:
            case BACKUP_RPC_COMMIT_RESP:
            default:
                throw BackupInvalidRPCOpException();
        };
    } catch (BackupException e) {
        fprintf(stderr, ">>> BackupException: %s\n", e.message.c_str());
    }
    SendRPC(&resp);
}

void __attribute__ ((noreturn))
BackupServer::Run()
{
    while (true)
        HandleRPC();
}


} // namespace RAMCloud

