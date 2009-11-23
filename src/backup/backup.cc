#include <backup/backup.h>

#include <shared/backuprpc.h>

#include <cstdio>
#include <cassert>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <error.h>
#include <errno.h>

namespace RAMCloud {

enum { SEGMENT_SIZE = 1 <<  23 };

BackupException::~BackupException() {}

BackupServer::BackupServer(Net *net_impl)
        : net(net_impl), log_fd(-1), seg(0), seg_off(0)
{
    log_fd = open("backup.log",
                  O_CREAT | O_NOATIME | O_TRUNC | O_WRONLY,
                  S_IRUSR | S_IWUSR);
    if (log_fd == -1)
        throw BackupLogIOException();

    seg = new char[SEGMENT_SIZE];
}

BackupServer::~BackupServer()
{
    Flush();
    delete[] seg;

    int r = close(log_fd);
    if (r == -1)
        throw BackupLogIOException();
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
BackupServer::DoWrite(const char *data, size_t data_len)
{
    debug_dump64(data, data_len);
    if (data_len + seg_off > SEGMENT_SIZE)
        throw BackupSegmentOverflowException();
    memcpy(&seg[seg_off], data, data_len);
    seg_off += data_len;
}

void
BackupServer::Write(const backup_rpc *req, backup_rpc *resp)
{
    printf("Handling Write - total msg len %lu\n", req->hdr.len);

    int data_len = req->hdr.len - BACKUP_RPC_HDR_LEN;
    DoWrite(&req->write_req.data[0], data_len);

    resp->hdr.type = BACKUP_RPC_WRITE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
    resp->write_resp.ok = 1;
}

void
BackupServer::Flush()
{
    printf("Flushing active segment to disk\n");
    ssize_t r = write(log_fd, seg, seg_off);
    if (r != static_cast<ssize_t>(seg_off))
        throw BackupLogIOException();
}

void
BackupServer::DoCommit()
{
    Flush();
    seg_off = 0;
}

void
BackupServer::Commit(const backup_rpc *req, backup_rpc *resp)
{
    printf("Handling Commit - total msg len %lu\n", req->hdr.len);

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

    printf("got rpc type: 0x%08x, len 0x%08x\n", req->hdr.type, req->hdr.len);
    
    try {
        switch((enum backup_rpc_type) req->hdr.type) {
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
        fprintf(stderr, e.message.c_str());
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

