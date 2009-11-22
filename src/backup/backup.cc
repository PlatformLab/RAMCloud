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

BackupServer::BackupServer(Net *net_impl)
        : net(net_impl), log_fd(-1)
{
    log_fd = open("backup.log",
                  O_CREAT | O_NOATIME | O_TRUNC | O_WRONLY,
                  S_IRUSR | S_IWUSR);
    if (log_fd == -1)
        throw "God hates ponies";
}

BackupServer::~BackupServer()
{
    int r = close(log_fd);
    if (r == -1)
        throw "God hates ponies";
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
BackupServer::Write(const backup_rpc *req, backup_rpc *resp)
{
    printf("Handling Write - total msg len %lu\n", req->hdr.len);

    int data_len = req->hdr.len - BACKUP_RPC_HDR_LEN;

    debug_dump64(&req->write_req.data[0], data_len);
    ssize_t r = write(log_fd, &req->write_req.data[0], data_len);
    if (r != data_len) {
        assert(r == -1);
        error(-1, errno, "Failed to write log");
    }

    resp->hdr.type = BACKUP_RPC_WRITE_RESP;
    resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
    resp->write_resp.ok = 1;
}

void
BackupServer::Commit(const backup_rpc *req, backup_rpc *resp)
{
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
    
    switch((enum backup_rpc_type) req->hdr.type) {
        case BACKUP_RPC_HEARTBEAT_REQ: Heartbeat(req, &resp); break;
        case BACKUP_RPC_WRITE_REQ:     Write(req, &resp);     break;
        case BACKUP_RPC_COMMIT_REQ:    Commit(req, &resp);    break;
            
        case BACKUP_RPC_HEARTBEAT_RESP:
        case BACKUP_RPC_WRITE_RESP:
        case BACKUP_RPC_COMMIT_RESP:
            throw "server received RPC response";
            
        default:
            throw "received unknown RPC type";
    };
    SendRPC(&resp);
}

void __attribute__ ((noreturn))
BackupServer::Run() {
    while (true)
        HandleRPC();
}


} // namespace RAMCloud

