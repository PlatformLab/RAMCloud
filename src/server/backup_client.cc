#include <server/backup_client.h>

#include <shared/object.h>
#include <shared/backuprpc.h>

#include <cassert>
#include <cstdio>

namespace RAMCloud {

BackupClient::BackupClient(Net *net_impl)
        : net(net_impl)
{
}

void
BackupClient::SendRPC(struct backup_rpc *rpc)
{
    net->Send(rpc, rpc->hdr.len);
}

void
BackupClient::RecvRPC(struct backup_rpc **rpc)
{
    size_t len = net->Recv(reinterpret_cast<void**>(rpc));
    assert(len == (*rpc)->hdr.len);
}

void
BackupClient::Heartbeat()
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_HEARTBEAT_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_HEARTBEAT_REQ_LEN);

    printf("Sending heartbeat to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    assert(resp->heartbeat_resp.ok = 1);
    printf("Heartbeat ok\n");
}

void
BackupClient::Write(const chunk_hdr *obj)
{
    char reqbuf[MAX_BACKUP_RPC_LEN];
    backup_rpc *req = reinterpret_cast<backup_rpc *>(reqbuf);

    uint64_t obj_size = sizeof(chunk_hdr) + obj->entries[0].len;

    req->hdr.type = BACKUP_RPC_WRITE_REQ;
    req->hdr.len = BACKUP_RPC_WRITE_REQ_LEN_WODATA + obj_size;
    if (req->hdr.len > MAX_BACKUP_RPC_LEN)
        throw BackupRPCException("Write RPC would be too long");

    printf("Sending Write to backup\n");
    memcpy(&req->write_req.data[0],
           obj,
           obj_size);
    debug_dump64(req, req->hdr.len);
    SendRPC(req);

    backup_rpc *resp;
    RecvRPC(&resp);

    assert(resp->write_resp.ok = 1);
    printf("Write ok\n");
}

void
BackupClient::Commit(std::vector<uintptr_t> freed)
{
}

} // namespace RAMCloud
