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

#include <config.h>

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

    printf("Sending Heartbeat to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    assert(resp->heartbeat_resp.ok == 1);
    printf("Heartbeat ok\n");
}

void
BackupClient::Write(const void *buf, uint32_t offset, uint32_t len)
{
    // TODO(stutsman) For the moment we don't have a choice here
    // we have to build this thing up in memory until the network
    // interface is changed so we can gather the header and the
    // data from two different places
    char reqbuf[MAX_RPC_LEN];
    backup_rpc *req = reinterpret_cast<backup_rpc *>(reqbuf);

    req->hdr.type = BACKUP_RPC_WRITE_REQ;
    req->hdr.len = BACKUP_RPC_WRITE_REQ_LEN_WODATA + len;
    if (req->hdr.len > MAX_RPC_LEN)
        throw BackupRPCException("Write RPC would be too long");

    req->write_req.off = offset;
    req->write_req.len = len;
    memcpy(&req->write_req.data[0], buf, len);

    SendRPC(req);

    backup_rpc *resp;
    RecvRPC(&resp);

    if (resp->write_resp.ok != 1) {
        resp->write_resp.message[resp->write_resp.len - 1] = '\0';
        char *m = resp->write_resp.message;
        printf("Exception on Write >>> %s\n", m);
        throw BackupRPCException(m);
    }
}

void
BackupClient::Commit()//std::vector<uintptr_t> freed)
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_COMMIT_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_COMMIT_REQ_LEN);

    printf("Sending Commit to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    if (resp->commit_resp.ok != 1) {
        resp->commit_resp.message[resp->commit_resp.len - 1] = '\0';
        char *m = resp->commit_resp.message;
        printf("Exception on Commit >>> %s\n", m);
        throw BackupRPCException(m);
    }
    printf("Commit ok\n");
}

} // namespace RAMCloud
