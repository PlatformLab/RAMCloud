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

#include <shared/backup_client.h>
#include <shared/object.h>
#include <shared/backuprpc.h>

#include <cassert>
#include <cstdio>

namespace RAMCloud {

enum { debug_noisy = false };

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
    if (len != (*rpc)->hdr.len)
        printf("got %lu, expected %lu\n", len, (*rpc)->hdr.len);
    assert(len == (*rpc)->hdr.len);
    if ((*rpc)->hdr.type == BACKUP_RPC_ERROR_RESP) {
        char *m = (*rpc)->error_resp.message;
        printf("Exception on backup operation >>> %s\n", m);
        throw BackupRPCException(m);
    }
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

    printf("Heartbeat ok\n");
}

void
BackupClient::Write(uint64_t seg_num,
                    uint32_t offset,
                    const void *buf,
                    uint32_t len)
{
    // TODO(stutsman) For the moment we don't have a choice here
    // we have to build this thing up in memory until the network
    // interface is changed so we can gather the header and the
    // data from two different places
    char reqbuf[MAX_RPC_LEN];
    backup_rpc *req = reinterpret_cast<backup_rpc *>(reqbuf);
    if (debug_noisy)
        printf("Sending Write to backup\n");

    req->hdr.type = BACKUP_RPC_WRITE_REQ;
    req->hdr.len = BACKUP_RPC_WRITE_REQ_LEN_WODATA + len;
    if (req->hdr.len > MAX_RPC_LEN)
        throw BackupRPCException("Write RPC would be too long");

    req->write_req.seg_num = seg_num;
    req->write_req.off = offset;
    req->write_req.len = len;
    memcpy(&req->write_req.data[0], buf, len);

    SendRPC(req);

    backup_rpc *resp;
    RecvRPC(&resp);
}

void
BackupClient::Commit(uint64_t seg_num)
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_COMMIT_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_COMMIT_REQ_LEN);

    req.commit_req.seg_num = seg_num;

    printf("Sending Commit to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    printf("Commit ok\n");
}

void
BackupClient::Free(uint64_t seg_num)
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_FREE_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_FREE_REQ_LEN);

    req.free_req.seg_num = seg_num;

    printf("Sending Free to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    printf("Free ok\n");
}

void
BackupClient::GetSegmentList(uint64_t *list,
                             uint64_t *count)
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_GETSEGMENTLIST_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_GETSEGMENTLIST_REQ_LEN);

    printf("Sending GetSegmentList to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    uint64_t *tmp_list = &resp->getsegmentlist_resp.seg_list[0];
    uint64_t tmp_count = resp->getsegmentlist_resp.seg_list_count;
    printf("Backup wants to restore %llu segments\n", tmp_count);

    if (*count < tmp_count)
        throw BackupRPCException("Provided a segment id buffer "
                                 "that was too small");
    // TODO(stutsman) we need to return this sorted and merged with
    // segs from other backups
    memcpy(list, tmp_list, tmp_count * sizeof(uint64_t));
    *count = tmp_count;

    printf("GetSegmentList ok\n");
}

void
BackupClient::GetSegmentMetadata(uint64_t seg_num,
                                 uint64_t *id_list,
                                 uint64_t *id_list_count)
{
}

void
BackupClient::Retrieve(uint64_t seg_num, void *dst)
{
    backup_rpc req;
    req.hdr.type = BACKUP_RPC_RETRIEVE_REQ;
    req.hdr.len = static_cast<uint32_t>(BACKUP_RPC_RETRIEVE_REQ_LEN);

    req.retrieve_req.seg_num = seg_num;

    printf("Sending Retrieve to backup\n");
    SendRPC(&req);

    backup_rpc *resp;
    RecvRPC(&resp);

    printf("Retrieved segment %llu of length %llu\n",
           seg_num, resp->retrieve_resp.data_len);
    memcpy(dst, resp->retrieve_resp.data, resp->retrieve_resp.data_len);

    printf("Retrieve ok\n");
}

} // namespace RAMCloud
