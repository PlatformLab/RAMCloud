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
#include <shared/backuprpc.h>

#include <cassert>
#include <cstdio>

namespace RAMCloud {

enum { debug_noisy = false };

/**
 * NOTICE:  The BackupHost takes care of deleting the Net object
 * once it is no longer needed.  The host should be considered to
 * have full ownership of it and the caller should discontinue any use
 * or responbility for it.
 */
BackupHost::BackupHost(Net *netimpl)
    : net(netimpl)
{
}

BackupHost::~BackupHost()
{
    // We delete the net we were handed from the constructor so the
    // creator doesn't need to worry about it.
    delete net;
}

void
BackupHost::SendRPC(struct backup_rpc *rpc)
{
    net->Send(rpc, rpc->hdr.len);
}

void
BackupHost::RecvRPC(struct backup_rpc **rpc)
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
BackupHost::Heartbeat()
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
BackupHost::Write(uint64_t seg_num,
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
BackupHost::Commit(uint64_t seg_num)
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
BackupHost::Free(uint64_t seg_num)
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
BackupHost::GetSegmentList(uint64_t *list,
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
BackupHost::GetSegmentMetadata(uint64_t seg_num,
                                 uint64_t *id_list,
                                 uint64_t *id_list_count)
{
}

void
BackupHost::Retrieve(uint64_t seg_num, void *dst)
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

// --- BackupClient ---

BackupClient::BackupClient()
    : host(0)
{
}

BackupClient::~BackupClient()
{
    if (host)
        delete host;
}

/**
 * NOTICE:  The BackupClient takes care of deleting the Net object
 * once it is no longer needed.  The client should be considered to
 * have full ownership of it and the caller should discontinue any use
 * or responbility for it.
 */
void
BackupClient::AddHost(Net *net)
{
    if (host)
        throw BackupRPCException("Only one backup host currently supported");
    host = new BackupHost(net);
}

void
BackupClient::Heartbeat()
{
    if (host)
        host->Heartbeat();
}

void
BackupClient::Write(uint64_t seg_num,
                    uint32_t offset,
                    const void *buf,
                    uint32_t len)
{
    if (host)
        host->Write(seg_num, offset, buf, len);
}

void
BackupClient::Commit(uint64_t seg_num)
{
    if (host)
        host->Commit(seg_num);
}

void
BackupClient::Free(uint64_t seg_num)
{
    if (host)
        host->Free(seg_num);
}

void
BackupClient::GetSegmentList(uint64_t *list,
                             uint64_t *count)
{
    if (host) {
        host->GetSegmentList(list, count);
        return;
    }
    *count = 0;
}

void
BackupClient::GetSegmentMetadata(uint64_t seg_num,
                                 uint64_t *id_list,
                                 uint64_t *id_list_count)
{
    if (host) {
        host->GetSegmentMetadata(seg_num, id_list, id_list_count);
        return;
    }
    *id_list_count = 0;
}

void
BackupClient::Retrieve(uint64_t seg_num, void *dst)
{
    if (host)
        host->Retrieve(seg_num, dst);
}

} // namespace RAMCloud
