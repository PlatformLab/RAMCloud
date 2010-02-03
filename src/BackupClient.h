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

#ifndef RAMCLOUD_SHARED_BACKUP_CLIENT_H
#define RAMCLOUD_SHARED_BACKUP_CLIENT_H

#include <Common.h>
#include <Net.h>

// requires 0x for cstdint
#include <stdint.h>

namespace RAMCloud {

class BackupHost {
  public:
    explicit BackupHost(Net *netimpl);
    ~BackupHost();
    void Heartbeat();
    void Write(uint64_t seg_num,
               uint32_t offset,
               const void *buf,
               uint32_t len);
    void Commit(uint64_t seg_num);
    void Free(uint64_t seg_num);
    void GetSegmentList(uint64_t *list, uint64_t *count);
    void GetSegmentMetadata(uint64_t seg_num,
                            uint64_t *id_list,
                            uint64_t *id_list_count);
    void Retrieve(uint64_t seg_num, void *dst);
  private:
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);
    Net *net;
    DISALLOW_COPY_AND_ASSIGN(BackupHost);
};

class BackupClient {
  public:
    explicit BackupClient();
    ~BackupClient();
    void AddHost(Net *net);
    void Heartbeat();
    void Write(uint64_t seg_num,
               uint32_t offset,
               const void *buf,
               uint32_t len);
    void Commit(uint64_t seg_num);
    void Free(uint64_t seg_num);
    void GetSegmentList(uint64_t *list, uint64_t *count);
    void GetSegmentMetadata(uint64_t seg_num,
                            uint64_t *id_list,
                            uint64_t *id_list_count);
    void Retrieve(uint64_t seg_num, void *dst);
  private:
    BackupHost *host;
    DISALLOW_COPY_AND_ASSIGN(BackupClient);
};

} // namespace RAMCloud

#endif
