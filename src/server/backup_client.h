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

#ifndef RAMCLOUD_SERVER_BACKUP_CLIENT_H
#define RAMCLOUD_SERVER_BACKUP_CLIENT_H

#include <shared/common.h>
#include <shared/object.h>
#include <server/net.h>

// requires 0x for cstdint
#include <stdint.h>
#include <vector>

namespace RAMCloud {

class BackupClient {
  public:
    explicit BackupClient(Net *net_impl);
    void Heartbeat();
    void Write(const chunk_hdr *hdr);
    void Commit();//std::vector<uintptr_t> freed);
  private:
    DISALLOW_COPY_AND_ASSIGN(BackupClient);
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);
    Net *net;
};

} // namespace RAMCloud

#endif
