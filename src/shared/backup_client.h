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
#include <server/net.h>

// requires 0x for cstdint
#include <stdint.h>

namespace RAMCloud {

class BackupClient {
  public:
    explicit BackupClient(Net *net_impl);
    void Heartbeat();
    void Write(uint32_t offset, const void *buf, uint32_t len);
    void Commit(uint64_t new_seg_num);
    void Free(uint64_t seg_num);
    // TODO - what do we want here?  Somekind of stateful get next seg
    // with ids EXCEPT we need to be careful about
    /** Input: prev_seg_num: get segment data for most segemnt after
     *         this
     *  Output: seg_num: location of return value indicating which
     *         segment this metadata is for.  If this is
     *         INVALID_SEGMENT_NUM then all remaining fields are
     *         invalid.
     *  Output: seg_list: start of buffer for results
     *  Input/Ouput: seg_list_count: as input contains the size of the
     *         buffer, as output contains the number of valid entries
     *         in seg_list
     */
    void GetSegmentMetadata(uint64_t prev_seg_num,
                            uint64_t *seg_num,
                            uint64_t *seg_list,
                            uint64_t *seg_list_count);
    void Retrieve(uint64_t seg_num);
  private:
    DISALLOW_COPY_AND_ASSIGN(BackupClient);
    void SendRPC(struct backup_rpc *rpc);
    void RecvRPC(struct backup_rpc **rpc);
    Net *net;
};

} // namespace RAMCloud

#endif
