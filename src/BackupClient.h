/* Copyright (c) 2009-2010 Stanford University
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

#ifndef RAMCLOUD_BACKUPCLIENT_H
#define RAMCLOUD_BACKUPCLIENT_H

#include <list>

#include "Client.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "Object.h"
#include "ServerList.pb.h"
#include "Transport.h"

namespace RAMCloud {

// TODO(stutsman) delete this soon!
class TabletMap {
};

/**
 * A backup consisting of a single remote host.  BackupClient's primary
 * role is to proxy calls via RPCs to a particular backup server.
 */
class BackupClient : public Client {
  public:
    explicit BackupClient(Transport::SessionRef session);
    ~BackupClient();

    void closeSegment(uint64_t masterId, uint64_t segmentId);
    void freeSegment(uint64_t masterId, uint64_t segmentId);
    void getRecoveryData(uint64_t masterId,
                         uint64_t segmentId,
                         const TabletMap& tablets,
                         Buffer& resp);
    Transport::SessionRef getSession();
    void openSegment(uint64_t masterId, uint64_t segmentId);
    vector<uint64_t> startReadingData(uint64_t masterId);
    void writeSegment(uint64_t masterId,
                      uint64_t segmentId,
                      uint32_t offset,
                      const void *buf,
                      uint32_t length);

  private:
    /**
     * Performance metric from the response in the most recent RPC (as
     * requested by selectPerfCounter). If no metric was requested and done
     * most recent RPC, then this value is 0.
     */
    uint32_t counterValue;

    /**
     * A session with a backup server.
     */
    Transport::SessionRef session;

    /**
     * Completion status from the most recent RPC completed for this client.
     */
    Status status;

    DISALLOW_COPY_AND_ASSIGN(BackupClient);
};

} // namespace RAMCloud

#endif
