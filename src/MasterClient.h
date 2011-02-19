
/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_MASTERCLIENT_H
#define RAMCLOUD_MASTERCLIENT_H

#include "Client.h"
#include "Common.h"
#include "ObjectFinder.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * TODO
 */
class MasterClient : public Client {
  public:
    explicit MasterClient(Transport::SessionRef session) : session(session) {}
    uint64_t create(uint32_t tableId, const void* buf, uint32_t length,
                    uint64_t* version = NULL, bool async = false);
    void ping();
    void read(uint32_t tableId, uint64_t id, Buffer* value,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void recover(uint64_t masterId, uint64_t partitionId,
                 const ProtoBuf::Tablets& tablets,
                 const ProtoBuf::ServerList& backups);
    void remove(uint32_t tableId, uint64_t id,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void setTablets(const ProtoBuf::Tablets& tablets);
    void write(uint32_t tableId, uint64_t id, const void* buf,
               uint32_t length, const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL, bool async = false);

  protected:
    Transport::SessionRef session;
    DISALLOW_COPY_AND_ASSIGN(MasterClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_MASTERCLIENT_H
