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

#ifndef RAMCLOUD_RAMCLOUDCLIENT_H
#define RAMCLOUD_RAMCLOUDCLIENT_H

#include "Client.h"
#include "Common.h"
#include "ObjectFinder.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * The RamCloudClient class provides the primary interface used by applications to
 * access a RAMCloud cluster.
 *
 * Each RamCloudClient object provides access to a particular RAMCloud cluster;
 * all of the RAMCloud RPC requests appear as methods on this object.
 */
class RamCloudClient : public Client {
  public:
    explicit RamCloudClient(const char* serviceLocator);
    uint64_t create(uint32_t tableId, const void* buf, uint32_t length,
                    uint64_t* version = NULL);
    void createTable(const char* name);
    void dropTable(const char* name);
    uint32_t openTable(const char* name);
    void ping();
    void read(uint32_t tableId, uint64_t id, Buffer* value,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void remove(uint32_t tableId, uint64_t id,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void write(uint32_t tableId, uint64_t id, const void* buf,
               uint32_t length, const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL);

  protected:
    Transport::SessionRef session; //!< For now we only know how to talk
                                   //!< to a single RAMCloud server; this
                                   //!< is a handle for that server.
    ObjectFinder objectFinder;

    DISALLOW_COPY_AND_ASSIGN(RamCloudClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_RAMCLOUDCLIENT_H
