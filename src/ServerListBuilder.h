/* Copyright (c) 2010-2011 Stanford University
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

#ifndef RAMCLOUD_SERVERLISTBUILDER_H
#define RAMCLOUD_SERVERLISTBUILDER_H

#include <initializer_list>

#include "ServerList.h"

namespace RAMCloud {

/**
 * This class provides a convenient way to build ProtoBuf::ServerList objects.
 */
struct ServerListBuilder {
    /**
     * \param[out] servers
     *      Server entries will be appended to this list.
     */
    explicit ServerListBuilder(ProtoBuf::ServerList& servers)
        : servers(servers)
    {
    }

    /**
     * Invocations of this method can be placed directly next to each other,
     * since it returns a reference to 'this' object. This allows the caller to
     * build up a server list very succinctly, which is useful for testing.
     *
     * These parameters are the same as the ones defined in ServerList.proto.
     *
     * Example:
     * ServerListBuilder{serverList}
     *      ({MASTER_SERVICE}, 123, "mock:host=one")
     *      ({BACKUP_SERVICE}, 123, "mock:host=two");
     */
    ServerListBuilder&
    operator()(std::initializer_list<WireFormat::ServiceType> services,
               uint64_t id,
               const char* locator,
               uint32_t backupReadMBytesPerSec = 0,
               uint64_t replicationId = 0,
               ServerStatus status = ServerStatus::UP)
    {
        ProtoBuf::ServerList_Entry& server(*servers.add_server());
        ServiceMask serviceMask(services);
        server.set_services(serviceMask.serialize());
        server.set_server_id(id);
        server.set_service_locator(locator);
        server.set_expected_read_mbytes_per_sec(backupReadMBytesPerSec);
        server.set_replication_id(replicationId);
        server.set_status(uint32_t(status));
        return *this;
    }

    ProtoBuf::ServerList& servers;
};

} // namespace RAMCloud

#endif // RAMCLOUD_SERVERLISTBUILDER_H
