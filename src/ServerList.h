/* Copyright (c) 2011-2012 Stanford University
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

#ifndef RAMCLOUD_SERVERLIST_H
#define RAMCLOUD_SERVERLIST_H

#include <mutex>

#include "ServiceMask.h"
#include "ServerId.h"
#include "ServerList.pb.h"
#include "Transport.h"
#include "Tub.h"

#include "AbstractServerList.h"

namespace RAMCloud {

/**
 * A ServerList maintains a mapping of coordinator-allocated ServerIds to
 * the service locators that address particular servers. Here a "server"
 * is not a physical machine, but rather a specific instance of a RAMCloud
 * server process. This class is used by masters (the coordinator uses a
 * CoordinatorServerList instead).
 *
 * This class is thread-safe (monitor- style lock) and supports ServerTrackers.
 * The tracker will be fed updates whenever servers come or go (add, crashed,
 * removed).
 *
 * This class publicly extends AbstractServerList to provide a common
 * interface to READ from map of ServerIds and (un)register trackers.
 */
class ServerList : public AbstractServerList {
  PUBLIC:
    explicit ServerList(Context* context);
    ~ServerList();

    ServerId operator[](uint32_t indexNumber);
    uint64_t applyServerList(const ProtoBuf::ServerList& list);

  PROTECTED:
    /// Internal Use Only - Does not grab locks
    ServerDetails* iget(ServerId id);
    ServerDetails* iget(uint32_t index);
    size_t isize() const;

    /// Slots in the server list.
    std::vector<Tub<ServerDetails>> serverList;

  PRIVATE:
    void testingAdd(const ServerDetails server);
    void testingCrashed(ServerId serverId);
    void testingRemove(ServerId serverId);

    DISALLOW_COPY_AND_ASSIGN(ServerList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SERVERLIST_H
