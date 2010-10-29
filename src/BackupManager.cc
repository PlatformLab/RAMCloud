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

#include "BackupClient.h"
#include "BackupManager.h"
#include "Buffer.h"

namespace RAMCloud {

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with.  See addHost() to add remote backups.
 */
BackupManager::BackupManager(CoordinatorClient* coordinator, uint32_t replicas)
    : coordinator(coordinator)
    , hosts()
    , openHosts()
    , replicas(replicas)
    , segments()
{
}

/// Free up all BackupClients.
BackupManager::~BackupManager()
{
    foreach (BackupClient* host, openHosts)
        delete host;
}

// See BackupClient::closeSegment.
void
BackupManager::closeSegment(uint64_t masterId,
                            uint64_t segmentId)
{
    TEST_LOG("%lu, %lu", masterId, segmentId);
    foreach (BackupClient* host, openHosts) {
        // TODO(stutsman) Exception during one of the closes?
        host->closeSegment(masterId, segmentId);
        delete host;
    }
    openHosts.clear();
}

// See BackupClient::freeSegment.
void
BackupManager::freeSegment(uint64_t masterId,
                           uint64_t segmentId)
{
    TEST_LOG("%lu, %lu", masterId, segmentId);
    uint32_t count = 0;
    for (SegmentMap::iterator it = segments.find(segmentId);
         it != segments.end();
         ++it)
    {
        BackupClient host(it->second);
        host.freeSegment(masterId, segmentId);
        segments.erase(it);
        count++;
    }
    if (count != replicas)
        LOG(WARNING, "Only freed %u segments rather than %u", count, replicas);
}

// See BackupClient::openSegment.
void
BackupManager::openSegment(uint64_t masterId,
                           uint64_t segmentId)
{
    TEST_LOG("%lu, %lu", masterId, segmentId);
    selectOpenHosts();
    foreach (BackupClient* host, openHosts) {
        host->openSegment(masterId, segmentId);
        segments.insert(SegmentMap::value_type(segmentId, host->getSession()));
    }
}

void
BackupManager::recover(uint64_t masterId,
                       const TabletMap& tablets)
{
    DIE("Unimplemented");
}

// See BackupClient::writeSegment.
void
BackupManager::writeSegment(uint64_t masterId,
                            uint64_t segmentId,
                            uint32_t offset,
                            const void *data,
                            uint32_t len)
{
    TEST_LOG("%lu, %lu, %u, ..., %u", masterId, segmentId, offset, len);
    // TODO(stutsman) Exception during one of the writes?
    foreach (BackupClient* host, openHosts)
        host->writeSegment(masterId, segmentId, offset, data, len);
}

// - private -

/**
 * Open segments on replica number of backups.  Caller must ensure that
 * no segments are currently open on any backups and that there are
 * enough hosts to choose from.
 */
void
BackupManager::selectOpenHosts()
{
    if (!coordinator) {
        if (replicas)
            DIE("No coordinator given, replication requirements can't be met.");
        else
            return;
    }

    // TODO(ongaro): it's probably not ok to get a new server list this often
    coordinator->getServerList(hosts);

    const uint32_t numHosts(static_cast<uint32_t>(hosts.server_size()));

    uint32_t numBackupClients = 0;
    foreach (const ProtoBuf::ServerList::Entry& entry, hosts.server()) {
        if (entry.server_type() == ProtoBuf::BACKUP)
            ++numBackupClients;
    }

    if (numBackupClients < replicas)
        DIE("Not enough backups to meet replication requirement");

    if (!openHosts.empty())
        DIE("Cannot select new backups when some are already open");

    uint64_t random = generateRandom();
    uint32_t i = 0;
    while (i < replicas) {
        uint32_t index = random % numHosts;
        const ProtoBuf::ServerList::Entry& host(hosts.server(index));
        if (host.server_type() == ProtoBuf::BACKUP) {
            LOG(DEBUG, "Backing up to %s", host.service_locator().c_str());
            Transport::SessionRef session =
                transportManager.getSession(host.service_locator().c_str());
            openHosts.push_back(new BackupClient(session));
            i++;
        }
        random++;
    }
}

// See BackupClient::startReadingData.
void
BackupManager::startReadingData(uint64_t masterId)
{
    DIE("Unimplemented");
}

} // namespace RAMCloud
