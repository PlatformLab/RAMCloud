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

#include <boost/lexical_cast.hpp>

#include "BackupClient.h"
#include "BackupManager.h"
#include "Buffer.h"
#include "MasterServer.h"

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
    pair<SegmentMap::iterator, SegmentMap::iterator> iters(
        segments.equal_range(segmentId));
    SegmentMap::iterator& it = iters.first;
    while (it != iters.second) {
        BackupClient host(it->second);
        host.freeSegment(masterId, segmentId);
        SegmentMap::iterator current = it;
        it++;
        segments.erase(current);
        count++;
    }
    if (count != replicas)
        LOG(WARNING, "Freed %u segments rather than %u", count, replicas);
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

/**
 * Collect all the filtered log segments from backups for a set of tablets
 * formerly belonging to a crashed master which is being recovered and pass
 * them to the recovery master to have them replayed.
 *
 * \param recoveryMaster
 *      A reference to the Master which owns this BackupManager and which will
 *      be responsible for the tablets.  This is used to provide a callback
 *      for the Master to replay individual segments.
 * \param masterId
 *      The id of the crashed master for which recoveryMaster will be taking
 *      over ownership of tablets.
 * \param tablets
 *      A set of tables with key ranges describing which poritions of which
 *      tables recoveryMaster should have replayed to it.
 * \param backups
 *      A list of backup locators along with a segmentId specifying for each
 *      segmentId a backup who can provide a filtered recovery data segment.
 *      A particular segment may be listed more than once if it has multiple
 *      viable backups, hence a particular backup locator can also be listed
 *      many times.
 */
void
BackupManager::recover(MasterServer& recoveryMaster,
                       uint64_t masterId,
                       const ProtoBuf::Tablets& tablets,
                       const ProtoBuf::ServerList& backups)
{
    LOG(NOTICE, "Recovering master %lu, %u tablets, %u hosts",
        masterId, tablets.tablet_size(), backups.server_size());
    // for each backup that names an unrec seg getRecData, pass to Server
    uint64_t segmentIdToRecover = ~(0ul);
    bool wasRecovered = true;
    for (int i = 0; i < backups.server_size(); i++) {
        const ProtoBuf::ServerList::Entry& server(backups.server(i));
        const string& locator = server.service_locator();
        if (!server.has_segment_id()) {
            LOG(WARNING,
                "ServerList of backups for recovery must contain segmentIds");
            continue;
        }
        // if we already recovered a segment with this id, skip this server
        if (wasRecovered && segmentIdToRecover == server.segment_id()) {
            LOG(DEBUG, "skipping %s, already recovered %lu",
                locator.c_str(), segmentIdToRecover);
            continue;
        }
        if (server.server_type() != ProtoBuf::BACKUP) {
            LOG(WARNING,
                "ServerList of backups for recovery shouldn't contain MASTERs");
            continue;
        }
        if (!wasRecovered)
            break;
        segmentIdToRecover = server.segment_id();
        wasRecovered = false;

        Buffer resp;
        try {
            LOG(DEBUG, "Getting recovery data for segment %lu from %s",
                segmentIdToRecover, locator.c_str());
            BackupClient backup(transportManager.getSession(locator.c_str()));
            backup.getRecoveryData(masterId, segmentIdToRecover, tablets, resp);
            LOG(DEBUG, "Got it");
        } catch (const TransportException& e) {
            // TODO(ongaro): change these to e.str().c_str once the unit tests
            // stop testing the exact string
            LOG(WARNING, "Couldn't contact %s, trying next backup; "
                "failure was: %s", locator.c_str(), e.message.c_str());
            continue;
        } catch (const ClientException& e) {
            // TODO(ongaro): change these to e.str().c_str once the unit tests
            // stop testing the exact string
            LOG(WARNING, "getRecoveryData failed on %s, trying next backup; "
                "failure was: %s", locator.c_str(), e.toString());
            continue;
        }
        recoveryMaster.recoverSegment(segmentIdToRecover,
                                      resp.getRange(0, resp.getTotalLength()),
                                      resp.getTotalLength());
        wasRecovered = true;
    }
    if (!wasRecovered) {
        LOG(ERROR, "*** Failed to recover segment id %lu, the recovered "
            "master state is corrupted, aborting recovery",
            segmentIdToRecover);
        // TODO(stutsman) at least need to clean up the hashtable
        throw SegmentRecoveryFailedException(HERE);
    }
}

/**
 * For testing; manually provides a list of backups to choose from so
 * this BackupManager won't try to talk to its coordinator (which is
 * presumably NULL).
 *
 * \param hosts
 *      A list of hosts to choose from when selecting places to
 *      put backups.
 */
void
BackupManager::setHostList(const ProtoBuf::ServerList& hosts)
{
    this->hosts = hosts;
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
    if (!replicas)
        return;

    uint32_t numHosts(static_cast<uint32_t>(hosts.server_size()));
    if (numHosts < replicas) {
        updateHostListFromCoordinator();
        numHosts = hosts.server_size();
        if (numHosts < replicas)
            DIE("Not enough backups to meet replication requirement");
    }

    if (!openHosts.empty())
        DIE("Cannot select new backups when some are already open");

    uint64_t random = generateRandom();
    uint32_t i = 0;
    while (i < replicas) {
        uint32_t index = random % numHosts;
        const ProtoBuf::ServerList::Entry& host(hosts.server(index));
        LOG(DEBUG, "Backing up to %s", host.service_locator().c_str());
        Transport::SessionRef session =
            transportManager.getSession(host.service_locator().c_str());
        openHosts.push_back(new BackupClient(session));
        i++;
        random++;
    }
}

/**
 * Populate the host list by fetching a list of hosts from the coordinator.
 */
void
BackupManager::updateHostListFromCoordinator()
{
    if (!coordinator)
        DIE("No coordinator given, replication requirements can't be met.");
    coordinator->getBackupList(hosts);
}

} // namespace RAMCloud
