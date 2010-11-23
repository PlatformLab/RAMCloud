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

#include <boost/scoped_ptr.hpp>

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
    LOG(DEBUG, "openSegment %lu, %lu", masterId, segmentId);
    selectOpenHosts();
    foreach (BackupClient* host, openHosts) {
        LOG(DEBUG, "Opening %lu, %lu on an open backup", masterId, segmentId);
        host->openSegment(masterId, segmentId);
        segments.insert(SegmentMap::value_type(segmentId, host->getSession()));
    }
}

// return the index of the next seg to recover
int
selectBackup(const ProtoBuf::ServerList& backups,
             int startIndex, bool wasRecovered, bool& done)
{
    uint64_t segmentIdToRecover;
    if (startIndex == -1)
        segmentIdToRecover = ~(0ul);
    else
        segmentIdToRecover = backups.server(startIndex).segment_id();

    for (int i = startIndex + 1; i < backups.server_size(); i++) {
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
        return i;
    }
    if (!wasRecovered) {
        LOG(ERROR, "*** Failed to recover segment id %lu, the recovered "
            "master state is corrupted, aborting recovery",
            segmentIdToRecover);
        done = false;
        throw SegmentRecoveryFailedException(HERE);
    }
    done = true;
    return -1;
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

#ifdef PERF_DEBUG_RECOVERY_SERIAL
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
            BackupClient::GetRecoveryData(backup, masterId, segmentIdToRecover,
                                          tablets, resp)();
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
#else
    Buffer buffer1;
    Buffer buffer2;

    // The buffer which is about to be replayed.
    Buffer* foreBuffer = &buffer1;
    int foreIndex;
    uint64_t foreSegmentId;

    // The buffer which is prefetching.
    Buffer* backBuffer = &buffer2;
    int backIndex;
    uint64_t backSegmentId;

    bool done = false;
    backIndex = selectBackup(backups, -1, true, done);
    if (done)
        return;
    const ProtoBuf::ServerList::Entry* server = &backups.server(backIndex);
    backSegmentId = server->segment_id();
    BackupClient backup(
        transportManager.getSession(server->service_locator().c_str()));
    boost::scoped_ptr<BackupClient::GetRecoveryData> cont(
        new BackupClient::GetRecoveryData(backup,
            masterId, backSegmentId, tablets, *backBuffer));

    while (!done) {
        // Get the results from a previous fetch.
        const string& locator = server->service_locator();
        try {
            if (cont) {
                LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                    backSegmentId, locator.c_str());
                (*cont)();
                LOG(DEBUG, "Got it: %u bytes", backBuffer->getTotalLength());
                cont.reset();
            }
        } catch (const TransportException& e) {
            LOG(DEBUG, "Couldn't contact %s, trying next backup; "
                "failure was: %s", locator.c_str(), e.str().c_str());
            throw SegmentRecoveryFailedException(HERE);
            // TODO(stutsman) create cont for next backup with and continue
            continue;
        } catch (const ClientException& e) {
            LOG(DEBUG, "getRecoveryData failed on %s, trying next backup; "
                "failure was: %s", locator.c_str(), e.str().c_str());
            throw SegmentRecoveryFailedException(HERE);
            // TODO(stutsman) create cont for next backup with and continue
            continue;
        }

        foreIndex = backIndex;
        foreSegmentId = backSegmentId;
        std::swap(foreBuffer, backBuffer);

        // Kick off a new fetch and store the continuation.
        backIndex = selectBackup(backups, backIndex, true, done);
        if (!done) {
            server = &backups.server(backIndex);
            backSegmentId = server->segment_id();
            BackupClient backup(
                transportManager.getSession(server->service_locator().c_str()));
            cont.reset(new BackupClient::GetRecoveryData(backup,
                masterId, backSegmentId, tablets, *backBuffer));
        }

        // Processes the results from the buffer that has completed fetching.
        // TODO(stutsman) if an exception is thrown here, retry
        LOG(DEBUG, "Recovering with segment size %u",
            foreBuffer->getTotalLength());
        recoveryMaster.recoverSegment(
            foreSegmentId,
            foreBuffer->getRange(0, foreBuffer->getTotalLength()),
            foreBuffer->getTotalLength());
        foreBuffer->reset();
    }
#endif
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
        LOG(NOTICE, "Need backups, fetching server list from coordinator");
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
