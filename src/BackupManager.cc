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

// --- SegmentLocatorChooser ---

/**
 * \param list
 *      A list of servers along with the segment id they have backed up.
 *      See Recovery for details on this format.
 */
SegmentLocatorChooser::SegmentLocatorChooser(const ProtoBuf::ServerList& list)
    : map()
    , ids()
{
    foreach (const ProtoBuf::ServerList::Entry& server, list.server()) {
        if (!server.has_segment_id()) {
            LOG(WARNING,
                "List of backups for recovery must contain segmentIds");
            continue;
        }
        if (server.server_type() != ProtoBuf::BACKUP) {
            LOG(WARNING,
                "List of backups for recovery shouldn't contain MASTERs");
            continue;
        }
        map.insert(make_pair(server.segment_id(), server.service_locator()));
    }
    std::transform(map.begin(), map.end(), back_inserter(ids),
                   &first<uint64_t, string>);
    // not the most efficient approach in the world...
    SegmentIdList::iterator newEnd = std::unique(ids.begin(), ids.end());
    ids.erase(newEnd, ids.end());
    std::random_shuffle(ids.begin(), ids.end());
}

/**
 * Provide locators for potential backup server to contact to find
 * segment data during recovery.
 *
 * \param segmentId
 *      A segment id for a segment that needs to be recovered.
 * \return
 *      A service locator string indicating a location where this segment
 *      can be recovered from.
 * \throw SegmentRecoveryFailedException
 *      If the requested segment id has no remaining potential backup
 *      locations.
 */
const string&
SegmentLocatorChooser::get(uint64_t segmentId)
{
    LocatorMap::size_type count = map.count(segmentId);

    ConstLocatorRange range = map.equal_range(segmentId);
    if (range.first == range.second)
        throw SegmentRecoveryFailedException(HERE);

    LocatorMap::size_type random = generateRandom() % count;
    LocatorMap::const_iterator it = range.first;
    for (LocatorMap::size_type i = 0; i < random; ++i)
        ++it;
    return it->second;
}

/**
 * Returns a randomly ordered list of segment ids which acts as a
 * schedule for recovery.
 */
const SegmentLocatorChooser::SegmentIdList&
SegmentLocatorChooser::getSegmentIdList()
{
    return ids;
}

/**
 * Remove the locator as a potential backup location for a particular
 * segment.
 *
 * \param segmentId
 *      The id of a segment which could not be located at the specified
 *      backup locator.
 * \param locator
 *      The locator string that should not be returned from future calls
 *      to get for this particular #segmentId.
 */
void
SegmentLocatorChooser::markAsDown(uint64_t segmentId, const string& locator)
{
    LocatorRange range = map.equal_range(segmentId);
    if (range.first == map.end())
        return;

    for (LocatorMap::iterator it = range.first;
         it != range.second; ++it)
    {
        if (it->second == locator) {
            map.erase(it);
            return;
        }
    }
}

// --- BackupManager ---

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

    if (!mockRandomValue)
        srand(rdtsc());
    else
        srand(0);
#ifdef PERF_DEBUG_RECOVERY_SERIAL
    // for each backup that names an unrec seg getRecData, pass to Server
    SegmentLocatorChooser chooser(backups);
    for (SegmentLocatorChooser::SegmentIdList::const_iterator it =
            chooser.getSegmentIdList().begin();
         it != chooser.getSegmentIdList().end();
         ++it)
    {
        uint64_t segmentIdToRecover = *it;
        const string& locator = chooser.get(segmentIdToRecover);

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
    }
#else
    Buffer buffer1;
    Buffer buffer2;

    // The buffer which is about to be replayed.
    Buffer* foreBuffer = &buffer1;
    uint64_t foreSegmentId;

    // The buffer which is prefetching.
    Buffer* backBuffer = &buffer2;
    uint64_t backSegmentId;
    const string* backSegmentLocator;

    SegmentLocatorChooser chooser(backups);
    SegmentLocatorChooser::SegmentIdList::const_iterator segIdsIt =
        chooser.getSegmentIdList().begin();
    SegmentLocatorChooser::SegmentIdList::const_iterator segIdsEnd =
        chooser.getSegmentIdList().end();

    if (segIdsIt == segIdsEnd)
        return;

    backSegmentId = *segIdsIt;
    backSegmentLocator = &chooser.get(backSegmentId);
    BackupClient backup(
        transportManager.getSession(backSegmentLocator->c_str()));
    boost::scoped_ptr<BackupClient::GetRecoveryData> cont(
        new BackupClient::GetRecoveryData(backup,
            masterId, backSegmentId, tablets, *backBuffer));

    while (segIdsIt != segIdsEnd) {
        // Get the results from a previous fetch.
        try {
            if (cont) {
                LOG(DEBUG, "Waiting on recovery data for segment %lu from %s",
                    backSegmentId, backSegmentLocator->c_str());
                (*cont)();
                LOG(DEBUG, "Got it: %u bytes", backBuffer->getTotalLength());
                cont.reset();
            }
        } catch (const TransportException& e) {
            LOG(DEBUG, "Couldn't contact %s, trying next backup; failure was: "
                "%s", backSegmentLocator->c_str(), e.str().c_str());
            throw SegmentRecoveryFailedException(HERE);
            // TODO(stutsman) create cont for next backup with and continue
            continue;
        } catch (const ClientException& e) {
            LOG(DEBUG, "getRecoveryData failed on %s, trying next backup; "
                "failure was: %s",
                 backSegmentLocator->c_str(), e.str().c_str());
            throw SegmentRecoveryFailedException(HERE);
            // TODO(stutsman) create cont for next backup with and continue
            continue;
        }

        foreSegmentId = backSegmentId;
        std::swap(foreBuffer, backBuffer);

        // Kick off a new fetch and store the continuation.
        segIdsIt++;
        if (segIdsIt != segIdsEnd) {
            backSegmentId = *segIdsIt;
            backSegmentLocator = &chooser.get(backSegmentId);
            BackupClient backup(
                transportManager.getSession(backSegmentLocator->c_str()));
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
