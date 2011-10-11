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
#include "CycleCounter.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
#include "Segment.h"

namespace RAMCloud {

namespace {
/**
 * Packs and unpacks the user_data field of BackupSelector.hosts.
 * Used in BackupSelector.
 */
struct AbuserData{
  private:
    union X {
        struct {
            /**
             * Disk bandwidth of the host in MB/s
             */
            uint32_t bandwidth;
            /**
             * Number of primary segments this master has stored on the backup.
             */
            uint32_t numSegments;
        };
        /**
         * Raw user_data field.
         */
        uint64_t user_data;
    } x;
  public:
    explicit AbuserData(const ProtoBuf::ServerList::Entry* host)
        : x()
    {
        x.user_data = host->user_data();
    }
    X* operator*() { return &x; }
    X* operator->() { return &x; }
    /**
     * Return the expected number of milliseconds the backup would take to read
     * from its disk all of the primary segments this master has stored on it
     * plus an additional segment.
     */
    uint32_t getMs() {
        // unit tests, etc default to 100 MB/s
        uint32_t bandwidth = x.bandwidth ?: 100;
        if (bandwidth == 1u)
            return 1u;
        return downCast<uint32_t>((x.numSegments + 1) * 1000UL *
                                  Segment::SEGMENT_SIZE /
                                  1024 / 1024 / bandwidth);
    }
};

} // anonymous namespace

// --- BackupSelector ---

/**
 * Constructor.
 * \param coordinator
 *      See #coordinator.
 */
BackupManager::BackupSelector::BackupSelector(CoordinatorClient* coordinator)
    : updateHostListThrower()
    , coordinator(coordinator)
    , hosts()
    , hostsOrder()
    , numUsedHosts(0)
{
}

/**
 * Choose backups for a segment.
 * \param[in] numBackups
 *      The number of backups to choose.
 * \param[out] backups
 *      An array of numBackups entries in which to return the chosen backups.
 *      The first entry should store the primary replica.
 */
void
BackupManager::BackupSelector::select(uint32_t numBackups, Backup* backups[])
{
    if (numBackups == 0)
        return;
    while (hosts.server_size() == 0)
        updateHostListFromCoordinator();

    // Select primary (the least loaded of 5 random backups):
    auto& primary = backups[0];
    primary = getRandomHost();
    for (uint32_t i = 0; i < 5 - 1; ++i) {
        auto candidate = getRandomHost();
        if (AbuserData(primary).getMs() > AbuserData(candidate).getMs())
            primary = candidate;
    }
    AbuserData h(primary);
    LOG(DEBUG, "Chose backup with %u segments and %u MB/s disk bandwidth "
        "(expected time to read on recovery is %u ms)",
        h->numSegments, h->bandwidth, h.getMs());
    ++h->numSegments;
    primary->set_user_data(h->user_data);

    // Select secondaries:
    for (uint32_t i = 1; i < numBackups; ++i)
        backups[i] = selectAdditional(i, backups);
}

/**
 * Choose a random backup that does not conflict with an existing set of
 * backups.
 * \param[in] numBackups
 *      The number of entries in the \a backups array.
 * \param[in] backups
 *      An array of numBackups entries, none of which may conflict with the
 *      returned backup.
 */
BackupManager::BackupSelector::Backup*
BackupManager::BackupSelector::selectAdditional(uint32_t numBackups,
                                                const Backup* const backups[])
{
    while (true) {
        for (uint32_t i = 0; i < uint32_t(hosts.server_size()) * 2; ++i) {
            auto host = getRandomHost();
            if (!conflictWithAny(host, numBackups, backups))
                return host;
        }
        // The constraints must be unsatisfiable with the current backup list.
        LOG(NOTICE, "Current list of backups is insufficient, refreshing");
        updateHostListFromCoordinator();
    }
}

/**
 * Return a random backup.
 * Guaranteed to return all backups at least once after
 * any hosts.server_size() * 2 consecutive calls.
 * \pre
 *      The backup list is not empty.
 * \return
 *      A random backup.
 *
 * Conceptually, the algorithm operates as follows:
 * A set of candidate backups is initialized with the entire list of backups,
 * and a set of used backups starts off empty. With each call to getRandomHost,
 * one backup is chosen from the set of candidates and is moved into the set of
 * used backups. This is the backup that is returned. Once the set of
 * candidates is exhausted, start over.
 *
 * In practice, the algorithm is implemented efficiently:
 * Every index into the list of hosts is stored in the #hostsOrder array. The
 * backups referred to by hostsOrder[0] through hostsOrder[numUsedHosts - 1]
 * make up the set of used hosts, while the backups referred to by the
 * remainder of the array make up the candidate backups.
 */
BackupManager::BackupSelector::Backup*
BackupManager::BackupSelector::getRandomHost()
{
    assert(hosts.server_size() > 0);
    if (numUsedHosts >= hostsOrder.size())
        numUsedHosts = 0;
    uint32_t i = numUsedHosts;
    ++numUsedHosts;
    uint32_t j = i + downCast<uint32_t>(generateRandom() %
                                        (hostsOrder.size() - i));
    std::swap(hostsOrder[i], hostsOrder[j]);
    return hosts.mutable_server(hostsOrder[i]);
}

/**
 * Return whether it is unwise to place a replica on backup 'a' given that a
 * replica exists on backup 'b'. For example, it is unwise to place two
 * replicas on the same backup or on backups that share a common power source.
 */
bool
BackupManager::BackupSelector::conflict(const Backup* a, const Backup* b) const
{
    if (a == b)
        return true;
    // TODO(ongaro): Add other notions of conflicts, such as same rack.
    return false;
}

/**
 * Return whether it is unwise to place a replica on backup 'a' given that
 * replica exists on 'backups'. See #conflict.
 */
bool
BackupManager::BackupSelector::conflictWithAny(const Backup* a,
                                           uint32_t numBackups,
                                           const Backup* const backups[]) const
{
    for (uint32_t i = 0; i < numBackups; ++i) {
        if (conflict(a, backups[i]))
            return true;
    }
    return false;
}

/**
 * Populate the host list by fetching a list of hosts from the coordinator.
 */
void
BackupManager::BackupSelector::updateHostListFromCoordinator()
{
    updateHostListThrower();
    if (!coordinator)
        DIE("No coordinator given, replication requirements can't be met.");
    // TODO(ongaro): This forgets about the number of primaries
    //               stored on each backup.
    coordinator->getBackupList(hosts);

    hostsOrder.clear();
    hostsOrder.reserve(hosts.server_size());
    for (uint32_t i = 0; i < uint32_t(hosts.server_size()); ++i)
        hostsOrder.push_back(i);
    numUsedHosts = 0;
}

// --- BackupManager::OpenSegment ---

/**
 * Constructor.
 * Must be constructed on #sizeOf(backupManager.replicas) bytes of space.
 * The arguments are the same as those to #BackupManager::openSegment.
 */
BackupManager::OpenSegment::OpenSegment(BackupManager& backupManager,
                                        uint64_t segmentId,
                                        const void* data,
                                        uint32_t len)
    : backupManager(backupManager)
    , segmentId(segmentId)
    , data(data)
    , openLen(len)
    , offsetQueued(len)
    , closeQueued(false)
    , listEntries()
    , backups(backupManager.replicas)
{
}

/**
 * Eventually replicate the \a len bytes of data starting at \a offset into the
 * segment.
 * Guarantees that no replica will see this write until it has seen all
 * previous writes on this segment.
 * \pre
 *      All previous segments have been closed (at least locally).
 * \param offset
 *      The number of bytes into the segment through which to replicate.
 * \param closeSegment
 *      Whether to close the segment after writing this data. If this is true,
 *      the caller's OpenSegment pointer is invalidated upon the return of this
 *      function.
 */
void
BackupManager::OpenSegment::write(uint32_t offset,
                                  bool closeSegment)
{
    TEST_LOG("%lu, %lu, %u, %d",
             *backupManager.masterId, segmentId, offset, closeSegment);

    // offset monotonically increases
    assert(offset >= offsetQueued);
    offsetQueued = offset;

    // immutable after close
    assert(!closeQueued);
    closeQueued = closeSegment;
    if (closeQueued) {
        LOG(DEBUG, "Segment %lu closed (length %d)", segmentId, offsetQueued);
        ++metrics->master.segmentCloseCount;
    }
}

// --- BackupManager ---

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with.
 * \param coordinator
 *      \copydoc coordinator
 * \param masterId
 *      \copydoc masterId
 * \param replicas
 *      \copydoc replicas
 */
BackupManager::BackupManager(CoordinatorClient* coordinator,
                             const Tub<uint64_t>& masterId,
                             uint32_t replicas)
    : coordinator(coordinator)
    , masterId(masterId)
    , backupSelector(coordinator)
    , replicas(replicas)
    , segments()
    , openSegmentPool(OpenSegment::sizeOf(replicas))
    , openSegmentList()
    , outstandingRpcs(0)
    , activeTime()
{
}

/**
 * Create a BackupManager, initially with no backup hosts to communicate
 * with. This manager is constructed the same way as a previous manager.
 * This is used, for instance, by the LogCleaner to obtain a private
 * BackupManager that is configured equivalently to the Log's own
 * manager (without having to share the two).
 * 
 * \param prototype
 *      The BackupManager that serves as a prototype for this newly
 *      created one. The same masterId, number of replicas, and
 *      coordinator are used.
 */
BackupManager::BackupManager(BackupManager* prototype)
    : coordinator(prototype->coordinator)
    , masterId(prototype->masterId)
    , backupSelector(prototype->coordinator)
    , replicas(prototype->replicas)
    , segments()
    , openSegmentPool(OpenSegment::sizeOf(replicas))
    , openSegmentList()
    , outstandingRpcs(0)
    , activeTime()
{
}

BackupManager::~BackupManager()
{
    sync();
    while (!openSegmentList.empty())
        unopenSegment(&openSegmentList.front());
}

/**
 * Ask backups to discard a segment.
 */
void
BackupManager::freeSegment(uint64_t segmentId)
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    TEST_LOG("%lu, %lu", *masterId, segmentId);

    // Make sure this segment isn't open:
    foreach (auto& openSegment, openSegmentList) {
        if (openSegment.segmentId == segmentId) {
            unopenSegment(&openSegment);
            break;
        }
    }

    // Free the segment on its backups:
    const auto iters = segments.equal_range(segmentId);
    foreach (auto item, iters)
        BackupClient(item.second.session).freeSegment(*masterId, segmentId);
    segments.erase(iters.first, iters.second);
}

/**
 * Eventually begin replicating a segment on backups.
 *
 * \param segmentId
 *      A unique identifier for this segment. The caller must ensure this
 *      segment is not already open.
 * \param data
 *      Location at which data to be replicated for this segment begins.
 * \param len
 *      The number of bytes to send atomically to backups with the open segment
 *      RPC.
 * \return
 *      A pointer to an OpenSegment object that is valid only until that
 *      segment is closed.
 */
BackupManager::OpenSegment*
BackupManager::openSegment(uint64_t segmentId, const void* data, uint32_t len)
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    LOG(DEBUG, "openSegment %lu, %lu, ..., %u", *masterId, segmentId, len);
    auto* p = openSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    auto* openSegment = new(p) OpenSegment(*this, segmentId, data, len);
    openSegmentList.push_back(*openSegment);
    return openSegment;
}

/// Internal helper for #sync().
bool
BackupManager::isSynced()
{
    // TODO(ongaro): Change to return (rpcsInFlight == 0)?
    //               Will need to call proceed in openSegment and write.
    foreach (auto& segment, openSegmentList) {
        if (replicas == 0 && segment.closeQueued)
            return false;
        foreach (auto& backup, segment.backups) {
            if (!backup || backup->rpc)
                return false;
            if (backup->closeSent != segment.closeQueued ||
                backup->offsetSent != segment.offsetQueued) {
                return false;
            }
        }
    }
    return true;
}

/**
 * Wait until all written data has been acknowledged by the backups for all
 * segments.
 */
void
BackupManager::sync()
{
    {
        CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
        while (!isSynced()) {
            proceedNoMetrics();
        }
    } // block ensures that _ is destroyed and counter stops
    assert(outstandingRpcs == 0);
}

/**
 * Make progress on replicating the log to backups, but don't block.
 * This method checks for completion of outstanding backup operations and
 * starts new ones when possible.
 */
void
BackupManager::proceed()
{
    CycleCounter<RawMetric> _(&metrics->master.backupManagerTicks);
    proceedNoMetrics();
}

/// \copydoc proceed()
void
BackupManager::proceedNoMetrics()
{
    // Reap all outstanding RPCs.
    // Note: cannot use foreach because unopenSegment modifies openSegmentList.
    auto it = openSegmentList.begin();
    while (it != openSegmentList.end()) {
        auto& segment = *it;
        ++it;
        if (!replicas || segment.backups[0]) {
            bool closeDone = segment.closeQueued;
            foreach (auto& backup, segment.backups) {
                if (backup->rpc && backup->rpc->isReady()) {
                    LOG(DEBUG, "Wait %lu.%lu", segment.segmentId,
                        &backup - &segment.backups[0]);
                    (*backup->rpc)();
                    backup->rpc.destroy();
                    outstandingRpcs--;
                    backup->openIsDone = true;
                    if (backup->closeSent)
                        backup->closeTicks.destroy();
                }
                closeDone &= (!backup->rpc && backup->closeSent);
            }
            if (closeDone) {
                 LOG(DEBUG, "Closed segment %lu, %lu",
                     *masterId, segment.segmentId);
                unopenSegment(&segment);
            }
        }
    }

    // send opens
    uint32_t i = 0;
    foreach (auto& segment, openSegmentList) {
        if (i++ == 4) // pick something >= 3 to throttle number of open RPCs
            break;
        bool openDone = true;
        foreach (auto& backup, segment.backups)
            openDone &= backup && backup->openIsDone;
        if (openDone)
            continue;

        if (replicas != 0 && !segment.backups[0]) {
            // No open request has been sent:
            // select backups, initialize backups,
            // and tell each of the backups to open the segment.
            ProtoBuf::ServerList::Entry* backupHosts[replicas];
            backupSelector.select(replicas, backupHosts);
            auto flags = BackupWriteRpc::OPENPRIMARY;

            uint32_t i = 0;
            foreach (auto& backup, segment.backups) {
                auto host = backupHosts[i++];

                LOG(DEBUG, "Opening segment %lu, %lu.%lu on backup %s",
                    *masterId, segment.segmentId, &backup - &segment.backups[0],
                    host->service_locator().c_str());
                auto session = Context::get().transportManager->getSession(
                                        host->service_locator().c_str());
                // TODO(stutsman): catch exceptions
                backup.construct(session);
                segments.insert({segment.segmentId,
                                 ReplicaLocation(host->server_id(), session)});
                LOG(DEBUG, "Send open %lu.%lu", segment.segmentId,
                    &backup - &segment.backups[0]);
                backup->rpc.construct(backup->client,
                                      *masterId, segment.segmentId,
                                      0, segment.data, segment.openLen,
                                      flags);
                flags = BackupWriteRpc::OPEN;
                backup->offsetSent = segment.openLen;
                outstandingRpcs++;
            }
        }
        break; // opening segments should be serialized
    }

    // send writes+closes
    i = 0;
    foreach (auto& segment, openSegmentList) {
        if (i++ == 4) // pick something to throttle the number of write RPCs
            break;
        // check if 'segment' can proceed with a write/close
        bool nextOpenDone = true;
        auto next = openSegmentList.iterator_to(segment);
        ++next;
        if (next != openSegmentList.end()) {
            foreach (auto& backup, next->backups)
                nextOpenDone &= backup && backup->openIsDone;
        }
        if (segment.closeQueued && !nextOpenDone)
            break; // waiting for next segment's open response

        foreach (auto& backup, segment.backups) {
            if (!backup)
                break; // haven't started open yet
            if (backup->rpc)
                continue; // RPC already active
            if (backup->closeSent == segment.closeQueued &&
                backup->offsetSent == segment.offsetQueued)
                continue; // already synced
            if (segment.closeQueued &&
                &backup == &segment.backups[0] + replicas - 1) {
                if (metrics->master.logSyncBytes) {
                    backup->closeTicks.construct(
                        &metrics->master.logSyncCloseTicks);
                    ++metrics->master.logSyncCloseCount;
                } else {
                    backup->closeTicks.construct(
                        &metrics->master.backupCloseTicks);
                    ++metrics->master.backupCloseCount;
                }
            }

            uint32_t writeBytes = segment.offsetQueued - backup->offsetSent;
            BackupWriteRpc::Flags flags = segment.closeQueued ?
                BackupWriteRpc::CLOSE : BackupWriteRpc::NONE;

            // Throttle the largest sync we're willing to send. This avoids
            // clogging up the backup for a long time with an 8MB chunk to
            // eat through.
            if (writeBytes > MAX_WRITE_RPC_BYTES) {
                writeBytes = MAX_WRITE_RPC_BYTES;
                flags = BackupWriteRpc::NONE;
            }

            backup->rpc.construct(backup->client,
                                  *masterId,
                                  segment.segmentId,
                                  backup->offsetSent,
                                  (static_cast<const char*>(segment.data) +
                                   backup->offsetSent),
                                  writeBytes,
                                  flags);
            backup->offsetSent += writeBytes;
            backup->closeSent = (flags == BackupWriteRpc::CLOSE);
            LOG(DEBUG, "Send write %lu.%lu (close=%d, offset=%d)",
                segment.segmentId, &backup - &segment.backups[0],
                segment.closeQueued, segment.offsetQueued);
            outstandingRpcs++;
        }
    }
    if (outstandingRpcs > 0) {
        if (!activeTime)
            activeTime.construct(&metrics->master.replicationTicks);
    } else {
        activeTime.destroy();
    }
}

// - private -

/**
 * Remove the segment from openSegmentList, call its destructor,
 * and free its memory.
 * This is the opposite of #openSegment.
 */
void
BackupManager::unopenSegment(OpenSegment* openSegment)
{
    erase(openSegmentList, *openSegment);
    openSegment->~OpenSegment();
    openSegmentPool.free(openSegment);
}

} // namespace RAMCloud
