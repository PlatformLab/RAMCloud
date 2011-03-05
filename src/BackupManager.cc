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
#include "Metrics.h"
#include "Segment.h"

namespace RAMCloud {

// --- BackupManager::OpenSegment ---

namespace {
/**
 * Packs and unpacks the user_data field of backupManager.hosts.
 * Used in BackupManager.
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
    template<typename T>
    explicit AbuserData(const T* host)
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
        return downCast<uint32_t>((x.numSegments + 1) * 1000UL *
                                  Segment::SEGMENT_SIZE /
                                  1024 / 1024 / bandwidth);
    }
};

template <typename HostList, typename Set>
auto
pickRandomUnusedHost(HostList& hostList, const Set& usedHosts) ->
    decltype(hostList.mutable_server(0))
{
    assert(static_cast<uint64_t>(hostList.server_size()) > usedHosts.size());
    while (true) {
        uint32_t index = downCast<uint32_t>(generateRandom() %
                                            hostList.server_size());
        auto host = hostList.mutable_server(index);
        if (!contains(usedHosts, host))
            return host;
    }
}
} // anonymous namespace

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
    , offsetQueued(len)
    , closeQueued(false)
    , listEntries()
{
    // Select backups, initialize backups,
    // and tell each of the backups to open the segment:
    backupManager.ensureSufficientHosts();
    auto flags = BackupWriteRpc::OPENPRIMARY;
    auto& hostList = backupManager.hosts;
    std::set<decltype(hostList.mutable_server(0))> usedHosts;

    foreach (auto& backup, backupIter()) {
        auto host = pickRandomUnusedHost(hostList, usedHosts);
        if (flags == BackupWriteRpc::OPENPRIMARY) {
            // Select the least loaded of 5 random backups:
            for (uint32_t i = 0; i < 4; ++i) {
                auto candidate = pickRandomUnusedHost(hostList, usedHosts);
                if (AbuserData(host).getMs() > AbuserData(candidate).getMs())
                    host = candidate;
            }
            AbuserData h(host);
            LOG(DEBUG, "Chose backup with "
                "%u segments and %u MB/s disk bandwidth "
                "(expected time to read on recovery is %u ms)",
                h->numSegments, h->bandwidth, h.getMs());
            ++h->numSegments;
            host->set_user_data(h->user_data);
        }
        usedHosts.insert(host);

        LOG(DEBUG, "Opening segment %lu, %lu on backup %s",
            *backupManager.masterId, segmentId,
            host->service_locator().c_str());
        auto session =
            transportManager.getSession(host->service_locator().c_str());
        new(&backup) Backup(session);
        backupManager.segments.insert({segmentId, session});
        backup.writeSegmentTub.construct(backup.client,
                                         *backupManager.masterId, segmentId,
                                         0, data, len, flags);
        flags = BackupWriteRpc::OPEN;
        backup.offsetSent = len;
    }
    // Wait for segment open acknowledgements from backups:
    CycleCounter<Metric> _(&metrics->master.segmentOpenStallTicks);
    waitForWriteRequests();
}

BackupManager::OpenSegment::~OpenSegment()
{
    // Call destructors on Backup objects that were constructed
    // with placement new:
    foreach (auto& backup, backupIter())
        backup.~Backup();
}

/**
 * Eventually replicate the \a len bytes of data starting at \a offset into the
 * segment.
 * Waits for all previous segments to finish replicating and guarantees that no
 * replica will see this write until it has seen all previous writes on this
 * segment.
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
    CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
    TEST_LOG("%lu, %lu, %u, %d",
             *backupManager.masterId, segmentId, offset, closeSegment);

    if (this != &backupManager.openSegmentList.front()) {
        // If this isn't the earliest open segment, the one before must be the
        // front and must have a close queued. Wait for its close to be
        // acknowledged, then this must be at the front of the list.
        backupManager.openSegmentList.front().sync();
        assert(this == &backupManager.openSegmentList.front());
    }

    // offset monotonically increases
    assert(offset >= offsetQueued);
    offsetQueued = offset;

    // immutable after close
    assert(!closeQueued);
    closeQueued = closeSegment;
    if (closeQueued)
        ++metrics->master.segmentCloseCount;

    sendWriteRequests();
}

/**
 * Wait until all written data has been acknowledged by the backups for this
 * segment.
 */
void
BackupManager::OpenSegment::sync()
{
    {
        CycleCounter<Metric> _(&metrics->master.segmentWriteStallTicks);
        waitForWriteRequests();
    }
    sendWriteRequests();
    {
        CycleCounter<Metric> _(&metrics->master.segmentWriteStallTicks);
        waitForWriteRequests();
    }
    if (closeQueued) {
        LOG(DEBUG, "Closed segment %lu, %lu",
            *backupManager.masterId, segmentId);
        backupManager.unopenSegment(this);
        return; // 'this' instance has been destroyed
    }
}

/// Try to send all queued requests to the backups.
void
BackupManager::OpenSegment::sendWriteRequests()
{
    foreach (auto& backup, backupIter()) {
        if (backup.writeSegmentTub)
            continue;
        if (backup.closeSent == closeQueued &&
            backup.offsetSent == offsetQueued)
            continue;
        backup.writeSegmentTub.construct(backup.client,
                                         *backupManager.masterId,
                                         segmentId,
                                         backup.offsetSent,
                                         (static_cast<const char*>(data) +
                                          backup.offsetSent),
                                         offsetQueued - backup.offsetSent,
                                         closeQueued ? BackupWriteRpc::CLOSE
                                                     : BackupWriteRpc::NONE);
        backup.offsetSent = offsetQueued;
        backup.closeSent = closeQueued;
    }
}

/// Wait for outstanding requests from the backups.
void
BackupManager::OpenSegment::waitForWriteRequests()
{
    foreach (auto& backup, backupIter()) {
        if (!backup.writeSegmentTub)
            continue;
        // TODO(stutsman) Exception during one of the writes?
        (*backup.writeSegmentTub)();
        backup.writeSegmentTub.destroy();
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
    , hosts()
    , replicas(replicas)
    , segments()
    , openSegmentPool(OpenSegment::sizeOf(replicas))
    , openSegmentList()
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
    CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
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
    foreach (auto item, iters) {
        auto session = item.second;
        BackupClient(session).freeSegment(*masterId, segmentId);
    }
    segments.erase(iters.first, iters.second);
}

/**
 * Begin replicating a new segment on backups.
 *
 * Opening a segment happens synchronously and does not depend on other open
 * segments. The first \a len bytes of \a data are replicated immediately to
 * backups without regard to whether other open segments have been fully
 * replicated or even closed. This method returns once all replicas have opened
 * the segment and received the immediate data.
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
    CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
    LOG(DEBUG, "openSegment %lu, %lu, ..., %u", *masterId, segmentId, len);
    auto* p = openSegmentPool.malloc();
    if (p == NULL)
        DIE("Out of memory");
    auto* openSegment = new(p) OpenSegment(*this, segmentId, data, len);
    openSegmentList.push_back(*openSegment);
    return openSegment;
}

/**
 * Wait until all written data has been acknowledged by the backups for all
 * segments.
 */
void
BackupManager::sync()
{
    CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
    if (openSegmentList.empty())
        return;
    // At most one segment can have outstanding data,
    // so it's sufficient to sync just the first segment.
    openSegmentList.front().sync();
}

// - private -

/**
 * Make sure #hosts contains at least #replicas entries.
 */
void
BackupManager::ensureSufficientHosts()
{
    if (!replicas)
        return;

    uint32_t numHosts(hosts.server_size());
    if (numHosts < replicas) {
        LOG(NOTICE, "Need backups, fetching server list from coordinator");
        updateHostListFromCoordinator();
        numHosts = hosts.server_size();
        if (numHosts < replicas)
            DIE("Not enough backups to meet replication requirement "
                "(have %u, need %u)", numHosts, replicas);
    }
}

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
