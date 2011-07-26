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
        if (bandwidth == 1u)
            return 1u;
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
    , openLen(len)
    , offsetQueued(len)
    , closeQueued(false)
    , listEntries()
{
    // Call constructors on Tub<Backup> array
    foreach (auto& backup, backupIter())
        new(&backup) Tub<Backup>();
}

BackupManager::OpenSegment::~OpenSegment()
{
    // Call destructors on Tub<Backup> array
    foreach (auto& backup, backupIter())
        backup.~Tub();
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

/// Internal helper for #sync().
bool
BackupManager::isSynced()
{
    foreach (auto& segment, openSegmentList) {
        if (!replicas && segment.closeQueued)
            return false;
        foreach (auto& backup, segment.backupIter()) {
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
    uint64_t initTicks = metrics->master.backupManagerTicks;
    {
        CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
        while (!isSynced()) {
            proceedNoMetrics();
        }
    } // block ensures that _ is destroyed and counter stops
    serverStats.totalBackupSyncNanos += Cycles::toNanoseconds(
            metrics->master.backupManagerTicks - initTicks);
    serverStats.totalBackupSyncs++;
}

/**
 * Make progress on replicating the log to backups, but don't block.
 * This method checks for completion of outstanding backup operations and
 * starts new ones when possible.
 */
void
BackupManager::proceed()
{
    CycleCounter<Metric> _(&metrics->master.backupManagerTicks);
    proceedNoMetrics();
}

/// \copydoc proceed()
void
BackupManager::proceedNoMetrics()
{
    // reap all outstanding RPCs
    auto it = openSegmentList.begin();
    while (it != openSegmentList.end()) {
        auto& segment = *it;
        if (replicas && !segment.backups[0]) {
            ++it;
        } else {
            bool closeDone = segment.closeQueued;
            foreach (auto& backup, segment.backupIter()) {
                if (backup->rpc && backup->rpc->isReady()) {
                    LOG(DEBUG, "Wait %lu.%lu", segment.segmentId,
                        &backup - segment.backups);
                    (*backup->rpc)();
                    backup->rpc.destroy();
                    backup->openIsDone = true;
                    if (backup->closeSent)
                        backup->closeTicks.destroy();
                }
                closeDone &= (!backup->rpc && backup->closeSent);
            }
            ++it;
            if (closeDone) {
                 LOG(DEBUG, "Closed segment %lu, %lu",
                     *masterId, segment.segmentId);
                unopenSegment(&segment);
            }
        }
    }

    // send opens
    uint32_t i = 0;
    it = openSegmentList.begin();
    while (it != openSegmentList.end()) {
        if (i++ == 4) // pick something >= 3 to throttle number of open RPCs
            break;
        auto& segment = *it;

        bool openDone = true;
        foreach (auto& backup, segment.backupIter())
            openDone &= backup && backup->openIsDone;
        if (openDone) {
            ++it;
            continue;
        }

        if (replicas && !segment.backups[0]) { // no open request has been sent
            // Select backups, initialize backups,
            // and tell each of the backups to open the segment:
            ensureSufficientHosts();
            auto flags = BackupWriteRpc::OPENPRIMARY;
            auto& hostList = hosts;
            std::set<decltype(hostList.mutable_server(0))> usedHosts;

            foreach (auto& backup, segment.backupIter()) {
                auto host = pickRandomUnusedHost(hostList, usedHosts);
                if (flags == BackupWriteRpc::OPENPRIMARY) {
                    // Select the least loaded of 5 random backups:
                    for (uint32_t j = 0; j < 4; ++j) {
                        auto candidate = pickRandomUnusedHost(hostList,
                                                              usedHosts);
                        if (AbuserData(host).getMs() == 1u) {
                            // if we saw magic value which means pick at
                            // uniform random
                            host = candidate;
                            break;
                        }
                        if (AbuserData(host).getMs() >
                            AbuserData(candidate).getMs()) {
                            host = candidate;
                        }
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
                    *masterId, segment.segmentId,
                    host->service_locator().c_str());
                auto session = transportManager.getSession(
                                        host->service_locator().c_str());
                backup.construct(session);
                segments.insert({segment.segmentId, session});
                LOG(DEBUG, "Send open %lu.%lu", segment.segmentId,
                    &backup - segment.backups);
                backup->rpc.construct(backup->client,
                                      *masterId, segment.segmentId,
                                      0, segment.data, segment.openLen,
                                      flags);
                flags = BackupWriteRpc::OPEN;
                backup->offsetSent = segment.openLen;
            }
        }
        break; // opening segments should be serialized
    }

    // send writes+closes
    i = 0;
    it = openSegmentList.begin();
    while (it != openSegmentList.end()) {
        if (i++ == 2) // pick something to throttle the number of write RPCs
            break;
        // check if the 'it' segment can proceed with a write/close
        bool nextOpenDone = true;
        auto next = it;
        ++next;
        if (next != openSegmentList.end()) {
            foreach (auto& backup, next->backupIter())
                nextOpenDone &= backup && backup->openIsDone;
        }
        if (it->closeQueued && !nextOpenDone)
            break; // waiting for next segment's open response

        foreach (auto& backup, it->backupIter()) {
            if (!backup)
                break; // haven't started open yet
            if (backup->rpc)
                continue; // RPC already active
            if (backup->closeSent == it->closeQueued &&
                backup->offsetSent == it->offsetQueued)
                continue; // already synced
            if (it->closeQueued &&
                &backup == it->backups + replicas - 1) {
                if (metrics->master.logSyncBytes) {
                    backup->closeTicks.construct(
                        &metrics->master.logSyncCloseTicks);
                    ++metrics->master.logSyncCloseCount;
                } else {
                    backup->closeTicks.construct(
                        &metrics->master.replayCloseTicks);
                    ++metrics->master.replayCloseCount;
                }
            }
            backup->rpc.construct(backup->client,
                                  *masterId,
                                  it->segmentId,
                                  backup->offsetSent,
                                  (static_cast<const char*>(it->data) +
                                   backup->offsetSent),
#if SPEEDHACK
                // Used to make recovery benchmarks faster:
                // If you're feeling brave, this isn't a primary backup, and
                // this is not on a recovery master, there's not a real need to
                // replicate the object data.
                (&backup - it->backups > 0 && !metrics->pid) ? 0 :
#endif
                                  it->offsetQueued - backup->offsetSent,
                                  it->closeQueued ? BackupWriteRpc::CLOSE
                                                    : BackupWriteRpc::NONE);
            backup->offsetSent = it->offsetQueued;
            backup->closeSent = it->closeQueued;
            LOG(DEBUG, "Send write %lu.%lu (close=%d, offset=%d)",
                it->segmentId, &backup - it->backups, it->closeQueued,
                it->offsetQueued);
        }
        ++it;
    }
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
        if (numHosts < replicas) {
            LOG(ERROR, "Not enough backups to meet replication requirement "
                "(have %u, need %u)", numHosts, replicas);
            throw InternalError(HERE, STATUS_INTERNAL_ERROR);
        }
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
    if (!coordinator) {
        LOG(ERROR, "No coordinator given, replication requirements "
            "can't be met.");
        throw InternalError(HERE, STATUS_INTERNAL_ERROR);
    }
    coordinator->getBackupList(hosts);
}

} // namespace RAMCloud
