/* Copyright (c) 2010-2014 Stanford University
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

#include <assert.h>
#include <stdint.h>

#include "Common.h"
#include "Fence.h"
#include "Log.h"
#include "LogCleaner.h"
#include "ShortMacros.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "ServerConfig.h"
#include "WallTime.h"

namespace RAMCloud {

/**
 * Construct a new LogCleaner object. The cleaner will not perform any garbage
 * collection until the start() method is invoked.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param config
 *      Server configuration from which the cleaner will extract any runtime
 *      parameters that affect its operation.
 * \param segmentManager
 *      The SegmentManager to query for newly cleanable segments, allocate
 *      survivor segments from, and report cleaned segments to.
 * \param replicaManager
 *      The ReplicaManager to use in backing up segments written out by the
 *      cleaner.
 * \param entryHandlers
 *     Class responsible for entries stored in the log. The cleaner will invoke
 *     it when they are being relocated during cleaning, for example.
 */
LogCleaner::LogCleaner(Context* context,
                       const ServerConfig* config,
                       SegmentManager& segmentManager,
                       ReplicaManager& replicaManager,
                       LogEntryHandlers& entryHandlers)
    : context(context),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      entryHandlers(entryHandlers),
      cleanableSegments(segmentManager, config, onDiskMetrics),
      writeCostThreshold(config->master.cleanerWriteCostThreshold),
      disableInMemoryCleaning(config->master.disableInMemoryCleaning),
      numThreads(config->master.cleanerThreadCount),
      segletSize(config->segletSize),
      segmentSize(config->segmentSize),
      doWorkTicks(0),
      doWorkSleepTicks(0),
      inMemoryMetrics(),
      onDiskMetrics(),
      threadMetrics(numThreads),
      threadsShouldExit(false),
      threads(),
      balancer(NULL)
{
    if (!segmentManager.initializeSurvivorReserve(numThreads *
                                                  SURVIVOR_SEGMENTS_TO_RESERVE))
        throw FatalError(HERE, "Could not reserve survivor segments");

    // TODO(rumble): get rid of this. it doesn't exist anymore other than to
    // support old scripts where wct = 0 implies single level cleaning
    if (writeCostThreshold == 0)
        disableInMemoryCleaning = true;

    // There's no point in doing compaction if we aren't replicating. It's
    // more efficient to just perform "disk" cleaning.
    if (config->master.numReplicas == 0) {
        LOG(NOTICE, "Replication factor is 0; memory compaction disabled.");
        disableInMemoryCleaning = true;
    }

    string balancerArg = config->master.cleanerBalancer;
    if (balancerArg.compare(0, 6, "fixed:") == 0) {
        string fixedPercentage = balancerArg.substr(6);
        balancer = new FixedBalancer(this, atoi(fixedPercentage.c_str()));
    } else if (balancerArg.compare(0, 15, "tombstoneRatio:") == 0) {
        string ratio = balancerArg.substr(15);
        balancer = new TombstoneRatioBalancer(this, atof(ratio.c_str()));
    } else {
        DIE("Unknown balancer specified: \"%s\"", balancerArg.c_str());
    }

    for (int i = 0; i < numThreads; i++)
        threads.push_back(NULL);
}

/**
 * Destroy the cleaner. Any running threads are stopped first.
 */
LogCleaner::~LogCleaner()
{
    stop();
    delete balancer;
    TEST_LOG("destroyed");
}

/**
 * Start the log cleaner, if it isn't already running. This spins a thread that
 * continually cleans if there's work to do until stop() is called.
 *
 * The cleaner will not do any work until explicitly enabled via this method.
 *
 * This method may be called any number of times, but it is not thread-safe.
 * That is, do not call start() and stop() in parallel.
 */
void
LogCleaner::start()
{
    for (int i = 0; i < numThreads; i++) {
        if (threads[i] == NULL)
            threads[i] = new std::thread(cleanerThreadEntry, this, context);
    }
}

/**
 * Halt the cleaner thread (if it is running). Once halted, it will do no more
 * work until start() is called again.
 *
 * This method may be called any number of times, but it is not thread-safe.
 * That is, do not call start() and stop() in parallel.
 */
void
LogCleaner::stop()
{
    threadsShouldExit = true;
    Fence::sfence();

    for (int i = 0; i < numThreads; i++) {
        if (threads[i] != NULL) {
            threads[i]->join();
            delete threads[i];
            threads[i] = NULL;
        }
    }

    threadsShouldExit = false;

    /// XXX- should set threadCnt to 0 so we can clean on disk again!
}

/**
 * Fill in the provided protocol buffer with metrics, giving other modules and
 * servers insight into what's happening in the cleaner.
 */
void
LogCleaner::getMetrics(ProtoBuf::LogMetrics_CleanerMetrics& m)
{
    m.set_poll_usec(POLL_USEC);
    m.set_max_cleanable_memory_utilization(MAX_CLEANABLE_MEMORY_UTILIZATION);
    m.set_live_segments_per_disk_pass(MAX_LIVE_SEGMENTS_PER_DISK_PASS);
    m.set_survivor_segments_to_reserve(SURVIVOR_SEGMENTS_TO_RESERVE);
    m.set_min_memory_utilization(MIN_MEMORY_UTILIZATION);
    m.set_min_disk_utilization(MIN_DISK_UTILIZATION);
    m.set_do_work_ticks(doWorkTicks);
    m.set_do_work_sleep_ticks(doWorkSleepTicks);
    inMemoryMetrics.serialize(*m.mutable_in_memory_metrics());
    onDiskMetrics.serialize(*m.mutable_on_disk_metrics());
    threadMetrics.serialize(*m.mutable_thread_metrics());
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

static volatile uint32_t threadCnt;

/**
 * Static entry point for the cleaner thread. This is invoked via the
 * std::thread() constructor. This thread performs continuous cleaning on an
 * as-needed basis.
 */
void
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    LOG(NOTICE, "LogCleaner thread started");

    CleanerThreadState state;
    state.threadNumber = __sync_fetch_and_add(&threadCnt, 1);
    try {
        while (1) {
            Fence::lfence();
            if (logCleaner->threadsShouldExit)
                break;

            logCleaner->doWork(&state);
        }
    } catch (const Exception& e) {
        DIE("Fatal error in cleaner thread: %s", e.what());
    }

    LOG(NOTICE, "LogCleaner thread stopping");
}

int
LogCleaner::getLiveObjectUtilization()
{
    return cleanableSegments.getLiveObjectUtilization();
}

int
LogCleaner::getUndeadTombstoneUtilization()
{
    return cleanableSegments.getUndeadTombstoneUtilization();
}

/**
 * Main cleaning loop, constantly invoked via cleanerThreadEntry(). If there
 * is cleaning to be done, do it now return. If no work is to be done, sleep for
 * a bit before returning (and getting called again), rather than banging on the
 * CPU.
 */
void
LogCleaner::doWork(CleanerThreadState* state)
{
    AtomicCycleCounter _(&doWorkTicks);

    threadMetrics.noteThreadStart();

    bool goToSleep = false;
    switch (balancer->requestTask(state)) {
    case Balancer::CLEAN_DISK:
      {
        CycleCounter<uint64_t> __(&state->diskCleaningTicks);
        doDiskCleaning();
        break;
      }

    case Balancer::COMPACT_MEMORY:
      {
        CycleCounter<uint64_t> __(&state->memoryCompactionTicks);
        doMemoryCleaning();
        break;
      }

    case Balancer::SLEEP:
        goToSleep = true;
        break;
    }

    threadMetrics.noteThreadStop();

    if (goToSleep) {
        AtomicCycleCounter __(&doWorkSleepTicks);
        // Jitter the sleep delay a little bit (up to 10%). It's not a big deal
        // if we don't, but it can make some locks look artificially contended
        // when there's no cleaning to be done and threads manage to caravan
        // together.
        useconds_t r = downCast<useconds_t>(generateRandom() % POLL_USEC) / 10;
        usleep(POLL_USEC + r);
    }
}

/**
 * Perform an in-memory cleaning pass. This takes a segment and compacts it,
 * re-packing all live entries together sequentially, allowing us to reclaim
 * some of the dead space.
 */
void
LogCleaner::doMemoryCleaning()
{
    TEST_LOG("called");
    AtomicCycleCounter _(&inMemoryMetrics.totalTicks);

    if (disableInMemoryCleaning)
        return;

    LogSegment* segment = getSegmentToCompact();
    if (segment == NULL)
        return;

    LogCleanerMetrics::InMemory<uint64_t> localMetrics;

    // If the segment happens to be empty then there's no data to move.
    const bool empty = (segment->getLiveBytes() == 0);
    if (empty)
        localMetrics.totalEmptySegmentsCompacted++;

    // Allocate a survivor segment to write into. This call may block if one
    // is not available right now.
    CycleCounter<uint64_t> waitTicks(&localMetrics.waitForFreeSurvivorTicks);
    LogSegment* survivor = segmentManager.allocSideSegment(
            SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
            segment);
    assert(survivor != NULL);
    waitTicks.stop();

    localMetrics.totalBytesInCompactedSegments +=
        segment->getSegletsAllocated() * segletSize;
    uint32_t liveScannedEntryTotalLengths[TOTAL_LOG_ENTRY_TYPES] = { 0 };

    // Take two passes, writing out the tombstones first. This makes the
    // dead tombstone scanner in CleanableSegmentManager more efficient
    // since it will only need to scan the front of the segment.
    for (int tombstonePass = 1; tombstonePass >= 0 && !empty; tombstonePass--) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();

            if (tombstonePass && type != LOG_ENTRY_TYPE_OBJTOMB)
                continue;
            if (!tombstonePass && type == LOG_ENTRY_TYPE_OBJTOMB)
                continue;

            Buffer buffer;
            it.appendToBuffer(buffer);
            Log::Reference reference = segment->getReference(it.getOffset());
            uint32_t bytesAppended = 0;
            RelocStatus s = relocateEntry(type,
                                          buffer,
                                          reference,
                                          survivor,
                                          localMetrics,
                                          &bytesAppended);
            if (expect_false(s == RELOCATION_FAILED))
                throw FatalError(HERE, "Entry didn't fit into survivor!");

            localMetrics.totalEntriesScanned[type]++;
            localMetrics.totalScannedEntryLengths[type] +=
                buffer.size();
            if (expect_true(s == RELOCATED)) {
                localMetrics.totalLiveEntriesScanned[type]++;
                localMetrics.totalLiveScannedEntryLengths[type] +=
                    buffer.size();
                liveScannedEntryTotalLengths[type] += bytesAppended;
            }
        }
    }

    // Be sure to update the usage statistics for the new segment. This
    // is done once here, rather than for each relocated entry because
    // it avoids the expense of atomically updating those fields.
    for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
        survivor->trackNewEntries(static_cast<LogEntryType>(i),
                    downCast<uint32_t>(localMetrics.totalLiveEntriesScanned[i]),
                    liveScannedEntryTotalLengths[i]);
    }

    survivor->close();
    uint32_t segletsToFree = computeFreeableSeglets(survivor);
    bool r = survivor->freeUnusedSeglets(segletsToFree);
    assert(r);
    assert(segment->getSegletsAllocated() >= survivor->getSegletsAllocated());

    uint64_t freeSegletsGained = segment->getSegletsAllocated() -
                                 survivor->getSegletsAllocated();
    if (freeSegletsGained == 0)
        balancer->compactionFailed();
    uint64_t bytesFreed = freeSegletsGained * segletSize;
    localMetrics.totalBytesFreed += bytesFreed;
    localMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();
    localMetrics.totalSegmentsCompacted++;

    // Merge our local metrics into the global aggregate counters.
    inMemoryMetrics.merge(localMetrics);

    AtomicCycleCounter __(&inMemoryMetrics.compactionCompleteTicks);
    segmentManager.compactionComplete(segment, survivor);
}

/**
 * Perform a disk cleaning pass if possible. Doing so involves choosing segments
 * to clean, extracting entries from those segments, writing them out into new
 * "survivor" segments, and alerting the segment manager upon completion.
 */
void
LogCleaner::doDiskCleaning()
{
    TEST_LOG("called");
    AtomicCycleCounter _(&onDiskMetrics.totalTicks);

    // Obtain the segments we'll clean in this pass. We're guaranteed to have
    // the resources to clean what's returned.
    LogSegmentVector segmentsToClean;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return;

    onDiskMetrics.memoryUtilizationAtStartSum +=
        segmentManager.getMemoryUtilization();

    // Extract the currently live entries of the segments we're cleaning and
    // sort them by age.
    EntryVector entries;
    getSortedEntries(segmentsToClean, entries);

    uint64_t maxLiveBytes = 0;
    uint32_t segletsBefore = 0;
    foreach (LogSegment* segment, segmentsToClean) {
        uint64_t liveBytes = segment->getLiveBytes();
        if (liveBytes == 0)
            onDiskMetrics.totalEmptySegmentsCleaned++;
        maxLiveBytes += liveBytes;
        segletsBefore += segment->getSegletsAllocated();
    }

    // Relocate the live entries to survivor segments. Be sure to use local
    // counters and merge them into our global metrics afterwards to avoid
    // cache line ping-ponging in the hot path.
    LogSegmentVector survivors;
    uint64_t entryBytesAppended = relocateLiveEntries(entries, survivors);

    uint32_t segmentsAfter = downCast<uint32_t>(survivors.size());
    uint32_t segletsAfter = 0;
    foreach (LogSegment* segment, survivors)
        segletsAfter += segment->getSegletsAllocated();

    TEST_LOG("used %u seglets and %u segments", segletsAfter, segmentsAfter);

    // If this doesn't hold, then our statistics are wrong. Perhaps
    // MasterService is issuing a log->free(), but is leaving a reference in
    // the hash table.
    assert(entryBytesAppended <= maxLiveBytes);

    uint32_t segmentsBefore = downCast<uint32_t>(segmentsToClean.size());
    assert(segletsBefore >= segletsAfter);
    assert(segmentsBefore >= segmentsAfter);

    uint64_t memoryBytesFreed = (segletsBefore - segletsAfter) * segletSize;
    uint64_t diskBytesFreed = (segmentsBefore - segmentsAfter) * segmentSize;
    onDiskMetrics.totalMemoryBytesFreed += memoryBytesFreed;
    onDiskMetrics.totalDiskBytesFreed += diskBytesFreed;
    onDiskMetrics.totalSegmentsCleaned += segmentsToClean.size();
    onDiskMetrics.totalSurvivorsCreated += survivors.size();
    onDiskMetrics.totalRuns++;
    onDiskMetrics.lastRunTimestamp = WallTime::secondsTimestamp();
    if (segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        onDiskMetrics.totalLowDiskSpaceRuns++;

    AtomicCycleCounter __(&onDiskMetrics.cleaningCompleteTicks);
    segmentManager.cleaningComplete(segmentsToClean, survivors);

    return;
}

/**
 * Choose the best segment to clean in memory. We greedily choose the segment
 * with the most freeable seglets. Care is taken to ensure that we determine the
 * number of freeable seglets that will keep the segment under our maximum
 * cleanable utilization after compaction. This ensures that we will always be
 * able to use the compacted version of this segment during disk cleaning.
 */
LogSegment*
LogCleaner::getSegmentToCompact()
{
    AtomicCycleCounter _(&inMemoryMetrics.getSegmentToCompactTicks);
    return cleanableSegments.getSegmentToCompact();
}

/**
 * Compute the best segments to clean on disk and return a set of them that we
 * are guaranteed to be able to clean while consuming no more space in memory
 * than they currently take up.
 *
 * \param[out] outSegmentsToClean
 *      Vector in which segments chosen for cleaning are returned.
 * \return
 *      Returns the total number of seglets allocated in the segments chosen for
 *      cleaning.
 */
void
LogCleaner::getSegmentsToClean(LogSegmentVector& outSegmentsToClean)
{
    AtomicCycleCounter _(&onDiskMetrics.getSegmentsToCleanTicks);
    cleanableSegments.getSegmentsToClean(outSegmentsToClean);
}

uint32_t
LogCleaner::computeFreeableSeglets(LogSegment* survivor)
{
    uint32_t segletsAllocated = survivor->getSegletsAllocated();
    assert(segletsAllocated == segmentSize / segletSize);

    // The survivor segment has at least as many seglets allocated as the one we
    // compacted since it was freshly allocated it has the maximum number of
    // seglets. We can therefore free the difference, which ensures a
    // 0 net gain, plus perhaps some extra seglets from space reclaimed from
    // dead entries.
    //
    // We need to be careful not to free every seglet, however, as we must
    // ensure that disk cleaning is guaranteed to make forward progress (recall
    // that reordering of entries and segment boundaries can increase the amount
    // of space occupied by live objects if we're unlucky).
    uint32_t liveBytes = survivor->getLiveBytes();
    uint32_t segletsNeeded = (100 * (liveBytes + segletSize)) /
        segletSize / LogCleaner::MAX_CLEANABLE_MEMORY_UTILIZATION;

    if (segletsAllocated > segletsNeeded)
        return segletsAllocated - segletsNeeded;

    return 0;
}

/**
 * Sort the given segment entries by their timestamp. Used to sort the survivor
 * data that is written out to multiple segments during disk cleaning. This
 * helps to segregate data we expect to live longer from those likely to be
 * shorter lived, which in turn can reduce future cleaning costs.
 *
 * This happens to sort younger objects first, but the opposite should work just as
 * well.
 *
 * \param entries
 *      Vector containing the entries to sort.
 */
void
LogCleaner::sortEntriesByTimestamp(EntryVector& entries)
{
    AtomicCycleCounter _(&onDiskMetrics.timestampSortTicks);
    std::sort(entries.begin(), entries.end(), TimestampComparer());
}

/**
 * Extract a complete list of entries from the given segments we're going to
 * clean and sort them by age.
 *
 * \param segmentsToClean
 *      Vector containing the segments to extract entries from.
 * \param[out] outEntries
 *      Vector containing sorted live entries in the segment.
 */
void
LogCleaner::getSortedEntries(LogSegmentVector& segmentsToClean,
                             EntryVector& outEntries)
{
    AtomicCycleCounter _(&onDiskMetrics.getSortedEntriesTicks);

    foreach (LogSegment* segment, segmentsToClean) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();
            Buffer buffer;
            it.appendToBuffer(buffer);
            uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);
            outEntries.push_back(Entry(segment->getReference(it.getOffset()),
                                       timestamp));
        }
    }

    sortEntriesByTimestamp(outEntries);

    // TODO(Steve): Push all of this crap into LogCleanerMetrics. It already
    // knows about the various parts of cleaning, so why not have simple calls
    // into it at interesting points of cleaning and let it extract the needed
    // metrics?
    foreach (LogSegment* segment, segmentsToClean) {
        onDiskMetrics.totalMemoryBytesInCleanedSegments +=
            segment->getSegletsAllocated() * segletSize;
        onDiskMetrics.totalDiskBytesInCleanedSegments += segmentSize;
        onDiskMetrics.cleanedSegmentMemoryHistogram.storeSample(
            segment->getMemoryUtilization());
        onDiskMetrics.cleanedSegmentDiskHistogram.storeSample(
            segment->getDiskUtilization());
    }

    TEST_LOG("%lu entries extracted from %lu segments",
        outEntries.size(), segmentsToClean.size());
}

/**
 * Given a vector of entries from segments being cleaned, write them out to
 * survivor segments in order and alert their owning module (MasterService,
 * usually), that they've been relocated.
 *
 * \param entries 
 *      Vector the entries from segments being cleaned that may need to be
 *      relocated.
 * \param outSurvivors
 *      The new survivor segments created to hold the relocated live data are
 *      returned here.
 * \return
 *      The number of live bytes appended to survivors is returned. This value
 *      includes any segment metadata overhead. This makes it directly
 *      comparable to the per-segment liveness statistics that also include
 *      overhead.
 */
uint64_t
LogCleaner::relocateLiveEntries(EntryVector& entries,
                            LogSegmentVector& outSurvivors)
{
    // We update metrics on the stack and then merge with the global counters
    // once at the end in order to avoid contention between cleaner threads.
    LogCleanerMetrics::OnDisk<uint64_t> localMetrics;
    CycleCounter<uint64_t> _(&localMetrics.relocateLiveEntriesTicks);

    LogSegment* survivor = NULL;
    uint64_t totalEntryBytesAppended = 0;
    uint32_t currentLiveEntries[TOTAL_LOG_ENTRY_TYPES] = { 0 };
    uint32_t currentLiveEntryLengths[TOTAL_LOG_ENTRY_TYPES] = { 0 };

    foreach (Entry& entry, entries) {
        Buffer buffer;
        LogEntryType type = entry.reference.getEntry(
            &segmentManager.getAllocator(), &buffer);
        Log::Reference reference = entry.reference;
        uint32_t bytesAppended = 0;
        RelocStatus s = relocateEntry(type,
                                      buffer,
                                      reference,
                                      survivor,
                                      localMetrics,
                                      &bytesAppended);

        if (expect_false(s == RELOCATION_FAILED)) {
            if (survivor != NULL) {
                for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
                    survivor->trackNewEntries(static_cast<LogEntryType>(i),
                                              currentLiveEntries[i],
                                              currentLiveEntryLengths[i]);
                }
                memset(currentLiveEntries, 0, sizeof(currentLiveEntries));
                memset(currentLiveEntryLengths, 0,
                       sizeof(currentLiveEntryLengths));
                closeSurvivor(survivor);
            }

            // Allocate a survivor segment to write into. This call may block if
            // one is not available right now.
            CycleCounter<uint64_t> waitTicks(
                &localMetrics.waitForFreeSurvivorsTicks);
            survivor = segmentManager.allocSideSegment(
                SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
                NULL);
            assert(survivor != NULL);
            waitTicks.stop();
            outSurvivors.push_back(survivor);

            s = relocateEntry(type,
                              buffer,
                              reference,
                              survivor,
                              localMetrics,
                              &bytesAppended);
            if (s == RELOCATION_FAILED)
                throw FatalError(HERE, "Entry didn't fit into empty survivor!");
        }

        localMetrics.totalEntriesScanned[type]++;
        localMetrics.totalScannedEntryLengths[type] += buffer.size();
        if (expect_true(s == RELOCATED)) {
            localMetrics.totalLiveEntriesScanned[type]++;
            localMetrics.totalLiveScannedEntryLengths[type] +=
                buffer.size();
            currentLiveEntries[type]++;
            currentLiveEntryLengths[type] += bytesAppended;
        }

        totalEntryBytesAppended += bytesAppended;
    }

    if (survivor != NULL) {
        for (size_t i = 0; i < TOTAL_LOG_ENTRY_TYPES; i++) {
            survivor->trackNewEntries(static_cast<LogEntryType>(i),
                                      currentLiveEntries[i],
                                      currentLiveEntryLengths[i]);
        }
        closeSurvivor(survivor);
    }

    // Ensure that the survivors have been synced to backups before proceeding.
    foreach (survivor, outSurvivors) {
        CycleCounter<uint64_t> __(&localMetrics.survivorSyncTicks);
        survivor->replicatedSegment->sync(survivor->getAppendedLength());
    }

    onDiskMetrics.merge(localMetrics);

    return totalEntryBytesAppended;
}

/**
 * Close a survivor segment we've written data to as part of a disk cleaning
 * pass and tell the replicaManager to begin flushing it asynchronously to
 * backups. Any unused seglets in the survivor will will be freed for use in
 * new segments.
 *
 * \param survivor
 *      The new disk segment we've written survivor data to.
 */
void
LogCleaner::closeSurvivor(LogSegment* survivor)
{
    AtomicCycleCounter _(&onDiskMetrics.closeSurvivorTicks);
    onDiskMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();

    survivor->close();

    // Once the replicatedSegment is told that the segment is closed, it will
    // begin replicating the contents. By closing survivors as we go, we can
    // overlap backup writes with filling up new survivors.
    survivor->replicatedSegment->close();

    // Immediately free any unused seglets.
    bool r = survivor->freeUnusedSeglets(survivor->getSegletsAllocated() -
                                         survivor->getSegletsInUse());
    assert(r);
}

bool
LogCleaner::Balancer::isMemoryLow(CleanerThreadState* thread)
{
    // T = Total % of memory in use (including tombstones, dead objects, etc).
    // L = Total % of memory in use by live objects.
    const int T = cleaner->segmentManager.getMemoryUtilization();
    const int L = cleaner->cleanableSegments.getLiveObjectUtilization();

    // We need to clean if memory is low and there's space that could be
    // reclaimed. It's not worth cleaning if almost everything is alive.
    int baseThreshold = std::max(90, (100 + L) / 2);
    if (T < baseThreshold)
        return false;

    // Employ multiple threads only when we fail to keep up with fewer of them.
    if (thread->threadNumber > 0) {
        int thresh = baseThreshold + 2 * static_cast<int>(thread->threadNumber);
        if (T < std::min(99, thresh))
            return false;
    }

    return true;
}

/**
 * This method is called by the memory compactor if it failed to free any memory
 * after processing a segment. This is a pretty good signal that it might be
 * time to run the disk cleaner.
 */
void
LogCleaner::Balancer::compactionFailed()
{
    compactionFailures++;
}

LogCleaner::Balancer::CleaningTask
LogCleaner::Balancer::requestTask(CleanerThreadState* thread)
{
    if (isDiskCleaningNeeded(thread))
        return CLEAN_DISK;

    if (!cleaner->disableInMemoryCleaning && isMemoryLow(thread))
        return COMPACT_MEMORY;

    return SLEEP;
}

LogCleaner::TombstoneRatioBalancer::TombstoneRatioBalancer(LogCleaner* cleaner,
                                                           double ratio)
    : Balancer(cleaner)
    , ratio(ratio)
{
    LOG(NOTICE, "Using tombstone ratio balancer with ratio = %f", ratio);
    if (ratio < 0 || ratio > 1)
        DIE("Invalid tombstoneRatio argument (%f). Must be in [0, 1].", ratio);
}

LogCleaner::TombstoneRatioBalancer::~TombstoneRatioBalancer()
{
}

bool
LogCleaner::TombstoneRatioBalancer::isDiskCleaningNeeded(
                                                    CleanerThreadState* thread)
{
    // Our disk cleaner is fast enough to chew up considerable backup bandwidth
    // with just one thread. If we're running with backups, then only permit
    // one thread to clean on disk.
    if (thread->threadNumber != 0 && !cleaner->disableInMemoryCleaning)
        return false;

    // If we're running out of disk space, we need to run the disk cleaner.
    if (cleaner->segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        return true;

    // If we're not low on memory then we need not clean. Note that this is
    // purposefully checked after first seeing if we're low on disk space.
    if (!isMemoryLow(thread))
        return false;

    // If we are low on memory, but the compactor is disabled, we must clean.
    if (cleaner->disableInMemoryCleaning)
        return true;

    // If the ratio is set high and the memory utilisation is also high, it's
    // possible that we won't have a large enough percentage of tombstones to
    // trigger disk cleaning, but will also not gain anything from compacting.
    if (compactionFailures > compactionFailuresHandled) {
        compactionFailuresHandled++;
        return true;
    }

    // We're low on memory and the compactor is enabled. We'll let the compactor
    // do its thing until tombstones start to pile up enough that running the
    // disk cleaner is needed to free some of them (by discarding the some of
    // the segments they refer to).
    //
    // The heuristic we'll use to determine when tombstones are piling up is
    // as follows: if tombstones that may be alive account for at least X% of
    // the space not used by live objects (that is, space that is eventually
    // reclaimable), then we should run the disk cleaner to hopefully make some
    // of them dead.
    const int U = cleaner->cleanableSegments.getUndeadTombstoneUtilization();
    const int L = cleaner->cleanableSegments.getLiveObjectUtilization();
    if (U >= static_cast<int>(ratio * (100 - L)))
        return true;

    return false;
}

LogCleaner::FixedBalancer::FixedBalancer(LogCleaner* cleaner,
                                         uint32_t cleaningPercentage)
    : Balancer(cleaner)
    , cleaningPercentage(cleaningPercentage)
{
    if (cleaner->disableInMemoryCleaning && cleaningPercentage < 100) {
        DIE("Memory compaction disabled, but wanted %d%% compaction!?",
            100 - cleaningPercentage);
    }
    LOG(NOTICE, "Using fixed balancer with %u%% disk cleaning",
        cleaningPercentage);
}

LogCleaner::FixedBalancer::~FixedBalancer()
{
}

bool
LogCleaner::FixedBalancer::isDiskCleaningNeeded(CleanerThreadState* thread)
{
    // See TombstoneRatioBalancer::isDiskCleaningNeeded for comments on this
    // first handful of conditions.
    if (thread->threadNumber != 0)
        return false;

    if (cleaner->segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION)
        return true;

    if (!isMemoryLow(thread))
        return false;

    // If the memory compactor has failed to free any space recent, it's a good
    // sign that we need disk cleaning.
    if (compactionFailures > compactionFailuresHandled) {
        compactionFailuresHandled++;
        return true;
    }

    uint64_t diskTicks = thread->diskCleaningTicks;
    uint64_t memoryTicks = thread->memoryCompactionTicks;
    uint64_t totalTicks = diskTicks + memoryTicks;

    if (totalTicks == 0)
        return true;

    if (100 * diskTicks / totalTicks > cleaningPercentage)
        return false;

    return true;
}

} // namespace
