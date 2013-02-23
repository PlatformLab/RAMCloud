/* Copyright (c) 2010-2012 Stanford University
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
      writeCostThreshold(config->master.cleanerWriteCostThreshold),
      disableInMemoryCleaning(config->master.disableInMemoryCleaning),
      candidates(),
      candidatesLock("LogCleaner::candidatesLock"),
      segletSize(config->segletSize),
      segmentSize(config->segmentSize),
      doWorkTicks(0),
      doWorkSleepTicks(0),
      inMemoryMetrics(),
      onDiskMetrics(),
      threadsShouldExit(false),
      numThreads(config->master.cleanerThreadCount),
      threads()
{
    if (!segmentManager.initializeSurvivorReserve(numThreads *
                                                  SURVIVOR_SEGMENTS_TO_RESERVE))
        throw FatalError(HERE, "Could not reserve survivor segments");

    if (writeCostThreshold == 0)
        disableInMemoryCleaning = true;

    for (int i = 0; i < numThreads; i++)
        threads.push_back(NULL);
}

/**
 * Destroy the cleaner. Any running threads are stopped first.
 */
LogCleaner::~LogCleaner()
{
    stop();
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
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Static entry point for the cleaner thread. This is invoked via the
 * std::thread() constructor. This thread performs continuous cleaning on an
 * as-needed basis.
 */
void
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    LOG(NOTICE, "LogCleaner thread started");

    try {
        while (1) {
            Fence::lfence();
            if (logCleaner->threadsShouldExit)
                break;

            logCleaner->doWork();
        }
    } catch (const Exception& e) {
        DIE("Fatal error in cleaner thread: %s", e.what());
    }

    LOG(NOTICE, "LogCleaner thread stopping");
}

/**
 * Main cleaning loop, constantly invoked via cleanerThreadEntry(). If there
 * is cleaning to be done, do it now return. If no work is to be done, sleep for
 * a bit before returning (and getting called again), rather than banging on the
 * CPU.
 */
void
LogCleaner::doWork()
{
    CycleCounter<uint64_t> _(&doWorkTicks);

    // Update our list of candidates.
    candidatesLock.lock();
    segmentManager.cleanableSegments(candidates);
    candidatesLock.unlock();

    // Perform memory and disk cleaning, if needed.
    bool mustCleanOnDisk = false;
    int memoryUse = segmentManager.getAllocator().getMemoryUtilization();
    if (memoryUse >= MIN_MEMORY_UTILIZATION) {
        if (disableInMemoryCleaning) {
            mustCleanOnDisk = true;
        } else {
            double writeCost = doMemoryCleaning();
            mustCleanOnDisk = (writeCost > writeCostThreshold);
        }
    }

    int diskUse = segmentManager.getSegmentUtilization();
    if (mustCleanOnDisk || diskUse >= MIN_DISK_UTILIZATION)
        doDiskCleaning();

    memoryUse = segmentManager.getAllocator().getMemoryUtilization();
    diskUse = segmentManager.getSegmentUtilization();
    if (memoryUse < MIN_MEMORY_UTILIZATION && diskUse < MIN_DISK_UTILIZATION) {
        CycleCounter<uint64_t> __(&doWorkSleepTicks);
        usleep(POLL_USEC);
    }
}

/**
 * Perform an in-memory cleaning pass. This takes a segment and compacts it,
 * re-packing all live entries together sequentially, allowing us to reclaim
 * some of the dead space.
 */
double
LogCleaner::doMemoryCleaning()
{
    TEST_LOG("called");
    CycleCounter<uint64_t> _(&inMemoryMetrics.totalTicks);

    uint32_t freeableSeglets;
    LogSegment* segment = getSegmentToCompact(freeableSeglets);
    if (segment == NULL)
        return std::numeric_limits<double>::max();

    // Allocate a survivor segment to write into. This call may block if one
    // is not available right now.
    CycleCounter<uint64_t> waitTicks(&inMemoryMetrics.waitForFreeSurvivorTicks);
    LogSegment* survivor = segmentManager.allocSideSegment(
            SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
            segment);
    assert(survivor != NULL);
    waitTicks.stop();

    inMemoryMetrics.totalBytesInCompactedSegments +=
        segment->getSegletsAllocated() * segletSize;

    for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
        LogEntryType type = it.getType();
        Buffer buffer;
        it.appendToBuffer(buffer);

        if (!relocateEntry(type, buffer, survivor, inMemoryMetrics))
            throw FatalError(HERE, "Entry didn't fit into survivor!");
    }

    uint32_t segletsToFree = survivor->getSegletsAllocated() -
                             segment->getSegletsAllocated() +
                             freeableSeglets;

    survivor->close();
    bool r = survivor->freeUnusedSeglets(segletsToFree);
    assert(r);

    double writeCost = 1 + static_cast<double>(survivor->getAppendedLength()) /
        (segletsToFree * segletSize);

    inMemoryMetrics.totalBytesFreed += freeableSeglets * segletSize;
    inMemoryMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();

    segmentManager.compactionComplete(segment, survivor);

    return writeCost;
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
    CycleCounter<uint64_t> _(&onDiskMetrics.totalTicks);

    // Obtain the segments we'll clean in this pass. We're guaranteed to have
    // the resources to clean what's returned.
    LogSegmentVector segmentsToClean;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return;

    // Extract the currently live entries of the segments we're cleaning and
    // sort them by age.
    LiveEntryVector liveEntries;
    getSortedEntries(segmentsToClean, liveEntries);

    uint32_t maxLiveBytes = 0;
    uint32_t segletsBefore = 0;
    foreach (LogSegment* segment, segmentsToClean) {
        maxLiveBytes += segment->getLiveBytes();
        segletsBefore += segment->getSegletsAllocated();
    }
    uint64_t bytesBefore = onDiskMetrics.totalBytesAppendedToSurvivors;

    // Relocate the live entries to survivor segments.
    LogSegmentVector survivors;
    relocateLiveEntries(liveEntries, survivors);

    uint32_t segmentsAfter = downCast<uint32_t>(survivors.size());
    uint32_t segletsAfter = 0;
    foreach (LogSegment* segment, survivors)
        segletsAfter += segment->getSegletsAllocated();

    TEST_LOG("used %u seglets and %u segments", segletsAfter, segmentsAfter);

    // If this doesn't hold, then our statistics are wrong. Perhaps
    // MasterService is probably issuing a log->free(), but is
    // leaving a reference in the hash table.
    assert((onDiskMetrics.totalBytesAppendedToSurvivors - bytesBefore)
        <= maxLiveBytes);

    uint32_t segmentsBefore = downCast<uint32_t>(segmentsToClean.size());
    assert(segletsBefore >= segletsAfter);
    assert(segmentsBefore >= segmentsAfter);

    onDiskMetrics.totalMemoryBytesFreed +=
        (segletsBefore - segletsAfter) * segletSize;
    onDiskMetrics.totalDiskBytesFreed +=
        (segmentsBefore - segmentsAfter) * segmentSize;

    CycleCounter<uint64_t> __(&onDiskMetrics.cleaningCompleteTicks);
    segmentManager.cleaningComplete(segmentsToClean, survivors);
}

/**
 * Choose the best segment to clean in memory. We greedily choose the segment
 * with the most freeable seglets. Care is taken to ensure that we determine the
 * number of freeable seglets that will keep the segment under our maximum
 * cleanable utilization after compaction. This ensures that we will always be
 * able to use the compacted version of this segment during disk cleaning.
 *
 * \param[out] outFreeableSeglets
 *      The maximum number of seglets the caller should be from this segment is
 *      returned here. Freeing any more may make it impossible to clean the
 *      resulting compacted segment on disk, which may deadlock the system if
 *      it prevents freeing up tombstones in other segments.
 */
LogSegment*
LogCleaner::getSegmentToCompact(uint32_t& outFreeableSeglets)
{
    CycleCounter<uint64_t> _(&inMemoryMetrics.getSegmentToCompactTicks);
    Lock guard(candidatesLock);

    size_t bestIndex = -1;
    uint32_t bestDelta = 0;
    for (size_t i = 0; i < candidates.size(); i++) {
        LogSegment* candidate = candidates[i];
        uint32_t liveBytes = candidate->getLiveBytes();
        uint32_t segletsNeeded = (100 * (liveBytes + segletSize - 1)) /
                                 segletSize / MAX_CLEANABLE_MEMORY_UTILIZATION;
        uint32_t segletsAllocated = candidate->getSegletsAllocated();
        uint32_t delta = segletsAllocated - segletsNeeded;
        if (segletsNeeded < segletsAllocated && delta > bestDelta) {
            bestIndex = i;
            bestDelta = delta;
        }
    }

    if (bestIndex == static_cast<size_t>(-1))
        return NULL;

    LogSegment* best = candidates[bestIndex];
    candidates[bestIndex] = candidates.back();
    candidates.pop_back();

    outFreeableSeglets = bestDelta;
    return best;
}

void
LogCleaner::sortSegmentsByCostBenefit(LogSegmentVector& segments)
{
    CycleCounter<uint64_t> _(&onDiskMetrics.costBenefitSortTicks);

    // Sort segments so that the best candidates are at the front of the vector.
    // We could probably use a heap instead and go a little faster, but it's not
    // easy to say how many top candidates we'd want to track in the heap since
    // they could each have significantly different numbers of seglets.
    std::sort(segments.begin(), segments.end(), CostBenefitComparer());
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
    CycleCounter<uint64_t> _(&onDiskMetrics.getSegmentsToCleanTicks);
    Lock guard(candidatesLock);

    sortSegmentsByCostBenefit(candidates);

    uint32_t totalSeglets = 0;
    uint64_t totalLiveBytes = 0;
    uint64_t maximumLiveBytes = MAX_LIVE_SEGMENTS_PER_DISK_PASS * segmentSize;
    vector<size_t> chosenIndices;

    for (size_t i = 0; i < candidates.size(); i++) {
        LogSegment* candidate = candidates[i];

        int utilization = candidate->getMemoryUtilization();
        if (utilization > MAX_CLEANABLE_MEMORY_UTILIZATION)
            continue;

        uint64_t liveBytes = candidate->getLiveBytes();
        if ((totalLiveBytes + liveBytes) > maximumLiveBytes)
            break;

        totalLiveBytes += liveBytes;
        totalSeglets += candidate->getSegletsAllocated();
        outSegmentsToClean.push_back(candidate);
        chosenIndices.push_back(i);
    }

    // Remove chosen segments from the list of candidates. At this point, we've
    // committed to cleaning what we chose and have guaranteed that we have the
    // necessary resources to complete the operation.
    reverse_foreach(size_t i, chosenIndices) {
        candidates[i] = candidates.back();
        candidates.pop_back();
    }

    TEST_LOG("%lu segments selected with %u allocated segments",
        chosenIndices.size(), totalSeglets);
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
LogCleaner::sortEntriesByTimestamp(LiveEntryVector& entries)
{
    CycleCounter<uint64_t> _(&onDiskMetrics.timestampSortTicks);
    std::sort(entries.begin(), entries.end(), TimestampComparer());
}

/**
 * Extract a complete list of entries from the given segments we're going to
 * clean and sort them by age.
 *
 * \param segmentsToClean
 *      Vector containing the segments to extract entries from.
 * \param[out] outLiveEntries
 *      Vector containing sorted live entries in the segment.
 */
void
LogCleaner::getSortedEntries(LogSegmentVector& segmentsToClean,
                             LiveEntryVector& outLiveEntries)
{
    CycleCounter<uint64_t> _(&onDiskMetrics.getSortedEntriesTicks);

    foreach (LogSegment* segment, segmentsToClean) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();
            Buffer buffer;
            it.appendToBuffer(buffer);
            uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);
            outLiveEntries.push_back(
                LiveEntry(segment, it.getOffset(), timestamp));
        }
    }

    sortEntriesByTimestamp(outLiveEntries);

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
        outLiveEntries.size(), segmentsToClean.size());
}

/**
 * Given a vector of entries from segments being cleaned, write them out to
 * survivor segments in order and alert their owning module (MasterService,
 * usually), that they've been relocated.
 *
 * \param liveEntries
 *      Vector the entries from segments being cleaned that may need to be
 *      relocated.
 * \param outSurvivors
 *      The new survivor segments created to hold the relocated live data are
 *      returned here.
 */
void
LogCleaner::relocateLiveEntries(LiveEntryVector& liveEntries,
                                LogSegmentVector& outSurvivors)
{
    CycleCounter<uint64_t> _(&onDiskMetrics.relocateLiveEntriesTicks);

    LogSegment* survivor = NULL;

    foreach (LiveEntry& entry, liveEntries) {
        Buffer buffer;
        LogEntryType type = entry.segment->getEntry(entry.offset, buffer);

        if (!relocateEntry(type, buffer, survivor, onDiskMetrics)) {
            if (survivor != NULL)
                closeSurvivor(survivor);

            // Allocate a survivor segment to write into. This call may block if
            // one is not available right now.
            CycleCounter<uint64_t> waitTicks(
                &onDiskMetrics.waitForFreeSurvivorsTicks);
            survivor = segmentManager.allocSideSegment(
                SegmentManager::FOR_CLEANING | SegmentManager::MUST_NOT_FAIL,
                NULL);
            assert(survivor != NULL);
            waitTicks.stop();
            outSurvivors.push_back(survivor);

            if (!relocateEntry(type, buffer, survivor, onDiskMetrics))
                throw FatalError(HERE, "Entry didn't fit into empty survivor!");
        }
    }

    if (survivor != NULL)
        closeSurvivor(survivor);

    foreach (survivor, outSurvivors) {
        bool r = survivor->freeUnusedSeglets(survivor->getSegletsAllocated() -
                                             survivor->getSegletsInUse());
        assert(r);

        // Ensure the survivor has been synced to backups before proceeding.
        survivor->replicatedSegment->sync(survivor->getAppendedLength());
    }
}

/**
 * Close a survivor segment we've written data to as part of a disk cleaning
 * pass and tell the replicaManager to begin flushing it asynchronously to
 * backups.
 *
 * \param survivor
 *      The new disk segment we've written survivor data to.
 */
void
LogCleaner::closeSurvivor(LogSegment* survivor)
{
    onDiskMetrics.totalBytesAppendedToSurvivors +=
        survivor->getAppendedLength();

    survivor->close();

    // Once the replicatedSegment is told that the segment is closed, it will
    // begin replicating the contents. By closing survivors as we go, we can
    // overlap backup writes with filling up new survivors.
    survivor->replicatedSegment->close();
}

/******************************************************************************
 * LogCleaner::CostBenefitComparer inner class
 ******************************************************************************/

/**
 * Construct a new comparison functor that compares segments by cost-benefit.
 * Used when selecting among candidate segments by first sorting them.
 */
LogCleaner::CostBenefitComparer::CostBenefitComparer()
    : now(WallTime::secondsTimestamp()),
      version(Cycles::rdtsc())
{
}

/**
 * Calculate the cost-benefit ratio (benefit/cost) for the given segment.
 */
uint64_t
LogCleaner::CostBenefitComparer::costBenefit(LogSegment* s)
{
    // If utilization is 0, cost-benefit is infinity.
    uint64_t costBenefit = -1UL;

    int utilization = s->getDiskUtilization();
    if (utilization != 0) {
        uint64_t timestamp = s->getAverageTimestamp();

        // This generally shouldn't happen, but is possible due to:
        //  1) Unsynchronized TSCs across cores (WallTime uses rdtsc).
        //  2) Unsynchronized clocks and "newer" recovered data in the
        //     log.
        //  3) Getting an inconsistent view of the spaceTimeSum and
        //     liveBytes segment counters due to buggy code.
        if (timestamp > now) {
            LOG(WARNING, "timestamp > now");
            timestamp = now;
        }

        uint64_t age = now - timestamp;
        costBenefit = ((100 - utilization) * age) / utilization;
    }

    return costBenefit;
}

/**
 * Compare two segment's cost-benefit ratios. Higher values (better cleaning
 * candidates) are better, so the less than comparison is inverted.
 */
bool
LogCleaner::CostBenefitComparer::operator()(LogSegment* a, LogSegment* b)
{
    // We must ensure that we maintain the weak strictly ordered constraint,
    // otherwise surprising things may happen in the stl algorithms when
    // segment statistics change and alter the computed cost-benefit of a
    // segment from one comparison to the next during the same sort operation.
    if (a->costBenefitVersion != version) {
        a->costBenefit = costBenefit(a);
        a->costBenefitVersion = version;
    }
    if (b->costBenefitVersion != version) {
        b->costBenefit = costBenefit(b);
        b->costBenefitVersion = version;
    }
    return a->costBenefit > b->costBenefit;
}


} // namespace
