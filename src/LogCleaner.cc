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
#include "WallTime.h"

namespace RAMCloud {

/**
 * Construct a new LogCleaner object. The cleaner will not perform any garbage
 * collection until the start() method is invoked.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param segmentManager
 *      The SegmentManager to query for newly cleanable segments, allocate
 *      survivor segments from, and report cleaned segments to.
 * \param replicaManager
 *      The ReplicaManager to use in backing up segments written out by the
 *      cleaner.
 * \param writeCostThreshold
 *      Threshold after which disk cleaning will be forced, even if disk space
 *      usage is not high. The treshold represents the maximum amount of work
 *      the in-memory cleaner should do on its own before trying to free up
 *      tombstone space by cleaning disk segments.
 */
LogCleaner::LogCleaner(Context& context,
                       SegmentManager& segmentManager,
                       ReplicaManager& replicaManager,
                       LogEntryHandlers& entryHandlers,
                       uint32_t writeCostThreshold)
    : context(context),
      segmentManager(segmentManager),
      replicaManager(replicaManager),
      entryHandlers(entryHandlers),
      writeCostThreshold(writeCostThreshold),
      candidates(),
      segletSize(segmentManager.getSegletSize()),
      threadShouldExit(false),
      thread()
{
    if (!segmentManager.initializeSurvivorReserve(SURVIVOR_SEGMENTS_TO_RESERVE))
        throw FatalError(HERE, "Could not reserve survivor segments");
}

LogCleaner::~LogCleaner()
{
    stop();
}

/**
 * Start the log cleaner, if it isn't already running. This spins a thread that
 * continually cleans if there's work to do until stop() is called.
 *
 * The cleaner will not do any work until explicitly enabled via this method.
 */
void
LogCleaner::start()
{
    if (!thread)
        thread.construct(cleanerThreadEntry, this, &context);
}

/**
 * Halt the cleaner thread (if it is running). Once halted, it will do no more
 * work until start() is called again.
 */
void
LogCleaner::stop()
{
    if (thread) {
        threadShouldExit = true;
        Fence::sfence();
        thread->join();
        threadShouldExit = false;
        thread.destroy();
    }
}

/**
 * Static entry point for the cleaner thread. This is invoked via the
 * std::thread() constructor. This thread performs continuous cleaning on an
 * as-needed basis.
 */
void
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    LOG(NOTICE, "LogCleaner thread started");

    while (1) {
        Fence::lfence();
        if (logCleaner->threadShouldExit)
            break;

        logCleaner->doWork();
    }

    LOG(NOTICE, "LogCleaner thread stopping");
}

/**
 * Main cleaning loop, invoked periodically via cleanerThreadEntry(). If there
 * is cleaning to be done, do it now and return true. If no work is to be done,
 * return false so that the caller may sleep for a bit, rather than banging on
 * the CPU.
 */
void
LogCleaner::doWork()
{
    // Update our list of candidates.
    segmentManager.cleanableSegments(candidates);

    // Perform memory and disk cleaning, if needed.
    double writeCost = 0;

    if (segmentManager.getMemoryUtilization() >= MIN_MEMORY_UTILIZATION) {
        writeCost = doMemoryCleaning();
    }

    if (writeCost > writeCostThreshold ||
      segmentManager.getSegmentUtilization() >= MIN_DISK_UTILIZATION) {
        doDiskCleaning();
    }

    if (segmentManager.getMemoryUtilization() < MIN_MEMORY_UTILIZATION &&
      segmentManager.getSegmentUtilization() < MIN_DISK_UTILIZATION) {
        usleep(POLL_USEC);
    }
}

double
LogCleaner::doMemoryCleaning()
{
    uint32_t freeableSeglets;
    LogSegment* segment = getSegmentToCompact(freeableSeglets);
    if (segment == NULL)
        return 0;

    // Only proceed once we have a survivor segment to work with.
    while (segmentManager.getFreeSurvivorCount() < 1)
        usleep(100);

    LogSegment* survivor = segmentManager.allocSurvivor(segment);

    for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
        LogEntryType type = it.getType();
        Buffer buffer;
        it.appendToBuffer(buffer);

        if (!entryHandlers.checkLiveness(type, buffer))
            continue;

        uint32_t timestamp = entryHandlers.getTimestamp(type, buffer);

        uint32_t newOffset;
        uint32_t priorLength = survivor->getAppendedLength();
        if (!survivor->append(type, buffer, newOffset))
            throw FatalError(HERE, "Entry didn't fit into survivor!");

        HashTable::Reference newReference((static_cast<uint64_t>(survivor->slot) << 24) | newOffset);
        if (entryHandlers.relocate(type, buffer, newReference)) {
            // TODO(Steve): Could just aggregate and do single update per survivor.
            uint32_t bytesUsed = survivor->getAppendedLength() - priorLength;
            survivor->statistics.increment(bytesUsed, timestamp);
        } else {
            // roll it back!
            //LOG(NOTICE, "must roll back!");
        }
    }

    uint32_t segletsToFree = survivor->getSegletsAllocated() -
                             segment->getSegletsAllocated() +
                             freeableSeglets;

    survivor->close();
    bool r = survivor->freeUnusedSeglets(segletsToFree);
    assert(r);

    double writeCost = 1 + static_cast<double>(survivor->getAppendedLength()) /
        (segletsToFree * segletSize);

    LOG(NOTICE, "Compacted segment %lu from %u seglets to %u seglets. WC = %.3f",
        segment->id, segment->getSegletsAllocated(),
        survivor->getSegletsAllocated(),
        writeCost);

    segmentManager.memoryCleaningComplete(segment);

    return writeCost;
}

void
LogCleaner::doDiskCleaning()
{
    // Obtain the segments we'll clean in this pass. We're guaranteed to have
    // the resources to clean what's returned.
    LogSegmentVector segmentsToClean;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return;

    // Extract the currently live entries of the segments we're cleaning and
    // sort them by age.
    LiveEntryVector liveEntries;
    getLiveSortedEntries(segmentsToClean, liveEntries);

    // Relocate the live entries to survivor segments.
    relocateLiveEntries(liveEntries);

    segmentManager.cleaningComplete(segmentsToClean);
LOG(NOTICE, "cleaning pass finished processing %lu segs", segmentsToClean.size());
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

class CostBenefitComparer {
  public:
    CostBenefitComparer()
        : now(WallTime::secondsTimestamp()),
          version(Cycles::rdtsc())
    {
    }

    uint64_t
    costBenefit(LogSegment* s)
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

    bool
    operator()(LogSegment* a, LogSegment* b)
    {
        // We must ensure that we maintain the weak strictly ordered constraint,
        // otherwise surprising things may happen in the stl algorithms when
        // segment statistics change and alter the computed cost-benefit of a
        // segment from one comparison to the next.
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

  private:
    /// WallTime timestamp when this object was constructed.
    uint64_t now;

    /// Unique identifier for this comparer instance. The cost-benefit for a
    /// particular LogSegment must not change within a comparer's lifetime,
    /// otherwise weird things can happen, for example A < B, B < C, C < A).
    uint64_t version;
};

void
LogCleaner::getSegmentsToClean(LogSegmentVector& outSegmentsToClean)
{
    // Sort segments so that the best candidates are at the front of the vector.
    // We could probably use a heap instead and go a little faster, but it's not
    // easy to say how many top candidates we'd want to track in the heap since
    // they could each have significantly different numbers of seglets.
    std::sort(candidates.begin(), candidates.end(), CostBenefitComparer());

    uint64_t totalLiveBytes = 0;
    uint64_t maximumLiveBytes = MAX_LIVE_SEGMENTS_PER_DISK_PASS *
                                segmentManager.getSegmentSize();
    vector<size_t> chosenIndices;

    for (size_t i = 0; i < candidates.size(); i++) {
        LogSegment* candidate = candidates[i];

        if (candidate->getMemoryUtilization() > MAX_CLEANABLE_MEMORY_UTILIZATION)
            continue;

        uint64_t liveBytes = candidate->getLiveBytes();
        if ((totalLiveBytes + liveBytes) > maximumLiveBytes)
            break;

        totalLiveBytes += liveBytes;
        outSegmentsToClean.push_back(candidate);
        chosenIndices.push_back(i);

        LOG(NOTICE, "-- Chose segment id %lu (at %p) with %d util", candidate->id, candidate, candidate->getMemoryUtilization());
    }

    // Remove chosen segments from the list of candidates. At this point, we've
    // committed to cleaning what we chose and have guaranteed that we have the
    // necessary resources to complete the operation.
    reverse_foreach(size_t i, chosenIndices) {
        candidates[i] = candidates.back();
        candidates.pop_back();
    }
}

void
LogCleaner::getLiveSortedEntries(LogSegmentVector& segmentsToClean,
                                 LiveEntryVector& outLiveEntries)
{
    foreach (LogSegment* segment, segmentsToClean) {
        uint32_t maxLiveBytes = segment->getLiveBytes();
        uint32_t totalLiveBytes = 0;

        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();
            Buffer buffer;
            it.appendToBuffer(buffer);

            if (!entryHandlers.checkLiveness(type, buffer))
                continue;

            outLiveEntries.push_back({ segment, it.getOffset(), entryHandlers.getTimestamp(type, buffer) });
            totalLiveBytes += buffer.getTotalLength();
        }

        // If this doesn't hold, then MasterService is probably issuing a
        // log->free(), but is leaving a reference in the hash table.
        assert(totalLiveBytes <= maxLiveBytes);
    }

    std::sort(outLiveEntries.begin(), outLiveEntries.end(), TimestampSorter());
}

void
LogCleaner::relocateLiveEntries(LiveEntryVector& liveEntries)
{
    // Only proceed once our pool of survivor segments is full.
    while (segmentManager.getFreeSurvivorCount() < SURVIVOR_SEGMENTS_TO_RESERVE)
        usleep(100);

    LogSegmentVector survivors;
    LogSegment* survivor = NULL;

    foreach (LiveEntry& entry, liveEntries) {
        Buffer buffer;
        LogEntryType type = entry.segment->getEntry(entry.offset, buffer);
        uint32_t newOffset;

        uint32_t priorLength = 0;
        if (survivor != NULL)
            priorLength = survivor->getAppendedLength();

        if (survivor == NULL || !survivor->append(type, buffer, newOffset)) {
            if (survivor != NULL) {
                survivor->close();
                // tell RS to start syncing it, but do so without blocking
            }

            survivor = segmentManager.allocSurvivor(1 /* XXX */);
            survivors.push_back(survivor);

            priorLength = survivor->getAppendedLength();
            if (!survivor->append(type, buffer, newOffset))
                throw FatalError(HERE, "Entry didn't fit into empty survivor!");
        }

        HashTable::Reference newReference((static_cast<uint64_t>(survivor->slot) << 24) | newOffset);
        if (entryHandlers.relocate(type, buffer, newReference)) {
            // TODO(Steve): Could just aggregate and do single update per survivor.
            uint32_t bytesUsed = survivor->getAppendedLength() - priorLength;
            survivor->statistics.increment(bytesUsed, entry.timestamp);
        } else {
            // roll it back!
            //LOG(NOTICE, "must roll back!");
        }
    }

    if (survivor != NULL)
        survivor->close();

    foreach (survivor, survivors) {
        bool r = survivor->freeUnusedSeglets(survivor->getSegletsAllocated() -
                                             survivor->getSegletsInUse());
        assert(r);
        // sync the survivor!
    }
}

} // namespace
