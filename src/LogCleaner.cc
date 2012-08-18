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
      candidates(),
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

        if (!logCleaner->doWork())
            usleep(LogCleaner::POLL_USEC);
    }

    LOG(NOTICE, "LogCleaner thread stopping");
}

/**
 * Main cleaning loop, invoked periodically via cleanerThreadEntry(). If there
 * is cleaning to be done, do it now and return true. If no work is to be done,
 * return false so that the caller may sleep for a bit, rather than banging on
 * the CPU.
 */
bool
LogCleaner::doWork()
{
    // Update our list of candidates.
    segmentManager.cleanableSegments(candidates);

    // Perform memory and disk cleaning, if needed.
    bool memoryCleaned = doMemoryCleaning();
    bool diskCleaned = doDiskCleaning();

    return (memoryCleaned || diskCleaned);
}

bool
LogCleaner::doMemoryCleaning()
{
    return false;
}

bool
LogCleaner::doDiskCleaning()
{
    // Obtain the segments we'll clean in this pass. We're guaranteed to have
    // the resources to clean what's returned.
    LogSegmentVector segmentsToClean;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return false;

    // Extract the currently live entries of the segments we're cleaning and
    // sort them by age.
    LiveEntryVector liveEntries;
    getLiveSortedEntries(segmentsToClean, liveEntries);

    // Relocate the live entries to survivor segments.
    relocateLiveEntries(liveEntries);

    segmentManager.cleaningComplete(segmentsToClean);
LOG(NOTICE, "cleaning pass finished processing %lu segs", segmentsToClean.size());
    return true;
}

class CostBenefitSorter {
  public:
    CostBenefitSorter ()
        : now(WallTime::secondsTimestamp())
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
        return costBenefit(a) > costBenefit(b);
    }

  private:
    // WallTime timestamp when this object was constructed.
    uint64_t now;
};

void
LogCleaner::getSegmentsToClean(LogSegmentVector& outSegmentsToClean)
{
    // Only proceed if our pool of survivor segments is full.
    if (segmentManager.getFreeSurvivorCount() != SURVIVOR_SEGMENTS_TO_RESERVE)
        return;

    // Sort segments so that the best candidates are at the end of the vector.
    std::sort(candidates.begin(), candidates.end(), CostBenefitSorter());

    uint64_t totalLiveBytes = 0;
    uint64_t maximumLiveBytes = MAX_LIVE_SEGMENTS_PER_DISK_PASS *
                                segmentManager.getSegmentSize();

    for (size_t i = 0; i < candidates.size(); i++) {
        LogSegment* candidate = candidates[i];

        if (candidate->getMemoryUtilization() > MAX_CLEANABLE_MEMORY_UTILIZATION)
            continue;

        uint64_t liveBytes = candidate->getLiveBytes();
        if ((totalLiveBytes + liveBytes) > maximumLiveBytes)
            break;

        totalLiveBytes += liveBytes;
        outSegmentsToClean.push_back(candidate);
        candidates[i] = candidates.back();
        candidates.pop_back();

        LOG(NOTICE, "-- Chose segment with %d util", candidate->getMemoryUtilization());
    }
}

void
LogCleaner::getLiveSortedEntries(LogSegmentVector& segmentsToClean,
                                 LiveEntryVector& outLiveEntries)
{
    foreach (LogSegment* segment, segmentsToClean) {
        for (SegmentIterator it(*segment); !it.isDone(); it.next()) {
            LogEntryType type = it.getType();
            Buffer buffer;
            it.appendToBuffer(buffer);

            if (!entryHandlers.checkLiveness(type, buffer))
                continue;

            outLiveEntries.push_back({ segment, it.getOffset(), entryHandlers.getTimestamp(type, buffer) });
        }
    }

    std::sort(outLiveEntries.begin(), outLiveEntries.end(), TimestampSorter());
}

void
LogCleaner::relocateLiveEntries(LiveEntryVector& liveEntries)
{
    LogSegmentVector survivors;
    LogSegment* survivor = NULL;

    foreach (LiveEntry& entry, liveEntries) {
        Buffer buffer;
        LogEntryType type = entry.segment->getEntry(entry.offset, buffer);
        uint32_t newOffset;

        if (survivor == NULL || !survivor->append(type, buffer, newOffset)) {
            if (survivor != NULL) {
                survivor->close();
                // tell RS to start syncing it, but do so without blocking
            }

            survivor = segmentManager.allocSurvivor(0 /* XXX */);
            survivors.push_back(survivor);

            if (!survivor->append(type, buffer, newOffset))
                throw FatalError(HERE, "Entry didn't fit into empty survivor!");
        }

        HashTable::Reference newReference((static_cast<uint64_t>(survivor->slot) << 24) | newOffset);
        if (entryHandlers.relocate(type, buffer, newReference)) {
            // TODO(Steve): Could just aggregate and do single update per survivor.
            survivor->statistics.increment(buffer.getTotalLength(), entry.timestamp);
        } else {
            // roll it back!
            //LOG(NOTICE, "must roll back!");
        }
    }

    if (survivor != NULL)
        survivor->close();

    foreach (survivor, survivors) {
        assert(survivor->freeUnusedSeglets(survivor->getSegletsAllocated() -
                                    survivor->getSegletsInUse()));
        // sync the survivor!
    }
}

} // namespace
