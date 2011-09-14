/* Copyright (c) 2010, 2011 Stanford University
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
#include "Log.h"
#include "LogCleaner.h"
#include "ShortMacros.h"
#include "Segment.h"
#include "SegmentIterator.h"
#include "WallTime.h"

namespace RAMCloud {

/**
 * Construct a new LogCleaner object.
 *
 * \param[in] log
 *      Pointer to the Log we'll be cleaning.
 * \param[in] backup
 *      The BackupManager to use for Segments written out by
 *      the cleaner.
 * \param[in] startThread
 *      If true, start a new thread that polls for work and calls
 *      the #clean method. If false, it's expected that the owner of
 *      this object will call the #clean method when they want cleaning
 *      to occur.
 */
LogCleaner::LogCleaner(Log* log, BackupManager *backup, bool startThread)
    : bytesFreedBeforeLastCleaning(0),
      scanList(),
      nextScannedSegmentId(0),
      cleanableSegments(),
      log(log),
      backup(backup),
      thread(),
      perfCounters()
{
    if (startThread)
        thread.construct(cleanerThreadEntry, this, &Context::get());
}

LogCleaner::~LogCleaner()
{
    halt();
}

/**
 * Attempt to do a cleaning pass. This will only actually clean if it's
 * worth doing at the moment. (What this means is presently ill-defined
 * and subject to drastic change).
 * 
 * \return
 *      true if cleaning was performed, otherwise false.
 */
bool
LogCleaner::clean()
{
    CycleCounter<uint64_t> totalTicks(&perfCounters.cleanTicks);
    PerfCounters before = perfCounters;

    // We must scan new segments the Log considers cleanable before they
    // are cleaned. Doing so in log order gives us a chance to fire callbacks
    // that maintain the TabletProfilers.
    scanNewCleanableSegments();

    SegmentVector segmentsToClean;
    SegmentEntryHandleVector liveEntries;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return false;

    getSortedLiveEntries(segmentsToClean, liveEntries);

    moveLiveData(liveEntries, segmentsToClean);
    perfCounters.segmentsCleaned += segmentsToClean.size();

    CycleCounter<uint64_t> logTicks(&perfCounters.cleaningCompleteTicks);
    log->cleaningComplete(segmentsToClean);
    logTicks.stop();

    totalTicks.stop();
    dumpCleaningPassStats(before);

    return true;
}

/**
 * Halt the cleaner thread (if one is running). Once halted, it cannot be
 * restarted. This method does not return until the cleaner thread has
 * terminated.
 */
void
LogCleaner::halt()
{
    if (thread) {
        thread->interrupt();
        thread->join();
    }
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

/**
 * Entry point for the cleaner thread. This is invoked via the
 * boost::thread() constructor. This thread performs continuous
 * cleaning on an as-needed basis.
 */
void
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    Context::Guard _(*context);
    LOG(NOTICE, "LogCleaner thread spun up");

    while (1) {
        boost::this_thread::interruption_point();
        if (!logCleaner->clean())
            usleep(LogCleaner::CLEANER_POLL_USEC);
    }
}

void
LogCleaner::dumpCleaningPassStats(PerfCounters& before)
{
    PerfCounters delta = perfCounters - before;

    double totalCleaningTicks = static_cast<double>(delta.cleanTicks);

    #define _pctAndTime(_x)                             \
        Cycles::toNanoseconds(delta._x) / 1000 / 1000,  \
        100.0 * static_cast<double>(delta._x) /         \
            totalCleaningTicks

    LOG(NOTICE, "============ Cleaning Pass Time Breakdown: ============");
    LOG(NOTICE, " Total: %lu ms",
        Cycles::toNanoseconds(delta.cleanTicks) / 1000 / 1000);
    LOG(NOTICE, "   Scan Segments:      %6lu ms   (%.2f%%)",
        _pctAndTime(scanTicks));
    LOG(NOTICE, "   Choose Segments:    %6lu ms   (%.2f%%)",
        _pctAndTime(getSegmentsTicks));
    LOG(NOTICE, "   Collect Live Data:  %6lu ms   (%.2f%%)",
        _pctAndTime(collectLiveEntriesTicks));
    LOG(NOTICE, "   Sort Live Data:     %6lu ms   (%.2f%%)",
        _pctAndTime(sortLiveEntriesTicks));
    LOG(NOTICE, "   Move Live Data:     %6lu ms   (%.2f%%)",
        _pctAndTime(moveLiveDataTicks));
    LOG(NOTICE, "     Pack Prior Segs:  %6lu ms   (%.2f%%)",
        _pctAndTime(packPriorTicks));
    LOG(NOTICE, "     Pack Last Seg:    %6lu ms   (%.2f%%)",
        _pctAndTime(packLastTicks));
    LOG(NOTICE, "     Close and Sync:   %6lu ms   (%.2f%%)",
        _pctAndTime(closeAndSyncTicks));
    LOG(NOTICE, "   Cleaning Complete:  %6lu ms   (%.2f%%)",
        _pctAndTime(cleaningCompleteTicks));

    #undef _pctAndTime

    double netCleanBytesPerSec =
        static_cast<double>((delta.segmentsCleaned - delta.segmentsGenerated)) *
        static_cast<double>(log->getSegmentCapacity()) /
        Cycles::toSeconds(delta.cleanTicks);
    LOG(NOTICE, "Cleaner rate: %.2f MB/s",
        netCleanBytesPerSec / 1024.0 / 1024.0);
}

/**
 * Scan all cleanable Segments in order of SegmentId and then add them to
 * the #cleanableList for future cleaning. If the next expected SegmentId
 * is not available, do nothing.
 *
 * See #scanSegment for more details.
 */
void
LogCleaner::scanNewCleanableSegments()
{
    CycleCounter<uint64_t> _(&perfCounters.scanTicks);

    log->getNewCleanableSegments(scanList);

    std::sort(scanList.begin(),
              scanList.end(),
              SegmentIdLessThan());

    while (scanList.size() > 0 &&
           scanList.back()->getId() == nextScannedSegmentId) {

        Segment* s = scanList.back();
        scanList.pop_back();
        scanSegment(s);
        cleanableSegments.push_back(s);
        nextScannedSegmentId++;
    }
}

/**
 * For a given Segment, scan all entries and call the scan callback function
 * registered with the type, if there is one.
 *
 * This method ensures that a callback is fired on every entry added to the
 * Log before it is cleaned. In addition, the callback is fired each time
 * the entry is relocated due to cleaning.
 *
 * \param[in] segment
 *      The Segment upon whose entries the callbacks are to be fired.
 */
void
LogCleaner::scanSegment(Segment* segment)
{
    for (SegmentIterator si(segment); !si.isDone(); si.next()) {
        const LogTypeCallback *cb = log->getCallbacks(si.getType());
        if (cb != NULL && cb->scanCB != NULL)
            cb->scanCB(si.getHandle(), cb->scanArg);
    }
}

/**
 * Decide which Segments, if any, to clean and return them in the provided
 * vector. This method implements the policy that determines which Segments
 * to clean, how many to clean, and whether or not to clean at all right
 * now. Note that any Segments returned from this method have already been
 * removed from the #cleanableSegments vector. If cleaning isn't performed,
 * they should be re-added to the vector, lest they be forgotten.
 *
 * \param[out] segmentsToClean
 *      Pointers to Segments that should be cleaned are appended to this
 *      empty vector. 
 */
void
LogCleaner::getSegmentsToClean(SegmentVector& segmentsToClean)
{
    CycleCounter<uint64_t> _(&perfCounters.getSegmentsTicks);

    assert(segmentsToClean.size() == 0);

    if (cleanableSegments.size() < CLEANED_SEGMENTS_PER_PASS)
        return;

    std::sort(cleanableSegments.begin(),
              cleanableSegments.end(),
              CostBenefitLessThan());

    // Calculate the write cost for the best candidate Segments, i.e.
    // the number of bytes we need to write out in total to write however
    // may new bytes of data that we can free up.  For us, this cost is
    // (1 / 1 - u). LFS was twice as high because segments had to be read
    // from disk before cleaning.
    uint64_t wantFreeBytes = log->getSegmentCapacity() *
                             CLEANED_SEGMENTS_PER_PASS;
    uint64_t totalLiveBytes = 0;
    uint64_t totalCapacity = 0;
    size_t i;
    for (i = 0; i < cleanableSegments.size(); i++) {
        Segment* s = cleanableSegments[cleanableSegments.size() - i - 1];
        assert(s != NULL);
        totalLiveBytes += (s->getCapacity() - s->getFreeBytes());
        totalCapacity += s->getCapacity();

        if ((totalCapacity - totalLiveBytes) >= wantFreeBytes) {
            i++;
            break;
        }
    }
    size_t numSegmentsToClean = i;

    // Abort if there aren't enough bytes to free.
    if ((totalCapacity - totalLiveBytes) < wantFreeBytes)
        return;

    // Calculate the write cost for the bytes we can free up.
    // We'll only clean if the write cost is sufficiently low.
    double u = static_cast<double>(totalLiveBytes) /
               static_cast<double>(totalCapacity);
    double writeCost = 1.0 / (1.0 - u);
    if (writeCost > MAXIMUM_CLEANABLE_WRITE_COST) {
        LOG(DEBUG, "writeCost (%.3f > %.3f) too high; not cleaning",
            writeCost, MAXIMUM_CLEANABLE_WRITE_COST);
        return;
    }

    LOG(NOTICE, "Cleaning %zd segments to free %lu bytes (writeCost is %.3f)",
        numSegmentsToClean, totalCapacity - totalLiveBytes, writeCost);

    // Ok, let's clean these suckers! Be sure to remove them from the vector
    // of candidate Segments so we don't try again in the future!
    for (i = 0; i < numSegmentsToClean; i++) {
        segmentsToClean.push_back(cleanableSegments.back());
        cleanableSegments.pop_back();
    }
}

/**
 * Given a vector of Segments, walk all of them and extract the log
 * entries that are currently still live, then sort them by age (oldest
 * first). This finds all of the data that will need to be moved to another
 * Segment during cleaning.
 *
 * Note that some of this data may expire before it gets written out,
 * but that's ok. The eviction callback is responsible for atomically
 * checking and updating their references, as well as returning a new
 * liveness boolean. See #moveLiveData. 
 *
 * \param[in] segments
 *      Vector of Segment pointers to extract live entries from.
 * \param[out] liveEntries
 *      Vector to put pointers to live entries on.
 */
void
LogCleaner::getSortedLiveEntries(SegmentVector& segments,
                                 SegmentEntryHandleVector& liveEntries)
{
    CycleCounter<uint64_t> collectTicks(&perfCounters.collectLiveEntriesTicks);
    foreach (Segment* segment, segments) {
        for (SegmentIterator i(segment); !i.isDone(); i.next()) {
            SegmentEntryHandle handle = i.getHandle();
            const LogTypeCallback *cb = log->getCallbacks(handle->type());
            if (cb != NULL && cb->livenessCB(handle, cb->livenessArg)) {
                assert(cb->timestampCB != NULL);
                liveEntries.push_back(SegmentEntryHandleAndAge(
                    handle, cb->timestampCB(handle)));
            }
        }
    }
    collectTicks.stop();

    CycleCounter<uint64_t> _(&perfCounters.sortLiveEntriesTicks);
    std::sort(liveEntries.begin(),
              liveEntries.end(),
              SegmentEntryAgeLessThan(log));
}

/**
 * Helper method for moveLiveData().
 *
 * We want to avoid the last new Segment we allocate having low
 * utilisation (for example, we could have allocated a whole Segment
 * just for the very last tiny object that was relocated). To avoid
 * this, we'll greedily clean the next best Segments in our cost-benefit
 * order so long as their live objects all fit in whatever space is left
 * over.
 *
 * \param[in] lastNewSegment
 *      The last Segment created by moveLiveData().
 * \param[out] segmentsToClean
 *      Vector of Segments to clean. If we clean any other Segments
 *      beyond what the getSegmentsToClean algorithm dictated, we need
 *      to add them here.
 * \return
 *      The number of objects relocated by the method. The caller can
 *      determine the number of bytes relocated by checking the
 *      utilisation of lastNewSegment before and after the call.
 */
uint32_t
LogCleaner::moveToFillSegment(Segment* lastNewSegment,
                              SegmentVector& segmentsToClean)
{
    CycleCounter<uint64_t> _(&perfCounters.packLastTicks);

    if (!packLastOptimisation)
        return 0;

    if (lastNewSegment == NULL)
        return 0;

    int utilisationBefore = lastNewSegment->getUtilisation();
    uint32_t bytesRelocated = 0;
    uint32_t objectsRelocated = 0;

    // Keep going so long as we make progress.
    while (1) {
        bool madeProgress = false;

        for (size_t i = 0; i < cleanableSegments.size(); i++) {
            size_t segmentIndex = cleanableSegments.size() - i - 1;
            Segment* segment = cleanableSegments[segmentIndex];

            if (segment->getLiveBytes() > lastNewSegment->appendableBytes())
                continue;

            madeProgress = true;

            // There's no point in sorting the source's objects since they're
            // going into only one Segment.
            for (SegmentIterator it(segment); !it.isDone(); it.next()) {
                SegmentEntryHandle handle = it.getHandle();
                const LogTypeCallback *cb = log->getCallbacks(handle->type());
                if (cb == NULL || !cb->livenessCB(handle, cb->livenessArg))
                    continue;

                SegmentEntryHandle newHandle =
                    lastNewSegment->append(handle, false);
                assert(newHandle != NULL);

                if (cb->relocationCB(handle, newHandle, cb->relocationArg)) {
                    bytesRelocated += handle->totalLength();
                    objectsRelocated++;
                } else {
                    lastNewSegment->rollBack(newHandle);
                }
            }

            // Be sure to move the Segment just cleaned to the list of cleaned
            // Segments.
            segmentsToClean.push_back(segment);
            cleanableSegments.erase(cleanableSegments.begin() + segmentIndex);
        }

        if (!madeProgress)
            break;
    }

    LOG(NOTICE, "packed %u bytes into last Segment (utilisation before/after: "
        "%d%%/%d%%)", bytesRelocated, utilisationBefore,
        lastNewSegment->getUtilisation());

    return objectsRelocated;
}

/**
 * Move the specified live data to new Segments and call the appropriate
 * type handler to deal with the relocation. Any newly created Segments
 * are returned in the #segmentsAdded parameter. Upon return, all new
 * Segments have been closed and synced to backups, if any are used.
 * This is the function that relocates live data from Segments we're trying
 * to clean to new, compacted Segments. 
 *
 * Importantly, some data we're trying to write may no longer be live (e.g.
 * objects may have been overwritten or deleted since). This is fine, however,
 * since the eviction callback will only update if appropriate. However, since
 * we must write the data first, we must roll it back from the Segment if it's
 * not live anymore.
 *
 * \param[in] liveData
 *      Vector of SegmentEntryHandles of recently live data to move.
 * \param[out] segmentsToClean
 *      Vector of Segments from which liveData came. This is only to be used
 *      if we choose to clean addition Segmnts not previously specified (e.g.
 *      in the moveToFillSegment method when packing the last new Segment).
 */
void
LogCleaner::moveLiveData(SegmentEntryHandleVector& liveData,
                         SegmentVector& segmentsToClean)
{
    CycleCounter<uint64_t> _(&perfCounters.moveLiveDataTicks);

    Segment* currentSegment = NULL;
    SegmentVector segmentsAdded;

    foreach (SegmentEntryHandleAndAge& entryAndAge, liveData) {
        SegmentEntryHandle handle = entryAndAge.first;
        SegmentEntryHandle newHandle = NULL;

        // First try to write the object to Segments we already created
        // (rather than the latest one) in the hopes of packing them better
        // and getting the highest utilisation. It's possible, for instance,
        // that a large object caused us to create a new Segment, but the
        // previous one still has lots of free space for smaller objects.
        //
        // This is strictly better than leaving open space, even if we put
        // in objects that are much newer (and hence more likely to be
        // freed soon). The worst case is the same space is soon empty, but
        // we have the opportunity to do better if we can pack in more data
        // that ends up staying alive longer.
        //
        // If we end up cleaning to many Segments, this could get pretty
        // expensive as the destinations fill up. Should we sort/bucket by
        // free space, preclude ones with high utilisation already, or
        // randomly try a fixed number instead? At worst we'll end up
        // running through CLEANED_SEGMENTS_PER_PASS segments for each
        // object we write out.
        if (packPriorOptimisation) {
            CycleCounter<uint64_t> _(&perfCounters.packPriorTicks);
            foreach (Segment* segment, segmentsAdded) {
                newHandle = segment->append(handle, false);
                if (newHandle != NULL)
                    break;
            }
        }

        while (newHandle == NULL) {
            if (currentSegment != NULL)
                newHandle = currentSegment->append(handle, false);

            if (newHandle == NULL) {
                currentSegment = new Segment(log,
                                             log->allocateSegmentId(),
                                             log->getFromFreeList(),
                                             log->getSegmentCapacity(),
                                             backup,
                                             LOG_ENTRY_TYPE_UNINIT, NULL, 0);
                segmentsAdded.push_back(currentSegment);
                log->cleaningInto(currentSegment);
            }
        }

        const LogTypeCallback *cb = log->getCallbacks(handle->type());
        if (!cb->relocationCB(handle, newHandle, cb->relocationArg))
            currentSegment->rollBack(newHandle);
    }

    // End game: try to get good utilisation out of the last Segment.
    uint32_t extraObjects = moveToFillSegment(currentSegment, segmentsToClean);

    // Close and sync all newly created Segments.
    CycleCounter<uint64_t> syncTicks(&perfCounters.closeAndSyncTicks);
    foreach (Segment* segment, segmentsAdded)
        segment->close(true);
    syncTicks.stop();

    // Now we're done. Log a few stats.
    int totalUtilisation = 0;
    foreach (Segment* segment, segmentsAdded) {
        LOG(NOTICE, "created new Segment: ID %lu, utilisation %d%%",
            segment->getId(), segment->getUtilisation());
        totalUtilisation += segment->getUtilisation();
    }
    LOG(NOTICE, "cleaned %zd segments by relocating %zd entries into %zd new "
        "segments with %.2f%% average utilisation; net gain of %zd segments",
        segmentsToClean.size(), liveData.size() + extraObjects,
        segmentsAdded.size(),
        1.0 * totalUtilisation / static_cast<double>(segmentsAdded.size()),
        segmentsToClean.size() - segmentsAdded.size());

    perfCounters.segmentsGenerated += segmentsAdded.size();
}

} // namespace
