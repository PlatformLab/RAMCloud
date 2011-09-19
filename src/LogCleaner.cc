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
    CycleCounter<uint64_t> totalPassTicks;
    PerfCounters before = perfCounters;

    // We must scan new segments the Log considers cleanable before they
    // are cleaned. Doing so in log order gives us a chance to fire callbacks
    // that maintain the TabletProfilers.
    scanNewCleanableSegments();

    SegmentVector segmentsToClean;
    SegmentEntryHandleVector liveEntries;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0) {
        // Even if there's nothing to do, call into the Log
        // to give it a chance to free up Segments that were
        // waiting on existing references.
        log->cleaningComplete(segmentsToClean);
        return false;
    }

    perfCounters.cleaningPasses++;

    getSortedLiveEntries(segmentsToClean, liveEntries);

    moveLiveData(liveEntries, segmentsToClean);
    perfCounters.segmentsCleaned += segmentsToClean.size();

    CycleCounter<uint64_t> logTicks(&perfCounters.cleaningCompleteTicks);
    log->cleaningComplete(segmentsToClean);
    logTicks.stop();

    totalTicks.stop();
    perfCounters.cleaningPassTicks += totalPassTicks.stop();
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

    LOG(NOTICE, "============ Cleaning Pass Complete ============");

    double cleanedBytesPerSec =
        static_cast<double>(delta.segmentsCleaned) *
        static_cast<double>(log->getSegmentCapacity()) /
        Cycles::toSeconds(delta.cleaningPassTicks);

    double generatedBytesPerSec =
        static_cast<double>(delta.segmentsGenerated) *
        static_cast<double>(log->getSegmentCapacity()) /
        Cycles::toSeconds(delta.cleaningPassTicks);

    double netCleanBytesPerSec =
        static_cast<double>((delta.segmentsCleaned - delta.segmentsGenerated)) *
        static_cast<double>(log->getSegmentCapacity()) /
        Cycles::toSeconds(delta.cleaningPassTicks);

    LOG(NOTICE, "  Counters/Rates:");
    LOG(NOTICE, "    Total Cleaning Passes:         %9lu",
        perfCounters.cleaningPasses);
    LOG(NOTICE, "    Write Cost:                    %9.3f   (%.3f avg)",
        delta.writeCostSum,
        perfCounters.writeCostSum /
        static_cast<double>(perfCounters.cleaningPasses));
    LOG(NOTICE, "    Segments Cleaned:              %9lu   (%.2f MB/s)",
        delta.segmentsCleaned, cleanedBytesPerSec / 1024.0 / 1024.0);
    LOG(NOTICE, "    Segments Generated:            %9lu   (%.2f MB/s)",
        delta.segmentsGenerated, generatedBytesPerSec / 1024.0 / 1024.0);
    LOG(NOTICE, "    Net Clean Segments:            %9lu   (%.2f MB/s)",
        delta.segmentsCleaned - delta.segmentsGenerated,
        netCleanBytesPerSec / 1024.0 / 1024.0);
    LOG(NOTICE, "    Entries Checked for Liveness:  %9lu   (%.2f us/callback)",
        delta.entriesLivenessChecked,
        1.0e6 * Cycles::toSeconds(delta.livenessCallbackTicks) /
        static_cast<double>(delta.entriesLivenessChecked));
    LOG(NOTICE, "    Live Entries Relocated:        %9lu   (%.2f us/callback)",
        delta.liveEntriesRelocated,
        1.0e6 * Cycles::toSeconds(delta.relocationCallbackTicks) /
        static_cast<double>(delta.liveEntriesRelocated));
    LOG(NOTICE, "    Entries Rolled Back:           %9lu",
        delta.entriesRolledBack);
    LOG(NOTICE, "    Average Entry Size + Metadata: %9lu   (%lu bytes overall)",
        delta.liveEntryBytes / delta.entriesLivenessChecked,
        perfCounters.liveEntryBytes / perfCounters.entriesLivenessChecked);
    LOG(NOTICE, "    Cleaned Segment Utilisation:   %9.2f%%  (%.2f%% avg)",
        100.0 * static_cast<double>(delta.liveEntriesRelocated) /
        static_cast<double>(delta.entriesLivenessChecked),
        100.0 * static_cast<double>(perfCounters.liveEntriesRelocated) /
        static_cast<double>(perfCounters.entriesLivenessChecked));
    LOG(NOTICE, "    Generated Segment Utilisation: %9.2f%%  (%.2f%% avg)",
        static_cast<double>(delta.generatedUtilisationSum) /
        static_cast<double>(delta.segmentsGenerated),
        static_cast<double>(perfCounters.generatedUtilisationSum) /
        static_cast<double>(perfCounters.segmentsGenerated));
    LOG(NOTICE, "    Last Seg Packing Util Incr:    %9.1f%%  (%.1f%% avg)",
        static_cast<double>(delta.packLastImprovementSum) /
        static_cast<double>(delta.cleaningPasses),
        static_cast<double>(perfCounters.packLastImprovementSum) /
        static_cast<double>(perfCounters.cleaningPasses));
    LOG(NOTICE, "    Total Segs Pack Last Improved: %9lu   (%.2f%% of passes)",
        perfCounters.packLastDidWork,
        100.0 * static_cast<double>(perfCounters.packLastDidWork) /
        static_cast<double>(perfCounters.cleaningPasses));

    #define _pctAndTime(_x)                             \
        Cycles::toNanoseconds(delta._x) / 1000 / 1000,  \
        100.0 * static_cast<double>(delta._x) /         \
            static_cast<double>(delta.cleaningPassTicks)

    LOG(NOTICE, "  Time Breakdown:");
    LOG(NOTICE, "    Total:                      %9lu ms   (%lu avg)",
        Cycles::toNanoseconds(delta.cleaningPassTicks) / 1000 / 1000,
        Cycles::toNanoseconds(perfCounters.cleaningPassTicks) /
        perfCounters.cleaningPasses / 1000 / 1000);
    LOG(NOTICE, "      Scan Segments:            %9lu ms   (%.2f%%)",
        _pctAndTime(scanTicks));
    LOG(NOTICE, "      Choose Segments:          %9lu ms   (%.2f%%)",
        _pctAndTime(getSegmentsTicks));
    LOG(NOTICE, "      Collect Live Data:        %9lu ms   (%.2f%%)",
        _pctAndTime(collectLiveEntriesTicks));
    LOG(NOTICE, "        Check Liveness:         %9lu ms   (%.2f%%)",
        _pctAndTime(livenessCallbackTicks));
    LOG(NOTICE, "      Sort Live Data:           %9lu ms   (%.2f%%)",
        _pctAndTime(sortLiveEntriesTicks));
    LOG(NOTICE, "      Move Live Data:           %9lu ms   (%.2f%%)",
        _pctAndTime(moveLiveDataTicks));
    LOG(NOTICE, "        Segment Append:         %9lu ms   (%.2f%%)",
        _pctAndTime(segmentAppendTicks));
    LOG(NOTICE, "        Relocation Callback:    %9lu ms   (%.2f%%)",
        _pctAndTime(relocationCallbackTicks));
    LOG(NOTICE, "        Pack Last Seg:          %9lu ms   (%.2f%%)",
        _pctAndTime(packLastTicks));
    LOG(NOTICE, "        Close and Sync:         %9lu ms   (%.2f%%)",
        _pctAndTime(closeAndSyncTicks));
    LOG(NOTICE, "      Cleaning Complete:        %9lu ms   (%.2f%%)",
        _pctAndTime(cleaningCompleteTicks));

    #undef _pctAndTime

    size_t i;
    LOG(NOTICE, "  Entry Types Checked for Liveness:");
    for (i = 0; i < arrayLength(perfCounters.entryTypeCounts); i++) {
        if (perfCounters.entryTypeCounts[i] == 0)
            continue;
        LOG(NOTICE, "      %3zd ('%c')           %18lu   "
            "(%.2f%% avg, %.2f%% overall)",
            i, downCast<char>(i), delta.entryTypeCounts[i],
            100.0 * static_cast<double>(delta.entryTypeCounts[i]) /
            static_cast<double>(delta.entriesInCleanedSegments),
            100.0 * static_cast<double>(perfCounters.entryTypeCounts[i]) /
            static_cast<double>(perfCounters.entriesInCleanedSegments));
    }

    LOG(NOTICE, "  Entry Types Relocated:");
    for (i = 0; i < arrayLength(perfCounters.relocEntryTypeCounts); i++) {
        if (perfCounters.relocEntryTypeCounts[i] == 0)
            continue;
        LOG(NOTICE, "      %3zd ('%c')           %18lu   "
            "(%.2f%% avg, %.2f%% overall)",
            i, downCast<char>(i), delta.relocEntryTypeCounts[i],
            100.0 * static_cast<double>(delta.relocEntryTypeCounts[i]) /
            static_cast<double>(delta.liveEntriesRelocated),
            100.0 * static_cast<double>(perfCounters.relocEntryTypeCounts[i]) /
            static_cast<double>(perfCounters.liveEntriesRelocated));
    }
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

    perfCounters.writeCostSum += writeCost;

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

            perfCounters.entryTypeCounts[handle->type()]++;
            perfCounters.entriesInCleanedSegments++;

            const LogTypeCallback *cb = log->getCallbacks(handle->type());
            if (cb != NULL) {
                perfCounters.entriesLivenessChecked++;

                CycleCounter<uint64_t> livenessTicks(
                    &perfCounters.livenessCallbackTicks);
                bool isLive = cb->livenessCB(handle, cb->livenessArg);
                livenessTicks.stop();

                if (isLive) {
                    assert(cb->timestampCB != NULL);
                    liveEntries.push_back(
                        LiveSegmentEntry(handle, cb->timestampCB(handle)));
                    perfCounters.liveEntryBytes += handle->totalLength();
                }
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
 */
void
LogCleaner::moveToFillSegment(Segment* lastNewSegment,
                              SegmentVector& segmentsToClean)
{
    CycleCounter<uint64_t> _(&perfCounters.packLastTicks);

    if (lastNewSegment == NULL)
        return;

    int utilisationBefore = lastNewSegment->getUtilisation();

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

                perfCounters.entryTypeCounts[handle->type()]++;
                perfCounters.entriesInCleanedSegments++;

                const LogTypeCallback *cb = log->getCallbacks(handle->type());
                if (cb == NULL || !cb->livenessCB(handle, cb->livenessArg))
                    continue;

                SegmentEntryHandle newHandle =
                    lastNewSegment->append(handle, false);
                assert(newHandle != NULL);

                if (cb->relocationCB(handle, newHandle, cb->relocationArg)) {
                    perfCounters.liveEntriesRelocated++;
                    perfCounters.relocEntryTypeCounts[handle->type()]++;
                } else {
                    perfCounters.entriesRolledBack++;
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

    int gain = lastNewSegment->getUtilisation() - utilisationBefore;
    if (gain) {
        perfCounters.packLastDidWork++;
        perfCounters.packLastImprovementSum += gain;
    }
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

    SegmentVector segmentsAdded;
    PowerOfTwoSegmentBins segmentBins(perfCounters);

    for (size_t i = 0; i < liveData.size(); i++) {
        LiveSegmentEntry& liveEntry = liveData[i];

        // Try to prefetch ahead, if possible.
        if (i + PREFETCH_OFFSET < liveData.size()) {
            uint32_t maxFetch = MAX_PREFETCH_BYTES;
            prefetch(liveData[i + PREFETCH_OFFSET].handle,
                std::min(liveData[i + PREFETCH_OFFSET].totalLength, maxFetch));
        }

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

        SegmentEntryHandle handle = liveEntry.handle;
        SegmentEntryHandle newHandle = NULL;
        Segment* segmentUsed = NULL;

        while (newHandle == NULL) {
            segmentUsed = segmentBins.getSegment(handle->totalLength());
            if (segmentUsed == NULL) {
                Segment* newSeg = new Segment(log,
                                              log->allocateSegmentId(),
                                              log->getFromFreeList(),
                                              log->getSegmentCapacity(),
                                              backup,
                                              LOG_ENTRY_TYPE_UNINIT, NULL, 0);

                segmentsAdded.push_back(newSeg);
                segmentBins.addSegment(newSeg);
                log->cleaningInto(newSeg);
                continue;
            }

            CycleCounter<uint64_t> _(&perfCounters.segmentAppendTicks);
            newHandle = segmentUsed->append(handle, false);
        }

        const LogTypeCallback* cb = log->getCallbacks(handle->type());

        CycleCounter<uint64_t> relTicks(&perfCounters.relocationCallbackTicks);
        bool relocated = cb->relocationCB(handle, newHandle, cb->relocationArg);
        relTicks.stop();

        if (relocated) {
            perfCounters.liveEntriesRelocated++;
            perfCounters.relocEntryTypeCounts[handle->type()]++;
            segmentBins.updateSegment(segmentUsed);
        } else {
            perfCounters.entriesRolledBack++;
            segmentUsed->rollBack(newHandle);
        }
    }

    // End game: try to get good utilisation out of the last Segment.
    moveToFillSegment(segmentsAdded.back(), segmentsToClean);

    // Close and sync all newly created Segments.
    CycleCounter<uint64_t> syncTicks(&perfCounters.closeAndSyncTicks);
    foreach (Segment* segment, segmentsAdded)
        segment->close(true);
    syncTicks.stop();

    // Now we're done. Save some stats.
    foreach (Segment* segment, segmentsAdded)
        perfCounters.generatedUtilisationSum += segment->getUtilisation();

    perfCounters.segmentsGenerated += segmentsAdded.size();
}

} // namespace
