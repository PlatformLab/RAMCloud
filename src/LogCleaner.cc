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
 * Construct a new LogCleaner object.
 *
 * \param context
 *      Overall information about the RAMCloud server.
 * \param[in] log
 *      Pointer to the Log we'll be cleaning.
 * \param[in] replicaManager
 *      The ReplicaManager to use for Segments written out by
 *      the cleaner.
 * \param[in] startThread
 *      If true, start a new thread that polls for work and calls
 *      the #clean method. If false, it's expected that the owner of
 *      this object will call the #clean method when they want cleaning
 *      to occur.
 */
LogCleaner::LogCleaner(Context& context,
                       Log* log,
                       ReplicaManager *replicaManager,
                       bool startThread)
    : context(context),
      bytesFreedBeforeLastCleaning(0),
      scanList(),
      cleanableSegments(),
      log(log),
      replicaManager(replicaManager),
      thread(),
      threadShouldExit(false),
      perfCounters()
{
    if (startThread)
        thread.construct(cleanerThreadEntry, this, &context);
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

    // Get new cleanable segments from the log and scan them to keep
    // track of space used by tombstones.
    scanNewCleanableSegments();

    // Scan some cleanable Segments to update free space accounting.
    scanForFreeSpace();

    SegmentVector segmentsToClean;
    LiveSegmentEntryHandleVector liveEntries;
    std::vector<void*> cleanSegmentMemory;

    // Check to see if the Log is out of memory. If so, we need to do an
    // emergency cleaning pass.
    if (log->freeListCount() == 0) {
        if (!setUpEmergencyCleaningPass(liveEntries,
                                        cleanSegmentMemory,
                                        segmentsToClean)) {
            // Reset counters to ignore this failed pass.
            perfCounters = before;
            perfCounters.failedNormalPasses++;
            perfCounters.failedNormalPassTicks += totalTicks.stop();
            return false;
        }

        LOG(WARNING, "Starting emergency cleaning pass (%zd entries in %zd "
            "segments using %zd clean segments)", liveEntries.size(),
            cleanSegmentMemory.size(), segmentsToClean.size());

        perfCounters.cleaningPasses++;
        perfCounters.emergencyCleaningPasses++;
    } else {
        if (!setUpNormalCleaningPass(liveEntries,
                                     cleanSegmentMemory,
                                     segmentsToClean)) {
            // Reset counters to ignore this failed pass.
            perfCounters = before;
            perfCounters.failedEmergencyPasses++;
            perfCounters.failedEmergencyPassTicks += totalTicks.stop();
            return false;
        }

        perfCounters.cleaningPasses++;
    }

    moveLiveData(liveEntries, cleanSegmentMemory, segmentsToClean);
    perfCounters.segmentsCleaned += segmentsToClean.size();

    CycleCounter<uint64_t> logTicks(&perfCounters.cleaningCompleteTicks);
    log->cleaningComplete(segmentsToClean, cleanSegmentMemory);
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
        threadShouldExit = true;
        Fence::sfence();
        thread->join();
        threadShouldExit = false;
        thread.destroy();
    }
}

////////////////////////////////////////
/// Private Methods
////////////////////////////////////////

/**
 * Entry point for the cleaner thread. This is invoked via the
 * std::thread() constructor. This thread performs continuous
 * cleaning on an as-needed basis.
 */
void
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner, Context* context)
{
    LOG(NOTICE, "LogCleaner thread spun up");

    while (1) {
        Fence::lfence();
        if (logCleaner->threadShouldExit)
            break;
        if (!logCleaner->clean())
            usleep(LogCleaner::POLL_USEC);
    }
}

/**
 * Dump a table of various cleaning times and counters to the log
 * for human consumption.
 */
void
LogCleaner::dumpCleaningPassStats(PerfCounters& before)
{
    LogLevel level = DEBUG;

    PerfCounters delta = perfCounters - before;

    LOG(level, "============ %sCleaning Pass Complete ============",
        delta.emergencyCleaningPasses ? "EMERGENCY " : "");

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

    LOG(level, "  Counters/Rates:");
    LOG(level, "    Total Cleaning Passes:          %9lu",
        perfCounters.cleaningPasses + perfCounters.emergencyCleaningPasses);
    LOG(level, "      Emergency Cleaning Passes:    %9lu   (%.2f%%)",
        perfCounters.emergencyCleaningPasses,
        100.0 * static_cast<double>(perfCounters.emergencyCleaningPasses) /
        static_cast<double>(perfCounters.cleaningPasses +
                            perfCounters.emergencyCleaningPasses));
    LOG(level, "    Write Cost:                     %9.3f   (%.3f avg)",
        delta.writeCostSum,
        perfCounters.writeCostSum /
        static_cast<double>(perfCounters.cleaningPasses));
    LOG(level, "    Segments Cleaned:               %9lu   (%.2f MB/s)",
        delta.segmentsCleaned, cleanedBytesPerSec / 1024.0 / 1024.0);
    LOG(level, "    Segments Generated:             %9lu   (%.2f MB/s)",
        delta.segmentsGenerated, generatedBytesPerSec / 1024.0 / 1024.0);
    LOG(level, "    Net Clean Segments:             %9lu   (%.2f MB/s)",
        delta.segmentsCleaned - delta.segmentsGenerated,
        netCleanBytesPerSec / 1024.0 / 1024.0);
    LOG(level, "    Entries Checked for Liveness:   %9lu   (%.2f us/callback)",
        delta.entriesLivenessChecked,
        1.0e6 * Cycles::toSeconds(delta.livenessCallbackTicks) /
        static_cast<double>(delta.entriesLivenessChecked));
    LOG(level, "    Live Entries Relocated:         %9lu   (%.2f us/callback)",
        delta.liveEntriesRelocated,
        1.0e6 * Cycles::toSeconds(delta.relocationCallbackTicks) /
        static_cast<double>(delta.liveEntriesRelocated));
    LOG(level, "    Entries Rolled Back:            %9lu",
        delta.entriesRolledBack);
    LOG(level, "    Average Entry Size + Metadata:  %9lu   "
        "(%lu bytes overall)",
        (delta.entriesLivenessChecked == 0) ? 0 :
        delta.liveEntryBytes / delta.entriesLivenessChecked,
        (perfCounters.entriesLivenessChecked == 0) ? 0 :
        perfCounters.liveEntryBytes / perfCounters.entriesLivenessChecked);
    LOG(level, "    Cleaned Segment Utilisation:    %9.2f%%  (%.2f%% avg)",
        100.0 * static_cast<double>(delta.liveEntriesRelocated) /
        static_cast<double>(delta.entriesLivenessChecked),
        100.0 * static_cast<double>(perfCounters.liveEntriesRelocated) /
        static_cast<double>(perfCounters.entriesLivenessChecked));
    LOG(level, "    Generated Segment Utilisation:  %9.2f%%  (%.2f%% avg)",
        static_cast<double>(delta.generatedUtilisationSum) /
        static_cast<double>(delta.segmentsGenerated),
        static_cast<double>(perfCounters.generatedUtilisationSum) /
        static_cast<double>(perfCounters.segmentsGenerated));
    LOG(level, "    Last Seg Packing Util Incr:     %9.1f%%  (%.1f%% avg)",
        static_cast<double>(delta.packLastImprovementSum) /
        static_cast<double>(delta.cleaningPasses),
        static_cast<double>(perfCounters.packLastImprovementSum) /
        static_cast<double>(perfCounters.cleaningPasses));
    LOG(level, "    Total Segs Pack Last Improved:  %9lu   (%.2f%% of passes)",
        perfCounters.packLastDidWork,
        100.0 * static_cast<double>(perfCounters.packLastDidWork) /
        static_cast<double>(perfCounters.cleaningPasses));
    LOG(level, "    Segs Scanned for Free Space:    %9lu   (%lu overall)",
        delta.scanForFreeSpaceSegments, perfCounters.scanForFreeSpaceSegments);
    LOG(level, "      Attempts That Found Free Space: %7.3f%%   (overall)",
        100.0 * static_cast<double>(perfCounters.scanForFreeSpaceProgress) /
        static_cast<double>(perfCounters.scanForFreeSpaceSegments));

    #define _pctAndTime(_x)                             \
        Cycles::toNanoseconds(delta._x) / 1000 / 1000,  \
        100.0 * static_cast<double>(delta._x) /         \
            static_cast<double>(delta.cleaningPassTicks)

    LOG(level, "  Time Breakdown:");
    LOG(level, "    Total:                       %9lu ms   (%lu avg)",
        Cycles::toNanoseconds(delta.cleaningPassTicks) / 1000 / 1000,
        Cycles::toNanoseconds(perfCounters.cleaningPassTicks) /
        perfCounters.cleaningPasses / 1000 / 1000);
    LOG(level, "      Scan New Segments:         %9lu ms   (%.2f%%)",
        _pctAndTime(newScanTicks));
    LOG(level, "      Scan For Free Space:       %9lu ms   (%.2f%%)",
        _pctAndTime(scanForFreeSpaceTicks));
    LOG(level, "      Choose Segments:           %9lu ms   (%.2f%%)",
        _pctAndTime(getSegmentsTicks));
    LOG(level, "      Collect Live Data:         %9lu ms   (%.2f%%)",
        _pctAndTime(collectLiveEntriesTicks));
    LOG(level, "        Check Liveness:          %9lu ms   (%.2f%%)",
        _pctAndTime(livenessCallbackTicks));
    LOG(level, "      Sort Live Data:            %9lu ms   (%.2f%%)",
        _pctAndTime(sortLiveEntriesTicks));
    LOG(level, "      Move Live Data:            %9lu ms   (%.2f%%)",
        _pctAndTime(moveLiveDataTicks));
    LOG(level, "        Segment Append:          %9lu ms   (%.2f%%, "
        "%.2f MB/s)",
        _pctAndTime(segmentAppendTicks),
        static_cast<double>(delta.liveEntryBytes) / 1024.0 / 1024.0);
    LOG(level, "        Relocation Callback:     %9lu ms   (%.2f%%)",
        _pctAndTime(relocationCallbackTicks));
    LOG(level, "        Pack Last Seg:           %9lu ms   (%.2f%%)",
        _pctAndTime(packLastTicks));
    LOG(level, "        Close and Sync:          %9lu ms   (%.2f%%)",
        _pctAndTime(closeAndSyncTicks));
    LOG(level, "      Cleaning Complete:         %9lu ms   (%.2f%%)",
        _pctAndTime(cleaningCompleteTicks));

    #undef _pctAndTime

    size_t i;
    LOG(level, "  Entry Types Checked for Liveness:");
    for (i = 0; i < arrayLength(perfCounters.entryTypeCounts); i++) {
        if (perfCounters.entryTypeCounts[i] == 0)
            continue;
        LOG(level, "      %3zd ('%c')           %18lu    "
            "(%.2f%% avg, %.2f%% overall)",
            i, downCast<char>(i), delta.entryTypeCounts[i],
            100.0 * static_cast<double>(delta.entryTypeCounts[i]) /
            static_cast<double>(delta.entriesInCleanedSegments),
            100.0 * static_cast<double>(perfCounters.entryTypeCounts[i]) /
            static_cast<double>(perfCounters.entriesInCleanedSegments));
    }

    LOG(level, "  Entry Types Relocated:");
    for (i = 0; i < arrayLength(perfCounters.relocEntryTypeCounts); i++) {
        if (perfCounters.relocEntryTypeCounts[i] == 0)
            continue;
        LOG(level, "      %3zd ('%c')           %18lu    "
            "(%.2f%% avg, %.2f%% overall)",
            i, downCast<char>(i), delta.relocEntryTypeCounts[i],
            100.0 * static_cast<double>(delta.relocEntryTypeCounts[i]) /
            static_cast<double>(delta.liveEntriesRelocated),
            100.0 * static_cast<double>(perfCounters.relocEntryTypeCounts[i]) /
            static_cast<double>(perfCounters.liveEntriesRelocated));
    }

    uint64_t histogram[10];
    uint64_t totalUtil = 0;
    memset(histogram, 0, sizeof(histogram));
    foreach (CleanableSegment& cs, cleanableSegments) {
        int idx = std::min(arrayLength(histogram) - 1,
            cs.segment->getUtilisation() / arrayLength(histogram));
        histogram[idx]++;
        totalUtil += cs.segment->getUtilisation();
    }
    double cumulative = 0;
    LOG(level, "  Cleanable Segment Utilisation Histogram (avg %.2f%%):",
        static_cast<double>(totalUtil) /
        static_cast<double>(cleanableSegments.size()));
    for (i = 0; i < arrayLength(histogram); i++) {
        size_t startPct = (100 * i) / arrayLength(histogram);
        size_t endPct = std::min(static_cast<size_t>(100),
                                (100 * (i + 1)) / arrayLength(histogram));
        double pct = 100.0 * static_cast<double>(histogram[i]) /
            static_cast<double>(cleanableSegments.size());
        cumulative += pct;
        char endChar = (i == arrayLength(histogram) - 1) ? ']' : ')';

        LOG(level, "       [%2zd%% - %3zd%%%c:  %5.1f%%   %5.1f%%",
            startPct, endPct, endChar, pct, cumulative);
    }
}

/**
 * Allocate the specified number of clean segments (to use for survivor segment
 * data) from the Log. Return true if enough could be allocated, otherwise if
 * we come up short, return whatever we got to the log and return false.
 */
bool
LogCleaner::getCleanSegmentMemory(size_t segmentsNeeded,
                                  std::vector<void*>& cleanSegmentMemory)
{
    try {
        for (size_t i = 0; i < segmentsNeeded; i++) {
            cleanSegmentMemory.push_back(
                log->getSegmentMemoryForCleaning(false));
        }
        return true;
    } catch (LogOutOfMemoryException& e) {
        // We didn't get enough. Return any allocated segment memory to the log.
        SegmentVector empty;
        log->cleaningComplete(empty, cleanSegmentMemory);
        cleanSegmentMemory.clear();
        return false;
    }
}

/**
 * Compute the write cost for the bytes that can be freed by cleaning
 * one or more segments.
 *
 * \param totalCapacity
 *      The total capacity of the segments containing the live bytes.
 *
 * \param liveBytes
 *      The number of bytes that need to be relocated.
 */
double
LogCleaner::writeCost(uint64_t totalCapacity, uint64_t liveBytes)
{
    double u = static_cast<double>(liveBytes) /
               static_cast<double>(totalCapacity);
    double writeCost = 1.0 / (1.0 - u);

    return writeCost;
}

/**
 * Returns true if the given write cost is worth cleaning for, else false.
 */
bool
LogCleaner::isCleanable(double _writeCost)
{
    return _writeCost <= MAXIMUM_CLEANABLE_WRITE_COST;
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
    CycleCounter<uint64_t> _(&perfCounters.newScanTicks);

    log->getNewCleanableSegments(scanList);

    while (scanList.size() > 0) {

        Segment* s = scanList.back();
        scanList.pop_back();

        uint32_t implicitlyFreeableEntries, implicitlyFreeableBytes;
        scanSegment(s, &implicitlyFreeableEntries, &implicitlyFreeableBytes);
        cleanableSegments.push_back({ s, implicitlyFreeableEntries,
                                         implicitlyFreeableBytes });
    }
}

/**
 * For a given Segment, scan all entries to count the number of entries that
 * are not explicitly freed along with the total number of bytes they're
 * consuming. This can be used to help decide which semgments should be
 * regularly scanned to compute an up-to-date account of the free space.
 *
 * \param[in] segment
 *      The Segment upon whose entries the callbacks are to be fired.
 *
 * \param[out] implicitlyFreeableEntries
 *      If non-NULL, store the number of entries in this Segment that will
 *      not be explicitly marked free (i.e. we'll have to query to find
 *      out and update appropriate counters). 
 *
 * \param[out] implicitlyFreeableBytes
 *      If non-NULL, store the number of bytes the implicitlyFreeableEntries
 *      consume in this Segment, including overhead (e.g. metadata).
 */
void
LogCleaner::scanSegment(Segment*  segment,
                        uint32_t* implicitlyFreeableEntries,
                        uint32_t* implicitlyFreeableBytes)
{
    uint32_t _implicitlyFreeableEntries = 0;
    uint32_t _implicitlyFreeableBytes = 0;

    for (SegmentIterator si(segment); !si.isDone(); si.next()) {
        const LogTypeInfo *ti = log->getTypeInfo(si.getType());
        if (ti != NULL && !ti->explicitlyFreed) {
            _implicitlyFreeableEntries++;
            _implicitlyFreeableBytes = si.getHandle()->totalLength();
        }
    }

    if (implicitlyFreeableEntries != NULL)
        *implicitlyFreeableEntries = _implicitlyFreeableEntries;
    if (implicitlyFreeableBytes != NULL)
        *implicitlyFreeableBytes = _implicitlyFreeableBytes;
}

/**
 * For some log entries (e.g. tombstones), the log is not made aware
 * when they become free. It's up to us to calculate the free space
 * for them.
 */
void
LogCleaner::scanForFreeSpace()
{
    CycleCounter<uint64_t> _(&perfCounters.scanForFreeSpaceTicks);

    foreach (CleanableSegment& cs, cleanableSegments) {
        // Only bother scanning if the difference would affect the
        // cleanability of this Segment.

        if (isCleanable(
          writeCost(cs.segment->getCapacity(), cs.segment->getLiveBytes()))) {
            continue;
        }

        uint32_t maxFreeableBytes =
            cs.implicitlyFreeableBytes - cs.implicitlyFreedBytes;
        if (!isCleanable(
          writeCost(cs.segment->getCapacity(),
                    cs.segment->getLiveBytes() - maxFreeableBytes))) {
            continue;
        }

        scanSegmentForFreeSpace(cs);
    }
}

/**
 * Scan the given Segment's entries and calculate the amount of space taken
 * up by implicitly freeable entries. Entries such as Tombstones are not
 * explicitly freed, so it's up to us to find out if they're no longer needed
 * and update the Segment's statistics.
 */
void
LogCleaner::scanSegmentForFreeSpace(CleanableSegment& cleanableSegment)
{
    uint32_t freeByteSum = 0;
    uint32_t freedEntries = 0;
    uint64_t freeSpaceTimeSum = 0;

    Segment* segment = cleanableSegment.segment;
    for (SegmentIterator si(segment); !si.isDone(); si.next()) {
        const LogTypeInfo *ti = log->getTypeInfo(si.getType());
        if (ti != NULL && !ti->explicitlyFreed) {
            LogEntryHandle h = si.getHandle();
            if (!ti->livenessCB(h, ti->livenessArg)) {
                freedEntries++;
                freeByteSum += h->totalLength();
                if (ti->timestampCB != NULL)
                    freeSpaceTimeSum += h->totalLength() * ti->timestampCB(h);
            }
        }
    }

    if (freedEntries != cleanableSegment.implicitlyFreedEntries) {
        cleanableSegment.implicitlyFreedBytes = freeByteSum;
        cleanableSegment.implicitlyFreedEntries = freedEntries;
        perfCounters.scanForFreeSpaceProgress++;
    }

    segment->setImplicitlyFreedCounts(freeByteSum, freeSpaceTimeSum);

    perfCounters.scanForFreeSpaceSegments++;
}

/**
 * Decide which Segments, if any, to clean and return them in the provided
 * vector. This method implements the policy that determines which Segments
 * to clean, how many to clean, and whether or not to clean at all right
 * now. Note that any Segments returned from this method have NOT already
 * been removed from the #cleanableSegments vector. If cleaning is performed
 * they should be removed from that vector.
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
        size_t segmentIndex = cleanableSegments.size() - i - 1;
        Segment* s = cleanableSegments[segmentIndex].segment;
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

    double cost = writeCost(totalCapacity, totalLiveBytes);

    // We'll only clean if the write cost is sufficiently low.
    if (!isCleanable(cost)) {
        LOG(DEBUG, "writeCost (%.3f > %.3f) too high; not cleaning",
            cost, MAXIMUM_CLEANABLE_WRITE_COST);
        return;
    }

    // Ok, let's clean these suckers! The caller must be sure to remove them
    // from the vector of candidate Segments so we don't try again in the
    // future!
    for (i = 0; i < numSegmentsToClean; i++) {
        size_t segmentIndex = cleanableSegments.size() - i - 1;
        segmentsToClean.push_back(cleanableSegments[segmentIndex].segment);
    }

    // For unit testing's benefit.
    LOG(DEBUG, "writeCost %.2f", cost);
}

/**
 * Walk a segment, extract all live entries, and append them to the
 * provided output vector. Return the total number of bytes in live
 * entries.
 *
 * \param[in] segment
 *      Pointer to the Segment to scan.
 * \param[out] liveEntries
 *      Vector to put pointers to live entries on.
 * \return
 *      The number of bytes in live entries scanned from the input segment.
 *      This tally does NOT include any Segment metadata overhead - only
 *      the bytes in the entries themselves.
 */
size_t
LogCleaner::getLiveEntries(Segment* segment,
                           LiveSegmentEntryHandleVector& liveEntries)
{
    uint64_t liveEntryBytes = 0;

    CycleCounter<uint64_t> collectTicks(&perfCounters.collectLiveEntriesTicks);
    for (SegmentIterator i(segment); !i.isDone(); i.next()) {
        SegmentEntryHandle handle = i.getHandle();

        perfCounters.entryTypeCounts[handle->type()]++;
        perfCounters.entriesInCleanedSegments++;

        const LogTypeInfo *ti = log->getTypeInfo(handle->type());
        if (ti != NULL) {
            perfCounters.entriesLivenessChecked++;

            CycleCounter<uint64_t> livenessTicks(
                &perfCounters.livenessCallbackTicks);
            bool isLive = ti->livenessCB(handle, ti->livenessArg);
            livenessTicks.stop();

            if (isLive) {
                assert(ti->timestampCB != NULL);
                liveEntries.push_back({ handle, ti->timestampCB(handle) });
                liveEntryBytes += handle->length();
                perfCounters.liveEntryBytes += handle->totalLength();
            }
        }
    }

    return liveEntryBytes;
}

/**
 * Given a vector of Segments, walk all of them and extract the log
 * entries that are currently still live, then sort them by age (oldest
 * first). This finds all of the data that will need to be moved to another
 * Segment during cleaning. The method returns the maximum number of clean
 * Segments we'll need to allocate in order to clean all of the live data.
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
 * \return
 *      The number of bytes in live entries scanned from the input segments.
 *      This tally does NOT include any Segment metadata overhead - only
 *      the bytes in the entries themselves.
 */
size_t
LogCleaner::getSortedLiveEntries(SegmentVector& segments,
                                 LiveSegmentEntryHandleVector& liveEntries)
{
    uint64_t liveEntryBytes = 0;

    foreach (Segment* segment, segments)
        liveEntryBytes += getLiveEntries(segment, liveEntries);

    CycleCounter<uint64_t> _(&perfCounters.sortLiveEntriesTicks);
    std::sort(liveEntries.begin(),
              liveEntries.end(),
              SegmentEntryAgeLessThan(log));

    return liveEntryBytes;
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
            Segment* segment = cleanableSegments[segmentIndex].segment;

            if (segment->getLiveBytes() > lastNewSegment->appendableBytes())
                continue;

            madeProgress = true;

            // There's no point in sorting the source's objects since they're
            // going into only one Segment.
            for (SegmentIterator it(segment); !it.isDone(); it.next()) {
                SegmentEntryHandle handle = it.getHandle();

                perfCounters.entryTypeCounts[handle->type()]++;
                perfCounters.entriesInCleanedSegments++;

                const LogTypeInfo *ti = log->getTypeInfo(handle->type());
                if (ti == NULL || !ti->livenessCB(handle, ti->livenessArg))
                    continue;

                SegmentEntryHandle newHandle =
                    lastNewSegment->append(handle, false);
                assert(newHandle != NULL);

                if (ti->relocationCB(handle, newHandle, ti->relocationArg)) {
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
 * \param[in] cleanSegmentMemory
 *      Vector of clean segment memory to use to write the live data in to.
 *      This contains the maximum number of clean segments we could possibly
 *      need to move all of the live data. This preallocation of segments from
 *      the log and ensures that this method can complete the cleaning pass.
 *      Memory that is used from this vector should be removed from it. If any
 *      remain after the method completes they will be returned to the log for
 *      immediate reuse.
 * \param[out] segmentsToClean
 *      Vector of Segments from which liveData came. This is only to be used
 *      if we choose to clean addition Segmnts not previously specified (e.g.
 *      in the moveToFillSegment method when packing the last new Segment).
 */
void
LogCleaner::moveLiveData(LiveSegmentEntryHandleVector& liveData,
                         std::vector<void*>& cleanSegmentMemory,
                         SegmentVector& segmentsToClean)
{
    CycleCounter<uint64_t> _(&perfCounters.moveLiveDataTicks);

    LogPosition headPosition = log->headOfLog();
    SegmentVector segmentsAdded;
    PowerOfTwoSegmentBins segmentBins(perfCounters);

    // Ensure we'll avoid the zombie apocalypse by maintaining the invariant
    // that only objects from segments of IDs lower than the current log
    // head will be included in the survivor segments the cleaner creates.
    {
        uint64_t maxSegmentId = 0;
        foreach (Segment* segment, segmentsToClean)
            maxSegmentId = std::max(maxSegmentId, segment->getId());
        assert(maxSegmentId < headPosition.segmentId());
    }

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
                // This should never fail. The caller should have
                // pre-allocated all we need.
                void* segmentMemory = cleanSegmentMemory.back();
                cleanSegmentMemory.pop_back();

                Segment* newSeg = new Segment(log,
                                              false, // !isLogHead
                                              log->allocateSegmentId(),
                                              segmentMemory,
                                              log->getSegmentCapacity(),
                                              replicaManager,
                                              LOG_ENTRY_TYPE_UNINIT, NULL, 0,
                                              headPosition.segmentId());

                segmentsAdded.push_back(newSeg);
                segmentBins.addSegment(newSeg);
                log->cleaningInto(newSeg);
                continue;
            }

            CycleCounter<uint64_t> _(&perfCounters.segmentAppendTicks);
            newHandle = segmentUsed->append(handle, false);
        }

        const LogTypeInfo* ti = log->getTypeInfo(handle->type());

        CycleCounter<uint64_t> relTicks(&perfCounters.relocationCallbackTicks);
        bool relocated = ti->relocationCB(handle, newHandle, ti->relocationArg);
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
    if (segmentsAdded.size() > 0)
        moveToFillSegment(segmentsAdded.back(), segmentsToClean);

    // Close and sync all newly created Segments.
    CycleCounter<uint64_t> syncTicks(&perfCounters.closeAndSyncTicks);
    foreach (Segment* segment, segmentsAdded)
        segment->close(NULL, true);
    syncTicks.stop();

    // Now we're done. Save some stats.
    foreach (Segment* segment, segmentsAdded)
        perfCounters.generatedUtilisationSum += segment->getUtilisation();

    perfCounters.segmentsGenerated += segmentsAdded.size();
}

/**
 * Set everything up to run a normal cost-benefit cleaning pass. This
 * includes choosing which segments to clean, extracting live entries,
 * and allocating space for survivor data.
 *
 * \param[out] liveEntries
 *      Vector of live entries to store references to entries in segments that
 *      will be cleaned.
 *
 * \param[out] cleanSegmentMemory
 *      Vector of segment memory that will be used for the survivor segments.
 *      This is preallocated to ensure that cleaning can proceed.
 *
 * \param[out] segmentsToClean
 *      Vector of segments that will be cleaned.
 *
 * \return
 *      Returns false if normal cleaning cannot proceed. Otherwise, returns
 *      true and the out parameters are appropriate set up for an invocation
 *      of moveLiveData.
 */
bool
LogCleaner::setUpNormalCleaningPass(LiveSegmentEntryHandleVector& liveEntries,
                                    std::vector<void*>& cleanSegmentMemory,
                                    SegmentVector& segmentsToClean)
{
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0) {
        // Even if there's nothing to do, call into the Log
        // to give it a chance to free up Segments that were
        // waiting on existing references.
        log->cleaningComplete(segmentsToClean, cleanSegmentMemory);
        return false;
    }

    liveEntries.reserve(segmentsToClean.size() *
        (log->getSegmentCapacity() / MIN_ENTRY_BYTES));
    uint64_t liveEntryBytes = getSortedLiveEntries(segmentsToClean,
                                                   liveEntries);
    size_t segmentsNeeded = Segment::maximumSegmentsNeededForEntries(
                                                liveEntries.size(),
                                                liveEntryBytes,
                                                log->maximumBytesPerAppend,
                                                log->getSegmentCapacity());

    // Try to allocate the number of clean segments we'll need to complete
    // this pass up front. There's no guarantee that we'll have enough. If we
    // don't, just return. The log will soon run out and we'll kick into
    // emergency cleaning mode.
    if (!getCleanSegmentMemory(segmentsNeeded, cleanSegmentMemory)) {
        LOG(WARNING, "Cleaning pass failed: insufficient free log memory!");
        return false;
    }

    // TODO(Rumble): liveEntryBytes does _not_ include SegmentEntry counts.
    perfCounters.writeCostSum += writeCost(segmentsToClean.size() *
        log->getSegmentCapacity(), liveEntryBytes);

    // We're guaranteed to succeed in this cleaning pass. Remove the segments
    // we'll be cleaning and proceed.
    for (size_t i = 0; i < segmentsToClean.size(); i++)
        cleanableSegments.pop_back();

    return true;
}

/**
 * Set everything up to run an emergency cleaning pass. This includes
 * choosing which segments to clean, extracting live entries, and
 * allocating space for survivor data.
 *
 * We need to use the emergency reserve segments the log has saved for
 * this eventuality. But to ensure forward progress, we must
 * be able to clean such that we can free at least the number of
 * emergency segments used (to keep that supply replenished) plus one
 * (so the log can be usefully appended to).
 *
 * \param[out] liveEntries
 *      Vector of live entries to store references to entries in segments that
 *      will be cleaned.
 *
 * \param[out] cleanSegmentMemory
 *      Vector of segment memory that will be used for the survivor segments.
 *      This is preallocated to ensure that cleaning can proceed.
 *
 * \param[out] segmentsToClean
 *      Vector of segments that will be cleaned.
 *
 * \return
 *      Returns false if emergency cleaning cannot proceed. Otherwise,
 *      returns true and the out parameters are appropriate set up for
 *      an invocation of moveLiveData.
 */
bool
LogCleaner::setUpEmergencyCleaningPass(
                                    LiveSegmentEntryHandleVector& liveEntries,
                                    std::vector<void*>& cleanSegmentMemory,
                                    SegmentVector& segmentsToClean)
{
    // Get whatever emergency segment memory we have to deal with
    // these situations.
    while (1) {
        void* memoryBlock = log->getSegmentMemoryForCleaning(true);
        if (memoryBlock == NULL)
            break;
        cleanSegmentMemory.push_back(memoryBlock);
    }

    // Walk the Segments in order of increasing utilisation.
    CycleCounter<uint64_t> getSegmentsTicks(&perfCounters.getSegmentsTicks);
    std::sort(cleanableSegments.begin(),
              cleanableSegments.end(),
              UtilisationLessThan());

    uint64_t totalCapacity = 0;
    uint64_t totalLiveBytes = 0;
    bool abort = false;
    size_t i;
    for (i = 0; i < cleanableSegments.size(); i++) {
        size_t segmentIndex = cleanableSegments.size() - i - 1;
        Segment* s = cleanableSegments[segmentIndex].segment;
        segmentsToClean.push_back(s);

        totalCapacity += s->getCapacity();
        totalLiveBytes += getLiveEntries(s, liveEntries);
        size_t segmentsNeeded = Segment::maximumSegmentsNeededForEntries(
                                                    liveEntries.size(),
                                                    totalLiveBytes,
                                                    log->maximumBytesPerAppend,
                                                    log->getSegmentCapacity());

        // If we don't have enough clean segments, there's nothing we can do.
        if (segmentsNeeded > cleanSegmentMemory.size()) {
            LOG(WARNING, "Too many free segments (%zd) needed for emergency "
                "cleaning.", segmentsNeeded);
            abort = true;
            break;
        }

        // Need [1 + the number of segments we'd consume for the survivors]
        // in order to make forward progress.
        if (segmentsToClean.size() > segmentsNeeded)
            break;
    }

    if (i == cleanableSegments.size()) {
        LOG(WARNING, "Could not find any segments to clean. Memory truly "
            "exhausted. Please move your data!");
        abort = true;
    }

    // Return if cleaning can proceed.
    if (!abort) {
        // Sort the live entries.
        SegmentVector empty;
        getSortedLiveEntries(empty, liveEntries);

        // Be sure to remove the Segments we'll clean from the list.
        for (i = 0; i < segmentsToClean.size(); i++)
            cleanableSegments.pop_back();

        // Also, calculate and update our write cost stats while here.
        // TODO(Rumble): totalLiveBytes does _not_ include SegmentEntry counts.
        double cost = writeCost(totalCapacity, totalLiveBytes);
        perfCounters.writeCostSum += cost;

        return true;
    }

    // We can't do any work now, so clean up.

    // Return the emergency memory to the log.
    SegmentVector empty;
    log->cleaningComplete(empty, cleanSegmentMemory);

    // It's possible that our sort by utilisation is off (implicitly freed entry
    // counts may not be accurate), so scan some segments to update the counts
    // for the next pass.
    for (i = 0; i < EMERGENCY_CLEANING_RANDOM_FREE_SPACE_SCANS; i++) {
        if (cleanableSegments.size() == 0)
            break;

        size_t indexA = generateRandom() % cleanableSegments.size();
        size_t indexB = generateRandom() % cleanableSegments.size();

        if (cleanableSegments[indexA].timesRandomlyScanned <
          cleanableSegments[indexB].timesRandomlyScanned) {
            scanSegmentForFreeSpace(cleanableSegments[indexA]);
            cleanableSegments[indexA].timesRandomlyScanned++;
        } else {
            scanSegmentForFreeSpace(cleanableSegments[indexB]);
            cleanableSegments[indexB].timesRandomlyScanned++;
        }
    }

    liveEntries.clear();
    cleanSegmentMemory.clear();
    segmentsToClean.clear();

    return false;
}

} // namespace
