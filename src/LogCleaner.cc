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
      thread()
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
 */
void
LogCleaner::clean()
{
    uint64_t logBytesFreed = log->getBytesFreed();

    // We must scan new segments the Log considers cleanable before they
    // are cleaned. Doing so in log order gives us a chance to fire callbacks
    // that maintain the TabletProfilers.
    scanNewCleanableSegments();

    if ((logBytesFreed - bytesFreedBeforeLastCleaning) < MIN_CLEANING_DELTA)
        return;

    SegmentVector segmentsToClean;
    SegmentEntryHandleVector liveEntries;
    getSegmentsToClean(segmentsToClean);

    if (segmentsToClean.size() == 0)
        return;

    getSortedLiveEntries(segmentsToClean, liveEntries);
    moveLiveData(liveEntries);
    log->cleaningComplete(segmentsToClean);

    bytesFreedBeforeLastCleaning = logBytesFreed;
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
        logCleaner->clean();
        usleep(LogCleaner::CLEANER_POLL_USEC);
    }
}

/**
 * CostBenefit comparison functor for a vector of Segment pointers.
 * This is used to sort our array of cleanable Segments based on each
 * one's associated cost-benefit calculation. Higher values (the better
 * candidates) come last. This lets us easily remove them by popping
 * the back, rather than pulling from the front and shifting all elements
 * down.
 */
struct CostBenefitLessThan {
  public:
    CostBenefitLessThan()
        : now(secondsTimestamp())
    {
    }

    uint64_t
    costBenefit(Segment *s)
    {
        uint64_t costBenefit = ~(0UL);          // empty Segments are priceless

        int utilisation = s->getUtilisation();
        if (utilisation != 0) {
            uint64_t timestamp = s->getAverageTimestamp();

            // Mathematically this should be assured, however, a few issues can
            // potentially pop up:
            //  1) improper synchronisation in Segment.cc
            //  2) unsynchronised clocks and "newer" recovered data in the Log
            //  3) unsynchronised TSCs (WallTime uses rdtsc)
            assert(timestamp <= now);

            uint64_t age = now - timestamp;
            costBenefit = ((100 - utilisation) * age) / utilisation;
        }

        return costBenefit;
    }

    bool
    operator()(Segment* a, Segment* b)
    {
        return costBenefit(a) < costBenefit(b);
    }

  private:
    uint64_t now;
};

/**
 * Comparison functor that is used to sort Segment entries based on their
 * age (timestamp). Older entries come first (i.e. those with lower timestamps).
 */
struct SegmentEntryAgeLessThan {
  public:
    explicit SegmentEntryAgeLessThan(Log* log)
        : log(log)
    {
    }

    bool
    operator()(const SegmentEntryHandle a, const SegmentEntryHandle b)
    {
        const LogTypeCallback *aLogCB = log->getCallbacks(a->type());
        const LogTypeCallback *bLogCB = log->getCallbacks(b->type());

        assert(aLogCB != NULL);
        assert(bLogCB != NULL);

        return aLogCB->timestampCB(a) < bLogCB->timestampCB(b);
    }

  private:
    Log* log;
};

/**
 * Comparison functor used to sort Segments by their IDs. Higher (newer)
 * IDs come first.
 */
struct SegmentIdLessThan {
  public:
    bool
    operator()(const Segment* a, const Segment* b)
    {
        return a->getId() > b->getId();
    }
};

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
    assert(segmentsToClean.size() == 0);

    if (cleanableSegments.size() < SEGMENTS_PER_CLEANING_PASS)
        return;

    std::sort(cleanableSegments.begin(),
              cleanableSegments.end(),
              CostBenefitLessThan());

    // Calculate the write cost for the best candidate Segments, i.e.
    // the number of bytes we need to write out in total to write however
    // may new bytes of data that we can free up.  For us, this cost is
    // (1 / 1 - u). LFS was twice as high because segments had to be read
    // from disk before cleaning.
    uint64_t totalLiveBytes = 0;
    uint64_t totalCapacity = 0;
    for (size_t i = 0; i < SEGMENTS_PER_CLEANING_PASS; i++) {
        Segment* s = cleanableSegments[cleanableSegments.size() - i - 1];
        assert(s != NULL);
        totalLiveBytes += (s->getCapacity() - s->getFreeBytes());
        totalCapacity += s->getCapacity();
    }
    double u = static_cast<double>(totalLiveBytes) /
               static_cast<double>(totalCapacity);
    double writeCost = 1 / (1 - u);

    LOG(DEBUG, "writeCost is %.3f\n", writeCost);

    if (writeCost > MAXIMUM_CLEANABLE_WRITE_COST) {
        LOG(DEBUG, "writeCost (%.3f > %.3f) too high; not cleaning",
            writeCost, MAXIMUM_CLEANABLE_WRITE_COST);
        return;
    }

    // Ok, let's clean these suckers! Be sure to remove them from the vector
    // of candidate Segments so we don't try again in the future!
    for (size_t i = 0; i < SEGMENTS_PER_CLEANING_PASS; i++) {
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
    foreach (Segment* segment, segments) {
        for (SegmentIterator i(segment); !i.isDone(); i.next()) {
            SegmentEntryHandle seh = i.getHandle();
            const LogTypeCallback *logCB = log->getCallbacks(seh->type());
            if (logCB != NULL && logCB->livenessCB(seh, logCB->livenessArg))
                liveEntries.push_back(seh);
        }
    }

    std::sort(liveEntries.begin(),
              liveEntries.end(),
              SegmentEntryAgeLessThan(log));
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
 */
void
LogCleaner::moveLiveData(SegmentEntryHandleVector& liveData)
{
    Segment* currentSegment = NULL;
    SegmentVector segmentsAdded;

    // XXX: We shouldn't just stop using a Segment if it didn't fit the
    //      last object. We should keep considering them for future objects.
    //      This is strictly better than leaving open space; even if we put
    //      in objects that are much newer (and hence more likely to be
    //      freed soon). The worst case is the same space is soon empty, but
    //      we have the opportunity to do better if we can pack in more data
    //      that ends up staying alive longer.

    foreach (SegmentEntryHandle handle, liveData) {
        SegmentEntryHandle newHandle = NULL;

        do {
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
        } while (newHandle == NULL);

        const LogTypeCallback *cb = log->getCallbacks(handle->type());
        if (!cb->relocationCB(handle, newHandle, cb->relocationArg))
            currentSegment->rollBack(newHandle);
    }

    foreach (Segment* segment, segmentsAdded)
        segment->close(true);
}

} // namespace
