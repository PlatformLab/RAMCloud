/* Copyright (c) 2010 Stanford University
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
      cleanableSegments(log->getNumberOfSegments()),
      log(log),
      backup(backup),
      thread()
{
    if (startThread)
        thread.construct(cleanerThreadEntry, this);
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

    if ((logBytesFreed - bytesFreedBeforeLastCleaning) < MIN_CLEANING_DELTA)
        return;

    SegmentVector segmentsToClean;
    SegmentVector liveSegments;
    SegmentEntryHandleVector liveEntries;

    getSegmentsToClean(segmentsToClean);
    if (segmentsToClean.size() == 0)
        return;

    getSortedLiveEntries(segmentsToClean, liveEntries);
    moveLiveData(liveEntries, liveSegments);
    log->cleaningComplete(segmentsToClean, liveSegments);

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
LogCleaner::cleanerThreadEntry(LogCleaner* logCleaner)
{
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
 * one's associated cost-benefit calculation.
 */
struct CostBenefitLessThan {
  public:
    CostBenefitLessThan()
        : now(secondsTimestamp())
    {
    }

    uint64_t
    costBenefit(const Segment *s) const
    {
        uint64_t costBenefit = ~(0UL);          // empty Segments are priceless

        int utilisation = s->getUtilisation();
        if (utilisation != 0) {
            uint64_t timestamp = s->getAverageTimestamp();
            assert(timestamp <= now);
            uint64_t age = now - timestamp;
            costBenefit = ((100 - utilisation) * age) / utilisation;
        }

        return costBenefit;
    }

    bool
    operator()(const Segment* a, const Segment* b) const
    {
        return costBenefit(a) < costBenefit(b);
    }

  private:
    uint64_t now;
};

/**
 * Comparison functor that is used to sort Segment Entries based on their
 * age (timestamp).
 */
struct SegmentEntryAgeLessThan {
  public:
    SegmentEntryAgeLessThan(Log* log)
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
 * Decide which Segments, if any, to clean and return them in the provided
 * vector. This method implements the policy that determines which Segments
 * to clean, how many to clean, and whether or not to clean at all right
 * now. 
 *
 * \param[out] segmentsToClean
 *      Pointers to Segments that should be cleaned are appended to this
 *      empty vector. 
 */
void
LogCleaner::getSegmentsToClean(SegmentVector& segmentsToClean)
{
    assert(segmentsToClean.size() == 0);

    log->getNewActiveSegments(cleanableSegments);

    std::sort(cleanableSegments.begin(),
              cleanableSegments.end(),
              CostBenefitLessThan());

    // should we clean now?

    // okay, we're cleaning. how many segments? 
    
    // store pointers to the segs we're cleaning in the out vector
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
 * \param[out] segmentsAdded
 *      Vector of Segment pointers. This is used to return any Segments
 *      created in moving the data.
 */
void
LogCleaner::moveLiveData(SegmentEntryHandleVector& liveData,
                         SegmentVector& segmentsAdded)
{
    Segment* currentSegment = NULL;

    // TODO: We shouldn't just stop using a Segment if it didn't fit the
    //       last object. We should keep considering them for future objects.
    //       This is strictly better than leaving open space, even if we put
    //       in objects that are much newer (and hence more likely to be
    //       freed soon). The worst case is the same (space is empty), but
    //       we have the opportunity to get lucky and pack more data that
    //       will stay alive.

    foreach (SegmentEntryHandle handle, liveData) {
        uint64_t offsetInSegment;
        SegmentEntryHandle newHandle = NULL;
        
        do {
            if (currentSegment != NULL)
                newHandle = currentSegment->append(handle, &offsetInSegment, false);

            if (newHandle == NULL) {
                currentSegment = new Segment(log,
                                             log->allocateSegmentId(),
                                             log->getFromFreeList(),
                                             log->getSegmentCapacity(),
                                             backup,
                                             LOG_ENTRY_TYPE_UNINIT, NULL, 0);
                segmentsAdded.push_back(currentSegment);
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
