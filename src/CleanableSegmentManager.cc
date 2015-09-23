/* Copyright (c) 2013-2015 Stanford University
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

#include "CleanableSegmentManager.h"
#include "LogCleaner.h"
#include "Object.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "TestLog.h"
#include "WallTime.h"
#include "MasterService.h"

namespace RAMCloud {

#ifdef TESTING
int CleanableSegmentManager::mockLiveObjectUtilization = 0;
#endif

CleanableSegmentManager::CleanableSegmentManager(
                                    SegmentManager& segmentManager,
                                    const ServerConfig* config,
                                    Context* context,
                                    LogCleanerMetrics::OnDisk<>& onDiskMetrics)
    : context(context)
    , segmentManager(segmentManager)
    , lastUpdateTimestamp(0)
    , liveObjectBytes(0)
    , undeadTombstoneBytes(0)
    , segmentsToCleaner(0)
    , tombstoneScans(0)
    , costBenefitCandidates()
    , compactionCandidates()
    , tombstoneScanCandidates()
    , lock("CleanableSegmentManager::lock")
    , segletSize(config->segletSize)
    , segmentSize(config->segmentSize)
    , segletsPerSegment(config->segmentSize / config->segletSize)
    , onDiskMetrics(onDiskMetrics)
{
}

CleanableSegmentManager::~CleanableSegmentManager()
{
}

int
CleanableSegmentManager::getLiveObjectUtilization()
{
#ifdef TESTING
    if (mockLiveObjectUtilization != 0) {
        return mockLiveObjectUtilization;
    }
#endif
    Lock guard(lock);
    update(guard);

    uint64_t totalSegletBytes = segmentManager.getAllocator().getTotalCount(
        SegletAllocator::DEFAULT) * segletSize;
    if (totalSegletBytes == 0)
        return 0;
    return downCast<int>((100 * liveObjectBytes) / totalSegletBytes);
}

int
CleanableSegmentManager::getUndeadTombstoneUtilization()
{
    Lock guard(lock);
    update(guard);

    uint64_t totalSegletBytes = segmentManager.getAllocator().getTotalCount(
        SegletAllocator::DEFAULT) * segletSize;
    if (totalSegletBytes == 0)
        return 0;
    return downCast<int>((100 * undeadTombstoneBytes) / totalSegletBytes);
}

LogSegment*
CleanableSegmentManager::getSegmentToCompact()
{
    Lock guard(lock);
    update(guard);

    if (compactionCandidates.empty())
        return NULL;

    LogSegment& segment = *compactionCandidates.begin();
    eraseFromAll(&segment, guard);
    segmentsToCleaner++;
    return &segment;
}

void
CleanableSegmentManager::getSegmentsToClean(LogSegmentVector& outSegsToClean)
{
    Lock guard(lock);
    update(guard);

    foreach (LogSegment& segment, costBenefitCandidates) {
        onDiskMetrics.allSegmentsDiskHistogram.storeSample(
            segment.getDiskUtilization());
    }

    uint32_t totalSeglets = 0;
    uint64_t totalLiveBytes = 0;
    uint64_t maximumLiveBytes = LogCleaner::MAX_LIVE_SEGMENTS_PER_DISK_PASS *
                                segmentSize;

    foreach (LogSegment& candidate, costBenefitCandidates) {
        int utilization = candidate.getMemoryUtilization();
        if (utilization > LogCleaner::MAX_CLEANABLE_MEMORY_UTILIZATION)
            continue;

        uint64_t liveBytes = candidate.getLiveBytes();
        if ((totalLiveBytes + liveBytes) > maximumLiveBytes)
            break;

        totalLiveBytes += liveBytes;
        totalSeglets += candidate.getSegletsAllocated();
        outSegsToClean.push_back(&candidate);
        segmentsToCleaner++;
    }

    foreach (LogSegment* s, outSegsToClean)
        eraseFromAll(s, guard);

    TEST_LOG("%lu segments selected with %u allocated segments",
        outSegsToClean.size(), totalSeglets);
}

uint64_t
CleanableSegmentManager::computeCompactionCostBenefitScore(LogSegment* segment)
{
    uint64_t liveBytes = segment->getLiveBytes();
    uint64_t liveSeglets = (liveBytes + segletSize - 1) / segletSize;
    uint64_t unusedSeglets = segment->getSegletsAllocated() - liveSeglets;
    uint64_t unusedBytes = unusedSeglets * segletSize;

    if (liveBytes == 0) {
        // A segment with no live data that has already been compacted will
        // still retain one seglet to store the segment's header. That last
        // seglet may only be freed by the disk cleaner, since recovery relies
        // on information present in the header to figure out if it has found
        // all log segments. If we were to ditch it, we could end up writing an
        // empty segment in response to a backup failure, which would cause
        // future master recovery to fail.
        assert(segment->getSegletsAllocated() > 0);
        if (segment->getSegletsAllocated() == 1)
            return 0;

        return std::numeric_limits<uint64_t>::max();
    }

    return 1000 * unusedBytes / liveBytes;
}

void
CleanableSegmentManager::update(Lock& guard)
{
    scanSegmentTombstones(guard);

    uint64_t now = Cycles::rdtsc();

    bool stale = false;
    if (Cycles::toMicroseconds(now - lastUpdateTimestamp) >= UPDATE_USEC)
        stale = true;

    bool tooFewCandidates = false;
    if (costBenefitCandidates.size() <= MIN_SEGMENTS_BEFORE_UPDATE_DELAY)
        tooFewCandidates = true;

    if (!stale && !tooFewCandidates)
        return;

    lastUpdateTimestamp = now;
    liveObjectBytes = 0;
    undeadTombstoneBytes = 0;

    // Update cost-benefit and scan scores, and update our aggregate statistics
    // for old segments.
    CostBenefitTree::iterator cbit(costBenefitCandidates.begin());
    while (cbit != costBenefitCandidates.end()) {
        LogSegment& segment = *cbit;

        // Push the iterator forward now since we may invalidate it by
        // updating the current segment
        cbit++;

        // If UPDATE_USEC is >= 1e6, then it's likely that every score will
        // have changed because the age component has increased by 1. However
        // this may help a bit if UPDATE_USEC is dialed down.
        uint64_t score = computeCleaningCostBenefitScore(&segment);
        if (score != segment.cachedCleaningCostBenefitScore) {
            erase(costBenefitCandidates, segment);
            segment.cachedCleaningCostBenefitScore = score;
            costBenefitCandidates.insert(segment);
        }

        uint64_t compactionScore = computeCompactionCostBenefitScore(&segment);
        if (compactionScore != segment.cachedCompactionCostBenefitScore) {
            erase(compactionCandidates, segment);
            segment.cachedCompactionCostBenefitScore = compactionScore;
            compactionCandidates.insert(segment);
        }

        uint64_t tombstoneScanScore = computeTombstoneScanScore(&segment);
        if (tombstoneScanScore != segment.cachedTombstoneScanScore) {
            erase(tombstoneScanCandidates, segment);
            segment.cachedTombstoneScanScore = tombstoneScanScore;
            tombstoneScanCandidates.insert(segment);
        }

        const LogEntryType objType = LOG_ENTRY_TYPE_OBJ;
        liveObjectBytes += segment.entryLengths[objType] -
                           segment.deadEntryLengths[objType];

        const LogEntryType tombType = LOG_ENTRY_TYPE_OBJTOMB;
        undeadTombstoneBytes += segment.entryLengths[tombType] -
                                segment.deadEntryLengths[tombType];
    }

    // Get new candidates from the SegmentManager and insert them into the
    // trees. Update our aggregate statistics as we go.

    LogSegmentVector newCandidates;
    segmentManager.cleanableSegments(newCandidates);
    foreach (LogSegment* segment, newCandidates) {
        segment->cachedCleaningCostBenefitScore =
            computeCleaningCostBenefitScore(segment);
        segment->cachedCompactionCostBenefitScore =
            computeCompactionCostBenefitScore(segment);
        segment->cachedTombstoneScanScore =
            computeTombstoneScanScore(segment);
        insertInAll(segment, guard);

        liveObjectBytes += segment->entryLengths[LOG_ENTRY_TYPE_OBJ] -
                           segment->deadEntryLengths[LOG_ENTRY_TYPE_OBJ];
        undeadTombstoneBytes += segment->entryLengths[LOG_ENTRY_TYPE_OBJTOMB];
    }

    assert(costBenefitCandidates.size() == compactionCandidates.size());
}

/**
 * Scan the best candidate in tombstoneScanCandidates for dead tombstones and
 * update the segment's statistics. This lets the getSegmentsToClean() and
 * getSegmentToCompact() methods return better candidates. Otherwise, we would
 * not know which tombstones are dead.
 *
 * Note that this method will drop the monitor lock while scanning the segment
 * to avoid holding up other threads. While being scanned, the segment will not
 * be present in the costBenefitCandidate nor in the compactionCandidate trees.
 * The lock is reacquired before returning.
 */
void
CleanableSegmentManager::scanSegmentTombstones(Lock& guard)
{
    if (SCAN_TOMBSTONES_EVERY_N_SEGMENTS == 0)
        return;
    if (tombstoneScans > (segmentsToCleaner / SCAN_TOMBSTONES_EVERY_N_SEGMENTS))
        return;
    if (tombstoneScanCandidates.empty())
        return;

    tombstoneScans++;

    // Choose a segment and remove it from the other candidate trees
    // temporarily so we don't have to worry about concurrency. This
    // lets us safely drop the monitor lock while we scan it.
    LogSegment& s = *tombstoneScanCandidates.begin();
    eraseFromAll(&s, guard);
    lock.unlock();

    uint32_t tombstonesScanned = 0;
    uint32_t deadTombstones = 0;
    uint32_t deadTombstoneLengths = 0;
    uint32_t totalTombstones = s.entryCounts[LOG_ENTRY_TYPE_OBJTOMB];
    for (SegmentIterator it(s); !it.isDone(); it.next()) {
        // Bail out early if we've seen all of the tombstones. If the LogCleaner
        // compacts segments with tombstones at the front we can avoid looking
        // at most of the segment.
        if (tombstonesScanned == totalTombstones)
            break;

        if (it.getType() != LOG_ENTRY_TYPE_OBJTOMB)
            continue;

        tombstonesScanned++;

        Buffer buffer;
        it.appendToBuffer(buffer);
        ObjectTombstone tomb(buffer);
        // Protect tombstones which are still in the hash table since their
        // references are removed asynchronously.
        Key key(tomb.getTableId(), tomb.getKey(), tomb.getKeyLength());
        if (context->getMasterService()->objectManager.keyPointsAtReference(
                key, s.getReference(it.getOffset())))
            continue;
        if (!segmentManager.doesIdExist(tomb.getSegmentId())) {
            deadTombstones++;
            // Magic constant indicates the likely full length
            // in the log. Should add a static method to Segment that computes
            // this properly.
            deadTombstoneLengths += it.getLength() + 2;
        }
    }

    s.deadEntryCounts[LOG_ENTRY_TYPE_OBJTOMB] = deadTombstones;
    s.deadEntryLengths[LOG_ENTRY_TYPE_OBJTOMB] = deadTombstoneLengths;
    s.cachedCleaningCostBenefitScore = computeCleaningCostBenefitScore(&s);
    s.cachedCompactionCostBenefitScore = computeCompactionCostBenefitScore(&s);
    s.cachedTombstoneScanScore = computeTombstoneScanScore(&s);
    s.lastTombstoneScanTimestamp = WallTime::secondsTimestamp();

    lock.lock();
    insertInAll(&s, guard);
}

/**
 * Calculate the cost-benefit ratio (benefit/cost) for the given segment.
 */
uint64_t
CleanableSegmentManager::computeCleaningCostBenefitScore(LogSegment* s)
{
    // If utilization is 0, cost-benefit is infinity.
    uint64_t costBenefit = -1UL;

    int utilization = s->getDiskUtilization();
    if (utilization != 0) {
        uint32_t now = WallTime::secondsTimestamp();
        uint32_t timestamp = s->creationTimestamp;

        // This generally shouldn't happen, but is possible due to:
        //  1) Unsynchronized TSCs across cores (WallTime uses rdtsc).
        //  2) Unsynchronized clocks and "newer" recovered data in the log.
        if (timestamp > now) {
            LOG(WARNING, "timestamp > now");
            timestamp = now;
        }

        uint64_t age = now - timestamp;
        costBenefit = ((100 - utilization) * age) / utilization;
    }

    return costBenefit;
}

uint64_t
CleanableSegmentManager::computeTombstoneScanScore(LogSegment* s)
{
    uint64_t tombstoneSeglets = s->entryLengths[LOG_ENTRY_TYPE_OBJTOMB] /
                                segletSize;
    uint64_t timeSinceLastScan = WallTime::secondsTimestamp() -
                                 s->lastTombstoneScanTimestamp;
    return tombstoneSeglets * timeSinceLastScan;
}

void
CleanableSegmentManager::insertInAll(LogSegment* s, Lock& guard)
{
    costBenefitCandidates.insert(*s);
    compactionCandidates.insert(*s);
    tombstoneScanCandidates.insert(*s);
}

void
CleanableSegmentManager::eraseFromAll(LogSegment* s, Lock& guard)
{
    erase(costBenefitCandidates, *s);
    erase(compactionCandidates, *s);
    erase(tombstoneScanCandidates, *s);
}

string
CleanableSegmentManager::toString()
{
    string s;

    s += "costBenefitCandidates [ ";
    foreach (LogSegment& segment, costBenefitCandidates) {
        s += format("id=%lu,cb=%lu ",
                    segment.id,
                    segment.cachedCleaningCostBenefitScore);
    }
    s += "]";

    s += " compactionCandidates [ ";
    foreach (LogSegment& segment, compactionCandidates)
        s += format("id=%lu,cb=%lu ",
                    segment.id,
                    segment.cachedCompactionCostBenefitScore);
    s += "]";

    s += " tombstoneScanCandidates [ ";
    foreach (LogSegment& segment, tombstoneScanCandidates)
        s += format("id=%lu,ss=%lu ",
                    segment.id,
                    segment.cachedTombstoneScanScore);
    s += "]";

    return s;
}

} // namespace
