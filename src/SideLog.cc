/* Copyright (c) 2012 Stanford University
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

#include "SideLog.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a new SideLog for the given log. This object may be used
 * to append large numbers of entries efficiently and then atomically commit
 * them to the given log.
 *
 * \param log
 *      Entries appended to this SideLog will be committed to this
 *      given log when the commit() method is invoked.
 */
SideLog::SideLog(Log* log)
    : AbstractLog(log->entryHandlers,
                  log->segmentManager,
                  log->replicaManager,
                  log->segmentSize),
      log(log),
      segmentManager(log->segmentManager),
      replicaManager(log->replicaManager),
      segments(),
      forCleaner(false)
{
}

/**
 * Construct a new SideLog for the given log that will be used during
 * cleaning. The object constructed is special only in that it will allocate
 * segments from the SegmentManager using a special pool reserved for cleaning
 * (to avoid deadlock during low memory situations).
 *
 * Note that this constructor should never be invoked by any other code. You
 * almost certainly want the single-argument constructor above.
 */
SideLog::SideLog(Log* log, LogCleaner* cleaner)
    : AbstractLog(log->entryHandlers,
                  log->segmentManager,
                  log->replicaManager,
                  log->segmentSize),
      log(log),
      segmentManager(log->segmentManager),
      replicaManager(log->replicaManager),
      segments(),
      forCleaner(true)
{
}

/*
 * Destroy this object. Any appends made after the last commit() method call
 * will be implicitly aborted (removed from the in-memory and on-disk logs).
 */
SideLog::~SideLog()
{
    // Destruction is an implicit abort of any segments not committed.
    if (segments.empty())
        return;

    LOG(DEBUG, "Aborting %lu uncommitted segment(s)", segments.size());
    // JIRA Issue: RAM-667: Add commit, remove free.
    segmentManager->freeUnusedSideSegments(segments);
}

/**
 * Commit all appended entries by merging them into the log. When this method
 * returns, all entries will be durable on backups and will be observed in
 * the event of log replay (for example, during failure recovery).
 *
 * When this method returns, the state of the SideLog will be reset
 * to the state after its construction. This means that is may be reused to
 * append additional entries and commit() may be invoked again in the future
 * to merge those into the log.
 */
void
SideLog::commit()
{
    Tub<Lock> lock;
    lock.construct(appendLock);

    if (segments.empty())
        return;

    // The last segment will still be open. Close it and begin replication.
    LogSegment* lastSegmentAllocated = segments.back();
    lastSegmentAllocated->close();
    lastSegmentAllocated->replicatedSegment->close();

    // Ensure that replication has completed on all segments.
    foreach (LogSegment* segment, segments)
        segment->replicatedSegment->sync();

    // Tell SegmentManager to make these segments part of the log. They will
    // become part of the on-disk log when the next head segment is opened and
    // a new digest is written.
    segmentManager->injectSideSegments(segments);

    // Wipe the list so that this SideLog can be used again to commit a
    // different batch of entries.
    segments.clear();

    log->totalLiveBytes += totalLiveBytes;
    totalLiveBytes = 0;

    metrics.mergeInto(log);
    metrics.reset();

    // Force the head to roll over so a new digest goes out. The caller also
    // acquires the appendLock, so drop it first. This is safe. We don't care
    // about any races. We only want the log head to change.
    lock.destroy();
    log->rollHeadOver();
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * Allocate a new segment to service appends. This is used by the AbstractLog
 * superclass when a new segment is needed. The segment returned will not be
 * part of the log proper. Any segments previously allocated by this call will
 * not be appended to again.
 *
 * This method must be called with AbstractLog's appendLock held.
 *
 * \param mustNotFail
 *      If true, this method must return a valid LogSegment pointer and may
 *      block as long as needed. If false, it should return immediately with
 *      a NULL pointer if no segments are available.
 */
LogSegment*
SideLog::allocNextSegment(bool mustNotFail)
{
    assert(!appendLock.try_lock());

    LogSegment* segment;
    if (mustNotFail)
        segment = segmentManager->allocSideSegment(
            SegmentManager::MUST_NOT_FAIL, NULL);
    else
        segment = segmentManager->allocSideSegment(0, NULL);

    if (segment == NULL)
        return NULL;

    if (!segments.empty()) {
        // Close the last segment so that replication may begin on it
        // immediately. Hopefully the replication will overlap with the
        // future appends on the new segment.
        LogSegment* lastSegmentAllocated = segments.back();
        lastSegmentAllocated->close();
        lastSegmentAllocated->replicatedSegment->close();
    }

    segments.push_back(segment);
    return segment;
}

} // namespace
