/* Copyright (c) 2011 Stanford University
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

#include "Common.h"
#include "LogIterator.h"
#include "Segment.h"

/*
 * NOTES:
 *  - appendLock can screw with concurrent iteration of the head segment.
 *    Perhaps we want a reader-writer lock.
 */

namespace RAMCloud {

//
// PUBLIC METHODS
//

/**
 * Construct a new LogIterator on a particular Log. The iterator may be used to
 * walk the entire log in order (i.e. every entry can be iterated over in the
 * order in which it was added to the log). However, this is not be particularly
 * meaningful if cleaning is enabled, since entries could have been moved within
 * the log.
 *
 * Since additional appends to the log may occur during iteration, locks have to
 * sometimes be briefly acquired when stepping between Segments (since the list
 * of active Segments may be changing). Furthermore, once the Log head is
 * reached, it is locked to avoid any modifications during iteration and not
 * unlocked until the iterator is destroyed). Locking is not done for any of the
 * closed Segments, however, as they are immutable. Finally, although cleaning
 * may occur during iteration, any cleaned segments will have been pinned (they
 * are not permitted to be reclaimed so long as any iterator exists) so as to
 * avoid any races. This effectively delays the return of any freed memory to
 * the system until all iterators are either destroyed or have finished walking
 * the entire log. (We can relax this in the future to unpin segments after all
 * iterators have passed over them).
 *
 * Locking the head once we reach it and keeping it locked until the iterator
 * has been destroyed allows the caller to ensure that once they've iterated
 * the entire log no new modifications can be made until they decide to permit
 * them again.
 *
 * Do note that running in parallel with cleaning means that the same entry may
 * be iterated over multiple times (i.e. if the cleaner has relocated it).
 */
LogIterator::LogIterator(Log& log)
    : log(log),
      segmentList(),
      currentIterator(),
      currentSegmentId(Segment::INVALID_SEGMENT_ID),
      headLocked(false)
{
    // If there's no log head yet we need to preclude any appends.
    log.listLock.lock();
    if (log.head == NULL) {
        log.appendLock.lock();
        headLocked = true;
    }
    log.iteratorCreated();
    log.listLock.unlock();

    next();
}

LogIterator::~LogIterator()
{
    if (headLocked)
        log.appendLock.unlock();
    std::lock_guard<SpinLock> lock(log.listLock);
    log.iteratorDestroyed();
}

/**
 * Test whether or not the iterator has finished stepping through the entire
 * log.
 *
 * \return
 *      True if all entries have been iterated over, else false if there are
 *      more.
 */
bool
LogIterator::isDone() const
{
    if (currentIterator && !currentIterator->isDone())
        return false;

    if (segmentList.size() > 0)
        return false;

    return headLocked;
}

/**
 * Advance the iterator forward by one entry, if possible.
 */
void
LogIterator::next()
{
    if (currentIterator && !currentIterator->isDone()) {
        currentIterator->next();
        return;
    }

    // We've exhausted the current segment. Now try the next one, if there
    // is one.

    if (currentIterator)
        currentIterator.destroy();

    std::lock_guard<SpinLock> lock(log.listLock);

    if (segmentList.size() == 0)
        populateSegmentList(currentSegmentId + 1);

    if (segmentList.size() == 0)
        return;

    if (segmentList.back() == log.head) {
        log.appendLock.lock();
        headLocked = true;
    }

    currentIterator.construct(segmentList.back());
    currentSegmentId = segmentList.back()->getId();

    // If we've just iterated over the head, then the only segments
    // that can exist in the log with higher IDs must have been generated
    // by the cleaner prior to iteration.
    //
    // TODO(rumble): It's a bummer that we're holding the head locked. Should
    // we not iterate over these segments before the head?
    if (headLocked && log.head != segmentList.back())
        assert(currentIterator->isCleanerSegment());

    segmentList.pop_back();
}

/**
 * Test whether or not the iterator is currently on the head of the log. When
 * iterating the head all log appends are delayed until the iterator has been
 * destroyed. This method allows the caller to know when they may be seriously
 * impacting system performance.
 */
bool
LogIterator::onHead() const
{
    return headLocked;
}

/**
 * Obtain a handle to the entry currently being iterated over.
 */
SegmentEntryHandle
LogIterator::getHandle() const
{
    return currentIterator->getHandle();
}

//
// PRIVATE METHODS
//

/**
 * Populate our list of segments from the log. Only segments newer than or
 * equal to the specified Segment ID are added. This method should only be
 * called when the current list of segments is empty.
 *
 * This method must be called with the log's listLock held.
 */
void
LogIterator::populateSegmentList(uint64_t nextSegmentId)
{
    assert(segmentList.size() == 0);

    // Walk the lists to collect the closed Segments. Since the cleaner
    // is locked out of inserting survivor segments and freeing cleaned
    // ones so long as any iterators still exist, we need only consider
    // what is presently part of the log.
    Log::SegmentList *lists[] = {
        &log.cleanableNewList,
        &log.cleanableList,
        &log.freePendingDigestAndReferenceList,
        NULL
    };

    for (int i = 0; lists[i] != NULL; i++) {
        foreach (Segment &s, *lists[i]) {
            if (s.getId() >= nextSegmentId)
                segmentList.push_back(&s);
        }
    }

    if (log.head != NULL && log.head->getId() >= nextSegmentId)
        segmentList.push_back(log.head);

    // Sort in descending order (so we can pop oldest ones off the back).
    std::sort(segmentList.begin(),
              segmentList.end(),
              SegmentIdLessThan());
}

} // namespace RAMCloud
