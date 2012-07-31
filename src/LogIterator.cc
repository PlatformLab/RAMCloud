/* Copyright (c) 2011-2012 Stanford University
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
      currentSegment(NULL),
      currentIterator(),
      headLocked(false)
{
    log.segmentManager.logIteratorCreated();

    // If there's no log head yet we need to preclude any appends.
    log.appendLock.lock();
    if (log.head == NULL)
        headLocked = true;
    else
        log.appendLock.unlock();

    next();
}

/**
 * Destroy the iterator. Once the last iterator on a log has been destroyed,
 * it may resume completing garbage collection. If the head segment was locked
 * for iteration, appends will also be re-enabled.
 */
LogIterator::~LogIterator()
{
    if (headLocked)
        log.appendLock.unlock();
    log.segmentManager.logIteratorDestroyed();
}

/**
 * Test whether or not the iterator has finished stepping through the entire
 * log. More concretely, if the current entry is valid, this will return false.
 * After next() has been called on the last valid entry, this will return true.
 *
 * \return
 *      True if all entries have been iterated over, else false if there are
 *      more.
 */
bool
LogIterator::isDone()
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
    if (currentIterator) {
        currentIterator->next();
        if (!currentIterator->isDone())
            return;
    }

    // We've exhausted the current segment. Now try the next one, if there
    // is one.

    uint64_t currentSegmentId = -1;
    if (currentSegment != NULL) {
        currentSegmentId = currentSegment->id;
        currentSegment = NULL;
        currentIterator.destroy();
    }

    if (segmentList.size() == 0)
        populateSegmentList(currentSegmentId + 1);

    if (segmentList.size() == 0) {
        assert(headLocked);
        return;
    }

    if (!headLocked) {
        log.appendLock.lock();

        if (segmentList.back() == log.head)
            headLocked = true;
        else
            log.appendLock.unlock();
    }

    currentSegment = segmentList.back();
    currentIterator.construct(*currentSegment);

    // If we've just iterated over the head, then the only segments
    // that can exist in the log with higher IDs must have been generated
    // by the cleaner prior to iteration.
    //
    // TODO(rumble): It's a bummer that we're holding the head locked. Should
    // we not iterate over these segments before the head?
    if (headLocked && log.head != segmentList.back())
        {} // XXX assert(currentIterator->isCleanerSegment());

    segmentList.pop_back();
}

/**
 * Test whether or not the iterator is currently on the head of the log. When
 * iterating the head all log appends are delayed until the iterator has been
 * destroyed. This method allows the caller to know when they may be seriously
 * impacting system performance.
 */
bool
LogIterator::onHead()
{
    return headLocked;
}

/**
 * Get the entry currently being iterated over.
 */
LogEntryType
LogIterator::getType()
{
    return currentIterator->getType();
}

/**
 * Get the length of the entry currently being iterated over.
 */
uint32_t
LogIterator::getLength()
{
    return currentIterator->getLength();
}

/**
 * Append the contents of the current entry being iterated over to the given
 * buffer.
 *
 * \param buffer
 *      Buffer to append the entry's contents to.
 */
uint32_t
LogIterator::appendToBuffer(Buffer& buffer)
{
    return currentIterator->appendToBuffer(buffer);
}

/**
 * Convenience method that resets the given buffer and appends the current
 * entry's contents to it. See appendToBuffer for more documentation.
 */
uint32_t
LogIterator::setBufferTo(Buffer& buffer)
{
    buffer.reset();
    return appendToBuffer(buffer);
}

/******************************************************************************
 * PRIVATE METHODS
 ******************************************************************************/

/**
 * (Re-)Populate our internal list of active segments by querying the
 * SegmentManager for any active segments with IDs greater than or equal to the
 * given value. Our list will then be sorted such that the next ID to iterator
 * over (the lowest ID) is at the back of the list.
 *
 * \param nextSegmentId
 *      Add active segments to our list whose identifiers are greater than or
 *      equal to this value. This parameter avoids iterating over the same
 *      segment multiple times.
 */
void
LogIterator::populateSegmentList(uint64_t nextSegmentId)
{
    log.segmentManager.getActiveSegments(nextSegmentId, segmentList);

    // Sort in descending order (so we can pop oldest ones off the back).
    std::sort(segmentList.begin(),
              segmentList.end(),
              SegmentIdLessThan());
}

} // namespace RAMCloud
